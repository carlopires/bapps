use crate::ServiceGeneration;
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::HashMap,
    fmt,
    marker::PhantomData,
    rc::Rc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OwnerId(pub(crate) u64);

/// A typed name for a service in a [`Registry`]; usually a `static`.
#[derive(Debug)]
pub struct ServiceKey<T: 'static> {
    name: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: 'static> Copy for ServiceKey<T> {}

impl<T: 'static> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: 'static> ServiceKey<T> {
    /// A key named `name` (unique per type).
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    /// Its name.
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct RegistryKey {
    name: &'static str,
    type_id: TypeId,
}

struct Entry {
    owner: Option<OwnerId>,
    /// The owning generation. Its entries stop being advertised as soon as it
    /// stops accepting work (draining), and are removed when it exits.
    liveness: Option<ServiceGeneration>,
    value: Box<dyn Any>,
}

impl Entry {
    fn advertised(&self) -> bool {
        self.liveness
            .as_ref()
            .is_none_or(ServiceGeneration::is_accepting)
    }
}

/// Typed, shard-local service lookup. Entries registered by a service
/// generation ([`ChildContext::register`](crate::ChildContext::register))
/// disappear with it; global entries stay until removed.
#[derive(Clone, Default)]
pub struct Registry {
    inner: Rc<RefCell<HashMap<RegistryKey, Entry>>>,
}

/// Why a registration failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegistryError {
    /// Another owner already holds this key.
    AlreadyRegistered {
        /// The key's name.
        name: &'static str,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyRegistered { name } => write!(f, "service {name:?} is already registered"),
        }
    }
}

impl std::error::Error for RegistryError {}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a value with no owning generation (shard-wide resources such
    /// as a byte budget).
    ///
    /// # Errors
    ///
    /// [`RegistryError::AlreadyRegistered`] when the key is taken.
    pub fn register_global<T>(&self, key: ServiceKey<T>, value: T) -> Result<(), RegistryError>
    where
        T: Clone + 'static,
    {
        self.register_impl(key, value, None)
    }

    /// Register or overwrite a global value.
    pub fn replace_global<T>(&self, key: ServiceKey<T>, value: T)
    where
        T: Clone + 'static,
    {
        let registry_key = make_key(key);
        self.inner.borrow_mut().insert(
            registry_key,
            Entry {
                owner: None,
                liveness: None,
                value: Box::new(value),
            },
        );
    }

    /// A clone of the value under `key`, unless absent or its generation is
    /// draining.
    pub fn get<T>(&self, key: ServiceKey<T>) -> Option<T>
    where
        T: Clone + 'static,
    {
        let registry_key = make_key(key);
        let entries = self.inner.borrow();
        entries
            .get(&registry_key)
            .filter(|entry| entry.advertised())
            .and_then(|entry| entry.value.downcast_ref::<T>())
            .cloned()
    }

    /// Whether [`Self::get`] would return a value.
    pub fn contains<T>(&self, key: ServiceKey<T>) -> bool
    where
        T: Clone + 'static,
    {
        self.get(key).is_some()
    }

    /// Remove the entry under `key` and return its value.
    pub fn remove<T>(&self, key: ServiceKey<T>) -> Option<T>
    where
        T: Clone + 'static,
    {
        let registry_key = make_key(key);
        self.inner
            .borrow_mut()
            .remove(&registry_key)
            .and_then(|entry| entry.value.downcast::<T>().ok())
            .map(|value| *value)
    }

    /// The names of every registered key.
    pub fn names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self
            .inner
            .borrow()
            .iter()
            .filter(|(_, entry)| entry.advertised())
            .map(|(key, _)| key.name)
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    pub(crate) fn register_owned<T>(
        &self,
        key: ServiceKey<T>,
        value: T,
        owner: OwnerId,
        liveness: ServiceGeneration,
    ) -> Result<(), RegistryError>
    where
        T: Clone + 'static,
    {
        self.register_impl(key, value, Some((owner, liveness)))
    }

    /// Remove exactly the entries registered by `owner`. Owner IDs are unique
    /// per generation, so a late cleanup from an old generation cannot touch a
    /// replacement's entry, even under the same key.
    pub(crate) fn remove_owner(&self, owner: OwnerId) {
        self.inner
            .borrow_mut()
            .retain(|_, entry| entry.owner != Some(owner));
    }

    fn register_impl<T>(
        &self,
        key: ServiceKey<T>,
        value: T,
        owner: Option<(OwnerId, ServiceGeneration)>,
    ) -> Result<(), RegistryError>
    where
        T: Clone + 'static,
    {
        let registry_key = make_key(key);
        let mut entries = self.inner.borrow_mut();
        if entries.contains_key(&registry_key) {
            return Err(RegistryError::AlreadyRegistered { name: key.name });
        }
        let (owner, liveness) = owner.unzip();
        entries.insert(
            registry_key,
            Entry {
                owner,
                liveness,
                value: Box::new(value),
            },
        );
        Ok(())
    }
}

fn make_key<T: 'static>(key: ServiceKey<T>) -> RegistryKey {
    RegistryKey {
        name: key.name,
        type_id: TypeId::of::<T>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bapps_trio::CancelScope;

    static KEY: ServiceKey<u32> = ServiceKey::new("svc");

    fn generation(id: u64) -> (ServiceGeneration, CancelScope) {
        let scope = CancelScope::new();
        (
            ServiceGeneration::new("svc".into(), id, scope.clone()),
            scope,
        )
    }

    #[test]
    fn late_cleanup_of_an_old_owner_keeps_the_replacement_entry() {
        let registry = Registry::new();
        let (old, _) = generation(1);
        registry
            .register_owned(KEY, 1, OwnerId(1), old.clone())
            .unwrap();
        registry.remove_owner(OwnerId(1)); // old generation exits
        let (new, _) = generation(2);
        registry.register_owned(KEY, 2, OwnerId(2), new).unwrap();

        registry.remove_owner(OwnerId(1)); // duplicate or late cleanup
        assert_eq!(registry.get(KEY), Some(2));
    }

    #[test]
    fn a_draining_owner_is_no_longer_advertised() {
        let registry = Registry::new();
        let (owner, scope) = generation(1);
        registry.register_owned(KEY, 1, OwnerId(1), owner).unwrap();
        assert_eq!(registry.get(KEY), Some(1));
        assert_eq!(registry.names(), ["svc"]);

        scope.cancel(); // stop requested: Draining
        assert_eq!(registry.get(KEY), None);
        assert!(registry.names().is_empty());
        // Still owned: a replacement cannot register until the owner exits.
        let (next, _) = generation(2);
        assert!(registry.register_owned(KEY, 2, OwnerId(2), next).is_err());
    }
}
