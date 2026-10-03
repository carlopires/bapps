use bapps_otp::{Registry, ServiceKey};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Handle(u64);

static HANDLE: ServiceKey<Handle> = ServiceKey::new("handle");

#[test]
fn global_registry_is_typed() {
    let registry = Registry::new();
    registry
        .register_global(HANDLE, Handle(42))
        .expect("register");
    assert_eq!(registry.get(HANDLE), Some(Handle(42)));
    assert!(registry.contains(HANDLE));
    assert_eq!(registry.remove(HANDLE), Some(Handle(42)));
    assert!(!registry.contains(HANDLE));
}
