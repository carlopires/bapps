//! Framework-owned service-generation liveness.

use std::{
    cell::{Cell, RefCell},
    fmt,
    rc::Rc,
};

use bapps_trio::{CancelScope, Cancelled, sync::Condition};

/// Lifecycle phase of one supervised service generation.
///
/// ```text
/// Starting --readiness--> Ready --stop requested--> Draining --exit--> Stopped
/// Starting --stop requested before readiness------> Draining
/// ```
///
/// The linearization point for admission is the instant the generation's
/// cancellation scope is cancelled (supervisor stop, parent shutdown, or a
/// failed service-owned task). From then on the generation is `Draining`:
/// still alive so it can clean up, but no longer accepting new operations and
/// no longer advertised through the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GenerationPhase {
    /// Initializing; accepts operations from services started after it.
    Starting,
    /// Readiness handshake completed.
    Ready,
    /// Stop requested; finishing cleanup. Rejects new operations.
    Draining,
    /// Exited (normally, by failure, panic or force-abort).
    Stopped,
}

impl GenerationPhase {
    /// Whether new operations may start in this phase.
    pub fn is_accepting(self) -> bool {
        matches!(self, Self::Starting | Self::Ready)
    }
}

/// Liveness of one generation of a service: handles keep a clone and fail
/// fast once a restart has replaced their generation.
#[derive(Clone)]
pub struct ServiceGeneration {
    inner: Rc<Inner>,
}

struct Inner {
    path: String,
    id: u64,
    alive: Cell<bool>,
    ready: Cell<bool>,
    scope: RefCell<CancelScope>,
    changed: Condition,
}

/// A call reached a generation that no longer accepts work.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServiceUnavailable {
    /// The service's path.
    pub path: String,
    /// The generation that refused.
    pub generation: u64,
    /// Its phase at the time.
    pub phase: GenerationPhase,
}

impl fmt::Display for ServiceUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self.phase {
            GenerationPhase::Draining => "is draining and accepts no new work",
            _ => "is no longer running",
        };
        write!(
            f,
            "service {} generation {} {state}",
            self.path, self.generation
        )
    }
}

impl std::error::Error for ServiceUnavailable {}

impl ServiceGeneration {
    pub(crate) fn new(path: String, id: u64, scope: CancelScope) -> Self {
        Self {
            inner: Rc::new(Inner {
                path,
                id,
                alive: Cell::new(true),
                ready: Cell::new(false),
                scope: RefCell::new(scope),
                changed: Condition::new(),
            }),
        }
    }

    /// The service's path in the runtime tree.
    pub fn path(&self) -> &str {
        &self.inner.path
    }

    /// This generation's number (increases with every restart).
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    /// This generation's current phase.
    pub fn phase(&self) -> GenerationPhase {
        if !self.inner.alive.get() {
            GenerationPhase::Stopped
        } else if self.inner.scope.borrow().is_cancelled() {
            GenerationPhase::Draining
        } else if self.inner.ready.get() {
            GenerationPhase::Ready
        } else {
            GenerationPhase::Starting
        }
    }

    /// `true` until the generation exits, including while it drains.
    pub fn is_alive(&self) -> bool {
        self.inner.alive.get()
    }

    /// `true` while new operations may start: `Starting` or `Ready`.
    pub fn is_accepting(&self) -> bool {
        self.phase().is_accepting()
    }

    /// The cancellation scope of this exact supervised generation.
    pub fn scope(&self) -> CancelScope {
        self.inner.scope.borrow().clone()
    }

    /// Fail fast when a cached handle points at an exited/restarted generation.
    /// A draining generation is still alive; use [`Self::operation_scope`] to
    /// gate new work.
    pub fn ensure_alive(&self) -> Result<(), ServiceUnavailable> {
        if self.is_alive() {
            Ok(())
        } else {
            Err(self.unavailable())
        }
    }

    /// The scope of one operation owned both by `caller` and by this
    /// generation: it is cancelled when either is. Fails fast unless the
    /// generation is accepting work, so neither a stale handle nor a draining
    /// generation ever starts a new operation.
    ///
    /// This is the standard shape of a service handle method:
    ///
    /// ```ignore
    /// let scope = self.generation.operation_scope(caller)?;
    /// self.outbox.send_in(&scope, Command::Get { key, reply }).await?;
    /// ```
    pub fn operation_scope(&self, caller: &CancelScope) -> Result<CancelScope, ServiceUnavailable> {
        if !self.is_accepting() {
            return Err(self.unavailable());
        }
        Ok(CancelScope::any([caller.clone(), self.scope()]))
    }

    /// Wait until this generation has stopped.
    ///
    /// # Errors
    ///
    /// [`Cancelled`] when the current cancel scope is cancelled first.
    pub async fn wait_stopped(&self) -> Result<(), Cancelled> {
        while self.is_alive() {
            let observed = self.inner.changed.generation();
            if !self.is_alive() {
                break;
            }
            self.inner.changed.wait_for_change(observed).await?;
        }
        Ok(())
    }

    fn unavailable(&self) -> ServiceUnavailable {
        ServiceUnavailable {
            path: self.inner.path.clone(),
            generation: self.inner.id,
            phase: self.phase(),
        }
    }

    pub(crate) fn replace_scope(&self, scope: CancelScope) {
        *self.inner.scope.borrow_mut() = scope;
    }

    pub(crate) fn mark_ready(&self) {
        self.inner.ready.set(true);
    }

    pub(crate) fn mark_stopped(&self) {
        self.inner.scope.borrow().cancel_by(
            bapps_trio::CancelReason::NurseryClosing,
            "generation exited",
        );
        if self.inner.alive.replace(false) {
            self.inner.changed.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_scope_is_owned_by_caller_and_generation() {
        let generation = ServiceGeneration::new("svc".into(), 1, CancelScope::new());
        let caller = CancelScope::new();
        let by_caller = generation.operation_scope(&caller).expect("alive");
        let by_generation = generation
            .operation_scope(&CancelScope::new())
            .expect("alive");

        caller.cancel();
        assert!(by_caller.is_cancelled());
        assert!(!by_generation.is_cancelled());

        generation.mark_stopped();
        assert!(by_generation.is_cancelled());
        assert_eq!(
            generation.operation_scope(&CancelScope::new()).unwrap_err(),
            ServiceUnavailable {
                path: "svc".into(),
                generation: 1,
                phase: GenerationPhase::Stopped,
            }
        );
    }

    #[test]
    fn phases_follow_readiness_stop_request_and_exit() {
        let scope = CancelScope::new();
        let generation = ServiceGeneration::new("svc".into(), 1, scope.clone());
        assert_eq!(generation.phase(), GenerationPhase::Starting);
        assert!(generation.operation_scope(&CancelScope::new()).is_ok());

        generation.mark_ready();
        assert_eq!(generation.phase(), GenerationPhase::Ready);

        scope.cancel();
        assert_eq!(generation.phase(), GenerationPhase::Draining);
        assert!(generation.is_alive(), "draining is still alive");
        assert_eq!(
            generation
                .operation_scope(&CancelScope::new())
                .unwrap_err()
                .phase,
            GenerationPhase::Draining,
            "a draining generation admits no new work"
        );

        generation.mark_stopped();
        assert_eq!(generation.phase(), GenerationPhase::Stopped);
    }
}
