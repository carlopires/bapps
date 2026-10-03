//! Trio task groups owned by one supervised service generation.

use std::{cell::Cell, fmt, future::Future, rc::Rc};

use bapps_trio::{
    CancelScope, Cancelled, NurseryHandle, SpawnError, TaskClass, sync::Condition,
    with_cancel_scope,
};

#[derive(Clone, Default)]
pub(crate) struct ServiceTaskStats {
    active: Rc<Cell<usize>>,
    changed: Condition,
}

impl ServiceTaskStats {
    pub(crate) fn active(&self) -> usize {
        self.active.get()
    }
}

struct ActiveGuard {
    stats: ServiceTaskStats,
}

impl ActiveGuard {
    fn enter(stats: ServiceTaskStats) -> Self {
        stats.active.set(stats.active.get().saturating_add(1));
        Self { stats }
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.stats
            .active
            .set(self.stats.active.get().saturating_sub(1));
        self.stats.changed.notify_all();
    }
}

/// Transient tasks owned by one long-lived service generation.
///
/// The task remains a Trio child, so nursery failure semantics still apply.
/// This wrapper adds a second owner: ending the service generation cancels all
/// tasks created through this handle, even if the outer nursery is still alive.
#[derive(Clone)]
pub struct ServiceTasks {
    handle: NurseryHandle<String>,
    lifetime: CancelScope,
    stats: ServiceTaskStats,
}

impl ServiceTasks {
    pub(crate) fn new(handle: NurseryHandle<String>) -> Self {
        Self {
            handle,
            lifetime: CancelScope::new(),
            stats: ServiceTaskStats::default(),
        }
    }

    pub fn active_tasks(&self) -> usize {
        self.stats.active()
    }

    /// Wait for capacity before immediately calling spawn. Admission is local:
    /// do not insert another await between this return and spawning the task.
    /// Counts include scheduled tasks, not only tasks that have been polled.
    pub async fn wait_below(&self, limit: usize) -> Result<(), Cancelled> {
        assert!(limit > 0, "service task limit must be non-zero");
        while self.active_tasks() >= limit {
            let observed = self.stats.changed.generation();
            self.stats.changed.wait_for_change(observed).await?;
        }
        Ok(())
    }

    pub fn cancellation_scope(&self) -> CancelScope {
        self.lifetime.clone()
    }

    pub fn cancel_all(&self) {
        self.lifetime.cancel();
    }

    pub fn spawn<F, Fut, E>(&self, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: fmt::Display + 'static,
    {
        self.spawn_into(TaskClass::Default, task)
    }

    pub fn spawn_into<F, Fut, E>(&self, class: TaskClass, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: fmt::Display + 'static,
    {
        let lifetime = self.lifetime.clone();
        // Reserve at submission, not at first poll: admission cannot overshoot.
        let active = ActiveGuard::enter(self.stats.clone());
        self.handle.spawn_into(class, move |nursery_scope| {
            let operation_scope = CancelScope::any([nursery_scope, lifetime]);
            async move {
                let _active = active;
                with_cancel_scope(operation_scope.clone(), task(operation_scope))
                    .await
                    .map_err(|error| error.to_string())
            }
        })
    }

    pub(crate) fn stats(&self) -> ServiceTaskStats {
        self.stats.clone()
    }
}
