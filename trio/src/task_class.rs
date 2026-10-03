//! Application-level scheduling classes mapped onto Glommio task queues.

use std::{rc::Rc, time::Duration};

use glommio::{Latency, Shares, TaskQueueHandle, executor};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskClass {
    Default,
    ForegroundRead,
    ForegroundWrite,
    Replication,
    Repair,
    Compaction,
    Maintenance,
}

/// Queue mapping for one Glommio executor/shard.
///
/// `current()` maps every task class to the current queue. `storage_defaults()`
/// creates separate queues with intentionally simple teaching defaults.
#[derive(Clone)]
pub struct TaskQueues {
    /// `None` under the deterministic lab, which has no Glommio queues.
    inner: Option<Rc<QueueSet>>,
}

struct QueueSet {
    default: TaskQueueHandle,
    foreground_read: TaskQueueHandle,
    foreground_write: TaskQueueHandle,
    replication: TaskQueueHandle,
    repair: TaskQueueHandle,
    compaction: TaskQueueHandle,
    maintenance: TaskQueueHandle,
}

impl TaskQueues {
    pub fn current() -> Self {
        if crate::lab::is_active() {
            return Self { inner: None };
        }
        let q = executor().current_task_queue();
        Self {
            inner: Some(Rc::new(QueueSet {
                default: q,
                foreground_read: q,
                foreground_write: q,
                replication: q,
                repair: q,
                compaction: q,
                maintenance: q,
            })),
        }
    }

    /// Teaching defaults, not production tuning.
    pub fn storage_defaults() -> Self {
        if crate::lab::is_active() {
            return Self { inner: None };
        }
        let ex = executor();
        let default = ex.current_task_queue();
        let foreground_read = ex.create_task_queue(
            Shares::Static(1000),
            Latency::Matters(Duration::from_millis(1)),
            "foreground-read",
        );
        let foreground_write = ex.create_task_queue(
            Shares::Static(900),
            Latency::Matters(Duration::from_millis(2)),
            "foreground-write",
        );
        let replication = ex.create_task_queue(
            Shares::Static(500),
            Latency::Matters(Duration::from_millis(5)),
            "replication",
        );
        let repair = ex.create_task_queue(Shares::Static(150), Latency::NotImportant, "repair");
        let compaction =
            ex.create_task_queue(Shares::Static(100), Latency::NotImportant, "compaction");
        let maintenance =
            ex.create_task_queue(Shares::Static(50), Latency::NotImportant, "maintenance");

        Self {
            inner: Some(Rc::new(QueueSet {
                default,
                foreground_read,
                foreground_write,
                replication,
                repair,
                compaction,
                maintenance,
            })),
        }
    }

    /// The Glommio queue for `class`.
    ///
    /// # Panics
    ///
    /// Under the deterministic lab, which has no Glommio queues.
    pub fn queue(&self, class: TaskClass) -> TaskQueueHandle {
        self.glommio_queue(class)
            .expect("TaskQueues::queue has no Glommio queue under the lab executor")
    }

    pub(crate) fn glommio_queue(&self, class: TaskClass) -> Option<TaskQueueHandle> {
        let set = self.inner.as_ref()?;
        Some(match class {
            TaskClass::Default => set.default,
            TaskClass::ForegroundRead => set.foreground_read,
            TaskClass::ForegroundWrite => set.foreground_write,
            TaskClass::Replication => set.replication,
            TaskClass::Repair => set.repair,
            TaskClass::Compaction => set.compaction,
            TaskClass::Maintenance => set.maintenance,
        })
    }
}
