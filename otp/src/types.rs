use std::{fmt, time::Duration};

use bapps_trio::TaskClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restart {
    /// Restart after every non-supervisor-initiated exit.
    Permanent,
    /// Restart only after an abnormal exit (`Failure`, `Panic`, or `Killed`).
    Transient,
    /// Never restart after the child exits.
    Temporary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildType {
    Worker,
    Supervisor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shutdown {
    /// Request cooperative cancellation, wait this long, then force-abort the
    /// same nursery-owned task if it still has not returned.
    Graceful(Duration),
    /// Force-abort immediately. Useful only for deliberately non-cooperative
    /// workers; most services should use `Graceful`.
    BrutalKill,
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::Graceful(Duration::from_secs(5))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestartIntensity {
    pub max_restarts: usize,
    pub within: Duration,
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            within: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    Normal,
    Shutdown,
    Failure(String),
    Panic(String),
    /// The cooperative shutdown grace period expired and the framework
    /// force-aborted the owned task.
    Killed,
}

impl ExitReason {
    pub fn is_abnormal(&self) -> bool {
        matches!(self, Self::Failure(_) | Self::Panic(_) | Self::Killed)
    }

    pub fn should_restart(&self, restart: Restart) -> bool {
        match restart {
            Restart::Permanent => !matches!(self, Self::Shutdown),
            Restart::Transient => self.is_abnormal(),
            Restart::Temporary => false,
        }
    }
}

impl fmt::Display for ExitReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::Shutdown => write!(f, "shutdown"),
            Self::Failure(message) => write!(f, "failure: {message}"),
            Self::Panic(message) => write!(f, "panic: {message}"),
            Self::Killed => write!(f, "killed after shutdown grace period"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailboxSnapshot {
    pub name: String,
    pub depth: usize,
    pub capacity: usize,
    pub closed: bool,
    pub senders: usize,
    /// Slots held by outstanding send permits.
    pub reserved: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitRecord {
    pub generation: u64,
    pub at: Duration,
    pub reason: ExitReason,
    /// Why the generation's cancellation started, when it was cancelled
    /// before exiting (a requested stop, a restart, a sibling failure).
    /// `None` when it exited on its own.
    pub cause: Option<bapps_trio::CancelCause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSnapshot {
    pub path: String,
    pub parent: String,
    pub name: String,
    pub child_type: ChildType,
    pub status: NodeStatus,
    pub restart: Restart,
    pub shutdown: Shutdown,
    pub task_class: TaskClass,
    pub generation: u64,
    pub restart_count: u64,
    pub last_exit: Option<ExitReason>,
    /// Live framework-owned service-task count for the current generation.
    pub active_tasks: usize,
    /// Live mailboxes created through `ChildContext::mailbox`.
    pub mailboxes: Vec<MailboxSnapshot>,
    /// Bounded recent exit history, newest last.
    pub recent_exits: Vec<ExitRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorSnapshot {
    pub path: String,
    pub parent: Option<String>,
    pub name: String,
    pub status: NodeStatus,
    pub strategy: Strategy,
    pub restart_intensity: RestartIntensity,
    pub restart_count: u64,
    pub last_exit: Option<ExitReason>,
    pub active_children: usize,
    pub recent_exits: Vec<ExitRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeNodeSnapshot {
    Supervisor(SupervisorSnapshot),
    Child(ChildSnapshot),
}
