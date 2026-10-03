use std::{fmt, time::Duration};

use bapps_trio::TaskClass;

/// Which children a supervisor restarts when one fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Strategy {
    /// Only the failed child.
    OneForOne,
    /// Every child: stop them all in reverse order, start them all again.
    OneForAll,
    /// The failed child and every child started after it (its dependents).
    RestForOne,
}

/// When a child is restarted after it exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restart {
    /// Restart after every non-supervisor-initiated exit.
    Permanent,
    /// Restart only after an abnormal exit (`Failure`, `Panic`, or `Killed`).
    Transient,
    /// Never restart after the child exits.
    Temporary,
}

/// Whether a child is a worker or a nested supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildType {
    /// A service.
    Worker,
    /// A nested supervisor.
    Supervisor,
}

/// How a supervisor stops a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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

/// How many restarts a supervisor allows in a time window before it gives
/// up and fails to its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RestartIntensity {
    /// Restarts allowed within the window.
    pub max_restarts: usize,
    /// The window.
    pub within: Duration,
}

impl RestartIntensity {
    /// Allow at most `max_restarts` restarts within any `within` window;
    /// one more escalates to the parent.
    pub fn new(max_restarts: usize, within: Duration) -> Self {
        Self {
            max_restarts,
            within,
        }
    }
}

impl Default for RestartIntensity {
    fn default() -> Self {
        Self {
            max_restarts: 5,
            within: Duration::from_secs(10),
        }
    }
}

/// How a service generation ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExitReason {
    /// It returned `Ok(())` on its own.
    Normal,
    /// It stopped because its supervisor asked it to.
    Shutdown,
    /// It returned an error (rendered).
    Failure(String),
    /// It panicked (the panic message).
    Panic(String),
    /// The cooperative shutdown grace period expired and the framework
    /// force-aborted the owned task.
    Killed,
}

impl ExitReason {
    /// Failure, panic or forced kill.
    pub fn is_abnormal(&self) -> bool {
        matches!(self, Self::Failure(_) | Self::Panic(_) | Self::Killed)
    }

    /// Whether a child with `restart` policy is restarted after this exit.
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

/// Lifecycle state of a node in the runtime tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeStatus {
    /// Started, readiness not yet reported.
    Starting,
    /// Ready and running.
    Running,
    /// Asked to stop; draining.
    Stopping,
    /// Stopped normally.
    Stopped,
    /// Stopped abnormally.
    Failed,
}

/// A mailbox's state at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MailboxSnapshot {
    /// Its name.
    pub name: String,
    /// Messages queued.
    pub depth: usize,
    /// Its capacity.
    pub capacity: usize,
    /// Whether it is closed.
    pub closed: bool,
    /// Live senders.
    pub senders: usize,
    /// Slots held by outstanding send permits.
    pub reserved: usize,
}

/// One recorded exit of a service generation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExitRecord {
    /// The generation that exited.
    pub generation: u64,
    /// When, on the shard's clock ([`bapps_trio::current_clock`]).
    pub at: Duration,
    /// How it ended.
    pub reason: ExitReason,
    /// Why the generation's cancellation started, when it was cancelled
    /// before exiting (a requested stop, a restart, a sibling failure).
    /// `None` when it exited on its own.
    pub cause: Option<bapps_trio::CancelCause>,
}

/// A child's state in the runtime tree.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChildSnapshot {
    /// Its path.
    pub path: String,
    /// Its supervisor's path.
    pub parent: String,
    /// Its name.
    pub name: String,
    /// Worker or supervisor.
    pub child_type: ChildType,
    /// Its lifecycle state.
    pub status: NodeStatus,
    /// Its restart policy.
    pub restart: Restart,
    /// Its shutdown policy.
    pub shutdown: Shutdown,
    /// Its scheduling class.
    pub task_class: TaskClass,
    /// Its current generation.
    pub generation: u64,
    /// Restarts so far.
    pub restart_count: u64,
    /// How its last generation ended.
    pub last_exit: Option<ExitReason>,
    /// Live framework-owned service-task count for the current generation.
    pub active_tasks: usize,
    /// Live mailboxes created through `ChildContext::mailbox`.
    pub mailboxes: Vec<MailboxSnapshot>,
    /// Bounded recent exit history, newest last.
    pub recent_exits: Vec<ExitRecord>,
}

/// A supervisor's state in the runtime tree.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SupervisorSnapshot {
    /// Its path.
    pub path: String,
    /// Its parent's path; `None` for the root.
    pub parent: Option<String>,
    /// Its name.
    pub name: String,
    /// Its lifecycle state.
    pub status: NodeStatus,
    /// Its restart strategy.
    pub strategy: Strategy,
    /// Its restart intensity.
    pub restart_intensity: RestartIntensity,
    /// Restarts it performed so far.
    pub restart_count: u64,
    /// How it last ended, if it was restarted.
    pub last_exit: Option<ExitReason>,
    /// Children running now.
    pub active_children: usize,
    /// Its children's recent exits.
    pub recent_exits: Vec<ExitRecord>,
}

/// A node of the runtime tree.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TreeNodeSnapshot {
    /// A supervisor.
    Supervisor(SupervisorSnapshot),
    /// A child.
    Child(ChildSnapshot),
}
