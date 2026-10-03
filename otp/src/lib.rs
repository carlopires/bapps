//! `bapps_otp`: OTP-shaped service architecture for one Glommio shard.
//!
//! Layering is deliberate:
//!
//! - `bapps_trio` owns **transient task lifetime** and cancellation.
//! - `bapps_otp` owns **long-lived service failure topology** and lifecycle.
//! - Glommio owns **where work executes**.
//! - Rust owns **memory and resource lifetime**.
//!
//! v0.2 adds framework-owned service generations, bounded observable local
//! mailboxes, service-owned Trio task groups, bounded graceful shutdown with a
//! structured force-abort fallback, and richer runtime-tree introspection.
//!
//! The library remains shard-local. Cross-shard and cross-node failure
//! propagation must use explicit messages/protocols.

#![forbid(unsafe_code)]

mod application;
mod child;
mod error;
mod generation;
mod mailbox;
mod registry;
mod service_tasks;
mod supervisor;
mod tree;
mod types;

pub use application::Application;
pub use child::{ChildContext, ChildSpec, ServiceFactory, ServiceFuture};
pub use error::OtpError;
pub use generation::{GenerationPhase, ServiceGeneration, ServiceUnavailable};
pub use mailbox::{
    LocalMailbox, LocalReceiver, LocalSender, MailboxError, SendPermit, TrySendError,
};
pub use registry::{Registry, RegistryError, ServiceKey};
pub use service_tasks::ServiceTasks;
pub use supervisor::SupervisorSpec;
pub use tree::RuntimeTree;
pub use types::{
    ChildSnapshot, ChildType, ExitReason, ExitRecord, MailboxSnapshot, NodeStatus, Restart,
    RestartIntensity, Shutdown, Strategy, SupervisorSnapshot, TreeNodeSnapshot,
};

pub use bapps_trio::{CancelScope, StopOutcome, TaskClass, TaskQueues, TaskStatus};
