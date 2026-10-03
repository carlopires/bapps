//! `bapps_trio`: Trio-shaped structured concurrency for Glommio.
//!
//! The crate intentionally separates four concerns:
//!
//! - **task lifetime**: [`Nursery`] and [`CancelScope`]
//! - **multi-owner cancellation**: [`CancelScope::any`]
//! - **startup/lifecycle handshakes**: [`TaskStatus`] / [`Nursery::start`]
//! - **CPU scheduling policy**: [`TaskClass`] mapped to Glommio task queues
//!
//! v0.2 also exposes an intentionally narrow [`OwnedTask`] capability for
//! supervisor/framework code that needs bounded graceful shutdown followed by
//! forceful cancellation of the *same nursery-owned task*.
//!
//! It is deliberately local to one Glommio executor/shard. Cross-core and
//! cross-node cancellation must travel through explicit messages.
//!
//! This repository is educational. It is not a production-hardened runtime.

#![forbid(unsafe_code)]

pub mod cancel;
pub mod foreign;
pub mod lab;
pub mod nursery;
pub mod obligation;
pub mod sync;
pub mod task_class;
pub mod testing;
pub mod time;
pub mod to_thread;

pub use cancel::{
    CancelCause, CancelReason, CancelScope, Cancelled, current_cancel_scope, with_cancel_scope,
};
pub use foreign::{cancel_on, cancel_on_any, cancel_on_current};
pub use nursery::{
    Nursery, NurseryError, NurseryHandle, OwnedTask, SpawnError, StartError, StopOutcome,
    TaskStatus, with_nursery, with_nursery_with_queues,
};
pub use obligation::{Obligation, ObligationStats, obligation_stats};
pub use task_class::{TaskClass, TaskQueues};
pub use time::{
    Clock, ClockRef, FailAfterError, MoveOnOutcome, RealClock, current_clock,
    current_effective_deadline, fail_after, fail_after_shielded, fail_at, move_on_after,
    move_on_at, remaining, sleep, sleep_until, with_clock,
};

/// Boxed, executor-local future used where a callback must borrow from a
/// nursery for the lifetime of the callback.
pub type LocalBoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + 'a>>;
