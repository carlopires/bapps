//! Small cancellation-aware synchronization primitives.

mod condition;
mod event;
pub(crate) mod wait_list;

pub use condition::Condition;
pub use event::Event;
