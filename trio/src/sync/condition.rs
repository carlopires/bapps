use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use super::wait_list::{Cancellation, WaitList, WaitSlot};
use crate::cancel::Cancelled;

/// Generation-based condition variable for executor-local tasks.
#[derive(Clone, Default)]
pub struct Condition {
    inner: Rc<Inner>,
}

#[derive(Default)]
struct Inner {
    generation: Cell<u64>,
    waiters: WaitList,
}

impl Condition {
    /// A condition at generation zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// The current generation; read it before checking the state you wait on.
    pub fn generation(&self) -> u64 {
        self.inner.generation.get()
    }

    /// Advance the generation and wake every waiter.
    pub fn notify_all(&self) {
        self.inner
            .generation
            .set(self.inner.generation.get().wrapping_add(1));
        self.inner.waiters.wake_all();
    }

    /// Wait until the generation differs from `observed`. Cancellable through
    /// the current cancel scope; a dropped wait releases its registration.
    pub fn wait_for_change(&self, observed: u64) -> ConditionWait {
        ConditionWait {
            condition: self.clone(),
            observed,
            slot: None,
            cancellation: Cancellation::FromTask,
        }
    }
}

/// Future of [`Condition::wait_for_change`]: the new generation, or
/// [`Cancelled`](crate::Cancelled).
pub struct ConditionWait {
    condition: Condition,
    observed: u64,
    slot: WaitSlot,
    cancellation: Cancellation,
}

impl Future for ConditionWait {
    type Output = Result<u64, Cancelled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let current = this.condition.generation();
        if current != this.observed {
            return Poll::Ready(Ok(current));
        }
        if let Some(cancelled) = this.cancellation.poll(cx) {
            return Poll::Ready(Err(cancelled));
        }
        this.condition
            .inner
            .waiters
            .register(&mut this.slot, cx.waker());
        let current = this.condition.generation();
        if current != this.observed {
            Poll::Ready(Ok(current))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ConditionWait {
    fn drop(&mut self) {
        self.condition.inner.waiters.unregister(&mut self.slot);
    }
}
