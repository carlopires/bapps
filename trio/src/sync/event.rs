use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use super::wait_list::{Cancellation, WaitList, WaitSlot};
use crate::cancel::Cancelled;

/// One-shot event. Once set, it stays set.
#[derive(Clone, Default)]
pub struct Event {
    inner: Rc<Inner>,
}

#[derive(Default)]
struct Inner {
    set: Cell<bool>,
    waiters: WaitList,
}

impl Event {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_set(&self) -> bool {
        self.inner.set.get()
    }

    pub fn set(&self) {
        if self.inner.set.replace(true) {
            return;
        }
        self.inner.waiters.wake_all();
    }

    /// Wait until set, or until the task's current cancel scope is cancelled.
    pub fn wait(&self) -> EventWait {
        EventWait {
            event: self.clone(),
            slot: None,
            cancellation: Cancellation::FromTask,
        }
    }

    /// Wait until set, ignoring cancellation. Framework shutdown paths use this
    /// when they must finish a bounded policy after outer cancellation.
    pub(crate) fn wait_unchecked(&self) -> EventWait {
        EventWait {
            event: self.clone(),
            slot: None,
            cancellation: Cancellation::Ignored,
        }
    }
}

pub struct EventWait {
    event: Event,
    slot: WaitSlot,
    cancellation: Cancellation,
}

impl Future for EventWait {
    type Output = Result<(), Cancelled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.event.is_set() {
            return Poll::Ready(Ok(()));
        }
        if let Some(cancelled) = this.cancellation.poll(cx) {
            return Poll::Ready(Err(cancelled));
        }
        this.event
            .inner
            .waiters
            .register(&mut this.slot, cx.waker());
        if this.event.is_set() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for EventWait {
    fn drop(&mut self) {
        self.event.inner.waiters.unregister(&mut self.slot);
    }
}
