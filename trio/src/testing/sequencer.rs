use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use crate::{
    cancel::Cancelled,
    sync::wait_list::{Cancellation, WaitList, WaitSlot},
};

/// Step barrier for deterministic race tests.
#[derive(Clone, Default)]
pub struct Sequencer {
    inner: Rc<Inner>,
}

#[derive(Default)]
struct Inner {
    step: Cell<u64>,
    waiters: WaitList,
}

impl Sequencer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn step(&self) -> u64 {
        self.inner.step.get()
    }

    pub fn advance(&self) -> u64 {
        let next = self.inner.step.get().wrapping_add(1);
        self.inner.step.set(next);
        self.inner.waiters.wake_all();
        next
    }

    pub fn wait_for(&self, target: u64) -> WaitFor {
        WaitFor {
            sequencer: self.clone(),
            target,
            slot: None,
            cancellation: Cancellation::FromTask,
        }
    }
}

pub struct WaitFor {
    sequencer: Sequencer,
    target: u64,
    slot: WaitSlot,
    cancellation: Cancellation,
}

impl Future for WaitFor {
    type Output = Result<(), Cancelled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.sequencer.step() >= this.target {
            return Poll::Ready(Ok(()));
        }
        if let Some(cancelled) = this.cancellation.poll(cx) {
            return Poll::Ready(Err(cancelled));
        }
        this.sequencer
            .inner
            .waiters
            .register(&mut this.slot, cx.waker());
        if this.sequencer.step() >= this.target {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for WaitFor {
    fn drop(&mut self) {
        self.sequencer.inner.waiters.unregister(&mut self.slot);
    }
}
