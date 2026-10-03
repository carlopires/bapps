//! Waker registrations owned by the wait that created them.
//!
//! Every wait future in this crate registers through a [`WaitList`] and keeps a
//! [`WaitSlot`]. Re-polling refreshes the same entry instead of appending a new
//! one, and dropping the wait removes it. A long-lived owner (a service
//! generation's cancel scope, a mailbox condition) therefore holds only the
//! wakers of waits that are still alive, no matter how many requests came and
//! went. In Glommio a retained waker also retains its task allocation, so this
//! is memory safety for long-running services, not just tidiness.

use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::cancel::{Cancelled, CancelledFuture, current_cancel_scope};

/// Identity of one registration. Ids are never reused, so a slot left over
/// from an entry that was already woken cannot remove someone else's entry.
pub(crate) type WaitSlot = Option<u64>;

/// Registered wakers, each with an optional payload (`TestClock` stores the
/// deadline). A `BTreeMap` keeps wake order deterministic: registration order.
pub(crate) struct WaitList<T = ()> {
    next_id: Cell<u64>,
    entries: RefCell<BTreeMap<u64, (T, Waker)>>,
}

impl<T> Default for WaitList<T> {
    fn default() -> Self {
        Self {
            next_id: Cell::new(0),
            entries: RefCell::new(BTreeMap::new()),
        }
    }
}

impl WaitList {
    pub(crate) fn register(&self, slot: &mut WaitSlot, waker: &Waker) {
        self.register_with(slot, (), waker);
    }
}

impl<T> WaitList<T> {
    /// Register `waker` for `slot`, or refresh the slot's existing entry.
    pub(crate) fn register_with(&self, slot: &mut WaitSlot, value: T, waker: &Waker) {
        let mut entries = self.entries.borrow_mut();
        if let Some(id) = *slot
            && let Some(entry) = entries.get_mut(&id)
        {
            entry.0 = value;
            if !entry.1.will_wake(waker) {
                entry.1.clone_from(waker);
            }
            return;
        }
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        entries.insert(id, (value, waker.clone()));
        *slot = Some(id);
    }

    /// Remove the slot's entry, if it is still registered.
    pub(crate) fn unregister(&self, slot: &mut WaitSlot) {
        if let Some(id) = slot.take() {
            self.entries.borrow_mut().remove(&id);
        }
    }

    /// The smallest payload among registered entries.
    pub(crate) fn min_value(&self) -> Option<T>
    where
        T: Ord + Copy,
    {
        self.entries
            .borrow()
            .values()
            .map(|(value, _)| *value)
            .min()
    }

    /// Remove and wake every entry.
    pub(crate) fn wake_all(&self) {
        self.wake_if(|_| true);
    }

    /// Remove and wake the entries whose payload satisfies `ready`.
    ///
    /// Wakers run after the internal borrow is released, so a woken task may
    /// re-register synchronously.
    pub(crate) fn wake_if(&self, mut ready: impl FnMut(&T) -> bool) {
        let woken: Vec<Waker> = {
            let mut entries = self.entries.borrow_mut();
            let ids: Vec<u64> = entries
                .iter()
                .filter(|(_, (value, _))| ready(value))
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| entries.remove(&id))
                .map(|(_, waker)| waker)
                .collect()
        };
        for waker in woken {
            waker.wake();
        }
    }
}

/// How a wait observes cancellation. The task-local scope is captured on the
/// first poll, because that is when the wait runs inside its task.
pub(crate) enum Cancellation {
    FromTask,
    Observed(CancelledFuture),
    Ignored,
}

impl Cancellation {
    /// `Some` when cancellation has won; otherwise registers `cx`'s waker.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Option<Cancelled> {
        if let Self::FromTask = self {
            *self = match current_cancel_scope() {
                Some(scope) => Self::Observed(scope.cancelled()),
                None => Self::Ignored,
            };
        }
        match self {
            Self::Observed(cancelled) => match Pin::new(cancelled).poll(cx) {
                Poll::Ready(cancelled) => Some(cancelled),
                Poll::Pending => None,
            },
            Self::FromTask | Self::Ignored => None,
        }
    }
}
