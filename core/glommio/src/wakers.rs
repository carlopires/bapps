//! Wakers you cannot quietly forget to wake.
//!
//! Every waiting primitive in this crate takes wakers while holding a lock and
//! wakes them after releasing it -- waking under the lock would leave the woken
//! poller blocking on it immediately. So the wakers travel, unattached, across
//! a scope boundary, and a bare `Vec<Waker>` in flight carries no evidence that
//! anyone still means to wake it.
//!
//! Dropping them there instead is silent: the waiter never runs. Worse, it does
//! not reliably *look* like a bug. Suppressing a wake by dropping the wakers
//! still let a parked executor notice within microseconds, because dropping a
//! glommio [`Waker`] from a foreign thread routes through the owning executor's
//! notifier and perturbs it. Only leaking them produced the hang the mistake
//! deserves -- so an implementation with the bug can pass its own tests, for a
//! reason that has nothing to do with it being correct.
//!
//! Hence a type. [`WakerList::take`] is the only way to remove wakers, it hands
//! back a [`PendingWakes`], and waking is the only thing that value can do.
//!
//! # What this catches, and what it does not
//!
//! Rust has no linear types, so nothing can force a `PendingWakes` to be
//! consumed. Two of the three cases are covered:
//!
//! | Mistake | Caught by | When |
//! |---|---|---|
//! | `list.take();` — result ignored | `#[must_use]` | compile time |
//! | `let owed = list.take();` then forgotten | drop guard | test run, loudly |
//! | `mem::forget(owed)` | nothing | — |
//!
//! The gap is a deliberate leak, which is not a mistake anyone makes by
//! accident -- and accidents are the only thing this is for.

use std::task::Waker;

/// Wakers waiting to be woken.
///
/// The only way to get one out is [`take`](Self::take), which hands back an
/// obligation rather than the wakers themselves.
#[derive(Debug, Default)]
pub(crate) struct WakerList(Vec<Waker>);

impl WakerList {
    pub(crate) const fn new() -> Self {
        WakerList(Vec::new())
    }

    /// Registers a waker to be woken by a later [`take`](Self::take).
    pub(crate) fn push(&mut self, waker: Waker) {
        self.0.push(waker);
    }

    /// Takes every waker, handing back the obligation to wake them.
    ///
    /// Call this while holding whatever lock guards the list, then release the
    /// lock, then [`wake`](PendingWakes::wake). That ordering is the reason the
    /// obligation is a separate value: it is meant to be carried out.
    #[must_use = "these wakers are owed a wake; call .wake() on the result"]
    pub(crate) fn take(&mut self) -> PendingWakes {
        PendingWakes(std::mem::take(&mut self.0))
    }
}

/// Wakers removed from a [`WakerList`] and not yet woken.
///
/// Waking is the only thing you can do with one. Dropping it without waking is
/// the bug this module exists to prevent, and does not go unremarked in debug
/// builds.
#[derive(Debug)]
#[must_use = "these wakers are owed a wake; call .wake()"]
pub(crate) struct PendingWakes(Vec<Waker>);

impl PendingWakes {
    /// An obligation to nobody, for a path that changed nothing anyone is
    /// waiting on.
    pub(crate) const fn none() -> Self {
        PendingWakes(Vec::new())
    }

    /// Wakes every waker, discharging the obligation.
    pub(crate) fn wake(mut self) {
        for waker in std::mem::take(&mut self.0) {
            waker.wake();
        }
    }
}

impl Drop for PendingWakes {
    fn drop(&mut self) {
        // Debug only, and never while unwinding: a panic in `drop` during a
        // panic aborts, which would turn a failing test into an unreadable one.
        #[cfg(debug_assertions)]
        if !self.0.is_empty() && !std::thread::panicking() {
            panic!(
                "{} waker(s) dropped without waking: whatever was waiting on them will \
                 never run. Call .wake() on the PendingWakes",
                self.0.len()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        rc::Rc,
        task::{RawWaker, RawWakerVTable, Waker},
    };

    /// A waker that records having been woken, and nothing else.
    fn counting_waker(count: Rc<Cell<usize>>) -> Waker {
        unsafe fn clone(data: *const ()) -> RawWaker {
            Rc::increment_strong_count(data as *const Cell<usize>);
            RawWaker::new(data, &VTABLE)
        }
        unsafe fn wake(data: *const ()) {
            let count = Rc::from_raw(data as *const Cell<usize>);
            count.set(count.get() + 1);
        }
        unsafe fn wake_by_ref(data: *const ()) {
            let count = &*(data as *const Cell<usize>);
            count.set(count.get() + 1);
        }
        unsafe fn drop_it(data: *const ()) {
            drop(Rc::from_raw(data as *const Cell<usize>));
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

        let ptr = Rc::into_raw(count) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
    }

    #[test]
    fn taking_an_obligation_wakes_everything_in_it() {
        let count = Rc::new(Cell::new(0));

        let mut list = WakerList::new();
        list.push(counting_waker(count.clone()));
        list.push(counting_waker(count.clone()));

        list.take().wake();
        assert_eq!(count.get(), 2);

        // Taking again owes nothing: the first take emptied the list.
        list.take().wake();
        assert_eq!(count.get(), 2, "take should have emptied the list");
    }

    #[test]
    fn an_empty_obligation_is_fine_to_drop() {
        let mut list = WakerList::new();
        drop(list.take()); // nothing owed, nothing to complain about
    }

    #[test]
    fn pushing_after_a_take_starts_a_fresh_obligation() {
        let count = Rc::new(Cell::new(0));

        let mut list = WakerList::new();
        list.push(counting_waker(count.clone()));
        list.take().wake();
        assert_eq!(count.get(), 1);

        list.push(counting_waker(count.clone()));
        list.take().wake();
        assert_eq!(
            count.get(),
            2,
            "the second waker should also have been woken"
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "dropped without waking")]
    fn dropping_an_owed_obligation_complains() {
        // The whole point: forgetting to wake is loud rather than silent.
        let count = Rc::new(Cell::new(0));
        let mut list = WakerList::new();
        list.push(counting_waker(count));

        let owed = list.take();
        drop(owed);
    }
}
