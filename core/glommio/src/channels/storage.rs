//! Where a channel keeps its state.
//!
//! Each channel in this module needs two forms: one that stays on the executor
//! that created it, and one that crosses cores. The difference between them is
//! `Rc<RefCell<_>>` against `Arc<Mutex<_>>`, and nothing else -- the ring
//! policy, the cursors, the close rules and the waker discipline are identical.
//! Written twice, the part that is hard to get right is the part that gets
//! duplicated.
//!
//! So it is written once, over this seam. A channel is generic in its storage,
//! defaulting to [`Local`], and its cross-core form is a type alias naming
//! [`Shared`] instead.
//!
//! # What the shapes here buy
//!
//! `Send`-ness *falls out of the storage* rather than being asserted: an `Rc`
//! makes the local form `!Send`, an `Arc<Mutex<_>>` makes the shared form
//! `Send + Sync` exactly when the state is `Send`. There is no `unsafe impl`
//! to audit; a reviewer checks a type parameter.
//!
//! [`Storage::with`] hands out `&mut S` for the duration of one call, so a
//! caller cannot hold the guard across an `await`. That is deliberate: it is
//! the bug people write by hand with an `Arc<Mutex<_>>`, and here it does not
//! typecheck rather than being discouraged in a comment.
//!
//! [`StorageExt::with_wakes`] exists because the correct order -- take the
//! wakers under the lock, release it, then wake -- should be the only order
//! that can be written. Waking under the lock leaves the woken poller blocking
//! on it immediately.
//!
//! # What it does not buy
//!
//! Nothing forces a caller of `with` to wake anybody. That gap is covered at
//! test time by [`PendingWakes`](crate::wakers::PendingWakes)'s drop guard,
//! for the reason given there: Rust has no linear types.
//!
//! Re-entering a storage from inside its own `with` closure is a borrow error
//! on [`Local`] and a deadlock on [`Shared`]. Waking outside the lock is what
//! keeps the channels clear of it.

use crate::wakers::{PendingWakes, WakerList};
use std::{
    cell::RefCell,
    fmt,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
};

/// State reachable through a lock appropriate to where it lives.
///
/// Implemented by [`Local`] and [`Shared`] only; it is sealed because the
/// channels here are written against the two of them, and a third would be
/// promising a stability this does not intend.
///
/// `Unpin` because a storage is a handle -- an `Rc` or an `Arc` -- and moving
/// one never moves the state it points at. Receivers are polled through
/// `Pin<&mut Self>`, and without this they could not reach their own fields.
///
/// Deliberately without a `new`: a storage that needed construction arguments
/// could not implement one, and no caller here would gain anything from it.
pub trait Storage<S>: Clone + Unpin + sealed::Sealed {
    /// Runs `f` with exclusive access to the state, releasing the lock when it
    /// returns.
    fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> R;
}

mod sealed {
    pub trait Sealed {}
    impl<S> Sealed for super::Local<S> {}
    impl<S> Sealed for super::Shared<S> {}
}

/// Mutating the state and waking what was waiting on it, in that order.
pub(crate) trait StorageExt<S>: Storage<S> {
    /// Mutates the state, then wakes -- outside the lock, which is the only
    /// order this shape can express.
    fn with_wakes<R>(&self, f: impl FnOnce(&mut S) -> (R, PendingWakes)) -> R {
        let (value, pending) = self.with(f);
        pending.wake();
        value
    }

    /// Wakes everything waiting on the state after `f` has changed it.
    ///
    /// The common case of [`with_wakes`](Self::with_wakes), for the channels
    /// that wake every waiter on every change.
    fn with_waking_all<R>(
        &self,
        wakers: impl Fn(&mut S) -> &mut WakerList,
        f: impl FnOnce(&mut S) -> R,
    ) -> R {
        self.with_wakes(|state| {
            let value = f(state);
            let pending = wakers(state).take();
            (value, pending)
        })
    }
}

impl<S, T: Storage<S>> StorageExt<S> for T {}

/// State on one executor, reached through a [`RefCell`].
///
/// `Rc`-based, and so `!Send`: a channel built on this cannot leave the core
/// that created it, and that is a compile error rather than a convention.
pub struct Local<S>(Rc<RefCell<S>>);

impl<S> Local<S> {
    pub(crate) fn new(state: S) -> Self {
        Local(Rc::new(RefCell::new(state)))
    }
}

impl<S> Local<S> {
    /// The state itself, borrowed for as long as the caller keeps it.
    ///
    /// Only local storage can offer this: a borrow outliving the call needs
    /// the value to stay put, which a lock shared between cores cannot
    /// promise. It exists for the `borrow()` methods the local channels had
    /// before this seam, and is why [`Storage`] does not try to abstract over
    /// guards.
    pub(crate) fn borrow(&self) -> std::cell::Ref<'_, S> {
        self.0.borrow()
    }
}

impl<S> Storage<S> for Local<S> {
    fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> R {
        f(&mut self.0.borrow_mut())
    }
}

/// State reachable from any thread, behind a [`Mutex`].
///
/// `Send + Sync` exactly when the state is `Send`, so a channel built on this
/// crosses cores, and one holding an `Rc` still does not.
pub struct Shared<S>(Arc<Mutex<S>>);

impl<S> Shared<S> {
    pub(crate) fn new(state: S) -> Self {
        Shared(Arc::new(Mutex::new(state)))
    }
}

impl<S> Storage<S> for Shared<S> {
    fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> R {
        // Poisoning is ignored rather than propagated. The state behind this
        // lock is a queue and some flags; a panic elsewhere leaves it a valid
        // queue and some flags. Propagating would turn one unrelated panic
        // into every peer on every core panicking too, which is a worse
        // failure than the one it reports.
        let mut guard = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut guard)
    }
}

// Hand-written rather than derived: a handle clones whether or not the state
// does, and `#[derive(Clone)]` would demand `S: Clone`.
impl<S> Clone for Local<S> {
    fn clone(&self) -> Self {
        Local(self.0.clone())
    }
}

impl<S> Clone for Shared<S> {
    fn clone(&self) -> Self {
        Shared(self.0.clone())
    }
}

impl<S: fmt::Debug> fmt::Debug for Local<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl<S: fmt::Debug> fmt::Debug for Shared<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        marker::PhantomData,
        rc::Rc,
        sync::Arc,
        task::{RawWaker, RawWakerVTable, Waker},
    };

    /// A waker that runs a closure when woken, and nothing else.
    ///
    /// Two of them, because the closures differ in what they may capture: the
    /// local one reaches a `Local` storage and so cannot be `Send`, which is
    /// the property under test rather than an inconvenience.
    fn local_waker(f: Rc<Box<dyn Fn()>>) -> Waker {
        unsafe fn clone(data: *const ()) -> RawWaker {
            Rc::increment_strong_count(data as *const Box<dyn Fn()>);
            RawWaker::new(data, &VTABLE)
        }
        unsafe fn wake(data: *const ()) {
            let f = Rc::from_raw(data as *const Box<dyn Fn()>);
            f();
        }
        unsafe fn wake_by_ref(data: *const ()) {
            (*(data as *const Box<dyn Fn()>))();
        }
        unsafe fn drop_it(data: *const ()) {
            drop(Rc::from_raw(data as *const Box<dyn Fn()>));
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

        let ptr = Rc::into_raw(f) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
    }

    type SharedFn = Box<dyn Fn() + Send + Sync>;

    fn shared_waker(f: Arc<SharedFn>) -> Waker {
        unsafe fn clone(data: *const ()) -> RawWaker {
            Arc::increment_strong_count(data as *const SharedFn);
            RawWaker::new(data, &VTABLE)
        }
        unsafe fn wake(data: *const ()) {
            let f = Arc::from_raw(data as *const SharedFn);
            f();
        }
        unsafe fn wake_by_ref(data: *const ()) {
            (*(data as *const SharedFn))();
        }
        unsafe fn drop_it(data: *const ()) {
            drop(Arc::from_raw(data as *const SharedFn));
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

        let ptr = Arc::into_raw(f) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
    }

    #[test]
    fn clones_of_a_local_storage_reach_the_same_state() {
        let one = Local::new(0u32);
        let two = one.clone();

        one.with(|state| *state += 1);
        two.with(|state| *state += 1);

        assert_eq!(one.with(|state| *state), 2);
    }

    #[test]
    fn clones_of_a_shared_storage_reach_the_same_state() {
        let one = Shared::new(0u32);
        let two = one.clone();

        let far = std::thread::spawn(move || two.with(|state| *state += 1));
        far.join().unwrap();
        one.with(|state| *state += 1);

        assert_eq!(one.with(|state| *state), 2);
    }

    #[test]
    fn with_returns_what_the_closure_returns() {
        assert_eq!(Local::new(7u32).with(|state| *state * 2), 14);
        assert_eq!(Shared::new(7u32).with(|state| *state * 2), 14);
    }

    // The point of the seam: which storage a channel is built on decides
    // whether it can cross a thread, with no `unsafe impl` anywhere.
    struct Probe<T>(PhantomData<T>);
    trait NotSend {
        fn is_send(&self) -> bool {
            false
        }
    }
    impl<T> NotSend for Probe<T> {}
    impl<T: Send> Probe<T> {
        fn is_send(&self) -> bool {
            true
        }
    }

    #[test]
    fn sendness_follows_the_storage() {
        assert!(
            !Probe::<Local<u32>>::is_send(&Probe(PhantomData)),
            "local storage must not cross cores"
        );
        assert!(
            Probe::<Shared<u32>>::is_send(&Probe(PhantomData)),
            "shared storage must cross cores"
        );

        fn assert_sync<T: Sync>() {}
        assert_sync::<Shared<u32>>();
    }

    #[test]
    fn shared_storage_of_a_thread_bound_value_does_not_cross() {
        // `Shared` is only `Send` when what it holds is: wrapping an `Rc` in
        // one must not launder it across cores.
        assert!(!Probe::<Shared<Rc<u32>>>::is_send(&Probe(PhantomData)));
    }

    #[test]
    fn with_wakes_wakes_after_releasing_the_lock() {
        // A waker that reaches back into the same storage. Woken under the
        // lock this panics with a borrow error (and would deadlock on the
        // shared storage); woken after it is released it simply works.
        let storage = Local::new(WakerList::new());
        let reentered = Rc::new(Cell::new(false));

        storage.with(|state| {
            state.push(local_waker(Rc::new(Box::new({
                let storage = storage.clone();
                let reentered = reentered.clone();
                move || {
                    storage.with(|_state| reentered.set(true));
                }
            }))))
        });

        let returned = storage.with_wakes(|state| (99u32, state.take()));

        assert_eq!(returned, 99);
        assert!(reentered.get(), "the waker never ran");
    }

    #[test]
    fn with_wakes_on_shared_storage_wakes_after_releasing_the_lock() {
        // Same check where getting it wrong deadlocks rather than panics.
        // A `Shared` cannot hold an `Rc`, so the flag is an `Arc`.
        let storage = Shared::new(WakerList::new());
        let reentered = Arc::new(std::sync::atomic::AtomicBool::new(false));

        storage.with(|state| {
            state.push(shared_waker(Arc::new(Box::new({
                let storage = storage.clone();
                let reentered = reentered.clone();
                move || {
                    storage
                        .with(|_state| reentered.store(true, std::sync::atomic::Ordering::Release));
                }
            }))))
        });

        storage.with_wakes(|state| ((), state.take()));

        assert!(reentered.load(std::sync::atomic::Ordering::Acquire));
    }
}
