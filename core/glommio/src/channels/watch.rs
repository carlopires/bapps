//! A channel that keeps only the latest value.
//!
//! Every receiver sees the most recent value rather than a queue of them, and
//! a receiver that falls behind simply skips what it missed. Configuration
//! that changes occasionally, and the current state of something a per-core
//! task must react to, are the shapes this fits.
//!
//! [`watch`] keeps every half on the executor that created it. [`shared`] is
//! the same channel over [`Shared`](super::storage::Shared) storage, for state
//! that every core watches.
//!
//! # Examples
//!
//! ```
//! use glommio::{channels::watch::watch, LocalExecutor};
//!
//! let ex = LocalExecutor::default();
//! ex.run(async {
//!     let (sender, mut receiver) = watch(0);
//!     sender.send(1).unwrap();
//!     receiver.changed().await.unwrap();
//!     assert_eq!(*receiver.borrow(), 1);
//! });
//! ```

use crate::{
    channels::storage::{Local, Shared, Storage, StorageExt},
    error::ResourceType,
    wakers::WakerList,
    GlommioError,
};
use futures_lite::Stream;
use std::{
    cell::Ref,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

/// The channel's state, reachable from every half.
///
/// Public only because it names the storage in the default type parameters
/// below. It has no callable surface.
#[doc(hidden)]
#[derive(Debug)]
pub struct State<T> {
    value: T,
    /// Bumped on every send. A receiver compares it against what it last saw,
    /// which is what lets a slow receiver skip values instead of queueing
    /// them.
    version: u64,
    wakers: WakerList,
    sender_gone: bool,
    receivers: usize,
}

impl<T> State<T> {
    fn new(initial: T) -> Self {
        State {
            value: initial,
            version: 0,
            wakers: WakerList::new(),
            sender_gone: false,
            receivers: 1,
        }
    }
}

/// Publishes new values. There is exactly one.
pub struct Sender<T, S: Storage<State<T>> = Local<State<T>>> {
    inner: S,
    /// `T` appears only inside the storage, and the storage is what decides
    /// the auto traits. A `fn() -> T` marker satisfies the type parameter
    /// without adding constraints of its own -- notably it stays `Unpin`
    /// whatever `T` is, as the `Rc` these were built on used to be.
    value: PhantomData<fn() -> T>,
}

/// Observes the latest value. Clone it to add another observer.
pub struct Receiver<T, S: Storage<State<T>> = Local<State<T>>> {
    inner: S,
    /// The version this receiver has already been told about.
    seen: u64,
    /// `T` appears only inside the storage, and the storage is what decides
    /// the auto traits. A `fn() -> T` marker satisfies the type parameter
    /// without adding constraints of its own -- notably it stays `Unpin`
    /// whatever `T` is, as the `Rc` these were built on used to be.
    value: PhantomData<fn() -> T>,
}

/// A [`Sender`] that can be moved to another executor. See [`shared`].
pub type SharedSender<T> = Sender<T, Shared<State<T>>>;

/// A [`Receiver`] that can be moved to another executor. See [`shared`].
pub type SharedReceiver<T> = Receiver<T, Shared<State<T>>>;

fn pair<T, S: Storage<State<T>>>(inner: S) -> (Sender<T, S>, Receiver<T, S>) {
    (
        Sender {
            inner: inner.clone(),
            value: PhantomData,
        },
        Receiver {
            inner,
            seen: 0,
            value: PhantomData,
        },
    )
}

/// Creates a watch channel holding `initial`, returning its two halves.
///
/// Every half stays on the executor that created it. For a channel whose
/// halves can be spread across cores, see [`shared`].
pub fn watch<T>(initial: T) -> (Sender<T>, Receiver<T>) {
    pair(Local::new(State::new(initial)))
}

/// Creates a watch channel whose halves can be sent between executors.
///
/// The same channel as [`watch`], over storage that crosses cores: one
/// publisher, and a receiver on each core that needs to react to the current
/// value. Nothing needs wiring up for the wake to arrive -- a glommio waker
/// carries its own executor's identity, so a receiver parked in the kernel is
/// woken wherever the send happens.
///
/// Every receiver takes the same lock to read, so this suits control-plane
/// fan-out -- configuration, the current leader, a shutdown flag -- rather
/// than a per-message data path.
///
/// # Examples
///
/// ```
/// use glommio::{channels::watch::shared, LocalExecutor, LocalExecutorBuilder, Placement};
///
/// let (sender, mut receiver) = shared(0);
///
/// let publisher = LocalExecutorBuilder::new(Placement::Unbound)
///     .spawn(move || async move { sender.send(7).unwrap() })
///     .unwrap();
///
/// let seen = LocalExecutor::default().run(async move {
///     receiver.changed().await.unwrap();
///     receiver.get()
/// });
///
/// publisher.join().unwrap();
/// assert_eq!(seen, 7);
/// ```
pub fn shared<T: Send>(initial: T) -> (SharedSender<T>, SharedReceiver<T>) {
    pair(Shared::new(State::new(initial)))
}

impl<T, S: Storage<State<T>>> Sender<T, S> {
    /// Replaces the value and wakes every receiver.
    ///
    /// # Errors
    ///
    /// If every receiver has been dropped there is nobody to tell, so the
    /// value is handed back inside [`GlommioError::Closed`].
    pub fn send(&self, value: T) -> Result<(), GlommioError<T>> {
        self.inner.with_wakes(|state| {
            let outcome = if state.receivers == 0 {
                Err(GlommioError::Closed(ResourceType::Channel(value)))
            } else {
                state.value = value;
                state.version += 1;
                Ok(())
            };
            (outcome, state.wakers.take())
        })
    }

    /// Runs `f` on the current value without changing it.
    ///
    /// The general form of [`borrow`](Sender::borrow), which exists on the
    /// local channel only: a shared channel's value lives behind a lock this
    /// call holds for exactly the duration of `f`, which is what keeps it from
    /// being held across an `await`.
    pub fn with_current<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.inner.with(|state| f(&state.value))
    }

    /// Returns whether every receiver has gone away.
    pub fn is_closed(&self) -> bool {
        self.inner.with(|state| state.receivers == 0)
    }
}

impl<T: Clone, S: Storage<State<T>>> Sender<T, S> {
    /// Clones the current value out.
    pub fn get(&self) -> T {
        self.with_current(T::clone)
    }
}

impl<T> Sender<T, Local<State<T>>> {
    /// Borrows the current value without changing it.
    ///
    /// Local only: a borrow that outlives the call needs the value to stay
    /// put, which a lock shared between cores cannot promise. The shared
    /// channel has [`with_current`](Sender::with_current) and
    /// [`get`](Sender::get) instead.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref::map(self.inner.borrow(), |state| &state.value)
    }
}

impl<T, S: Storage<State<T>>> Drop for Sender<T, S> {
    fn drop(&mut self) {
        self.inner
            .with_waking_all(|state| &mut state.wakers, |state| state.sender_gone = true);
    }
}

impl<T, S: Storage<State<T>>> Receiver<T, S> {
    /// Waits until a value newer than the last one this receiver was told
    /// about arrives, and marks it seen.
    ///
    /// Resolves immediately if one is already waiting. Intermediate values are
    /// skipped rather than queued.
    ///
    /// # Errors
    ///
    /// Fails once the sender has been dropped and no unseen value remains.
    pub fn changed(&mut self) -> Changed<'_, T, S> {
        Changed { receiver: self }
    }

    /// Runs `f` on the latest value.
    ///
    /// This does not mark the value as seen: only [`changed`](Self::changed)
    /// does that.
    pub fn with_current<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        self.inner.with(|state| f(&state.value))
    }
}

impl<T: Clone, S: Storage<State<T>>> Receiver<T, S> {
    /// Clones the latest value out, without marking it seen.
    pub fn get(&self) -> T {
        self.with_current(T::clone)
    }
}

impl<T> Receiver<T, Local<State<T>>> {
    /// Borrows the latest value.
    ///
    /// This does not mark the value as seen: only [`changed`](Self::changed)
    /// does that.
    ///
    /// Local only, for the reason given on [`Sender::borrow`].
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref::map(self.inner.borrow(), |state| &state.value)
    }
}

impl<T, S: Storage<State<T>>> Clone for Receiver<T, S> {
    fn clone(&self) -> Self {
        self.inner.with(|state| state.receivers += 1);
        Receiver {
            inner: self.inner.clone(),
            seen: self.seen,
            value: PhantomData,
        }
    }
}

impl<T, S: Storage<State<T>>> Drop for Receiver<T, S> {
    fn drop(&mut self) {
        self.inner.with(|state| state.receivers -= 1);
    }
}

impl<T: Clone, S: Storage<State<T>>> Stream for Receiver<T, S> {
    type Item = T;

    /// Yields the latest value each time one arrives, skipping any the
    /// consumer was too slow to see, and ends when the sender goes away.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let seen = &mut this.seen;

        this.inner.with(|state| {
            if state.version > *seen {
                *seen = state.version;
                return Poll::Ready(Some(state.value.clone()));
            }

            if state.sender_gone {
                return Poll::Ready(None);
            }

            state.wakers.push(cx.waker().clone());
            Poll::Pending
        })
    }
}

/// The future returned by [`Receiver::changed`].
#[derive(Debug)]
pub struct Changed<'a, T, S: Storage<State<T>> = Local<State<T>>> {
    receiver: &'a mut Receiver<T, S>,
}

impl<T, S: Storage<State<T>>> Future for Changed<'_, T, S> {
    type Output = Result<(), GlommioError<()>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = &mut self.get_mut().receiver;
        let seen = &mut receiver.seen;

        receiver.inner.with(|state| {
            // An unseen value outranks a departed sender: the last thing it
            // sent is still worth delivering.
            if state.version > *seen {
                *seen = state.version;
                return Poll::Ready(Ok(()));
            }

            if state.sender_gone {
                return Poll::Ready(Err(GlommioError::Closed(ResourceType::Channel(()))));
            }

            state.wakers.push(cx.waker().clone());
            Poll::Pending
        })
    }
}

impl<T, S: Storage<State<T>> + fmt::Debug> fmt::Debug for Sender<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, S: Storage<State<T>> + fmt::Debug> fmt::Debug for Receiver<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("inner", &self.inner)
            .field("seen", &self.seen)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::{cell::RefCell, rc::Rc, time::Duration};

    #[test]
    fn a_shared_watch_value_crosses_cores() {
        let (sender, mut receiver) = shared(0u32);

        let publisher = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move { sender.send(3).unwrap() })
            .unwrap();

        let seen = LocalExecutor::default().run(async move {
            receiver.changed().await.unwrap();
            receiver.get()
        });

        publisher.join().unwrap();
        assert_eq!(seen, 3);
    }

    #[test]
    fn a_parked_shared_receiver_is_woken_by_a_send() {
        let (sender, mut receiver) = shared(0u32);

        let publisher = std::thread::spawn(move || {
            // Long enough that the watching executor is parked in the kernel.
            std::thread::sleep(Duration::from_millis(300));
            let sent_at = std::time::Instant::now();
            sender.send(1).unwrap();
            sent_at
        });

        let woken_at = LocalExecutor::default().run(async move {
            receiver.changed().await.unwrap();
            std::time::Instant::now()
        });

        let sent_at = publisher.join().unwrap();
        let delay = woken_at.duration_since(sent_at);
        assert!(
            delay < Duration::from_millis(100),
            "a parked executor took {delay:?} to see the new value: it was not woken"
        );
    }

    #[test]
    fn a_shared_sender_sees_its_receivers_go_away() {
        let (sender, receiver) = shared(0u32);
        let second = receiver.clone();

        assert!(!sender.is_closed());
        drop(receiver);
        assert!(!sender.is_closed(), "one receiver remains");

        std::thread::spawn(move || drop(second)).join().unwrap();
        assert!(sender.is_closed());
    }

    #[test]
    fn dropping_a_shared_sender_ends_the_receiver() {
        let (sender, mut receiver) = shared(0u32);

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(sender);
        });

        let outcome = LocalExecutor::default().run(async move { receiver.changed().await });
        assert!(
            outcome.is_err(),
            "a receiver whose sender went away must not wait forever"
        );
    }

    #[test]
    fn a_receiver_starts_on_the_initial_value() {
        LocalExecutor::default().run(async {
            let (_sender, receiver) = watch(5);
            assert_eq!(*receiver.borrow(), 5);
        });
    }

    #[test]
    fn changed_resolves_after_a_send_and_the_value_is_visible() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = watch(0);
            sender.send(9).unwrap();
            receiver.changed().await.unwrap();
            assert_eq!(*receiver.borrow(), 9);
        });
    }

    #[test]
    fn changed_waits_until_something_is_sent() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = watch(0);
            let order = Rc::new(RefCell::new(Vec::new()));

            let waiter = crate::spawn_local({
                let order = order.clone();
                async move {
                    receiver.changed().await.unwrap();
                    order.borrow_mut().push("observed");
                    *receiver.borrow()
                }
            })
            .detach();

            Timer::new(Duration::from_millis(10)).await;
            order.borrow_mut().push("nothing sent yet");
            sender.send(4).unwrap();

            assert_eq!(waiter.await.unwrap(), 4);
            assert_eq!(
                *order.borrow(),
                vec!["nothing sent yet", "observed"],
                "changed() resolved before any value was sent"
            );
        });
    }

    #[test]
    fn every_receiver_observes_a_change() {
        LocalExecutor::default().run(async {
            let (sender, mut first) = watch(0);
            let mut second = first.clone();

            sender.send(1).unwrap();

            first.changed().await.unwrap();
            second.changed().await.unwrap();
            assert_eq!(*first.borrow(), 1);
            assert_eq!(*second.borrow(), 1);
        });
    }

    #[test]
    fn only_the_latest_value_is_kept() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = watch(0);
            sender.send(1).unwrap();
            sender.send(2).unwrap();
            sender.send(3).unwrap();

            receiver.changed().await.unwrap();
            assert_eq!(*receiver.borrow(), 3, "a watch keeps only the latest value");
        });
    }

    #[test]
    fn dropping_the_sender_wakes_a_waiting_receiver_with_an_error() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = watch(0);

            let waiter = crate::spawn_local(async move { receiver.changed().await }).detach();

            Timer::new(Duration::from_millis(10)).await;
            drop(sender);

            assert!(
                waiter.await.unwrap().is_err(),
                "a receiver waiting on a departed sender should be woken with an error"
            );
        });
    }

    #[test]
    fn a_receiver_is_a_stream_of_the_latest_values() {
        LocalExecutor::default().run(async {
            use futures_lite::StreamExt;

            let (sender, receiver) = watch(0);
            sender.send(1).unwrap();
            sender.send(2).unwrap();
            sender.send(3).unwrap();
            drop(sender);

            let seen: Vec<_> = receiver.collect().await;
            assert_eq!(
                seen,
                vec![3],
                "a watch stream yields the latest value, not every one"
            );
        });
    }

    #[test]
    fn sending_with_no_receivers_left_fails() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = watch(0);
            drop(receiver);
            assert!(sender.send(1).is_err());
        });
    }
}
