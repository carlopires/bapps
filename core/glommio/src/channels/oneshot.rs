//! A channel carrying exactly one value.
//!
//! Request/response pairing in front of a per-core worker is the shape this
//! exists for. A [`local_channel`](super::local_channel) bounded at one works
//! and says the wrong thing: it does not express that exactly one value will
//! ever be sent, and it cannot hand an unsent value back.
//!
//! [`oneshot`] keeps both halves on the executor that created them, which is
//! the right default. [`shared`] is the same channel over
//! [`Shared`](super::storage::Shared) storage, for the ask-and-reply idiom
//! across cores.
//!
//! # Examples
//!
//! ```
//! use glommio::{channels::oneshot::oneshot, LocalExecutor};
//!
//! let ex = LocalExecutor::default();
//! ex.run(async {
//!     let (sender, receiver) = oneshot();
//!     glommio::spawn_local(async move { sender.send(42).unwrap() }).detach();
//!     assert_eq!(receiver.await.unwrap(), 42);
//! });
//! ```

use crate::{
    channels::storage::{Local, Shared, Storage, StorageExt},
    error::ResourceType,
    wakers::WakerList,
    GlommioError,
};
use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

/// The channel's state, reachable from both halves.
///
/// Public only because it names the storage in [`Sender`] and [`Receiver`]'s
/// default type parameter. It has no callable surface.
#[doc(hidden)]
#[derive(Debug)]
pub struct State<T> {
    value: Option<T>,
    /// Holds the receiver's waker while it is suspended. A `WakerList` rather
    /// than an `Option<Waker>` so that taking it yields an obligation to wake
    /// rather than a value that can be dropped on the floor.
    waker: WakerList,
    /// Set when either half is dropped, so the survivor can tell the
    /// difference between "not yet" and "never".
    sender_gone: bool,
    receiver_gone: bool,
}

impl<T> State<T> {
    fn new() -> Self {
        State {
            value: None,
            waker: WakerList::new(),
            sender_gone: false,
            receiver_gone: false,
        }
    }
}

/// Sends the single value. Consumed by [`send`](Sender::send).
pub struct Sender<T, S: Storage<State<T>> = Local<State<T>>> {
    inner: S,
    /// `T` appears only inside the storage, and the storage is what decides
    /// the auto traits. A `fn() -> T` marker satisfies the type parameter
    /// without adding constraints of its own -- notably it stays `Unpin`
    /// whatever `T` is, as the `Rc` these were built on used to be.
    value: PhantomData<fn() -> T>,
}

/// Resolves to the single value, or to an error if the sender goes away first.
pub struct Receiver<T, S: Storage<State<T>> = Local<State<T>>> {
    inner: S,
    /// `T` appears only inside the storage, and the storage is what decides
    /// the auto traits. A `fn() -> T` marker satisfies the type parameter
    /// without adding constraints of its own -- notably it stays `Unpin`
    /// whatever `T` is, as the `Rc` these were built on used to be.
    value: PhantomData<fn() -> T>,
}

/// A [`Sender`] that can be moved to another executor. See [`shared`].
pub type SharedSender<T> = Sender<T, Shared<State<T>>>;

/// A [`Receiver`] that can be awaited on another executor. See [`shared`].
pub type SharedReceiver<T> = Receiver<T, Shared<State<T>>>;

fn pair<T, S: Storage<State<T>>>(inner: S) -> (Sender<T, S>, Receiver<T, S>) {
    (
        Sender {
            inner: inner.clone(),
            value: PhantomData,
        },
        Receiver {
            inner,
            value: PhantomData,
        },
    )
}

/// Creates a new one-shot channel, returning its two halves.
///
/// Both halves stay on the executor that created them. For a pair that can
/// cross cores, see [`shared`].
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    pair(Local::new(State::new()))
}

/// Creates a one-shot channel whose halves can be sent between executors.
///
/// The same channel as [`oneshot`], over storage that crosses cores: the
/// sender travels to the service that will answer, the receiver is awaited
/// where the question was asked. Nothing needs wiring up for the wake to
/// arrive -- a glommio waker carries its own executor's identity, so the
/// receiver's executor is woken out of the kernel wherever the send happens.
///
/// # Examples
///
/// ```
/// use glommio::{channels::oneshot::shared, LocalExecutor, LocalExecutorBuilder, Placement};
///
/// let (sender, receiver) = shared();
///
/// let answering = LocalExecutorBuilder::new(Placement::Unbound)
///     .spawn(move || async move { sender.send(42).unwrap() })
///     .unwrap();
///
/// let answer = LocalExecutor::default().run(async move { receiver.await });
/// answering.join().unwrap();
/// assert_eq!(answer.unwrap(), 42);
/// ```
pub fn shared<T: Send>() -> (SharedSender<T>, SharedReceiver<T>) {
    pair(Shared::new(State::new()))
}

impl<T, S: Storage<State<T>>> Sender<T, S> {
    /// Sends the value, waking the receiver if it is already waiting.
    ///
    /// # Errors
    ///
    /// If the receiver has been dropped the value has nowhere to go, so it is
    /// handed back inside [`GlommioError::Closed`] rather than dropped
    /// silently.
    pub fn send(self, value: T) -> Result<(), GlommioError<T>> {
        self.inner.with_wakes(|state| {
            let outcome = if state.receiver_gone {
                Err(GlommioError::Closed(ResourceType::Channel(value)))
            } else {
                state.value = Some(value);
                Ok(())
            };
            (outcome, state.waker.take())
        })
    }

    /// Returns whether the receiver has gone away, so a caller holding an
    /// expensive value can skip producing it.
    pub fn is_closed(&self) -> bool {
        self.inner.with(|state| state.receiver_gone)
    }
}

impl<T, S: Storage<State<T>>> Drop for Sender<T, S> {
    fn drop(&mut self) {
        self.inner
            .with_waking_all(|state| &mut state.waker, |state| state.sender_gone = true);
    }
}

impl<T, S: Storage<State<T>>> Future for Receiver<T, S> {
    type Output = Result<T, GlommioError<()>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Taking the value and registering the waker under one lock is what
        // makes the missed-wakeup race unwriteable: there is no window between
        // "no value yet" and "and here is where to tell me".
        self.inner.with(|state| {
            if let Some(value) = state.value.take() {
                return Poll::Ready(Ok(value));
            }

            if state.sender_gone {
                return Poll::Ready(Err(GlommioError::Closed(ResourceType::Channel(()))));
            }

            state.waker.push(cx.waker().clone());
            Poll::Pending
        })
    }
}

impl<T, S: Storage<State<T>>> Drop for Receiver<T, S> {
    fn drop(&mut self) {
        self.inner.with(|state| state.receiver_gone = true);
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
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::{
        cell::RefCell,
        rc::Rc,
        time::{Duration, Instant},
    };

    #[test]
    fn a_shared_value_crosses_cores() {
        // The control-plane idiom: a reply channel handed to a service on
        // another core, fired there, awaited here.
        let (sender, receiver) = shared::<u32>();

        let worker = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move { sender.send(9).unwrap() })
            .unwrap();

        let value = crate::LocalExecutor::default().run(receiver);

        worker.join().unwrap();
        assert_eq!(value.unwrap(), 9);
    }

    #[test]
    fn a_shared_receiver_is_woken_when_it_was_parked() {
        let (sender, receiver) = shared::<u32>();

        let sender_thread = std::thread::spawn(move || {
            // Long enough that the receiving executor has parked in the
            // kernel: waking it is the whole mechanism.
            std::thread::sleep(Duration::from_millis(300));
            let sent_at = std::time::Instant::now();
            sender.send(1).unwrap();
            sent_at
        });

        let received_at = crate::LocalExecutor::default()
            .run(async move { receiver.await.map(|_| Instant::now()) });

        let sent_at = sender_thread.join().unwrap();
        let delay = received_at.unwrap().duration_since(sent_at);
        assert!(
            delay < Duration::from_millis(100),
            "a parked executor took {delay:?} to see the reply: it was not woken"
        );
    }

    #[test]
    fn dropping_a_shared_sender_wakes_the_receiver_with_an_error() {
        let (sender, receiver) = shared::<u32>();

        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            drop(sender);
        });

        let outcome = crate::LocalExecutor::default().run(receiver);
        assert!(
            outcome.is_err(),
            "a receiver whose sender went away must not wait forever"
        );
    }

    #[test]
    fn sending_to_a_departed_shared_receiver_hands_the_value_back() {
        let (sender, receiver) = shared::<String>();
        drop(receiver);

        match sender.send("payload".to_string()) {
            Err(GlommioError::Closed(ResourceType::Channel(value))) => {
                assert_eq!(value, "payload")
            }
            other => panic!("expected the value back, got {other:?}"),
        }
    }

    #[test]
    fn a_sent_value_is_received() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = oneshot::<u32>();
            sender.send(7).unwrap();
            assert_eq!(receiver.await.unwrap(), 7);
        });
    }

    #[test]
    fn the_receiver_waits_until_the_value_is_sent() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = oneshot::<u32>();
            let order = Rc::new(RefCell::new(Vec::new()));

            let waiter = crate::spawn_local({
                let order = order.clone();
                async move {
                    let value = receiver.await.unwrap();
                    order.borrow_mut().push("received");
                    value
                }
            })
            .detach();

            Timer::new(Duration::from_millis(10)).await;
            order.borrow_mut().push("still waiting");
            sender.send(3).unwrap();

            assert_eq!(waiter.await.unwrap(), 3);
            assert_eq!(
                *order.borrow(),
                vec!["still waiting", "received"],
                "the receiver resolved before anything was sent"
            );
        });
    }

    #[test]
    fn dropping_the_sender_wakes_the_receiver_with_an_error() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = oneshot::<u32>();

            let waiter = crate::spawn_local(receiver).detach();

            Timer::new(Duration::from_millis(10)).await;
            drop(sender);

            assert!(
                waiter.await.unwrap().is_err(),
                "a receiver whose sender went away should be woken with an error"
            );
        });
    }

    #[test]
    fn sending_to_a_dropped_receiver_hands_the_value_back() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = oneshot::<String>();
            drop(receiver);

            match sender.send("payload".to_string()) {
                Err(GlommioError::Closed(ResourceType::Channel(value))) => {
                    assert_eq!(value, "payload", "the unsent value should come back");
                }
                other => panic!("expected the value back, got {other:?}"),
            }
        });
    }

    #[test]
    fn the_sender_can_see_that_the_receiver_is_gone() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = oneshot::<u32>();
            assert!(!sender.is_closed());
            drop(receiver);
            assert!(sender.is_closed());
        });
    }
}
