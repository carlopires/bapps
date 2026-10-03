//! Multi-consumer fan-out where every receiver sees every value.
//!
//! Every receiver observes every value sent after it subscribed. A receiver
//! that falls too far behind is told so -- [`RecvError::Lagged`] -- rather than
//! being allowed to hold the sender up, which is the difference between this
//! and a bounded [`local_channel`](super::local_channel).
//!
//! Where only the newest value matters, [`watch`](super::watch) is the better
//! fit: it keeps one value rather than a window of them.
//!
//! Every value is cloned once per receiver. That is not a limitation worth
//! working around: `Rc<T>` is itself `Clone`, so broadcasting `Rc<T>` gives
//! refcount-cheap fan-out under the same API.
//!
//! [`broadcast`] keeps every half on the executor that created it. [`shared`]
//! is the same channel over [`Shared`](super::storage::Shared) storage, for
//! fan-out to receivers on other cores.
//!
//! # Examples
//!
//! ```
//! use glommio::{channels::broadcast::broadcast, LocalExecutor};
//!
//! let ex = LocalExecutor::default();
//! ex.run(async {
//!     let (sender, mut receiver) = broadcast(16);
//!     let mut second = sender.subscribe();
//!
//!     sender.send(1).unwrap();
//!
//!     assert_eq!(receiver.recv().await.unwrap(), 1);
//!     assert_eq!(second.recv().await.unwrap(), 1);
//! });
//! ```

use crate::{
    channels::storage::{Local, Shared, Storage, StorageExt},
    error::ResourceType,
    wakers::{PendingWakes, WakerList},
    GlommioError,
};
use futures_lite::Stream;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

/// Why [`Receiver::recv`] could not produce a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// Every sender is gone and every retained value has been read.
    Closed,
    /// This receiver fell behind and the values it had not read were
    /// overwritten. Carries how many were missed; the next `recv` returns the
    /// oldest value still retained, so consumption can simply continue.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Closed => write!(f, "the broadcast channel is closed"),
            RecvError::Lagged(missed) => {
                write!(f, "the receiver lagged behind by {missed} values")
            }
        }
    }
}

impl std::error::Error for RecvError {}

/// Why [`Receiver::try_recv`] could not produce a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value is waiting, but the channel is still open.
    Empty,
    /// Every sender is gone and every retained value has been read.
    Closed,
    /// As [`RecvError::Lagged`].
    Lagged(u64),
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "no value is waiting"),
            TryRecvError::Closed => write!(f, "the broadcast channel is closed"),
            TryRecvError::Lagged(missed) => {
                write!(f, "the receiver lagged behind by {missed} values")
            }
        }
    }
}

impl std::error::Error for TryRecvError {}

/// The channel's state, reachable from every half.
///
/// Public only because it names the storage in the default type parameters
/// below. It has no callable surface.
#[doc(hidden)]
#[derive(Debug)]
pub struct State<T> {
    /// The retained window, oldest first, never longer than `capacity`.
    ///
    /// A value lives here until it is overwritten rather than until every
    /// receiver has read it. The retention bound is the same either way, and
    /// this keeps the bookkeeping to one cursor per receiver.
    values: VecDeque<(u64, T)>,
    capacity: usize,
    /// The sequence the next sent value will carry.
    next_seq: u64,
    wakers: WakerList,
    senders: usize,
    receivers: usize,
}

impl<T> State<T> {
    fn new(capacity: usize) -> Self {
        State {
            values: VecDeque::with_capacity(capacity),
            capacity,
            next_seq: 0,
            wakers: WakerList::new(),
            senders: 1,
            receivers: 1,
        }
    }

    /// The sequence of the oldest value still retained, if any.
    fn oldest(&self) -> Option<u64> {
        self.values.front().map(|(seq, _)| *seq)
    }

    /// Reads the value at `next`, advancing the cursor past it.
    ///
    /// The whole read decision under one lock, so that a receiver polling and
    /// a sender sending cannot interleave into a lost wakeup: the caller finds
    /// a value, learns it lagged, or registers to be told -- with no window
    /// between deciding and registering.
    fn take_next(&mut self, next: &mut u64) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        if let Some(oldest) = self.oldest() {
            if *next < oldest {
                let missed = oldest - *next;
                *next = oldest;
                return Err(TryRecvError::Lagged(missed));
            }
        }

        let found = self
            .values
            .iter()
            .find(|(seq, _)| *seq == *next)
            .map(|(_, value)| value.clone());

        match found {
            Some(value) => {
                *next += 1;
                Ok(value)
            }
            None if self.senders == 0 => Err(TryRecvError::Closed),
            None => Err(TryRecvError::Empty),
        }
    }
}

/// Sends values to every receiver. Clone it to send from more than one place.
pub struct Sender<T, S: Storage<State<T>> = Local<State<T>>> {
    inner: S,
    /// `T` appears only inside the storage, and the storage is what decides
    /// the auto traits. A `fn() -> T` marker satisfies the type parameter
    /// without adding constraints of its own -- notably it stays `Unpin`
    /// whatever `T` is, as the `Rc` these were built on used to be.
    value: PhantomData<fn() -> T>,
}

/// Receives every value sent after it subscribed.
pub struct Receiver<T, S: Storage<State<T>> = Local<State<T>>> {
    inner: S,
    /// The sequence this receiver wants next.
    next: u64,
    value: PhantomData<fn() -> T>,
}

/// A [`Sender`] that can be moved to another executor. See [`shared`].
pub type SharedSender<T> = Sender<T, Shared<State<T>>>;

/// A [`Receiver`] that can be moved to another executor. See [`shared`].
pub type SharedReceiver<T> = Receiver<T, Shared<State<T>>>;

/// Creates a broadcast channel retaining at most `capacity` values.
///
/// # Panics
///
/// Panics if `capacity` is zero: a channel that can retain nothing would
/// report every value as lagged.
pub fn broadcast<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    pair(Local::new(State::new(check(capacity))))
}

/// Creates a broadcast channel whose halves can be sent between executors.
///
/// The same channel as [`broadcast`], over storage that crosses cores.
/// Nothing needs wiring up for the wake to arrive -- a glommio waker carries
/// its own executor's identity, so a receiver parked in the kernel is woken
/// wherever the send happens.
///
/// Every receiver takes the same lock to read, so this suits control-plane
/// fan-out -- a checkpoint decision, a route becoming ready, a shutdown --
/// rather than a per-message data path. For that, give each core its own
/// channel and fan out with [`channel_mesh`](super::channel_mesh).
///
/// # Panics
///
/// As [`broadcast`], if `capacity` is zero.
///
/// # Examples
///
/// ```
/// use glommio::{channels::broadcast::shared, LocalExecutorBuilder, Placement};
///
/// let (sender, mut receiver) = shared(16);
///
/// let listener = LocalExecutorBuilder::new(Placement::Unbound)
///     .spawn(move || async move { receiver.recv().await.unwrap() })
///     .unwrap();
///
/// sender.send(1).unwrap();
/// assert_eq!(listener.join().unwrap(), 1);
/// ```
pub fn shared<T: Clone + Send>(capacity: usize) -> (SharedSender<T>, SharedReceiver<T>) {
    pair(Shared::new(State::new(check(capacity))))
}

fn check(capacity: usize) -> usize {
    assert!(
        capacity > 0,
        "a broadcast channel needs room for at least one value"
    );
    capacity
}

fn pair<T, S: Storage<State<T>>>(inner: S) -> (Sender<T, S>, Receiver<T, S>) {
    (
        Sender {
            inner: inner.clone(),
            value: PhantomData,
        },
        Receiver {
            inner,
            next: 0,
            value: PhantomData,
        },
    )
}

impl<T: Clone, S: Storage<State<T>>> Sender<T, S> {
    /// Sends a value to every receiver, dropping the oldest retained value if
    /// the channel is full.
    ///
    /// Returns how many receivers the value went to.
    ///
    /// # Errors
    ///
    /// With no receivers left the value has nowhere to go, so it is handed
    /// back inside [`GlommioError::Closed`] rather than dropped silently.
    pub fn send(&self, value: T) -> Result<usize, GlommioError<T>> {
        self.inner.with_wakes(|state| {
            let outcome = if state.receivers == 0 {
                Err(GlommioError::Closed(ResourceType::Channel(value)))
            } else {
                if state.values.len() == state.capacity {
                    state.values.pop_front();
                }

                let seq = state.next_seq;
                state.next_seq += 1;
                state.values.push_back((seq, value));
                Ok(state.receivers)
            };
            (outcome, state.wakers.take())
        })
    }

    /// Creates a receiver that will see values sent from now on, and none of
    /// those sent before.
    pub fn subscribe(&self) -> Receiver<T, S> {
        let next = self.inner.with(|state| {
            state.receivers += 1;
            state.next_seq
        });

        Receiver {
            inner: self.inner.clone(),
            next,
            value: PhantomData,
        }
    }

    /// How many receivers are listening.
    pub fn receiver_count(&self) -> usize {
        self.inner.with(|state| state.receivers)
    }
}

impl<T, S: Storage<State<T>>> Clone for Sender<T, S> {
    fn clone(&self) -> Self {
        self.inner.with(|state| state.senders += 1);
        Sender {
            inner: self.inner.clone(),
            value: PhantomData,
        }
    }
}

impl<T, S: Storage<State<T>>> Drop for Sender<T, S> {
    fn drop(&mut self) {
        self.inner.with_wakes(|state| {
            state.senders -= 1;
            // Only the last one closes the channel; until then there is
            // nothing new to tell anybody.
            let pending = if state.senders == 0 {
                state.wakers.take()
            } else {
                PendingWakes::none()
            };
            ((), pending)
        });
    }
}

impl<T: Clone, S: Storage<State<T>>> Receiver<T, S> {
    /// Waits for the next value.
    ///
    /// Returns [`RecvError::Lagged`] if values this receiver had not read were
    /// overwritten; the cursor then sits on the oldest retained value, so the
    /// next call resumes from there.
    pub fn recv(&mut self) -> Recv<'_, T, S> {
        Recv { receiver: self }
    }

    /// Takes the next value if one is waiting, without suspending.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let next = &mut self.next;
        self.inner.with(|state| state.take_next(next))
    }

    /// Reads the next value, or registers `waker` to be told when there is
    /// one -- both under a single lock.
    ///
    /// Deciding and registering separately would leave a window for a sender
    /// to fill the channel and wake nobody, which on a shared channel means
    /// two cores and a real lost wakeup rather than a theoretical one.
    fn poll_recv(&mut self, waker: &Context<'_>) -> Poll<Result<T, TryRecvError>> {
        let next = &mut self.next;
        self.inner.with(|state| match state.take_next(next) {
            Err(TryRecvError::Empty) => {
                state.wakers.push(waker.waker().clone());
                Poll::Pending
            }
            outcome => Poll::Ready(outcome),
        })
    }
}

impl<T, S: Storage<State<T>>> Clone for Receiver<T, S> {
    fn clone(&self) -> Self {
        self.inner.with(|state| state.receivers += 1);
        Receiver {
            inner: self.inner.clone(),
            next: self.next,
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
    type Item = Result<T, RecvError>;

    /// Yields `Err(RecvError::Lagged(n))` rather than skipping quietly, and
    /// ends when the channel closes.
    ///
    /// A lagging receiver is a fact its consumer usually wants to know -- a
    /// stream that hid it would turn a detectable gap into a silent one.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready(Some(Ok(value))),
            Poll::Ready(Err(TryRecvError::Lagged(missed))) => {
                Poll::Ready(Some(Err(RecvError::Lagged(missed))))
            }
            Poll::Ready(Err(TryRecvError::Closed)) => Poll::Ready(None),
            Poll::Ready(Err(TryRecvError::Empty)) => unreachable!("poll_recv suspends instead"),
        }
    }
}

/// The future returned by [`Receiver::recv`].
#[derive(Debug)]
pub struct Recv<'a, T, S: Storage<State<T>> = Local<State<T>>> {
    receiver: &'a mut Receiver<T, S>,
}

impl<T: Clone, S: Storage<State<T>>> Future for Recv<'_, T, S> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
            Poll::Ready(Err(TryRecvError::Lagged(missed))) => {
                Poll::Ready(Err(RecvError::Lagged(missed)))
            }
            Poll::Ready(Err(TryRecvError::Closed)) => Poll::Ready(Err(RecvError::Closed)),
            Poll::Ready(Err(TryRecvError::Empty)) => unreachable!("poll_recv suspends instead"),
        }
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
            .field("next", &self.next)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shared_value_reaches_receivers_on_other_cores() {
        let (sender, mut first) = shared::<u32>(16);
        let mut second = sender.subscribe();

        let one = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move { first.recv().await })
            .unwrap();
        let two = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move { second.recv().await })
            .unwrap();

        // Both receivers are on other cores; this thread is the sender. They
        // may not have subscribed to the value yet -- they will still see it,
        // because it is retained rather than delivered.
        sender.send(4).unwrap();

        assert_eq!(one.join().unwrap().unwrap(), 4);
        assert_eq!(two.join().unwrap().unwrap(), 4);
    }

    #[test]
    fn a_parked_shared_receiver_is_woken_by_a_send() {
        let (sender, mut receiver) = shared::<u32>(16);

        let publisher = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let sent_at = std::time::Instant::now();
            sender.send(1).unwrap();
            sent_at
        });

        let woken_at = crate::LocalExecutor::default().run(async move {
            receiver.recv().await.unwrap();
            std::time::Instant::now()
        });

        let sent_at = publisher.join().unwrap();
        let delay = woken_at.duration_since(sent_at);
        assert!(
            delay < Duration::from_millis(100),
            "a parked executor took {delay:?} to see the value: it was not woken"
        );
    }

    #[test]
    fn a_shared_receiver_that_falls_behind_is_told_so() {
        let (sender, mut receiver) = shared::<u32>(2);

        std::thread::spawn(move || {
            for value in 0..5 {
                sender.send(value).unwrap();
            }
        })
        .join()
        .unwrap();

        assert_eq!(receiver.try_recv(), Err(TryRecvError::Lagged(3)));
        assert_eq!(receiver.try_recv().unwrap(), 3);
    }

    #[test]
    fn dropping_every_shared_sender_closes_the_receiver() {
        let (sender, mut receiver) = shared::<u32>(4);
        let second = sender.clone();

        std::thread::spawn(move || {
            drop(sender);
            std::thread::sleep(Duration::from_millis(20));
            drop(second);
        });

        let outcome = crate::LocalExecutor::default().run(async move { receiver.recv().await });
        assert_eq!(outcome, Err(RecvError::Closed));
    }
    use crate::{timer::Timer, LocalExecutor};
    use std::{cell::RefCell, rc::Rc, time::Duration};

    #[test]
    fn every_receiver_sees_every_value() {
        LocalExecutor::default().run(async {
            let (sender, mut first) = broadcast(4);
            let mut second = sender.subscribe();

            sender.send(1).unwrap();
            sender.send(2).unwrap();

            assert_eq!(first.recv().await.unwrap(), 1);
            assert_eq!(first.recv().await.unwrap(), 2);
            assert_eq!(second.recv().await.unwrap(), 1);
            assert_eq!(second.recv().await.unwrap(), 2);
        });
    }

    #[test]
    fn recv_waits_until_a_value_is_sent() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = broadcast(4);
            let order = Rc::new(RefCell::new(Vec::new()));

            let waiter = crate::spawn_local({
                let order = order.clone();
                async move {
                    let value = receiver.recv().await.unwrap();
                    order.borrow_mut().push("received");
                    value
                }
            })
            .detach();

            Timer::new(Duration::from_millis(10)).await;
            order.borrow_mut().push("nothing sent yet");
            sender.send(8).unwrap();

            assert_eq!(waiter.await.unwrap(), 8);
            assert_eq!(
                *order.borrow(),
                vec!["nothing sent yet", "received"],
                "recv resolved before anything was sent"
            );
        });
    }

    #[test]
    fn a_later_subscriber_sees_only_later_values() {
        LocalExecutor::default().run(async {
            let (sender, mut early) = broadcast(4);
            sender.send(1).unwrap();

            let mut late = sender.subscribe();
            sender.send(2).unwrap();

            assert_eq!(early.recv().await.unwrap(), 1);
            assert_eq!(early.recv().await.unwrap(), 2);
            assert_eq!(
                late.recv().await.unwrap(),
                2,
                "a subscriber should not see values sent before it existed"
            );
        });
    }

    #[test]
    fn overflow_reports_how_much_was_missed_and_resumes_at_the_oldest_kept() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = broadcast(2);
            for value in 1..=5 {
                sender.send(value).unwrap();
            }

            // Capacity 2, five sent: values 1, 2 and 3 are gone.
            match receiver.recv().await {
                Err(RecvError::Lagged(missed)) => assert_eq!(missed, 3),
                other => panic!("expected Lagged(3), got {other:?}"),
            }

            assert_eq!(
                receiver.recv().await.unwrap(),
                4,
                "after lagging, the cursor should land on the oldest retained value"
            );
            assert_eq!(receiver.recv().await.unwrap(), 5);
        });
    }

    #[test]
    fn dropping_the_last_sender_wakes_a_waiting_receiver() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = broadcast::<u32>(4);

            let waiter = crate::spawn_local(async move { receiver.recv().await }).detach();

            Timer::new(Duration::from_millis(10)).await;
            drop(sender);

            assert!(matches!(waiter.await.unwrap(), Err(RecvError::Closed)));
        });
    }

    #[test]
    fn retained_values_are_drained_before_closed() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = broadcast(4);
            sender.send(1).unwrap();
            sender.send(2).unwrap();
            drop(sender);

            assert_eq!(receiver.recv().await.unwrap(), 1);
            assert_eq!(receiver.recv().await.unwrap(), 2);
            assert!(matches!(receiver.recv().await, Err(RecvError::Closed)));
        });
    }

    #[test]
    fn sending_with_no_receivers_hands_the_value_back() {
        LocalExecutor::default().run(async {
            let (sender, receiver) = broadcast::<String>(4);
            drop(receiver);

            match sender.send("payload".to_string()) {
                Err(GlommioError::Closed(ResourceType::Channel(value))) => {
                    assert_eq!(value, "payload")
                }
                other => panic!("expected the value back, got {other:?}"),
            }
        });
    }

    #[test]
    fn send_reports_how_many_receivers_it_reached() {
        LocalExecutor::default().run(async {
            let (sender, _first) = broadcast(4);
            assert_eq!(sender.send(1).unwrap(), 1);
            let _second = sender.subscribe();
            assert_eq!(sender.send(2).unwrap(), 2);
            assert_eq!(sender.receiver_count(), 2);
        });
    }

    #[test]
    fn a_receiver_is_a_stream_that_surfaces_lag() {
        LocalExecutor::default().run(async {
            use futures_lite::StreamExt;

            let (sender, receiver) = broadcast(2);
            for value in 1..=4 {
                sender.send(value).unwrap();
            }
            drop(sender);

            let seen: Vec<_> = receiver.collect().await;

            // Capacity 2 of 4 sent: the stream reports the gap rather than
            // hiding it, then yields what survived, then ends.
            assert!(matches!(seen[0], Err(RecvError::Lagged(2))));
            assert_eq!(seen[1].as_ref().unwrap(), &3);
            assert_eq!(seen[2].as_ref().unwrap(), &4);
            assert_eq!(
                seen.len(),
                3,
                "the stream should end when the channel closes"
            );
        });
    }

    #[test]
    fn try_recv_tells_empty_lagged_and_closed_apart() {
        LocalExecutor::default().run(async {
            let (sender, mut receiver) = broadcast(2);
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

            for value in 1..=4 {
                sender.send(value).unwrap();
            }
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Lagged(2))));
            assert_eq!(receiver.try_recv().unwrap(), 3);
            assert_eq!(receiver.try_recv().unwrap(), 4);

            drop(sender);
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Closed)));
        });
    }
}
