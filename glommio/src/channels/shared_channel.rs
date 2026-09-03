// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//
use crate::{
    channels::spsc_queue::{make, BufferHalf, Consumer, Producer},
    enclose,
    reactor::Reactor,
    sys::{self, SleepNotifier},
    GlommioError, ResourceType,
};
use futures_lite::{future, stream::Stream};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    sync::Arc,
    task::{Context, Poll},
};

type Result<T, V> = crate::Result<T, V>;

/// The `SharedReceiver` is the receiving end of the Shared Channel.
/// It implements [`Send`] so it can be passed to any thread. However,
/// it doesn't implement any method: before it is used it must be changed
/// into a [`ConnectedReceiver`], which then makes sure it will be used by
/// at most one thread.
///
/// It is technically possible to share this among multiple threads inside
/// a lock, although such design is discouraged and beats the purpose of a
/// spsc channel.
///
/// [`ConnectedReceiver`]: struct.ConnectedReceiver.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
pub struct SharedReceiver<T: Send + Sized> {
    state: Option<Arc<ReceiverState<T>>>,
}

/// The `SharedSender` is the sending end of the Shared Channel.
/// It implements [`Send`] so it can be passed to any thread. However,
/// it doesn't implement any method: before it is used it must be changed
/// into a [`ConnectedSender`], which then makes sure it will be used by
/// at most one thread.
///
/// It is technically possible to share this among multiple threads inside
/// a lock, although such design is discouraged and beats the purpose of a
/// spsc channel.
///
/// [`ConnectedSender`]: struct.ConnectedSender.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
pub struct SharedSender<T: Send + Sized> {
    state: Option<Arc<SenderState<T>>>,
}

impl<T: Send + Sized> fmt::Debug for SharedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            Some(s) => write!(f, "Unbound SharedSender {:?}", s.buffer),
            None => write!(f, "Bound SharedSender"),
        }
    }
}

impl<T: Send + Sized> fmt::Debug for SharedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            Some(s) => write!(f, "Unbound SharedReceiver: {:?}", s.buffer),
            None => write!(f, "Bound SharedReceiver"),
        }
    }
}

/// The `ConnectedReceiver` is the receiving end of the Shared Channel.
pub struct ConnectedReceiver<T: Send + Sized> {
    id: u64,
    state: Arc<ReceiverState<T>>,
    reactor: Weak<Reactor>,
    notifier: Arc<SleepNotifier>,
}

/// The `ConnectedSender` is the sending end of the Shared Channel.
pub struct ConnectedSender<T: Send + Sized> {
    id: u64,
    state: Arc<SenderState<T>>,
    reactor: Weak<Reactor>,
    notifier: Arc<SleepNotifier>,
}

/// A sender usable from a thread with no executor.
///
/// Minted by [`ConnectedSender::into_foreign`] on the executor that connected
/// the channel, and then moved anywhere: sending is a lock-free push followed
/// by a write to the peer's eventfd, neither of which needs a reactor of its
/// own. The handle outlives the executor that minted it.
///
/// Deliberately **not** [`Clone`]. The buffer underneath is strictly
/// single-producer, so two threads holding handles to it would corrupt the
/// heap -- the bug this crate's [`spsc_queue`](super::spsc_queue) fix exists
/// to prevent. If you need several foreign producers, give each its own
/// channel, or put a lock in front of one handle.
///
/// Only [`try_send`](Self::try_send) is offered. The awaiting
/// [`ConnectedSender::send`] depends on a free-space callback registered with
/// the local reactor, and a thread with no executor has nothing to register.
pub struct ForeignSender<T: Send + Sized> {
    state: Arc<SenderState<T>>,
    notifier: Arc<SleepNotifier>,
}

impl<T: Send + Sized> fmt::Debug for ForeignSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ForeignSender {{ .. }}")
    }
}

impl<T: Send + Sized> ForeignSender<T> {
    /// Sends a value, failing rather than waiting if the channel is full.
    ///
    /// # Errors
    ///
    /// [`GlommioError::Closed`] if the receiver is gone, or
    /// [`GlommioError::WouldBlock`] if the channel is full. Either way the
    /// value is handed back inside the error.
    pub fn try_send(&self, item: T) -> Result<(), T> {
        if self.state.buffer.consumer_disconnected()
            || self.state.buffer.buffer.producer_disconnected()
        {
            return Err(GlommioError::Closed(ResourceType::Channel(item)));
        }

        match self.state.buffer.try_push(item) {
            None => {
                self.notifier.notify(false);
                Ok(())
            }
            Some(item) => {
                if self.state.buffer.consumer_disconnected()
                    || self.state.buffer.buffer.producer_disconnected()
                {
                    Err(GlommioError::Closed(ResourceType::Channel(item)))
                } else {
                    Err(GlommioError::WouldBlock(ResourceType::Channel(item)))
                }
            }
        }
    }

    /// How many values the channel can still take before it is full.
    pub fn free_space(&self) -> usize {
        self.state.buffer.free_space()
    }
}

impl<T: Send + Sized> Drop for ForeignSender<T> {
    fn drop(&mut self) {
        // No reactor registration to remove: `into_foreign` did that on the
        // executor that had one. All that remains is to tell the peer.
        if !self.state.buffer.disconnect() {
            self.notifier.notify(false);
        }
    }
}

impl<T: Send + Sized> fmt::Debug for ConnectedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connected Receiver {}: {:?}", self.id, self.state.buffer)
    }
}

impl<T: Send + Sized> fmt::Debug for ConnectedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connected Sender {} : {:?}", self.id, self.state.buffer)
    }
}

#[derive(Debug)]
struct SenderState<V: Send + Sized> {
    buffer: Producer<V>,
}

#[derive(Debug)]
struct ReceiverState<V: Send + Sized> {
    buffer: Consumer<V>,
}

struct Connector<T: BufferHalf> {
    buffer: T,
    reactor: Weak<Reactor>,
}

impl<T: BufferHalf> Connector<T> {
    fn new(buffer: T, reactor: Weak<Reactor>) -> Self {
        Self { buffer, reactor }
    }
}

impl<T: BufferHalf> Future for Connector<T> {
    type Output = Arc<SleepNotifier>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let reactor = self.reactor.upgrade().unwrap();

        match self.buffer.peer_id() {
            0 => {
                reactor.add_shared_channel_connection_waker(cx.waker().clone());
                Poll::Pending
            }
            // usize::MAX (the disconnected) always has a placeholder notifier that never
            // returns its fd. So if the other side disconnected it will unblock us here
            id => Poll::Ready(sys::get_sleep_notifier_for(id).unwrap_or_else(|| {
                // The peer registered this id and then its executor went
                // away. Those two are not ordered against each other -- the
                // executor drops its notifier on its own schedule, while the
                // id stays in the buffer until the peer's half is dropped --
                // so the surviving side can land here holding an id whose
                // notifier is already gone. Unwrapping it panicked, roughly
                // one full-suite run in five.
                //
                // An executor that no longer exists cannot be woken and
                // cannot send, which is what being disconnected means. Say so
                // in the buffer as well, or this side would go on to wait
                // forever for a peer that cannot arrive.
                self.buffer.disconnect_peer();
                sys::get_sleep_notifier_for(usize::MAX)
                    .expect("the disconnected notifier is a static and always exists")
            })),
        }
    }
}

/// Creates a new `shared_channel` returning its sender and receiver
/// endpoints.
///
/// All shared channels must be bounded.
pub fn new_bounded<T: Send + Sized>(size: usize) -> (SharedSender<T>, SharedReceiver<T>) {
    let (producer, consumer) = make(size);
    (
        SharedSender {
            state: Some(Arc::new(SenderState { buffer: producer })),
        },
        SharedReceiver {
            state: Some(Arc::new(ReceiverState { buffer: consumer })),
        },
    )
}

impl<T: 'static + Send + Sized> SharedSender<T> {
    /// Connects this sender, returning a [`ConnectedSender`] that can be used
    /// to send data into this channel
    ///
    /// [`ConnectedSender`]: struct.ConnectedSender.html
    pub async fn connect(mut self) -> ConnectedSender<T> {
        let state = self.state.take().unwrap();
        let reactor = crate::executor().reactor();
        state.buffer.connect(reactor.id());
        let id = reactor.register_shared_channel(Box::new(enclose! {(state) move || {
            if state.buffer.consumer_disconnected() {
                state.buffer.capacity()
            } else {
                state.buffer.free_space()
            }
        }}));

        let reactor = Rc::downgrade(&reactor);
        let peer = Connector::new(state.buffer.clone_internal(), reactor.clone());
        let notifier = peer.await;
        ConnectedSender {
            id,
            state,
            reactor,
            notifier,
        }
    }
}

impl<T: Send + Sized> ConnectedSender<T> {
    /// Sends data into this channel.
    ///
    /// It returns a [`GlommioError::Closed`] if the receiver is destroyed.
    /// It returns a [`GlommioError::WouldBlock`] if this is a bounded channel
    /// that has no more capacity
    ///
    /// # Examples
    /// ```
    /// use futures_lite::StreamExt;
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .name("producer")
    ///     .spawn(move || async move {
    ///         let sender = sender.connect().await;
    ///         sender.try_send(0);
    ///     })
    ///     .unwrap();
    /// let receiver = LocalExecutorBuilder::default()
    ///     .name("receiver")
    ///     .spawn(move || async move {
    ///         let mut receiver = receiver.connect().await;
    ///         receiver.next().await.unwrap();
    ///     })
    ///     .unwrap();
    /// producer.join().unwrap();
    /// receiver.join().unwrap();
    /// ```
    ///
    /// [`BrokenPipe`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.BrokenPipe
    /// [`WouldBlock`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.WouldBlock
    /// [`Other`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.Other
    /// [`GlommioError`]: ../../struct.GlommioError.html
    pub fn try_send(&self, item: T) -> Result<(), T> {
        // This is a shared channel so state can change under our noses.
        // We test if the buffer is disconnected before sending to avoid
        // sending a value that will not be received (otherwise we would only
        // receive WouldBlock when the buffer capacity fills).
        //
        // However after we try_push(), we can still fail because the buffer
        // disconnected between now and then. That's okay as all we're trying to
        // do here is prevent unnecessary sends.
        //
        // Note that we check `producer_disconnected` because:
        // (1) senders can be referenced by multiple tasks simultaneously
        // (2) senders can be closed at any time using `Self::close` (which does not
        // automatically cause the receiver / consumer to drop)
        // (3) `Self::try_send` is used by async `Self::send`, which will not
        // unblock correctly if the channel was closed in another task
        if self.state.buffer.consumer_disconnected()
            || self.state.buffer.buffer.producer_disconnected()
        {
            return Err(GlommioError::Closed(ResourceType::Channel(item)));
        }
        match self.state.buffer.try_push(item) {
            None => {
                self.notifier.notify(false);
                Ok(())
            }
            Some(item) => {
                let res = if self.state.buffer.consumer_disconnected()
                    || self.state.buffer.buffer.producer_disconnected()
                {
                    GlommioError::Closed(ResourceType::Channel(item))
                } else {
                    GlommioError::WouldBlock(ResourceType::Channel(item))
                };
                Err(res)
            }
        }
    }

    /// Sends data into this channel when it is ready to receive it
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .name("producer")
    ///     .spawn(move || async move {
    ///         let sender = sender.connect().await;
    ///         sender.send(0).await;
    ///     })
    ///     .unwrap();
    /// let receiver = LocalExecutorBuilder::default()
    ///     .name("receiver")
    ///     .spawn(move || async move {
    ///         let mut receiver = receiver.connect().await;
    ///         receiver.recv().await.unwrap();
    ///     })
    ///     .unwrap();
    /// producer.join().unwrap();
    /// receiver.join().unwrap();
    /// ```
    pub async fn send(&self, item: T) -> Result<(), T> {
        let waiter = future::poll_fn(|cx| self.wait_for_room(cx));
        waiter.await;
        let res = self.try_send(item);
        if let Err(GlommioError::WouldBlock(_)) = &res {
            panic!("operation would block")
        }
        res
    }

    fn wait_for_room(&self, cx: &Context<'_>) -> Poll<()> {
        match self.state.buffer.free_space() > 0 || self.state.buffer.producer_disconnected() {
            true => Poll::Ready(()),
            false => {
                self.reactor
                    .upgrade()
                    .unwrap()
                    .add_shared_channel_waker(self.id, cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Close the sender
    pub fn close(&self) {
        if !self.state.buffer.disconnect() {
            if let Some(r) = self.reactor.upgrade() {
                self.notifier.notify(false);
                // wake other tasks `awaiting` the same sender letting them know sender is
                // closed; we don't `unregister_shared_channel` here because
                // another task could still be `await`ing this sender, in which
                // case we need to be able to `wake` it
                r.process_shared_channels_by_id(self.id);
            }
        }
    }
}

impl<T: 'static + Send + Sized> SharedReceiver<T> {
    /// Connects this receiver, returning a [`ConnectedReceiver`] that can be
    /// used to send data into this channel
    ///
    /// [`ConnectedReceiver`]: struct.ConnectedReceiver.html
    pub async fn connect(mut self) -> ConnectedReceiver<T> {
        let reactor = crate::executor().reactor();
        let state = self.state.take().unwrap();
        state.buffer.connect(reactor.id());
        let id = reactor.register_shared_channel(Box::new(enclose! { (state) move || {
            if state.buffer.producer_disconnected() {
                state.buffer.capacity()
            } else {
                state.buffer.size()
            }
        }}));

        let reactor = Rc::downgrade(&reactor);
        let peer = Connector::new(state.buffer.clone_internal(), reactor.clone());
        let notifier = peer.await;
        ConnectedReceiver {
            id,
            state,
            reactor,
            notifier,
        }
    }
}

impl<T: Send + Sized> ConnectedReceiver<T> {
    /// Receives data from this channel
    ///
    /// If the sender is no longer available it returns [`None`]. Otherwise,
    /// blocks until an item is available and returns it wrapped in [`Some`].
    ///
    /// Notice that this is also available as a Stream. Whether to consume from
    /// a stream or `recv` is up to the application. The biggest difference
    /// is that [`StreamExt`]'s [`next`] method takes a mutable reference to
    /// self. If the LocalReceiver is, say, behind an [`Rc`] it may be more
    /// ergonomic to `recv`.
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .name("producer")
    ///     .spawn(move || async move {
    ///         let sender = sender.connect().await;
    ///         sender.try_send(0u32);
    ///     })
    ///     .unwrap();
    /// let receiver = LocalExecutorBuilder::default()
    ///     .name("receiver")
    ///     .spawn(move || async move {
    ///         let mut receiver = receiver.connect().await;
    ///         let x = receiver.recv().await.unwrap();
    ///         assert_eq!(x, 0);
    ///     })
    ///     .unwrap();
    /// producer.join().unwrap();
    /// receiver.join().unwrap();
    /// ```
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    /// [`Some`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.Some
    /// [`StreamExt`]: https://docs.rs/futures-lite/2.6.0/futures_lite/stream/index.htmll
    /// [`next`]: https://docs.rs/futures-lite/2.6.0/futures_lite/stream/trait.StreamExt.html#method.next
    /// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
    pub async fn recv(&self) -> Option<T> {
        let waiter = future::poll_fn(|cx| self.recv_one(cx));
        waiter.await
    }

    fn recv_one(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.do_recv_one(cx, false)
    }

    fn do_recv_one(&self, cx: &mut Context<'_>, disconnected: bool) -> Poll<Option<T>> {
        match self.state.buffer.try_pop() {
            None => {
                if disconnected {
                    Poll::Ready(None)
                } else if self.state.buffer.producer_disconnected() {
                    // Double check in case the producer sent the last message and
                    // disconnected right after a `None` is returned from `try_pop`
                    self.do_recv_one(cx, true)
                } else {
                    self.reactor
                        .upgrade()
                        .unwrap()
                        .add_shared_channel_waker(self.id, cx.waker().clone());
                    Poll::Pending
                }
            }
            res => {
                self.notifier.notify(false);
                Poll::Ready(res)
            }
        }
    }
}

impl<T: Send + Sized> Stream for ConnectedReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.recv_one(cx)
    }
}

impl<T: Send + Sized> Drop for SharedSender<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            // Never connected, we must connect ourselves.
            if !state.buffer.disconnect() {
                let id = state.buffer.peer_id();
                if let Some(notifier) = sys::get_sleep_notifier_for(id) {
                    notifier.notify(false);
                }
            }
        }
    }
}

impl<T: Send + Sized> Drop for SharedReceiver<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            // Never connected, we must connect ourselves.
            if !state.buffer.disconnect() {
                let id = state.buffer.peer_id();
                if let Some(notifier) = sys::get_sleep_notifier_for(id) {
                    notifier.notify(false);
                }
            }
        }
    }
}

impl<T: Send + Sized> Drop for ConnectedReceiver<T> {
    fn drop(&mut self) {
        if !self.state.buffer.disconnect() {
            if let Some(r) = self.reactor.upgrade() {
                self.notifier.notify(false);
                r.unregister_shared_channel(self.id);
            }
        }
    }
}

impl<T: Send + Sized> ConnectedSender<T> {
    /// Converts this sender into one usable from a thread with no executor.
    ///
    /// The awaiting [`send`](Self::send) is given up in the process: see
    /// [`ForeignSender`].
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .spawn(move || async move {
    ///         let foreign = sender.connect().await.into_foreign();
    ///         std::thread::spawn(move || foreign.try_send(1).unwrap())
    ///             .join()
    ///             .unwrap();
    ///     })
    ///     .unwrap();
    ///
    /// let consumer = LocalExecutorBuilder::default()
    ///     .spawn(move || async move {
    ///         let receiver = receiver.connect().await;
    ///         assert_eq!(receiver.recv().await.unwrap(), 1);
    ///     })
    ///     .unwrap();
    ///
    /// producer.join().unwrap();
    /// consumer.join().unwrap();
    /// ```
    pub fn into_foreign(self) -> ForeignSender<T> {
        // The local reactor tracked this channel's free space so it could
        // decide whether to sleep. Nothing on this executor will send again,
        // so that registration goes now rather than at drop -- which is also
        // the last moment a reactor is in reach.
        if let Some(reactor) = self.reactor.upgrade() {
            reactor.unregister_shared_channel(self.id);
        }

        let foreign = ForeignSender {
            state: self.state.clone(),
            notifier: self.notifier.clone(),
        };

        // `ConnectedSender::drop` disconnects the buffer, which would close
        // the channel we are handing on.
        std::mem::forget(self);

        foreign
    }
}

impl<T: Send + Sized> Drop for ConnectedSender<T> {
    fn drop(&mut self) {
        if !self.state.buffer.disconnect() {
            if let Some(r) = self.reactor.upgrade() {
                self.notifier.notify(false);
                r.unregister_shared_channel(self.id)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        timer::{sleep, Timer},
        LocalExecutorBuilder, Placement,
    };
    use futures_lite::{FutureExt, StreamExt};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Arc,
        },
        time::Duration,
    };

    #[test]
    fn producer_consumer() {
        let (sender, receiver) = new_bounded(10);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                Timer::new(Duration::from_millis(10)).await;
                sender.try_send(100).unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await;
                assert_eq!(x.unwrap(), 100);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn producer_stream_consumer() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spin_before_park(Duration::from_millis(1000000))
            .spawn(move || async move {
                let sender = sender.connect().await;
                for _ in 0..10 {
                    sender.send(1).await.unwrap();
                    Timer::new(Duration::from_millis(1)).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spin_before_park(Duration::from_millis(1000000))
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn consumer_sleeps_before_producer_produces() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                Timer::new(Duration::from_millis(100)).await;
                let sender = sender.connect().await;
                sender.send(1).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let recv = receiver.recv().await.unwrap();
                assert_eq!(recv, 1);
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 0);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn producer_sleeps_before_consumer_consumes() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                // This will go right away because the channel fits 1 element
                sender.try_send(1).unwrap();
                // This will sleep. The consumer should unblock us
                sender.send(1).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                Timer::new(Duration::from_millis(100)).await;
                let receiver = receiver.connect().await;
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 2);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn producer_never_connects() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                drop(sender);
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver: ConnectedReceiver<usize> = receiver.connect().await;
                assert!(receiver.recv().await.is_none());
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn destroy_with_pending_wakers() {
        let (sender, receiver) = new_bounded::<u8>(1);

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect();
                let sender = sender.connect();
                let (receiver, sender) = futures::future::join(receiver, sender).await;

                future::poll_fn(move |cx| {
                    let mut f1 = receiver.recv().boxed_local();
                    assert_eq!(f1.poll(cx), Poll::Pending);
                    assert!(sender.try_send(1).is_ok());
                    let r = receiver.recv_one(cx);
                    assert_eq!(r, Poll::Ready(Some(1)));
                    r
                })
                .await;
                sleep(Duration::from_secs(1)).await;
            })
            .unwrap();

        ex2.join().unwrap();
    }

    #[test]
    fn consumer_never_connects() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                drop(receiver);
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                Timer::new(Duration::from_millis(100)).await;
                let sender: ConnectedSender<usize> = sender.connect().await;
                match sender.send(0).await {
                    Ok(_) => panic!("Should not have sent"),
                    Err(GlommioError::Closed(ResourceType::Channel(_))) => {
                        // all good
                    }
                    Err(other_err) => {
                        panic!("incorrect error type: '{other_err}' for channel send")
                    }
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn pass_function() {
        let (sender, receiver) = new_bounded(10);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                Timer::new(Duration::from_millis(10)).await;
                if sender.send(|| 32).await.is_err() {
                    panic!("send failed");
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await.unwrap();
                assert_eq!(32, x());
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn send_to_full_channel() {
        let (sender, receiver) = new_bounded(1);

        let status = Arc::new(AtomicUsize::new(0));
        let s1 = status.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                sender.send(0).await.unwrap();
                let x = sender.try_send(1);
                assert!(x.is_err());
                s1.store(1, Ordering::Relaxed);
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;

                while status.load(Ordering::Relaxed) == 0 {}
                let x = receiver.recv().await.unwrap();
                assert_eq!(0, x);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn non_copy_shared() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                let string1 = "Some string data here..".to_string();
                sender.send(string1).await.unwrap();
                let string2 = "different data..".to_string();
                sender.send(string2).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await.unwrap();
                assert_eq!(x, "Some string data here..".to_string());
                let y = receiver.recv().await.unwrap();
                assert_eq!(y, "different data..".to_string());
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn copy_shared() {
        let (sender, receiver) = new_bounded(2);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                sender.send(100usize).await.unwrap();
                sender.send(200usize).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await.unwrap();
                let y = receiver.recv().await.unwrap();
                assert_eq!(x, 100usize);
                assert_eq!(y, 200usize);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[derive(Debug)]
    struct WithDrop(Arc<AtomicUsize>, usize);

    impl Drop for WithDrop {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn shared_drop_gets_called() {
        let (sender, receiver) = new_bounded(1000);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..1000 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let y = receiver.recv().await.unwrap();
                drop(y);
                Timer::new(Duration::from_secs(1)).await;
                let y = receiver.recv().await.unwrap();
                drop(y);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn shared_drop_gets_called_reversed() {
        let (sender, receiver) = new_bounded(100);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..110 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let y = receiver.recv().await.unwrap();
                drop(y);
                let y = receiver.recv().await.unwrap();
                drop(y);
            })
            .unwrap();

        ex2.join().unwrap();
        ex1.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn shared_drop_cascade_drop_executor() {
        let (sender, receiver) = new_bounded(100);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..50 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let _resp = receiver.recv().await.unwrap();
            })
            .unwrap();
        ex2.join().unwrap();
        ex1.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn shared_drop_cascade_drop_executor_reverse() {
        let (sender, receiver) = new_bounded(100);
        let (connected_tx, connected_rx) = mpsc::channel();

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();
        let sender_connected = connected_tx.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                sender_connected.send(()).unwrap();
                for x in 0..50 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::SeqCst);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                connected_tx.send(()).unwrap();
                for x in 0..50 {
                    let resp = receiver.recv().await.unwrap();
                    assert_eq!(x, resp.1);
                }
            })
            .unwrap();

        // Both endpoints must be connected before destroying the sender's
        // executor. Otherwise the receiver can observe its peer ID after the
        // sender's reactor has already removed the corresponding notifier.
        connected_rx.recv().unwrap();
        connected_rx.recv().unwrap();
        drop(ex1);
        ex2.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn close_sender_while_blocked_on_send() {
        use std::sync::{Condvar, Mutex};

        let (sender, receiver) = new_bounded(10);
        let cv_mtx_1 = Arc::new((Condvar::new(), Mutex::new(0)));
        let cv_mtx_2 = Arc::clone(&cv_mtx_1);
        let cv_mtx_3 = Arc::clone(&cv_mtx_1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn({
                move || async move {
                    let s1 = Rc::new(sender.connect().await);
                    let s2 = Rc::clone(&s1);
                    let t1 = crate::executor().spawn_local(async move {
                        let mut ii = 0;
                        while s1.try_send(ii).is_ok() {
                            ii += 1;
                        }
                        s1.close();
                        *cv_mtx_1.1.lock().unwrap() = 1;
                        cv_mtx_1.0.notify_all();
                    });
                    #[allow(clippy::await_holding_lock)]
                    let t2 = crate::executor().spawn_local(async move {
                        let mut lck = cv_mtx_2
                            .0
                            .wait_while(cv_mtx_2.1.lock().unwrap(), |l| *l < 1)
                            .unwrap();
                        assert!(s2.send(-1).await.is_err());
                        *lck = 2;
                        cv_mtx_2.0.notify_all();
                    });
                    t1.await;
                    t2.await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                {
                    let _lck = cv_mtx_3
                        .0
                        .wait_while(cv_mtx_3.1.lock().unwrap(), |l| *l < 2)
                        .unwrap();
                };
                while let Some(v) = receiver.recv().await {
                    assert!(0 <= v);
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }
}

#[cfg(test)]
mod foreign_tests {
    use super::*;
    use crate::{LocalExecutorBuilder, Placement};
    use futures_lite::StreamExt;

    #[test]
    fn a_peer_whose_executor_is_gone_reads_as_disconnected() {
        // The race this stands in for: a peer connects, registering its
        // executor id in the buffer, and then that executor is destroyed. The
        // id and the notifier are not dropped together -- the executor drops
        // its notifier on its own schedule while the id sits in the buffer
        // until the peer's half is dropped -- so the surviving side can look
        // up an id whose notifier has already gone. It panicked on the
        // `unwrap`, about one full-suite run in five.
        //
        // Reproducing the window itself is timing; the state it produces is
        // not. An id no executor ever registered resolves to `None` from
        // `get_sleep_notifier_for` exactly as a dropped one does, so
        // connecting against it exercises the same path deterministically.
        const NEVER_REGISTERED: usize = usize::MAX - 4096;

        crate::LocalExecutor::default().run(async {
            let (sender, receiver) = new_bounded::<u32>(4);

            // Stand in for the departed peer: its id is in the buffer, and
            // there is no notifier behind it.
            assert!(
                sys::get_sleep_notifier_for(NEVER_REGISTERED).is_none(),
                "this id must have no notifier, or the test proves nothing"
            );
            sender
                .state
                .as_ref()
                .unwrap()
                .buffer
                .connect(NEVER_REGISTERED);

            let receiver = receiver.connect().await;

            assert!(
                receiver.state.buffer.producer_disconnected(),
                "a peer that cannot be woken has to read as disconnected, or \
                 this side waits forever for something that cannot arrive"
            );
            assert_eq!(
                receiver.recv().await,
                None,
                "reads must end rather than hang"
            );
        });
    }

    #[test]
    fn a_foreign_thread_can_send_into_an_executor() {
        let (sender, receiver) = new_bounded(16);

        let consumer = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let mut receiver = receiver.connect().await;
                let mut seen = Vec::new();
                while let Some(value) = receiver.next().await {
                    seen.push(value);
                }
                seen
            })
            .unwrap();

        // The handle is minted inside an executor and then leaves it: this
        // channel carries it back out to the test thread.
        let (handoff, collect) = std::sync::mpsc::channel();
        let producer = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let sender = sender.connect().await;
                handoff.send(sender.into_foreign()).unwrap();
            })
            .unwrap();

        let foreign = collect.recv().unwrap();
        // The executor that minted the handle is gone by now, which is the
        // point: the handle depends on the peer's notifier, not its own.
        producer.join().unwrap();

        for value in 1..=3u32 {
            foreign.try_send(value).unwrap();
        }
        drop(foreign);

        assert_eq!(consumer.join().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn a_parked_executor_is_woken_by_a_foreign_send() {
        let (sender, receiver) = new_bounded(4);

        let consumer = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let mut receiver = receiver.connect().await;
                let value = receiver.next().await;
                (value, std::time::Instant::now())
            })
            .unwrap();

        let (handoff, collect) = std::sync::mpsc::channel();
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let sender = sender.connect().await;
                handoff.send(sender.into_foreign()).unwrap();
            })
            .unwrap()
            .join()
            .unwrap();

        let foreign = collect.recv().unwrap();

        // Long enough that the consumer has run out of work and parked in the
        // kernel. Waking it is the eventfd's job, and nothing else will do it.
        std::thread::sleep(std::time::Duration::from_millis(300));
        let sent_at = std::time::Instant::now();
        foreign.try_send(1u32).unwrap();

        let (value, received_at) = consumer.join().unwrap();
        assert_eq!(value, Some(1));
        let delay = received_at.duration_since(sent_at);
        assert!(
            delay < std::time::Duration::from_millis(100),
            "a parked executor took {delay:?} to see a foreign send: it was not woken, \
             it noticed on its own schedule"
        );
    }

    #[test]
    fn a_full_channel_hands_the_value_back() {
        let (sender, receiver) = new_bounded(1);

        let (handoff, collect) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let consumer = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let mut receiver = receiver.connect().await;
                // Hold the channel full until the test says otherwise.
                wait.recv().unwrap();
                let mut seen = Vec::new();
                while let Some(value) = receiver.next().await {
                    seen.push(value);
                }
                seen
            })
            .unwrap();

        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let sender = sender.connect().await;
                handoff.send(sender.into_foreign()).unwrap();
            })
            .unwrap()
            .join()
            .unwrap();

        let foreign = collect.recv().unwrap();
        foreign.try_send(1u32).unwrap();

        // Capacity is one and nothing has been consumed yet.
        let mut rejected = None;
        for _ in 0..100 {
            if let Err(GlommioError::Closed(ResourceType::Channel(value)))
            | Err(GlommioError::WouldBlock(ResourceType::Channel(value))) =
                foreign.try_send(2u32)
            {
                rejected = Some(value);
                break;
            }
        }
        assert_eq!(
            rejected,
            Some(2),
            "a full channel should hand the value back"
        );

        release.send(()).unwrap();
        drop(foreign);
        assert!(!consumer.join().unwrap().is_empty());
    }

    #[test]
    fn dropping_the_foreign_sender_closes_the_channel() {
        let (sender, receiver) = new_bounded(4);

        let consumer = LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let mut receiver = receiver.connect().await;
                let mut count = 0;
                while receiver.next().await.is_some() {
                    count += 1;
                }
                count
            })
            .unwrap();

        let (handoff, collect) = std::sync::mpsc::channel();
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(move || async move {
                let sender = sender.connect().await;
                handoff.send(sender.into_foreign()).unwrap();
            })
            .unwrap()
            .join()
            .unwrap();

        let foreign = collect.recv().unwrap();
        foreign.try_send(1u32).unwrap();
        drop(foreign);

        assert_eq!(
            consumer.join().unwrap(),
            1,
            "the receiver should finish once the foreign sender is gone"
        );
    }
}
