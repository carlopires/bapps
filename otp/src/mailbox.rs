//! Shard-local bounded mailboxes plus the supervisor's private exit mailbox.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    fmt,
    marker::PhantomData,
    rc::Rc,
};

use bapps_trio::{CancelScope, Cancelled, sync::Condition, with_cancel_scope};

use crate::{ExitReason, types::MailboxSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MailboxError {
    Closed,
    Cancelled(Cancelled),
}

impl fmt::Display for MailboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "mailbox is closed"),
            Self::Cancelled(cancelled) => write!(f, "mailbox operation {cancelled}"),
        }
    }
}

impl std::error::Error for MailboxError {}

#[derive(Debug)]
pub enum TrySendError<T> {
    Full(T),
    Closed(T),
}

struct LocalInner<T> {
    name: String,
    queue: RefCell<VecDeque<T>>,
    capacity: usize,
    closed: Cell<bool>,
    senders: Cell<usize>,
    /// Slots promised to outstanding [`SendPermit`]s.
    reserved: Cell<usize>,
    not_empty: Condition,
    not_full: Condition,
}

impl<T> LocalInner<T> {
    fn has_room(&self) -> bool {
        self.queue.borrow().len() + self.reserved.get() < self.capacity
    }
}

/// Namespace/type marker for constructing a bounded shard-local mailbox.
///
/// The sender is cloneable; the receiver is single-owner. Waiting sends and
/// receives are cancellation-aware through `bapps_trio`.
pub struct LocalMailbox<T>(PhantomData<fn(T)>);

pub struct LocalSender<T> {
    inner: Rc<LocalInner<T>>,
}

pub struct LocalReceiver<T> {
    inner: Rc<LocalInner<T>>,
}

#[derive(Clone)]
pub(crate) struct MailboxStats {
    snapshot: Rc<dyn Fn() -> MailboxSnapshot>,
}

impl MailboxStats {
    pub(crate) fn snapshot(&self) -> MailboxSnapshot {
        (self.snapshot)()
    }
}

impl<T: 'static> LocalMailbox<T> {
    pub fn bounded(capacity: usize) -> (LocalSender<T>, LocalReceiver<T>) {
        Self::bounded_named("mailbox", capacity)
    }

    pub(crate) fn bounded_named(
        name: impl Into<String>,
        capacity: usize,
    ) -> (LocalSender<T>, LocalReceiver<T>) {
        assert!(capacity > 0, "mailbox capacity must be non-zero");
        let inner = Rc::new(LocalInner {
            name: name.into(),
            queue: RefCell::new(VecDeque::new()),
            capacity,
            closed: Cell::new(false),
            senders: Cell::new(1),
            reserved: Cell::new(0),
            not_empty: Condition::new(),
            not_full: Condition::new(),
        });
        (
            LocalSender {
                inner: inner.clone(),
            },
            LocalReceiver { inner },
        )
    }
}

impl<T> Clone for LocalSender<T> {
    fn clone(&self) -> Self {
        self.inner
            .senders
            .set(self.inner.senders.get().saturating_add(1));
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: 'static> LocalSender<T> {
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn depth(&self) -> usize {
        self.inner.queue.borrow().len()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.get()
    }

    pub fn snapshot(&self) -> MailboxSnapshot {
        snapshot_inner(&self.inner)
    }

    pub fn close(&self) {
        close_inner(&self.inner, false);
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.inner.closed.get() {
            return Err(TrySendError::Closed(value));
        }
        if !self.inner.has_room() {
            return Err(TrySendError::Full(value));
        }
        self.inner.queue.borrow_mut().push_back(value);
        self.inner.not_empty.notify_all();
        Ok(())
    }

    /// Send using the currently active Trio scope, if one exists.
    pub async fn send(&self, value: T) -> Result<(), MailboxError> {
        self.send_current(value).await
    }

    /// Send while explicitly binding cancellation to `scope`.
    pub async fn send_in(&self, scope: &CancelScope, value: T) -> Result<(), MailboxError> {
        with_cancel_scope(scope.clone(), self.send_current(value)).await
    }

    async fn send_current(&self, value: T) -> Result<(), MailboxError> {
        self.reserve_current()
            .await?
            .send(value)
            .map_err(|_| MailboxError::Closed)
    }

    /// Reserve one slot, waiting for capacity, bound to `scope`.
    ///
    /// Reserve *before* building a message that carries something that must
    /// not be lost (a reply channel, a reservation, a claim): once reserved,
    /// [`SendPermit::send`] cannot block and fails only if the receiver is
    /// gone, returning the value. Cancellation while waiting loses nothing,
    /// because nothing was built yet. Dropping an unused permit releases the
    /// slot.
    pub async fn reserve_in(&self, scope: &CancelScope) -> Result<SendPermit<T>, MailboxError> {
        with_cancel_scope(scope.clone(), self.reserve_current()).await
    }

    async fn reserve_current(&self) -> Result<SendPermit<T>, MailboxError> {
        loop {
            if let Some(scope) = bapps_trio::current_cancel_scope() {
                scope.check_cancelled().map_err(MailboxError::Cancelled)?;
            }
            if self.inner.closed.get() {
                return Err(MailboxError::Closed);
            }
            if self.inner.has_room() {
                self.inner.reserved.set(self.inner.reserved.get() + 1);
                return Ok(SendPermit {
                    inner: Some(self.inner.clone()),
                });
            }
            let observed = self.inner.not_full.generation();
            if self.inner.has_room() {
                continue;
            }
            self.inner
                .not_full
                .wait_for_change(observed)
                .await
                .map_err(MailboxError::Cancelled)?;
        }
    }

    pub(crate) fn stats(&self) -> MailboxStats {
        let weak = Rc::downgrade(&self.inner);
        let name = self.inner.name.clone();
        let capacity = self.inner.capacity;
        MailboxStats {
            snapshot: Rc::new(move || {
                weak.upgrade().map_or_else(
                    || MailboxSnapshot {
                        name: name.clone(),
                        depth: 0,
                        capacity,
                        closed: true,
                        senders: 0,
                        reserved: 0,
                    },
                    |inner| snapshot_inner(&inner),
                )
            }),
        }
    }
}

impl<T> Drop for LocalSender<T> {
    fn drop(&mut self) {
        let remaining = self.inner.senders.get().saturating_sub(1);
        self.inner.senders.set(remaining);
        if remaining == 0 {
            close_inner(&self.inner, false);
        }
    }
}

impl<T: 'static> LocalReceiver<T> {
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn depth(&self) -> usize {
        self.inner.queue.borrow().len()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.get()
    }

    pub fn snapshot(&self) -> MailboxSnapshot {
        snapshot_inner(&self.inner)
    }

    pub fn close(&self) {
        close_inner(&self.inner, true);
    }

    pub fn try_recv(&self) -> Result<Option<T>, MailboxError> {
        let value = self.inner.queue.borrow_mut().pop_front();
        if value.is_some() {
            self.inner.not_full.notify_all();
            return Ok(value);
        }
        if self.inner.closed.get() {
            Err(MailboxError::Closed)
        } else {
            Ok(None)
        }
    }

    /// Receive using the currently active Trio scope, if one exists.
    pub async fn recv(&self) -> Result<T, MailboxError> {
        self.recv_current().await
    }

    /// Receive while explicitly binding cancellation to `scope`.
    pub async fn recv_in(&self, scope: &CancelScope) -> Result<T, MailboxError> {
        with_cancel_scope(scope.clone(), self.recv_current()).await
    }

    async fn recv_current(&self) -> Result<T, MailboxError> {
        loop {
            if let Some(scope) = bapps_trio::current_cancel_scope() {
                scope.check_cancelled().map_err(MailboxError::Cancelled)?;
            }
            match self.try_recv() {
                Ok(Some(value)) => return Ok(value),
                Err(MailboxError::Closed) => return Err(MailboxError::Closed),
                Err(error) => return Err(error),
                Ok(None) => {}
            }

            let observed = self.inner.not_empty.generation();
            if !self.inner.queue.borrow().is_empty() {
                continue;
            }
            if self.inner.closed.get() {
                return Err(MailboxError::Closed);
            }
            self.inner
                .not_empty
                .wait_for_change(observed)
                .await
                .map_err(MailboxError::Cancelled)?;
        }
    }
}

impl<T> Drop for LocalReceiver<T> {
    fn drop(&mut self) {
        close_inner(&self.inner, true);
    }
}

fn close_inner<T>(inner: &LocalInner<T>, clear: bool) {
    inner.closed.set(true);
    if clear {
        inner.queue.borrow_mut().clear();
    }
    inner.not_empty.notify_all();
    inner.not_full.notify_all();
}

fn snapshot_inner<T>(inner: &LocalInner<T>) -> MailboxSnapshot {
    MailboxSnapshot {
        name: inner.name.clone(),
        depth: inner.queue.borrow().len(),
        capacity: inner.capacity,
        closed: inner.closed.get(),
        senders: inner.senders.get(),
        reserved: inner.reserved.get(),
    }
}

/// One reserved mailbox slot. See [`LocalSender::reserve_in`].
#[must_use = "send a value or drop the permit to release the slot"]
pub struct SendPermit<T> {
    inner: Option<Rc<LocalInner<T>>>,
}

impl<T> SendPermit<T> {
    /// Deliver `value` into the reserved slot. Never waits. Returns the value
    /// if the receiver has closed the mailbox.
    pub fn send(mut self, value: T) -> Result<(), T> {
        let inner = self.inner.take().expect("permit used once");
        inner.reserved.set(inner.reserved.get() - 1);
        if inner.closed.get() {
            inner.not_full.notify_all();
            return Err(value);
        }
        inner.queue.borrow_mut().push_back(value);
        inner.not_empty.notify_all();
        Ok(())
    }
}

impl<T> Drop for SendPermit<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.reserved.set(inner.reserved.get() - 1);
            inner.not_full.notify_all();
        }
    }
}

// --- Private supervisor exit mailbox ---------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct ChildExit {
    pub index: usize,
    pub generation: u64,
    pub reason: ExitReason,
}

#[derive(Clone, Default)]
pub(crate) struct ExitMailbox {
    queue: Rc<RefCell<VecDeque<ChildExit>>>,
    changed: Condition,
}

impl ExitMailbox {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, exit: ChildExit) {
        self.queue.borrow_mut().push_back(exit);
        self.changed.notify_all();
    }

    pub fn take(&self, index: usize, generation: u64) -> Option<ChildExit> {
        let position = self
            .queue
            .borrow()
            .iter()
            .position(|event| event.index == index && event.generation == generation)?;
        self.queue.borrow_mut().remove(position)
    }

    pub fn pop(&self) -> Option<ChildExit> {
        self.queue.borrow_mut().pop_front()
    }

    pub async fn wait_for(&self, index: usize, generation: u64) -> Result<ChildExit, Cancelled> {
        loop {
            if let Some(exit) = self.take(index, generation) {
                return Ok(exit);
            }
            let observed = self.changed.generation();
            if let Some(exit) = self.take(index, generation) {
                return Ok(exit);
            }
            self.changed.wait_for_change(observed).await?;
        }
    }

    pub async fn recv(&self) -> Result<ChildExit, Cancelled> {
        loop {
            if let Some(exit) = self.pop() {
                return Ok(exit);
            }

            let observed = self.changed.generation();
            if let Some(exit) = self.pop() {
                return Ok(exit);
            }
            self.changed.wait_for_change(observed).await?;
        }
    }

    pub async fn recv_or_cancel(
        &self,
        shutdown: &CancelScope,
    ) -> Result<Option<ChildExit>, Cancelled> {
        use std::{future::Future, future::poll_fn, task::Poll};

        if shutdown.is_cancelled() {
            return Ok(None);
        }
        if let Some(exit) = self.pop() {
            return Ok(Some(exit));
        }

        let mut recv = Box::pin(self.recv());
        let mut cancelled = Box::pin(shutdown.cancelled());

        poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(None));
            }
            if let Poll::Ready(exit) = recv.as_mut().poll(cx) {
                return Poll::Ready(exit.map(Some));
            }
            Poll::Pending
        })
        .await
    }
}
