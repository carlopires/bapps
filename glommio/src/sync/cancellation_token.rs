//! Hierarchical cancellation for task trees on one executor.
//!
//! A token cancels itself and everything below it, never upwards, so a request
//! can cancel its own sub-tasks without disturbing the connection it belongs
//! to.
//!
//! # Scope
//!
//! This token stays on the executor that created it: it is `!Send`, and so it
//! cannot itself carry a shutdown signal between shards. In a thread-per-core
//! program the shape that does work is one root token per executor, each
//! cancelled locally when a shared signal arrives:
//!
//! ```ignore
//! // on every shard
//! while shutdown.recv().await.is_some() {
//!     root.cancel();
//! }
//! ```
//!
//! # Examples
//!
//! ```
//! use glommio::{sync::CancellationToken, LocalExecutor};
//!
//! let ex = LocalExecutor::default();
//! ex.run(async {
//!     let root = CancellationToken::new();
//!     let request = root.child_token();
//!
//!     root.cancel();
//!
//!     assert!(request.is_cancelled());
//!     request.cancelled().await;
//! });
//! ```

use crate::wakers::{PendingWakes, WakerList};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

/// Waker registrations that can be withdrawn again.
///
/// A plain list is the wrong shape for a token that is watched in a loop.
/// `select!`ing on `cancelled()` each time round builds a fresh future, polls
/// it once and drops it, so an append-only list gains a clone of the same
/// waker per iteration and never loses one: memory grows with the loop, and
/// cancelling then wakes a single task once per iteration it ever ran.
///
/// Keys stay valid only until the token is cancelled, which is terminal:
/// afterwards `Cancelled::poll` returns without registering, so nothing new
/// takes a key that a live future still holds.
#[derive(Debug, Default)]
struct Registrations {
    slots: Vec<Option<Waker>>,
    free: Vec<usize>,
}

impl Registrations {
    fn register(&mut self, waker: Waker) -> usize {
        match self.free.pop() {
            Some(key) => {
                self.slots[key] = Some(waker);
                key
            }
            None => {
                self.slots.push(Some(waker));
                self.slots.len() - 1
            }
        }
    }

    /// Points an existing registration at `waker`, unless it already wakes the
    /// same task. Re-polling a future is not a new registration.
    fn refresh(&mut self, key: usize, waker: &Waker) {
        if let Some(slot) = self.slots.get_mut(key) {
            match slot {
                Some(existing) if existing.will_wake(waker) => {}
                slot => *slot = Some(waker.clone()),
            }
        }
    }

    fn withdraw(&mut self, key: usize) {
        if let Some(slot) = self.slots.get_mut(key) {
            if slot.take().is_some() {
                self.free.push(key);
            }
        }
    }

    #[cfg(test)]
    fn registered(&self) -> usize {
        self.slots.iter().flatten().count()
    }

    fn take(&mut self) -> PendingWakes {
        self.free.clear();
        let mut list = WakerList::new();
        for waker in self.slots.drain(..).flatten() {
            list.push(waker);
        }
        list.take()
    }
}

#[derive(Debug, Default)]
struct Node {
    cancelled: Cell<bool>,
    wakers: RefCell<Registrations>,
    /// Weak, so a child that outlives its parent keeps working rather than
    /// keeping the parent alive.
    children: RefCell<Vec<Weak<Node>>>,
    /// Held strongly. A foreign handle keeps its own `Arc`, so the state
    /// survives this node either way; holding it here is what lets `cancel`
    /// and `drop` reach across.
    foreign: RefCell<Vec<Arc<ForeignState>>>,
}

/// The state a [`ForeignCancellation`] shares with the token it came from.
///
/// Deliberately reachable without the origin: once the last origin `Rc` is
/// gone, nothing could ever set this flag again, so the far side must be able
/// to read "cancelled" out of an `Arc` it owns rather than chase a reference
/// back to something that no longer exists.
#[derive(Debug, Default)]
struct ForeignState {
    cancelled: AtomicBool,
    wakers: Mutex<Registrations>,
}

impl ForeignState {
    fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }

        // Taken under the lock, woken outside it: a woken poller would
        // otherwise block on the lock immediately.
        let pending = self.wakers.lock().unwrap().take();
        pending.wake();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl Node {
    fn cancel(&self) {
        if self.cancelled.replace(true) {
            // Already cancelled: the tree below has been dealt with.
            return;
        }

        let pending = self.wakers.borrow_mut().take();
        pending.wake();

        // Children that have been dropped simply fail to upgrade, which also
        // prunes them from the list.
        let children = std::mem::take(&mut *self.children.borrow_mut());
        for child in children {
            if let Some(child) = child.upgrade() {
                child.cancel();
            }
        }

        for state in std::mem::take(&mut *self.foreign.borrow_mut()) {
            state.cancel();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Unreachable is indistinguishable from cancelled, seen from another
        // core: no handle to cancel this token through survived the crossing,
        // so anything still waiting on it would wait forever.
        //
        // Note the asymmetry with local children, which outlive a dropped
        // parent uncancelled -- they can still be cancelled through their own
        // handle, and a foreign one cannot.
        for state in self.foreign.borrow().iter() {
            state.cancel();
        }
    }
}

/// A token that can be cancelled, cancelling every token derived from it.
///
/// Cloning gives another handle to the same token, not a child: cancelling
/// either cancels both.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    node: Rc<Node>,
}

impl CancellationToken {
    /// Creates a token with no parent.
    pub fn new() -> Self {
        CancellationToken {
            node: Rc::new(Node::default()),
        }
    }

    /// Creates a token cancelled when this one is, or immediately if this one
    /// is already cancelled.
    ///
    /// The immediate case matters: a task that subscribes just after shutdown
    /// has begun must not wait forever.
    pub fn child_token(&self) -> Self {
        let child = Rc::new(Node {
            cancelled: Cell::new(self.node.cancelled.get()),
            wakers: RefCell::new(Registrations::default()),
            children: RefCell::new(Vec::new()),
            foreign: RefCell::new(Vec::new()),
        });

        if !child.cancelled.get() {
            self.node.children.borrow_mut().push(Rc::downgrade(&child));
        }

        CancellationToken { node: child }
    }

    /// Creates a handle that can cross to another executor.
    ///
    /// The token itself is `!Send` and stays that way. This hands back
    /// something that is `Send + Clone`, which the destination turns back into
    /// an ordinary token with [`ForeignCancellation::attach`]. Clone it once
    /// per task you are sending across.
    ///
    /// ```
    /// use glommio::{sync::CancellationToken, LocalExecutorBuilder, Placement};
    ///
    /// let token = CancellationToken::new();
    /// let remote = token.foreign_child();
    ///
    /// let worker = LocalExecutorBuilder::new(Placement::Unbound)
    ///     .spawn(move || async move {
    ///         let shutdown = remote.attach();
    ///         shutdown.cancelled().await;
    ///     })
    ///     .unwrap();
    ///
    /// token.cancel();
    /// worker.join().unwrap();
    /// ```
    pub fn foreign_child(&self) -> ForeignCancellation {
        let state = Arc::new(ForeignState {
            cancelled: AtomicBool::new(self.node.cancelled.get()),
            wakers: Mutex::new(Registrations::default()),
        });

        if !state.is_cancelled() {
            self.node.foreign.borrow_mut().push(state.clone());
        }

        ForeignCancellation { state }
    }

    /// Cancels this token and every token derived from it.
    ///
    /// Cancelling twice is harmless, and cancellation never travels upwards.
    pub fn cancel(&self) {
        self.node.cancel();
    }

    /// Returns whether this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.node.cancelled.get()
    }

    /// Waits until this token is cancelled, returning immediately if it
    /// already has been.
    ///
    /// Dropping the returned future is safe: it leaves behind a waker that is
    /// discarded when cancellation eventually drains the list.
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            registration: None,
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        CancellationToken::new()
    }
}

/// The future returned by [`CancellationToken::cancelled`].
#[derive(Debug)]
pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    /// Where this future's waker sits, once it has parked. Withdrawn on drop,
    /// so a future that is polled and dropped without firing, which is what
    /// every turn of a `select!` loop does, leaves nothing behind.
    registration: Option<usize>,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }

        let this = self.get_mut();
        let mut wakers = this.token.node.wakers.borrow_mut();
        match this.registration {
            // Already parked: point the existing registration at the current
            // waker rather than adding another.
            Some(key) => wakers.refresh(key, cx.waker()),
            None => this.registration = Some(wakers.register(cx.waker().clone())),
        }
        Poll::Pending
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        // Nothing to withdraw once cancelled: the list was drained and the
        // keys with it, and no new registration can follow.
        if let Some(key) = self.registration {
            if !self.token.is_cancelled() {
                self.token.node.wakers.borrow_mut().withdraw(key);
            }
        }
    }
}

/// A [`CancellationToken`] handle that can be moved to another executor.
///
/// Obtained from [`CancellationToken::foreign_child`] and turned back into an
/// ordinary token by [`attach`](Self::attach). Cancelling the origin cancels
/// every token attached from this handle or its clones.
///
/// # Dropping the origin cancels
///
/// This differs from local tokens on purpose, and it matters.
///
/// Dropping a local parent leaves its children alive and uncancelled: they
/// hold their own `Rc` and can still be cancelled through their own handle.
///
/// Dropping the *origin* of a `ForeignCancellation` cancels everything
/// attached from it, because no handle to cancel it through survived the
/// crossing. Unreachable and cancelled are the same thing from the far side,
/// and the alternative is a task that waits for something that can no longer
/// happen.
///
/// This is what makes the common shutdown sequence safe:
///
/// ```ignore
/// if let Some(job) = running.remove(&id) {
///     job.shutdown.cancel();
/// }   // the origin token drops here, perhaps before a far core has attached
/// ```
///
/// Attaching after either the cancel or the drop gives an already-cancelled
/// token, never a hang.
///
/// # Timing
///
/// Cancellation is asynchronous across the boundary: `cancel()` returns
/// immediately, and the attached token becomes cancelled once its own executor
/// polls the task watching for it.
#[derive(Debug, Clone)]
pub struct ForeignCancellation {
    state: Arc<ForeignState>,
}

impl ForeignCancellation {
    /// Turns this handle back into an ordinary [`CancellationToken`].
    ///
    /// The result behaves like any other token -- [`child_token`] works,
    /// [`cancelled`] works in a `select!` arm -- and nothing downstream of it
    /// need know another core exists.
    ///
    /// Returns an already-cancelled token if the origin has cancelled, or has
    /// been dropped. Otherwise it spawns one detached task on this executor
    /// that waits for the origin and cancels the returned token.
    ///
    /// # Panics
    ///
    /// Panics if called outside an executor, since it spawns.
    ///
    /// [`child_token`]: CancellationToken::child_token
    /// [`cancelled`]: CancellationToken::cancelled
    pub fn attach(&self) -> CancellationToken {
        let token = CancellationToken::new();

        if self.state.is_cancelled() {
            token.cancel();
            return token;
        }

        let waiting = ForeignCancelled {
            state: self.state.clone(),
            registration: None,
        };
        let to_cancel = token.clone();

        crate::spawn_local(async move {
            waiting.await;
            to_cancel.cancel();
        })
        .detach();

        token
    }

    /// Whether the origin has cancelled, or has gone away.
    pub fn is_cancelled(&self) -> bool {
        self.state.is_cancelled()
    }
}

/// Waits for a [`ForeignState`] to be cancelled. `Send`, unlike everything
/// else here, because it is what the watching task parks on.
struct ForeignCancelled {
    state: Arc<ForeignState>,
    /// As in [`Cancelled`]: withdrawn on drop, so a poll that does not fire
    /// leaves nothing behind. Reached far less often here, since this is
    /// awaited once by the watching task rather than round a loop, but the
    /// registration has the same shape and should not need a different rule.
    registration: Option<usize>,
}

impl Future for ForeignCancelled {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.is_cancelled() {
            return Poll::Ready(());
        }

        let this = self.get_mut();
        let mut wakers = this.state.wakers.lock().unwrap();

        // Re-checked under the lock: the origin may have cancelled between the
        // read above and here, in which case nobody is left to wake us.
        if this.state.is_cancelled() {
            return Poll::Ready(());
        }

        match this.registration {
            Some(key) => wakers.refresh(key, cx.waker()),
            None => this.registration = Some(wakers.register(cx.waker().clone())),
        }
        Poll::Pending
    }
}

impl Drop for ForeignCancelled {
    fn drop(&mut self) {
        // As in `Cancelled`: nothing to withdraw once cancelled, since the
        // list was drained and no new registration can follow.
        if let Some(key) = self.registration {
            if !self.state.is_cancelled() {
                self.state.wakers.lock().unwrap().withdraw(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::{cell::RefCell, rc::Rc, time::Duration};

    #[test]
    fn cancellation_crosses_to_another_executor() {
        // The downstream shape: a token on the control core, work on another.
        let token = CancellationToken::new();
        let remote = token.foreign_child();

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move {
                let local = remote.attach();
                ready_tx.send(()).unwrap();
                local.cancelled().await;
                "observed"
            })
            .unwrap();

        ready_rx.recv().unwrap();
        token.cancel();

        assert_eq!(worker.join().unwrap(), "observed");
    }

    #[test]
    fn a_parked_executor_is_woken_by_a_foreign_cancel() {
        let token = CancellationToken::new();
        let remote = token.foreign_child();

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move {
                let local = remote.attach();
                ready_tx.send(()).unwrap();
                local.cancelled().await;
                std::time::Instant::now()
            })
            .unwrap();

        ready_rx.recv().unwrap();
        // Long enough that the far executor has run out of work and parked.
        // Waking it is the whole mechanism; nothing else will.
        std::thread::sleep(Duration::from_millis(300));
        let cancelled_at = std::time::Instant::now();
        token.cancel();

        let observed_at = worker.join().unwrap();
        let delay = observed_at.duration_since(cancelled_at);
        assert!(
            delay < Duration::from_millis(100),
            "a parked executor took {delay:?} to see the cancel: it was not woken"
        );
    }

    #[test]
    fn attaching_after_the_origin_cancelled_yields_a_cancelled_token() {
        let token = CancellationToken::new();
        let remote = token.foreign_child();
        token.cancel();

        let worker = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move {
                let local = remote.attach();
                assert!(local.is_cancelled());
                local.cancelled().await; // must not hang
            })
            .unwrap();

        worker.join().unwrap();
    }

    #[test]
    fn attaching_after_the_origin_was_dropped_yields_a_cancelled_token() {
        // The production race: `remove` takes the job out of the map, `cancel`
        // fires, and the origin drops at end of scope -- microseconds before a
        // chain on another core attaches. Waiting forever here is a stuck node.
        let token = CancellationToken::new();
        let remote = token.foreign_child();
        drop(token);

        let worker = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move {
                let local = remote.attach();
                assert!(
                    local.is_cancelled(),
                    "an origin that can never cancel again must read as cancelled"
                );
                local.cancelled().await; // must not hang
            })
            .unwrap();

        worker.join().unwrap();
    }

    #[test]
    fn an_attached_token_behaves_like_any_other() {
        let token = CancellationToken::new();
        let remote = token.foreign_child();

        let worker = crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
            .spawn(move || async move {
                let local = remote.attach();
                // Chains derive their own scopes from the job token.
                let grandchild = local.child_token();
                grandchild.cancelled().await;
                "grandchild cancelled"
            })
            .unwrap();

        std::thread::sleep(Duration::from_millis(50));
        token.cancel();

        assert_eq!(worker.join().unwrap(), "grandchild cancelled");
    }

    #[test]
    fn one_handle_cloned_reaches_several_executors() {
        let token = CancellationToken::new();
        let remote = token.foreign_child();

        let workers: Vec<_> = (0..3)
            .map(|_| {
                let remote = remote.clone();
                crate::LocalExecutorBuilder::new(crate::Placement::Unbound)
                    .spawn(move || async move {
                        remote.attach().cancelled().await;
                        1u32
                    })
                    .unwrap()
            })
            .collect();

        std::thread::sleep(Duration::from_millis(50));
        token.cancel();

        let total: u32 = workers.into_iter().map(|w| w.join().unwrap()).sum();
        assert_eq!(total, 3, "every clone should have been cancelled");
    }

    #[test]
    fn a_fresh_token_is_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancelled_waits_until_the_token_is_cancelled() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();
            let order = Rc::new(RefCell::new(Vec::new()));

            let waiter = crate::spawn_local({
                let token = token.clone();
                let order = order.clone();
                async move {
                    token.cancelled().await;
                    order.borrow_mut().push("observed");
                }
            })
            .detach();

            Timer::new(Duration::from_millis(10)).await;
            order.borrow_mut().push("not cancelled yet");
            token.cancel();

            waiter.await;
            assert_eq!(
                *order.borrow(),
                vec!["not cancelled yet", "observed"],
                "cancelled() resolved before the token was cancelled"
            );
        });
    }

    #[test]
    fn cancelled_returns_immediately_on_an_already_cancelled_token() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();
            token.cancel();
            token.cancelled().await;
        });
    }

    #[test]
    fn cancelling_a_parent_cancels_its_children_and_grandchildren() {
        let root = CancellationToken::new();
        let child = root.child_token();
        let grandchild = child.child_token();

        root.cancel();

        assert!(child.is_cancelled());
        assert!(
            grandchild.is_cancelled(),
            "cancellation must reach the whole tree"
        );
    }

    #[test]
    fn cancelling_a_child_leaves_its_parent_running() {
        let root = CancellationToken::new();
        let child = root.child_token();

        child.cancel();

        assert!(child.is_cancelled());
        assert!(!root.is_cancelled(), "cancellation must not travel upwards");
    }

    #[test]
    fn a_child_of_an_already_cancelled_parent_starts_cancelled() {
        let root = CancellationToken::new();
        root.cancel();

        let child = root.child_token();
        assert!(
            child.is_cancelled(),
            "subscribing after shutdown must not wait forever"
        );
    }

    #[test]
    fn clones_share_one_token() {
        let token = CancellationToken::new();
        let clone = token.clone();

        clone.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn dropping_a_parent_does_not_cancel_its_child() {
        let root = CancellationToken::new();
        let child = root.child_token();

        drop(root);

        assert!(
            !child.is_cancelled(),
            "dropping a token is not cancelling it"
        );
    }

    #[test]
    fn a_waiting_child_is_woken_when_the_parent_is_cancelled() {
        LocalExecutor::default().run(async {
            let root = CancellationToken::new();
            let child = root.child_token();

            let waiter = crate::spawn_local(async move { child.cancelled().await }).detach();

            Timer::new(Duration::from_millis(10)).await;
            root.cancel();

            waiter.await;
        });
    }
}

#[cfg(test)]
mod registration {
    use super::*;
    use crate::LocalExecutor;
    use futures_lite::future::poll_once;

    /// Watching a token in a loop is the idiom this type exists for: build a
    /// future, poll it, drop it, go round again. It must not cost anything per
    /// iteration.
    #[test]
    fn watching_in_a_loop_leaves_nothing_behind() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();

            for _ in 0..1_000 {
                assert!(poll_once(token.cancelled()).await.is_none());
            }

            assert_eq!(
                token.node.wakers.borrow().registered(),
                0,
                "a future that was polled and dropped without firing left its \
                 waker registered"
            );
        });
    }

    /// Re-polling the same future is not a new registration.
    #[test]
    fn re_polling_one_future_registers_once() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();
            let mut pending = Box::pin(token.cancelled());

            for _ in 0..10 {
                assert!(poll_once(&mut pending).await.is_none());
            }

            assert_eq!(token.node.wakers.borrow().registered(), 1);
            drop(pending);
            assert_eq!(token.node.wakers.borrow().registered(), 0);
        });
    }

    /// Withdrawn slots are reused rather than growing the list.
    #[test]
    fn withdrawn_slots_are_reused() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();
            for _ in 0..100 {
                assert!(poll_once(token.cancelled()).await.is_none());
            }
            assert_eq!(
                token.node.wakers.borrow().slots.len(),
                1,
                "one slot, taken and given back a hundred times"
            );
        });
    }

    #[test]
    fn a_waiting_future_is_still_woken() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();
            let child = token.child_token();

            let waiter = crate::spawn_local(async move { child.cancelled().await });
            crate::timer::sleep(std::time::Duration::from_millis(5)).await;
            token.cancel();

            waiter.await;
        });
    }
}

#[cfg(test)]
mod foreign_registration {
    use super::*;
    use crate::LocalExecutor;
    use futures_lite::future::poll_once;

    /// Same rule as the local future, checked the same way. This one is
    /// normally awaited once rather than in a loop, so the accumulation is
    /// unlikely to bite, but the registration should not behave differently
    /// depending on how hard it is to reach.
    #[test]
    fn polling_and_dropping_leaves_nothing_registered() {
        LocalExecutor::default().run(async {
            let token = CancellationToken::new();
            let remote = token.foreign_child();
            let state = remote.state.clone();

            for _ in 0..100 {
                let waiting = ForeignCancelled {
                    state: state.clone(),
                    registration: None,
                };
                assert!(poll_once(waiting).await.is_none());
            }

            assert_eq!(state.wakers.lock().unwrap().registered(), 0);
            assert_eq!(
                state.wakers.lock().unwrap().slots.len(),
                1,
                "one slot, taken and given back"
            );
        });
    }

    #[test]
    fn a_foreign_waiter_is_still_woken() {
        let token = CancellationToken::new();
        let remote = token.foreign_child();

        let worker = std::thread::spawn(move || {
            LocalExecutor::default().run(async move {
                remote.attach().cancelled().await;
            })
        });

        LocalExecutor::default().run(async {
            crate::timer::sleep(std::time::Duration::from_millis(20)).await;
            token.cancel();
        });

        worker.join().expect("the far side was woken");
    }
}
