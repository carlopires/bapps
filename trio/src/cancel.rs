//! Cooperative, hierarchical cancellation.
//!
//! A `CancelScope` may inherit from more than one parent. This is the key v0.2
//! extension: one transient operation can be owned simultaneously by, for
//! example, a caller request and a long-lived service generation without
//! pretending those owners are the same lifetime.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};

use task_local::task_local;

use crate::sync::wait_list::{WaitList, WaitSlot};

/// Why a scope was cancelled locally or by an inherited parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    Explicit,
    Deadline,
    NurseryFailure,
    NurseryClosing,
}

impl std::fmt::Display for CancelReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Explicit => "explicitly cancelled",
            Self::Deadline => "deadline expired",
            Self::NurseryFailure => "a sibling task failed",
            Self::NurseryClosing => "owner is shutting down",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled {
    pub reason: Option<CancelReason>,
}

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            Some(reason) => write!(f, "cancelled: {reason}"),
            None => f.write_str("cancelled"),
        }
    }
}

impl std::error::Error for Cancelled {}

/// Where a cancellation started. Recorded once, on the scope that was
/// cancelled directly; scopes that merely inherit it report the same record
/// with `inherited: true`. Bounded: one record per cancelled scope, no chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelCause {
    pub reason: CancelReason,
    /// Who initiated it, when the canceller said so (see
    /// [`CancelScope::cancel_by`]); `None` for plain `cancel`/`cancel_with`.
    pub origin: Option<Rc<str>>,
    /// Executor-local cancellation order. Among several cancelled owners of a
    /// multi-parent scope, the smallest sequence was cancelled first.
    pub sequence: u64,
    /// `false` when the queried scope itself was cancelled.
    pub inherited: bool,
}

impl std::fmt::Display for CancelCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)?;
        if let Some(origin) = &self.origin {
            write!(f, " by {origin}")?;
        }
        if self.inherited {
            f.write_str(" (inherited)")?;
        }
        Ok(())
    }
}

thread_local! {
    static NEXT_SEQUENCE: Cell<u64> = const { Cell::new(0) };
}

fn next_sequence() -> u64 {
    NEXT_SEQUENCE.with(|next| {
        let sequence = next.get();
        next.set(sequence + 1);
        sequence
    })
}

struct CancelNode {
    cancelled: Cell<bool>,
    cause: RefCell<Option<CancelCause>>,
    /// When this scope's own deadline expires, on the clock that set it
    /// ([`crate::current_clock`]). Set by the deadline helpers.
    deadline: Cell<Option<Duration>>,
    parents: Vec<Rc<CancelNode>>,
    waiters: WaitList,
}

impl CancelNode {
    /// This node and every ancestor, each exactly once, even when several
    /// `any` scopes share an ancestor.
    fn lineage(self: &Rc<Self>) -> Vec<Rc<CancelNode>> {
        let mut lineage: Vec<Rc<CancelNode>> = Vec::new();
        let mut pending = vec![self.clone()];
        while let Some(node) = pending.pop() {
            if lineage.iter().any(|seen| Rc::ptr_eq(seen, &node)) {
                continue;
            }
            pending.extend(node.parents.iter().cloned());
            lineage.push(node);
        }
        lineage
    }

    /// This scope's own cause, or else the earliest cause among ancestors.
    fn effective_cause(&self) -> Option<CancelCause> {
        if let Some(cause) = self.cause.borrow().as_ref() {
            return Some(cause.clone());
        }
        self.parents
            .iter()
            .filter_map(|parent| parent.effective_cause())
            .min_by_key(|cause| cause.sequence)
            .map(|cause| CancelCause {
                inherited: true,
                ..cause
            })
    }

    fn effective_cancelled(&self) -> bool {
        self.cancelled.get()
            || self
                .parents
                .iter()
                .any(|parent| parent.effective_cancelled())
    }
}

/// A cooperative cancellation region local to one Glommio shard.
///
/// Child scopes inherit cancellation from their parent. [`CancelScope::any`]
/// creates a scope that inherits from several owners: cancellation of *any*
/// parent cancels the derived operation. A shielded scope is a new root and
/// therefore does not inherit cancellation from any outer scope.
#[derive(Clone)]
pub struct CancelScope {
    inner: Rc<CancelNode>,
}

impl std::fmt::Debug for CancelScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelScope")
            .field("cancelled", &self.is_cancelled())
            .field("reason", &self.reason())
            .finish()
    }
}

impl Default for CancelScope {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelScope {
    /// Create an independent cancellation root.
    pub fn new() -> Self {
        Self::with_parents(Vec::new())
    }

    fn with_parents(parents: Vec<Rc<CancelNode>>) -> Self {
        Self {
            inner: Rc::new(CancelNode {
                cancelled: Cell::new(false),
                cause: RefCell::new(None),
                deadline: Cell::new(None),
                parents,
                waiters: WaitList::default(),
            }),
        }
    }

    /// Create a scope inheriting cancellation from this scope.
    pub fn child(&self) -> Self {
        Self::with_parents(vec![self.inner.clone()])
    }

    /// Create a scope cancelled when **any** supplied parent is cancelled.
    ///
    /// The resulting scope is still independently cancellable. An empty input
    /// is equivalent to [`CancelScope::new`]. This is intentionally shard-local;
    /// cross-core and cross-node cancellation must still be explicit messages.
    pub fn any<I>(parents: I) -> Self
    where
        I: IntoIterator<Item = CancelScope>,
    {
        Self::with_parents(parents.into_iter().map(|parent| parent.inner).collect())
    }

    /// Convenience for the common two-owner case.
    pub fn combined(&self, other: &Self) -> Self {
        Self::any([self.clone(), other.clone()])
    }

    /// Create a scope that intentionally does not inherit outer cancellation.
    pub fn shielded(&self) -> Self {
        let _ = self;
        Self::new()
    }

    pub fn cancel(&self) {
        self.cancel_with(CancelReason::Explicit);
    }

    pub fn cancel_with(&self, reason: CancelReason) {
        self.cancel_recording(reason, None);
    }

    /// Cancel and record who initiated it, for diagnostics. Only the first
    /// cancellation of a scope is recorded; later ones do not overwrite it.
    pub fn cancel_by(&self, reason: CancelReason, origin: impl Into<Rc<str>>) {
        self.cancel_recording(reason, Some(origin.into()));
    }

    fn cancel_recording(&self, reason: CancelReason, origin: Option<Rc<str>>) {
        if self.inner.cancelled.replace(true) {
            return;
        }
        *self.inner.cause.borrow_mut() = Some(CancelCause {
            reason,
            origin,
            sequence: next_sequence(),
            inherited: false,
        });
        self.inner.waiters.wake_all();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.effective_cancelled()
    }

    /// This scope's own deadline, if a deadline helper ([`crate::fail_after`],
    /// [`crate::fail_at`], [`crate::move_on_after`], [`crate::move_on_at`])
    /// created it. A time on [`crate::current_clock`].
    pub fn deadline(&self) -> Option<Duration> {
        self.inner.deadline.get()
    }

    pub(crate) fn set_deadline(&self, deadline: Duration) {
        self.inner.deadline.set(Some(deadline));
    }

    /// The earliest deadline among this scope and every scope it inherits
    /// cancellation from: the time by which this scope will be cancelled at
    /// the latest, unless someone cancels it sooner. A shielded scope is a new
    /// root, so outer deadlines do not reach it; a scope with several owners
    /// ([`Self::any`]) takes the earliest of theirs.
    pub fn effective_deadline(&self) -> Option<Duration> {
        self.inner
            .lineage()
            .iter()
            .filter_map(|node| node.deadline.get())
            .min()
    }

    /// The reason of [`Self::cause`].
    pub fn reason(&self) -> Option<CancelReason> {
        self.cause().map(|cause| cause.reason)
    }

    /// Why this scope is cancelled: its own cancellation if it has one,
    /// otherwise the *earliest* cancellation among the scopes it inherits
    /// from. Later cancellations never replace the initiating cause.
    pub fn cause(&self) -> Option<CancelCause> {
        self.inner.effective_cause()
    }

    /// Wait for cancellation of this scope or any ancestor.
    ///
    /// The wait registers on every scope it inherits from and unregisters from
    /// all of them when dropped, so short waits on long-lived scopes do not
    /// accumulate.
    pub fn cancelled(&self) -> CancelledFuture {
        CancelledFuture {
            scope: self.clone(),
            registrations: Vec::new(),
        }
    }

    pub fn check_cancelled(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            Err(Cancelled {
                reason: self.reason(),
            })
        } else {
            Ok(())
        }
    }
}

/// Future returned by [`CancelScope::cancelled`].
pub struct CancelledFuture {
    scope: CancelScope,
    /// One registration per scope in the lineage, created on first pending poll.
    registrations: Vec<(Rc<CancelNode>, WaitSlot)>,
}

impl CancelledFuture {
    fn ready(&self) -> Option<Cancelled> {
        self.scope.is_cancelled().then(|| Cancelled {
            reason: self.scope.reason(),
        })
    }
}

impl Future for CancelledFuture {
    type Output = Cancelled;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(cancelled) = this.ready() {
            return Poll::Ready(cancelled);
        }
        if this.registrations.is_empty() {
            this.registrations = this
                .scope
                .inner
                .lineage()
                .into_iter()
                .map(|node| (node, None))
                .collect();
        }
        for (node, slot) in &mut this.registrations {
            node.waiters.register(slot, cx.waker());
        }
        match this.ready() {
            Some(cancelled) => Poll::Ready(cancelled),
            None => Poll::Pending,
        }
    }
}

impl Drop for CancelledFuture {
    fn drop(&mut self) {
        for (node, slot) in &mut self.registrations {
            node.waiters.unregister(slot);
        }
    }
}

task_local! {
    static CURRENT_CANCEL_SCOPE: CancelScope;
}

pub fn current_cancel_scope() -> Option<CancelScope> {
    CURRENT_CANCEL_SCOPE.try_get().ok()
}

pub async fn with_cancel_scope<F, T>(scope: CancelScope, future: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_CANCEL_SCOPE.scope(scope, future).await
}
