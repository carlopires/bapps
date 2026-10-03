//! Bounded, explicit cross-core messages. This reference transport uses
//! async-channel; it is not a claim of Glommio SPSC/NUMA-optimal performance.
use crate::AppError;
use crate::{ReadyGate, ShardId};
use async_channel::{Receiver, Sender};
use bapps_otp::{ChildContext, TaskStatus};
use bapps_trio::{
    CancelReason, CancelScope, Obligation, TaskClass, cancel_on, sync::Condition, with_cancel_scope,
};
use glommio::timer::Timer;
use std::{
    cell::Cell,
    fmt,
    future::{Future, poll_fn},
    pin::Pin,
    rc::Rc,
    task::Poll,
    time::{Duration, Instant},
};

/// Time source for RPC deadlines. Deadlines stay absolute `Instant`s because
/// they cross executors; production uses the monotonic clock and Glommio
/// timers, protocol tests a virtual clock they advance explicitly.
pub(crate) trait RpcClock {
    fn now(&self) -> Instant;
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()>>>;
}

pub(crate) type ClockRef = Rc<dyn RpcClock>;

pub(crate) struct SystemClock;

impl RpcClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()>>> {
        let timer = Timer::new(deadline.saturating_duration_since(Instant::now()));
        Box::pin(async move {
            timer.await;
        })
    }
}

/// RPC time from the task's Trio clock: the lab's virtual `TestClock` under
/// `bapps_trio::testing::Lab`. Deadlines stay `Instant`s offset from `base`.
pub(crate) struct TrioClock {
    base: Instant,
    clock: bapps_trio::ClockRef,
}

impl TrioClock {
    pub(crate) fn current(base: Instant) -> Self {
        Self {
            base,
            clock: bapps_trio::current_clock(),
        }
    }
}

impl RpcClock for TrioClock {
    fn now(&self) -> Instant {
        self.base + self.clock.now()
    }
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()>>> {
        self.clock
            .sleep_until(deadline.saturating_duration_since(self.base))
    }
}

/// Identifies one cross-shard call, for logs and cancellation messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct CallId {
    /// The calling shard.
    pub source: ShardId,
    /// Its per-caller sequence number.
    pub sequence: u64,
}

/// Why a caller stopped waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Interruption {
    /// The caller's scope was cancelled.
    Cancelled,
    /// The call's deadline passed.
    Deadline,
}

/// Outcome classes of a failed call. See `docs/rpc-contract.md` for the
/// request states and race precedence behind each one.
///
/// Never executed, safe to retry: `InvalidShard`, `InvalidOptions`,
/// `Overloaded`, `NodeStopping`, `NotAdmitted`, `TargetStopped`.
/// Executed with a known result: `Remote`.
/// Admitted and interrupted, effects possible: `Cancelled`, `Deadline`,
/// `OutcomeUnknown`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallError {
    /// No shard with this index.
    InvalidShard(usize),
    /// This shard already has `max_outbound` calls outstanding.
    Overloaded,
    /// The node is stopping; nothing new is admitted.
    NodeStopping,
    /// The caller stopped before the destination queue admitted the request.
    /// It was never delivered, so it had no effect.
    NotAdmitted(Interruption),
    /// The destination stopped before starting the request. No effect.
    TargetStopped,
    /// The caller was cancelled after admission. `acknowledged` means the
    /// destination reached a terminal state for the request within the
    /// caller's cleanup grace; it is NOT a rollback guarantee. Without
    /// acknowledgement the request may still be running.
    Cancelled {
        /// Whether the destination confirmed the request reached a terminal
        /// state within the cleanup grace (not a rollback guarantee).
        acknowledged: bool,
    },
    /// Like `Cancelled`, for the request deadline.
    Deadline {
        /// As for `Cancelled`.
        acknowledged: bool,
    },
    /// The destination destroyed the handler before it finished (forced
    /// stop after its cleanup grace, a generation force-abort or a panic).
    /// Effects, async cleanup and nested work cannot be certified.
    OutcomeUnknown,
    /// The handler ran and returned this error.
    Remote(String),
    /// The [`CallOptions`] were invalid (zero or above
    /// [`RpcLimits::max_call_duration`]).
    InvalidOptions(String),
}
impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let acknowledged = |acknowledged: bool| {
            if acknowledged {
                "the destination acknowledged it"
            } else {
                "the request may still be running"
            }
        };
        match self {
            Self::InvalidShard(shard) => write!(f, "no shard {shard}"),
            Self::Overloaded => f.write_str("too many calls outstanding on this shard"),
            Self::NodeStopping => f.write_str("the node is stopping"),
            Self::NotAdmitted(Interruption::Cancelled) => {
                f.write_str("cancelled before the destination admitted the request")
            }
            Self::NotAdmitted(Interruption::Deadline) => {
                f.write_str("deadline passed before the destination admitted the request")
            }
            Self::TargetStopped => f.write_str("the destination stopped before starting it"),
            Self::Cancelled { acknowledged: ack } => {
                write!(f, "cancelled after admission; {}", acknowledged(*ack))
            }
            Self::Deadline { acknowledged: ack } => {
                write!(f, "deadline passed after admission; {}", acknowledged(*ack))
            }
            Self::OutcomeUnknown => {
                f.write_str("the handler was destroyed before finishing; its outcome is unknown")
            }
            Self::Remote(message) => write!(f, "the handler failed: {message}"),
            Self::InvalidOptions(reason) => write!(f, "invalid call options: {reason}"),
        }
    }
}
impl std::error::Error for CallError {}

/// Every bound of the cross-shard transport; see the architecture docs.
/// Set with [`AppBuilder::limits`](crate::AppBuilder::limits).
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RpcLimits {
    /// Requests that may queue for one destination shard (default 256).
    pub queue_capacity: usize,
    /// Handlers that may run at once on one shard's inbox (default 128).
    pub max_in_flight: usize,
    /// Calls one shard may have outstanding as a caller (default 256).
    pub max_outbound: usize,
    /// Ceiling on any call, whatever its own timeout (default 30 s).
    pub max_call_duration: Duration,
    /// How long a cancelled handler may run on to clean up (default 1 s).
    pub handler_cancel_grace: Duration,
}
impl Default for RpcLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            max_in_flight: 128,
            max_outbound: 256,
            max_call_duration: Duration::from_secs(30),
            handler_cancel_grace: Duration::from_secs(1),
        }
    }
}
impl RpcLimits {
    /// Check every bound is usable.
    ///
    /// # Errors
    ///
    /// [`AppError::Config`] when a count is zero or a duration is zero.
    pub fn validate(&self) -> Result<(), AppError> {
        if self.queue_capacity == 0 || self.max_in_flight == 0 || self.max_outbound == 0 {
            return Err(AppError::Config(
                "RPC queue/active/outbound limits must be non-zero".into(),
            ));
        }
        if self.max_call_duration.is_zero() || self.handler_cancel_grace.is_zero() {
            return Err(AppError::Config(
                "RPC duration and handler grace must be positive".into(),
            ));
        }
        Ok(())
    }

    /// Requests that may queue for one destination shard.
    #[must_use]
    pub fn with_queue_capacity(mut self, value: usize) -> Self {
        self.queue_capacity = value;
        self
    }

    /// Handlers that may run at once on one shard's inbox.
    #[must_use]
    pub fn with_max_in_flight(mut self, value: usize) -> Self {
        self.max_in_flight = value;
        self
    }

    /// Calls one shard may have outstanding as a caller.
    #[must_use]
    pub fn with_max_outbound(mut self, value: usize) -> Self {
        self.max_outbound = value;
        self
    }

    /// Ceiling on any call, whatever its own timeout.
    #[must_use]
    pub fn with_max_call_duration(mut self, value: Duration) -> Self {
        self.max_call_duration = value;
        self
    }

    /// How long a cancelled handler may keep running to clean up.
    #[must_use]
    pub fn with_handler_cancel_grace(mut self, value: Duration) -> Self {
        self.handler_cancel_grace = value;
        self
    }
}

/// Per-call settings for [`ShardClient::call`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct CallOptions {
    /// Longest wait (default 5 s), further capped by the caller's remaining
    /// time and by [`RpcLimits::max_call_duration`].
    pub timeout: Duration,
    /// Extra bounded time to await a terminal response after cancellation.
    pub cancellation_grace: Duration,
    /// Scheduling class of the handler on the destination (default
    /// `ForegroundRead`).
    pub task_class: TaskClass,
}
impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            cancellation_grace: Duration::from_secs(2),
            task_class: TaskClass::ForegroundRead,
        }
    }
}
impl CallOptions {
    /// Longest wait for this call (capped by the caller's remaining time and
    /// by [`RpcLimits::max_call_duration`]).
    #[must_use]
    pub fn with_timeout(mut self, value: Duration) -> Self {
        self.timeout = value;
        self
    }

    /// Extra time to await a terminal reply after cancelling.
    #[must_use]
    pub fn with_cancellation_grace(mut self, value: Duration) -> Self {
        self.cancellation_grace = value;
        self
    }

    /// Scheduling class of the handler on the destination shard.
    #[must_use]
    pub fn with_task_class(mut self, value: TaskClass) -> Self {
        self.task_class = value;
        self
    }
}

/// Counters of one side of the transport: from [`ShardClient::metrics`]
/// (calls made by this shard) or [`ShardInbox::metrics`] (requests handled
/// by this shard).
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct RpcMetrics {
    /// Calls made (client) or requests received (inbox).
    pub submitted: u64,
    /// Of those, finished.
    pub completed: u64,
    /// Of those, cancelled, timed out or with an unknown outcome.
    pub interrupted: u64,
    /// Calls outstanding (client) or handlers running (inbox) now.
    pub active: usize,
    /// Requests waiting in the inbox queue (always 0 for a client).
    pub queued: usize,
    /// `max_outbound` (client) or `queue_capacity` (inbox).
    pub capacity: usize,
}
#[derive(Default)]
struct Counters {
    submitted: Cell<u64>,
    completed: Cell<u64>,
    interrupted: Cell<u64>,
    active: Cell<usize>,
}
impl Counters {
    fn snapshot(&self, queued: usize, capacity: usize) -> RpcMetrics {
        RpcMetrics {
            submitted: self.submitted.get(),
            completed: self.completed.get(),
            interrupted: self.interrupted.get(),
            active: self.active.get(),
            queued,
            capacity,
        }
    }
}

pub(crate) struct Request<M, R> {
    id: CallId,
    message: M,
    deadline: Instant,
    class: TaskClass,
    cancel: Receiver<()>,
    reply: Sender<Result<R, CallError>>,
}
type Fabric<M, R> = (Vec<Sender<Request<M, R>>>, Vec<Receiver<Request<M, R>>>);
pub(crate) fn fabric<M, R>(shards: usize, capacity: usize) -> Fabric<M, R> {
    (0..shards)
        .map(|_| async_channel::bounded(capacity))
        .unzip()
}

/// Local capability for sending *owned messages* to another executor. Clones
/// share the caller-side admission limit. Rc deliberately keeps this !Send.
pub struct ShardClient<M, R> {
    inner: Rc<ClientInner<M, R>>,
}
struct ClientInner<M, R> {
    id: ShardId,
    sequence: Cell<u64>,
    senders: Vec<Sender<Request<M, R>>>,
    limits: RpcLimits,
    gate: ReadyGate,
    counters: Rc<Counters>,
    clock: ClockRef,
}
impl<M, R> Clone for ShardClient<M, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<M: Send + 'static, R: Send + 'static> ShardClient<M, R> {
    pub(crate) fn bind(
        id: ShardId,
        senders: Vec<Sender<Request<M, R>>>,
        limits: RpcLimits,
        gate: ReadyGate,
        clock: ClockRef,
    ) -> Self {
        Self {
            inner: Rc::new(ClientInner {
                id,
                sequence: Cell::new(0),
                senders,
                limits,
                gate,
                counters: Rc::new(Counters::default()),
                clock,
            }),
        }
    }
    /// The shard this client calls from.
    pub fn shard_id(&self) -> ShardId {
        self.inner.id
    }
    /// Shards in the node.
    pub fn shard_count(&self) -> usize {
        self.inner.senders.len()
    }
    /// This shard's outgoing-call counters.
    pub fn metrics(&self) -> RpcMetrics {
        self.inner
            .counters
            .snapshot(0, self.inner.limits.max_outbound)
    }
    /// Requests queued for shard `id`; `None` if it does not exist.
    pub fn queued_to(&self, id: ShardId) -> Option<usize> {
        self.inner.senders.get(id.0).map(Sender::len)
    }

    /// One send, no automatic retries. The deadline includes startup-gate wait,
    /// queue admission and receiver execution. On cooperative cancellation this
    /// method requests cancellation and waits a bounded time for acknowledgement.
    /// Dropping this future can only send a best-effort Cancel, not await it.
    pub async fn call(
        &self,
        scope: &CancelScope,
        target: ShardId,
        message: M,
        options: CallOptions,
    ) -> Result<R, CallError> {
        let sender = self
            .inner
            .senders
            .get(target.0)
            .ok_or(CallError::InvalidShard(target.0))?;
        if options.timeout.is_zero()
            || options.timeout > self.inner.limits.max_call_duration
            || options.cancellation_grace.is_zero()
        {
            return Err(CallError::InvalidOptions(
                "timeout must be positive and at most max_call_duration; grace must be positive"
                    .into(),
            ));
        }
        if self.inner.counters.active.get() >= self.inner.limits.max_outbound {
            return Err(CallError::Overloaded);
        }
        let _admission = Active::new(self.inner.counters.clone(), None);
        let clock = &*self.inner.clock;
        // Never wait longer than the caller has left: its effective deadline
        // caps `options.timeout`, and travels to the handler with the request.
        let caller_left = scope
            .effective_deadline()
            .map(|deadline| deadline.saturating_sub(bapps_trio::current_clock().now()));
        let timeout = caller_left.map_or(options.timeout, |left| left.min(options.timeout));
        let deadline = clock
            .now()
            .checked_add(timeout)
            .ok_or_else(|| CallError::InvalidOptions("timeout overflow".into()))?;
        // Waiting for readiness: nothing has been sent yet.
        before(scope, deadline, clock, self.inner.gate.wait(scope))
            .await
            .map_err(CallError::NotAdmitted)?
            .map_err(|_| CallError::NodeStopping)?;
        let sequence = self
            .inner
            .sequence
            .get()
            .checked_add(1)
            .ok_or_else(|| CallError::InvalidOptions("request ID exhausted".into()))?;
        self.inner.sequence.set(sequence);
        let id = CallId {
            source: self.inner.id,
            sequence,
        };
        let (cancel, cancelled) = async_channel::bounded(1);
        let (reply, replies) = async_channel::bounded(1);
        let mut guard = CancelOnDrop {
            sender: cancel,
            armed: true,
        };
        let request = Request {
            id,
            message,
            deadline,
            class: options.task_class,
            cancel: cancelled,
            reply,
        };
        // Waiting for queue capacity: interrupted, the request is dropped
        // unsent (a send completes in the same poll that enqueues it).
        before(scope, deadline, clock, sender.send(request))
            .await
            .map_err(CallError::NotAdmitted)?
            .map_err(|_| CallError::TargetStopped)?;
        self.inner
            .counters
            .submitted
            .set(self.inner.counters.submitted.get().saturating_add(1));
        // Admitted. Cancellation and deadline win over a reply that becomes
        // ready in the same poll; that reply then counts as acknowledgement.
        let result = before(scope, deadline, clock, replies.recv()).await;
        match result {
            Ok(reply) => {
                guard.armed = false;
                self.inner
                    .counters
                    .completed
                    .set(self.inner.counters.completed.get().saturating_add(1));
                // A closed channel without a reply means the destination
                // dropped the request before starting it (see `Reply`).
                reply.map_err(|_| CallError::TargetStopped)?
            }
            Err(cause) => {
                self.inner
                    .counters
                    .interrupted
                    .set(self.inner.counters.interrupted.get().saturating_add(1));
                guard.request_cancel();
                // Do not inherit the already-cancelled caller while awaiting the
                // terminal response. This is bounded protocol cleanup, not work.
                let cleanup = CancelScope::new();
                let until = clock
                    .now()
                    .checked_add(options.cancellation_grace)
                    .ok_or_else(|| CallError::InvalidOptions("grace overflow".into()))?;
                let reply = before(&cleanup, until, clock, replies.recv()).await;
                // Acknowledged: the destination reached a terminal state it
                // could vouch for (finished, cooperatively cancelled, or never
                // started). A forced `OutcomeUnknown` is not acknowledgement.
                let acknowledged = matches!(
                    reply,
                    Ok(Ok(Ok(_)))
                        | Ok(Ok(Err(CallError::Remote(_))))
                        | Ok(Ok(Err(CallError::Cancelled { acknowledged: true })))
                        | Ok(Ok(Err(CallError::Deadline { acknowledged: true })))
                        | Ok(Ok(Err(CallError::TargetStopped)))
                        | Ok(Err(_))
                );
                guard.armed = false;
                match cause {
                    Interruption::Deadline => Err(CallError::Deadline { acknowledged }),
                    Interruption::Cancelled => Err(CallError::Cancelled { acknowledged }),
                }
            }
        }
    }
}
struct CancelOnDrop {
    sender: Sender<()>,
    armed: bool,
}
impl CancelOnDrop {
    fn request_cancel(&self) {
        let _ = self.sender.try_send(());
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.request_cancel();
        }
    }
}

/// Receiver ownership stays within one shard. A lease prevents accidentally
/// running two consumers, but permits a fresh OTP service generation to bind.
pub struct ShardInbox<M, R> {
    inner: Rc<InboxInner<M, R>>,
}
struct InboxInner<M, R> {
    id: ShardId,
    receiver: Receiver<Request<M, R>>,
    leased: Cell<bool>,
    counters: Rc<Counters>,
    changed: Condition,
    limits: RpcLimits,
    clock: ClockRef,
}
impl<M, R> Clone for ShardInbox<M, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<M, R> ShardInbox<M, R> {
    pub(crate) fn bind(
        id: ShardId,
        receiver: Receiver<Request<M, R>>,
        limits: RpcLimits,
        clock: ClockRef,
    ) -> Self {
        Self {
            inner: Rc::new(InboxInner {
                id,
                receiver,
                leased: Cell::new(false),
                counters: Rc::new(Counters::default()),
                changed: Condition::new(),
                limits,
                clock,
            }),
        }
    }
    /// This shard's incoming-request counters.
    pub fn metrics(&self) -> RpcMetrics {
        self.inner
            .counters
            .snapshot(self.inner.receiver.len(), self.inner.limits.queue_capacity)
    }
    /// The shard this inbox belongs to.
    pub fn shard_id(&self) -> ShardId {
        self.inner.id
    }
}
/// One service generation's exclusive right to consume the inbox.
///
/// Queued requests belong to the inbox, not to a generation: a request no
/// generation has started is served by the next one. Only started requests
/// belong to the generation that started them, and they are never replayed.
struct Lease<M, R>(Rc<InboxInner<M, R>>);
impl<M, R> Drop for Lease<M, R> {
    fn drop(&mut self) {
        self.0.leased.set(false);
    }
}
impl<M, R> Drop for InboxInner<M, R> {
    fn drop(&mut self) {
        // The shard is gone: nothing will start what is still queued.
        self.receiver.close();
        while let Ok(request) = self.receiver.try_recv() {
            let _ = request.reply.try_send(Err(CallError::TargetStopped));
        }
    }
}

/// The destination's obligation to answer one started request exactly once.
///
/// Consumed by [`Reply::send`]. Dropped unsent -- the handler task was
/// destroyed by a generation force-abort or a panic -- it reports
/// `OutcomeUnknown`: the handler may have had effects. A request dropped
/// before its task started never creates a `Reply`, and its caller sees a
/// closed channel, meaning `TargetStopped`.
struct Reply<R> {
    sender: Option<Sender<Result<R, CallError>>>,
    /// Committed by a real reply; aborted by the `OutcomeUnknown` fallback.
    obligation: Option<Obligation>,
}
impl<R> Reply<R> {
    fn new(sender: Sender<Result<R, CallError>>) -> Self {
        Self {
            sender: Some(sender),
            obligation: Some(Obligation::new("shard RPC reply")),
        }
    }
    fn send(mut self, result: Result<R, CallError>) {
        if let (Some(sender), Some(obligation)) = (self.sender.take(), self.obligation.take()) {
            let _ = sender.try_send(result);
            obligation.commit();
        }
    }
}
impl<R> Drop for Reply<R> {
    fn drop(&mut self) {
        if let (Some(sender), Some(obligation)) = (self.sender.take(), self.obligation.take()) {
            let _ = sender.try_send(Err(CallError::OutcomeUnknown));
            obligation.abort();
        }
    }
}
struct Active {
    counters: Rc<Counters>,
    changed: Option<Condition>,
}
impl Active {
    fn new(counters: Rc<Counters>, changed: Option<Condition>) -> Self {
        counters.active.set(counters.active.get() + 1);
        Self { counters, changed }
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.counters
            .active
            .set(self.counters.active.get().saturating_sub(1));
        if let Some(changed) = &self.changed {
            changed.notify_all();
        }
    }
}

/// Run a typed shard-RPC endpoint as an OTP child. Handler futures are local,
/// owned by this service generation, and bounded separately from queued work.
/// Errors are replies; panics reach OTP through the service's Trio nursery.
pub async fn serve<M, R, F, Fut>(
    ctx: ChildContext,
    started: TaskStatus<()>,
    inbox: ShardInbox<M, R>,
    handler: F,
) -> Result<(), AppError>
where
    M: Send + 'static,
    R: Send + 'static,
    F: Fn(M, CancelScope) -> Fut + Clone + 'static,
    Fut: Future<Output = Result<R, String>> + 'static,
{
    if inbox.inner.leased.replace(true) {
        return Err(AppError::Config(
            "shard RPC inbox already has an owner".into(),
        ));
    }
    let _lease = Lease(inbox.inner.clone());
    let scope = ctx.scope();
    started.started(()).map_err(|_| AppError::Cancelled)?;
    loop {
        while inbox.inner.counters.active.get() >= inbox.inner.limits.max_in_flight {
            let observed = inbox.inner.changed.generation();
            if !matches!(
                cancel_on(&scope, inbox.inner.changed.wait_for_change(observed)).await,
                Ok(Ok(_))
            ) {
                return Ok(());
            }
        }
        let request = match cancel_on(&scope, inbox.inner.receiver.recv()).await {
            Err(_) => return Ok(()),
            Ok(Err(_)) => return Ok(()),
            Ok(Ok(request)) => request,
        };
        let handler = handler.clone();
        let grace = inbox.inner.limits.handler_cancel_grace;
        let clock = inbox.inner.clock.clone();
        let counters = inbox.inner.counters.clone();
        counters
            .submitted
            .set(counters.submitted.get().saturating_add(1));
        // Reserve before spawning: a burst cannot outrun the active-task cap.
        let reservation = Active::new(counters.clone(), Some(inbox.inner.changed.clone()));
        let class = request.class;
        ctx.tasks()
            .spawn_into(class, move |task_scope| async move {
                let _reservation = reservation;
                let Request {
                    id: _id,
                    message,
                    deadline,
                    cancel,
                    reply,
                    class: _,
                } = request;
                let reply = Reply::new(reply);
                let operation = task_scope.child();
                let result = execute(operation, deadline, grace, cancel, &*clock, move |scope| {
                    handler(message, scope)
                })
                .await;
                counters
                    .completed
                    .set(counters.completed.get().saturating_add(1));
                if matches!(
                    result,
                    Err(CallError::Cancelled { .. }
                        | CallError::Deadline { .. }
                        | CallError::OutcomeUnknown)
                ) {
                    counters
                        .interrupted
                        .set(counters.interrupted.get().saturating_add(1));
                }
                reply.send(result);
                Ok::<(), String>(())
            })
            .map_err(|e| AppError::System(format!("shard RPC worker spawn: {e:?}")))?;
    }
}

// Handler is polled cooperatively after Cancel so it can perform cleanup. A
// bounded grace is the escape hatch for foreign futures that ignore scopes.
async fn execute<Mk, Fut, R>(
    scope: CancelScope,
    deadline: Instant,
    grace: Duration,
    cancelled: Receiver<()>,
    clock: &dyn RpcClock,
    make: Mk,
) -> Result<R, CallError>
where
    Mk: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = Result<R, String>>,
{
    // Before the handler starts, every refusal is "never executed".
    if scope.is_cancelled() {
        return Err(CallError::TargetStopped);
    }
    if cancelled.is_closed() || !cancelled.is_empty() {
        return Err(CallError::Cancelled { acknowledged: true });
    }
    if clock.now() >= deadline {
        return Err(CallError::Deadline { acknowledged: true });
    }
    // The handler sees the caller's deadline (remaining(), and calls it makes
    // are capped by it). Enforced below by `deadline_timer`.
    let left = deadline.saturating_duration_since(clock.now());
    scope.set_deadline(bapps_trio::current_clock().now().saturating_add(left));
    let mut handler = Box::pin(with_cancel_scope(scope.clone(), make(scope.clone())));
    let mut peer_cancel = Box::pin(cancelled.recv());
    let mut owner_cancel = Box::pin(scope.cancelled());
    let mut deadline_timer = clock.sleep_until(deadline);
    let mut grace_timer = None;
    let mut cause = None;
    poll_fn(|cx| {
        if cause.is_none() {
            if owner_cancel.as_mut().poll(cx).is_ready() || peer_cancel.as_mut().poll(cx).is_ready()
            {
                cause = Some(CallError::Cancelled { acknowledged: true });
            } else if deadline_timer.as_mut().poll(cx).is_ready() {
                cause = Some(CallError::Deadline { acknowledged: true });
            }
            if let Some(error) = &cause {
                let reason = match error {
                    CallError::Deadline { .. } => CancelReason::Deadline,
                    _ => CancelReason::Explicit,
                };
                scope.cancel_with(reason);
                grace_timer = Some(clock.sleep_until(clock.now() + grace));
            }
        }
        if let Poll::Ready(result) = handler.as_mut().poll(cx) {
            return Poll::Ready(match cause.take() {
                Some(error) => Err(error),
                None => result.map_err(CallError::Remote),
            });
        }
        if let Some(timer) = &mut grace_timer
            && timer.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(CallError::OutcomeUnknown));
        }
        Poll::Pending
    })
    .await
    // `handler` is destroyed before the caller of execute sends a terminal reply.
}

/// Run `operation` unless `scope` is cancelled or `deadline` passes first.
/// Interruption wins over an operation that becomes ready in the same poll.
async fn before<F: Future>(
    scope: &CancelScope,
    deadline: Instant,
    clock: &dyn RpcClock,
    operation: F,
) -> Result<F::Output, Interruption> {
    let mut cancelled = Box::pin(scope.cancelled());
    let mut timer = clock.sleep_until(deadline);
    let mut operation = Box::pin(operation);
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Interruption::Cancelled));
        }
        if clock.now() >= deadline || timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Interruption::Deadline));
        }
        operation.as_mut().poll(cx).map(Ok)
    })
    .await
}

#[cfg(test)]
mod clocked_tests;
#[cfg(test)]
mod protocol_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn limits_reject_unbounded_or_zero_values() {
        assert!(RpcLimits::default().validate().is_ok());
        assert!(
            RpcLimits {
                max_in_flight: 0,
                ..RpcLimits::default()
            }
            .validate()
            .is_err()
        );
    }
    #[test]
    fn wire_values_can_cross_threads() {
        fn send<T: Send>() {}
        send::<Request<String, String>>();
    }
}
