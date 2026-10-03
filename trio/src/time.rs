//! Cancellation-aware time and deterministic test clocks.

use std::{
    cell::Cell,
    future::{Future, poll_fn},
    pin::Pin,
    rc::Rc,
    task::Poll,
    time::{Duration, Instant},
};

use glommio::timer::Timer;
use task_local::task_local;

use crate::{
    cancel::{CancelReason, CancelScope, Cancelled, current_cancel_scope, with_cancel_scope},
    sync::wait_list::{WaitList, WaitSlot},
};

pub type ClockRef = Rc<dyn Clock>;

pub trait Clock {
    fn now(&self) -> Duration;
    fn sleep_until(&self, deadline: Duration) -> Pin<Box<dyn Future<Output = ()> + 'static>>;
}

#[derive(Clone)]
pub struct RealClock {
    origin: Instant,
}

impl RealClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }

    fn sleep_until(&self, deadline: Duration) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        let wait = deadline.saturating_sub(self.now());
        Box::pin(async move {
            if !wait.is_zero() {
                Timer::new(wait).await;
            }
        })
    }
}

task_local! {
    static CURRENT_CLOCK: ClockRef;
}

thread_local! {
    static DEFAULT_CLOCK: ClockRef = Rc::new(RealClock::new());
}

pub fn current_clock() -> ClockRef {
    CURRENT_CLOCK
        .try_get()
        .unwrap_or_else(|_| DEFAULT_CLOCK.with(|clock| clock.clone()))
}

pub async fn with_clock<F, T>(clock: ClockRef, future: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_CLOCK.scope(clock, future).await
}

pub async fn sleep(duration: Duration) -> Result<(), Cancelled> {
    let clock = current_clock();
    let deadline = clock.now().saturating_add(duration);
    sleep_until(deadline).await
}

pub async fn sleep_until(deadline: Duration) -> Result<(), Cancelled> {
    let clock = current_clock();
    let mut timer = clock.sleep_until(deadline);
    let Some(scope) = current_cancel_scope() else {
        timer.await;
        return Ok(());
    };
    let mut cancelled = Box::pin(scope.cancelled());

    poll_fn(|cx| {
        if let Poll::Ready(()) = timer.as_mut().poll(cx) {
            return Poll::Ready(Ok(()));
        }
        if let Poll::Ready(cancelled) = cancelled.as_mut().poll(cx) {
            return Poll::Ready(Err(cancelled));
        }
        Poll::Pending
    })
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailAfterError {
    TooSlow,
    Cancelled(Cancelled),
}

impl std::fmt::Display for FailAfterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooSlow => f.write_str("deadline expired"),
            Self::Cancelled(cancelled) => cancelled.fmt(f),
        }
    }
}

impl std::error::Error for FailAfterError {}

#[derive(Debug)]
#[non_exhaustive]
pub struct MoveOnOutcome<T> {
    pub value: Option<T>,
    pub timed_out: bool,
    pub cancelled: bool,
}

enum DeadlineOutcome<T> {
    Body(T),
    TimedOut,
    Cancelled(Cancelled),
}

/// The current task's effective deadline: the earliest deadline among the
/// current cancel scope and every scope it inherits from (see
/// [`CancelScope::effective_deadline`]). A time on [`current_clock`]; `None`
/// when no enclosing deadline exists.
pub fn current_effective_deadline() -> Option<Duration> {
    current_cancel_scope().and_then(|scope| scope.effective_deadline())
}

/// How long until [`current_effective_deadline`], zero once it has passed.
/// Pass this to a remote call so the callee works within the caller's time.
pub fn remaining() -> Option<Duration> {
    current_effective_deadline().map(|deadline| deadline.saturating_sub(current_clock().now()))
}

async fn run_deadline<R, F, Fut>(
    scope: CancelScope,
    deadline: Duration,
    body: F,
) -> DeadlineOutcome<R>
where
    F: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = R>,
{
    let clock = current_clock();
    scope.set_deadline(deadline);

    let body_scope = scope.clone();
    let body_future = async move { body(body_scope).await };
    let body_future = with_clock(clock.clone(), with_cancel_scope(scope.clone(), body_future));
    let mut body_future = Box::pin(body_future);
    let mut timer = clock.sleep_until(deadline);
    let mut cancelled = Box::pin(scope.cancelled());

    let outcome = poll_fn(|cx| {
        if let Poll::Ready(value) = body_future.as_mut().poll(cx) {
            return Poll::Ready(DeadlineOutcome::Body(value));
        }
        // Parent/explicit cancellation wins over a coincident deadline.
        if let Poll::Ready(cancelled) = cancelled.as_mut().poll(cx) {
            return Poll::Ready(DeadlineOutcome::Cancelled(cancelled));
        }
        if let Poll::Ready(()) = timer.as_mut().poll(cx) {
            return Poll::Ready(DeadlineOutcome::TimedOut);
        }
        Poll::Pending
    })
    .await;

    match outcome {
        DeadlineOutcome::Body(value) => DeadlineOutcome::Body(value),
        DeadlineOutcome::Cancelled(cancelled) => {
            // Cooperative semantics: do not simply drop the body. Keep polling it
            // under the cancelled scope so cancellation-aware operations can unwind.
            let _ = body_future.await;
            DeadlineOutcome::Cancelled(cancelled)
        }
        DeadlineOutcome::TimedOut => {
            scope.cancel_by(CancelReason::Deadline, format!("deadline at {deadline:?}"));
            let _ = body_future.await;
            DeadlineOutcome::TimedOut
        }
    }
}

fn child_of_current() -> CancelScope {
    current_cancel_scope()
        .map(|parent| parent.child())
        .unwrap_or_default()
}

fn after(duration: Duration) -> Duration {
    current_clock().now().saturating_add(duration)
}

fn fail_outcome<R>(outcome: DeadlineOutcome<R>) -> Result<R, FailAfterError> {
    match outcome {
        DeadlineOutcome::Body(value) => Ok(value),
        DeadlineOutcome::TimedOut => Err(FailAfterError::TooSlow),
        DeadlineOutcome::Cancelled(cancelled) => Err(FailAfterError::Cancelled(cancelled)),
    }
}

fn move_on_outcome<R>(outcome: DeadlineOutcome<R>) -> MoveOnOutcome<R> {
    match outcome {
        DeadlineOutcome::Body(value) => MoveOnOutcome {
            value: Some(value),
            timed_out: false,
            cancelled: false,
        },
        DeadlineOutcome::TimedOut => MoveOnOutcome {
            value: None,
            timed_out: true,
            cancelled: false,
        },
        DeadlineOutcome::Cancelled(_) => MoveOnOutcome {
            value: None,
            timed_out: false,
            cancelled: true,
        },
    }
}

/// Run `body` in a child scope cancelled `duration` from now; a timeout is
/// [`FailAfterError::TooSlow`].
pub async fn fail_after<R, F, Fut>(duration: Duration, body: F) -> Result<R, FailAfterError>
where
    F: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = R>,
{
    fail_at(after(duration), body).await
}

/// Like [`fail_after`], with an absolute deadline on [`current_clock`].
pub async fn fail_at<R, F, Fut>(deadline: Duration, body: F) -> Result<R, FailAfterError>
where
    F: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = R>,
{
    fail_outcome(run_deadline(child_of_current(), deadline, body).await)
}

/// Run a deadline in a fresh scope that does not inherit outer cancellation.
///
/// This is for bounded cleanup or a durability-critical transition. It should
/// not be used to make ordinary work ignore caller cancellation.
pub async fn fail_after_shielded<R, F, Fut>(
    duration: Duration,
    body: F,
) -> Result<R, FailAfterError>
where
    F: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = R>,
{
    fail_outcome(run_deadline(CancelScope::new(), after(duration), body).await)
}

/// Run `body` in a child scope cancelled `duration` from now; a timeout is
/// reported in the outcome, not as an error.
pub async fn move_on_after<R, F, Fut>(duration: Duration, body: F) -> MoveOnOutcome<R>
where
    F: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = R>,
{
    move_on_at(after(duration), body).await
}

/// Like [`move_on_after`], with an absolute deadline on [`current_clock`].
pub async fn move_on_at<R, F, Fut>(deadline: Duration, body: F) -> MoveOnOutcome<R>
where
    F: FnOnce(CancelScope) -> Fut,
    Fut: Future<Output = R>,
{
    move_on_outcome(run_deadline(child_of_current(), deadline, body).await)
}

/// Manual monotonic clock for deterministic tests.
#[derive(Clone, Default)]
pub struct TestClock {
    inner: Rc<TestClockInner>,
}

#[derive(Default)]
struct TestClockInner {
    now: Cell<Duration>,
    /// Sleepers keyed by registration, carrying their deadline.
    sleepers: WaitList<Duration>,
}

impl TestClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared(&self) -> ClockRef {
        Rc::new(self.clone())
    }

    pub fn advance(&self, delta: Duration) {
        self.set(self.inner.now.get().saturating_add(delta));
    }

    /// The earliest deadline among pending sleeps, if any.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.inner.sleepers.min_value()
    }

    pub fn set(&self, now: Duration) {
        self.inner.now.set(now);
        self.inner.sleepers.wake_if(|deadline| *deadline <= now);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Duration {
        self.inner.now.get()
    }

    fn sleep_until(&self, deadline: Duration) -> Pin<Box<dyn Future<Output = ()> + 'static>> {
        Box::pin(TestSleep {
            clock: self.clone(),
            deadline,
            slot: None,
        })
    }
}

struct TestSleep {
    clock: TestClock,
    deadline: Duration,
    slot: WaitSlot,
}

impl Future for TestSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        if self.clock.now() >= self.deadline {
            return Poll::Ready(());
        }
        let this = &mut *self;
        this.clock
            .inner
            .sleepers
            .register_with(&mut this.slot, this.deadline, cx.waker());
        Poll::Pending
    }
}

impl Drop for TestSleep {
    fn drop(&mut self) {
        self.clock.inner.sleepers.unregister(&mut self.slot);
    }
}
