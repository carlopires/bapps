// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! glommio::timer is a module that provides timing related primitives.
mod interval;
pub(crate) mod slab;
mod timer_impl;
mod timing_wheel;

#[cfg(feature = "debugging")]
pub mod debugging;

pub(crate) mod reactor_adapter;

pub use interval::{interval, interval_at, Interval, MissedTickBehavior, Tick};
use std::{future::Future, time::Duration};
pub use timer_impl::{Timer, TimerActionOnce, TimerActionRepeat};

type Result<T> = crate::Result<T, ()>;

/// Sleep for some time on the current task. Explicit sleeps can introduce undesirable delays if not used correctly.
/// Consider using [crate::timer::try_timeout] instead if you are implementing timeout-like semantics or
/// [crate::timer::TimerActionOnce] if you need to schedule a future for some later date in the future without needing
/// to await.
///
/// ```
/// use glommio::{timer::sleep, LocalExecutor};
/// use std::time::Duration;
///
/// let ex = LocalExecutor::default();
///
/// ex.run(async {
///     sleep(Duration::from_millis(100)).await;
/// });
/// ```
pub async fn sleep(wait: std::time::Duration) {
    Timer::new(wait).await;
}

/// Executes a future with a specified timeout
///
/// Returns a `Result`, with `Ok` if the future ran to completion
/// or a [`GlommioError::TimedOut`] error if the timeout was reached
///
/// # Which future wins at the deadline
///
/// The inner future is polled before the timer, so one that completes exactly
/// as the deadline arrives reports success rather than racing. That bias is
/// deliberate: a caller that has the answer should be given it.
///
/// # This function is the narrow form
///
/// It only accepts futures that already return a [`crate::Result`], whose
/// error it flattens into its own. For anything else -- a future returning
/// `()`, an `Option`, or another crate's error type -- use
/// [`future::timeout`](crate::future::timeout), which hands the output back
/// untouched.
///
/// The names follow the ecosystem's `try_` convention, where `try_` marks the
/// `Result`-aware variant: `try_join`, `try_select`, `try_for_each`. Until
/// 0.11 this function was called `timeout`, which meant two public functions
/// shared that name.
///
/// ```
/// # use glommio::{
/// #    timer::{try_timeout, Timer},
/// #    LocalExecutor,
/// # };
/// # use std::time::Duration;
/// # let ex = LocalExecutor::default();
/// # ex.run(async {
/// try_timeout(Duration::from_millis(1), async move {
///     // this future will wait for 100ms, but won't complete, as the timeout is 1ms
///     Timer::new(Duration::from_millis(100)).await;
///     Ok(())
/// })
/// .await;
/// # });
/// ```
///
/// [`GlommioError::TimedOut`]: crate::GlommioError::TimedOut
pub async fn try_timeout<F, T>(dur: Duration, f: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    timer_impl::Timeout::new(f, dur).await
}
