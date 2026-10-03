//! Combinators over futures.
//!
//! [`timeout`] here races any future against a deadline, whatever it returns.
//! [`timer::try_timeout`](crate::timer::try_timeout) is the narrower form that
//! accepts only futures already returning a [`crate::Result`], and flattens
//! it. The `try_` prefix marks the `Result`-aware one, as it does for
//! `try_join` and `try_select`.

use crate::{timer::Timer, GlommioError};
/// The same shorthand `timer` uses: a glommio error carrying no payload.
type Result<T> = crate::Result<T, ()>;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// Races a future against a deadline, whatever it returns.
///
/// Returns the future's output if it finishes in time, or
/// [`GlommioError::TimedOut`] if the deadline arrives first. Unlike
/// [`timer::try_timeout`](crate::timer::try_timeout) the future may return anything at
/// all -- `()`, an `Option`, or somebody else's error type -- and its output is
/// handed back untouched rather than flattened.
///
/// # Which future wins at the deadline
///
/// The inner future is polled before the timer, so one that completes exactly
/// as the deadline arrives reports success rather than racing. That bias is
/// deliberate: a caller that has the answer should be given it.
///
/// # Examples
///
/// ```
/// use glommio::{future::timeout, timer::Timer, LocalExecutor};
/// use std::time::Duration;
///
/// let ex = LocalExecutor::default();
/// ex.run(async {
///     let answer = timeout(Duration::from_secs(10), async { 42 }).await;
///     assert_eq!(answer.unwrap(), 42);
///
///     let slow = timeout(Duration::from_millis(1), async {
///         Timer::new(Duration::from_secs(60)).await;
///     })
///     .await;
///     assert!(slow.is_err());
/// });
/// ```
pub async fn timeout<F, T>(dur: Duration, future: F) -> Result<T>
where
    F: Future<Output = T>,
{
    Timeout {
        dur,
        future,
        timer: Timer::new(dur),
    }
    .await
}

/// The future returned by [`timeout`].
struct Timeout<F> {
    dur: Duration,
    future: F,
    timer: Timer,
}

impl<F, T> Future for Timeout<F>
where
    F: Future<Output = T>,
{
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: `self` is pinned, and neither field is moved out -- each is
        // only projected to a pinned reference for polling.
        let this = unsafe { self.get_unchecked_mut() };

        // The inner future first, so that a future which is ready at the
        // instant the deadline arrives is not reported as timed out.
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(output) = future.poll(cx) {
            return Poll::Ready(Ok(output));
        }

        let timer = unsafe { Pin::new_unchecked(&mut this.timer) };
        if timer.poll(cx).is_ready() {
            Poll::Ready(Err(GlommioError::TimedOut(this.dur)))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::time::{Duration, Instant};

    #[test]
    fn a_future_that_finishes_in_time_yields_its_output() {
        LocalExecutor::default().run(async {
            let value = timeout(Duration::from_secs(10), async { 42u32 })
                .await
                .unwrap();
            assert_eq!(value, 42);
        });
    }

    #[test]
    fn any_output_type_is_accepted_not_only_results() {
        LocalExecutor::default().run(async {
            // The point of this combinator: none of these are glommio Results.
            assert_eq!(
                timeout(Duration::from_secs(10), async {}).await.unwrap(),
                ()
            );
            assert_eq!(
                timeout(Duration::from_secs(10), async { Some(1u8) })
                    .await
                    .unwrap(),
                Some(1)
            );
            let nested: std::result::Result<u8, String> =
                timeout(Duration::from_secs(10), async { Err("theirs".to_string()) })
                    .await
                    .unwrap();
            assert_eq!(nested, Err("theirs".to_string()));
        });
    }

    #[test]
    fn a_future_that_takes_too_long_times_out() {
        LocalExecutor::default().run(async {
            let outcome = timeout(Duration::from_millis(5), async {
                Timer::new(Duration::from_secs(60)).await;
                1u32
            })
            .await;

            assert!(matches!(outcome, Err(crate::GlommioError::TimedOut(_))));
        });
    }

    #[test]
    fn a_ready_future_wins_a_zero_duration_race() {
        LocalExecutor::default().run(async {
            // The deadline bias: the inner future is polled before the timer,
            // so something already complete reports success rather than
            // racing.
            assert_eq!(timeout(Duration::ZERO, async { 7u32 }).await.unwrap(), 7);
        });
    }

    #[test]
    fn the_timer_stops_when_the_future_finishes() {
        LocalExecutor::default().run(async {
            let started = Instant::now();
            timeout(Duration::from_secs(30), async {}).await.unwrap();
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "the combinator waited for the deadline instead of the future"
            );
        });
    }
}
