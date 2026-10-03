//! Repeating ticks with explicit missed-tick semantics.

use super::Timer;
use futures_lite::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

/// What an [`Interval`] does about ticks that went by while the consumer was
/// busy.
///
/// The three differ only after a consumer has been slower than the period, and
/// then they differ a great deal. Under backpressure a ticker that silently
/// bursts behaves very differently from one that skips, so this is chosen
/// rather than assumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MissedTickBehavior {
    /// Deliver the missed ticks back to back until the schedule is caught up.
    ///
    /// The number of ticks over a period of time is preserved, at the cost of
    /// a burst after every stall. This is the default, as it is in tokio.
    #[default]
    Burst,
    /// Restart the schedule from the moment the late tick was delivered.
    ///
    /// No burst, and every gap between ticks is at least one period -- but the
    /// ticks drift away from the original grid by however long the consumer
    /// was late.
    Delay,
    /// Drop the missed ticks and resume on the original grid.
    ///
    /// No burst and no drift: the next tick lands on the next multiple of the
    /// period from the start, as if the missed ones had never been owed.
    Skip,
}

/// A stream of ticks, one per period.
///
/// The first tick is immediate; later ones arrive one period apart. What
/// happens when a consumer is slower than the period is governed by
/// [`MissedTickBehavior`].
///
/// # Examples
///
/// ```
/// use glommio::{timer::interval, LocalExecutor};
/// use std::time::Duration;
///
/// let ex = LocalExecutor::default();
/// ex.run(async {
///     let mut ticker = interval(Duration::from_millis(10));
///     ticker.tick().await; // immediate
///     ticker.tick().await; // 10ms later
/// });
/// ```
#[derive(Debug)]
pub struct Interval {
    period: Duration,
    /// When the next tick is owed. In the past means one is owed now.
    next: Instant,
    behavior: MissedTickBehavior,
    timer: Option<Timer>,
}

/// Creates an interval ticking every `period`, starting immediately.
///
/// # Panics
///
/// Panics if `period` is zero.
pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now(), period)
}

/// Creates an interval ticking every `period`, with the first tick at `start`.
///
/// # Panics
///
/// Panics if `period` is zero.
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    assert!(
        period > Duration::ZERO,
        "an interval needs a period greater than zero"
    );

    Interval {
        period,
        next: start,
        behavior: MissedTickBehavior::default(),
        timer: None,
    }
}

impl Interval {
    /// Waits for the next tick, returning the instant it was owed.
    ///
    /// Note that this is the scheduled instant rather than the current time,
    /// which is what lets a caller notice how late it is.
    pub fn tick(&mut self) -> Tick<'_> {
        Tick { interval: self }
    }

    /// Returns what this interval does about missed ticks.
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.behavior
    }

    /// Sets what this interval does about missed ticks.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }

    /// The period between ticks.
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Moves `next` on after a tick was delivered at `now`.
    fn schedule_after(&mut self, now: Instant) {
        let owed = self.next;
        let simple = owed + self.period;

        self.next = if simple > now {
            // Not late: the next tick is one period after this one, whatever
            // the behavior, because nothing was missed.
            simple
        } else {
            match self.behavior {
                // Stay on the original grid and keep owing the missed ticks,
                // which is what makes them arrive back to back.
                MissedTickBehavior::Burst => simple,
                // Restart the schedule from now.
                MissedTickBehavior::Delay => now + self.period,
                // Stay on the original grid, but give up the missed ticks by
                // jumping to the next multiple of the period that is still
                // ahead of us.
                MissedTickBehavior::Skip => {
                    let behind = now.duration_since(owed);
                    let periods_missed = behind.as_nanos() / self.period.as_nanos();
                    owed + self.period * (periods_missed as u32 + 1)
                }
            }
        };
    }

    fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Instant> {
        let now = Instant::now();

        if now >= self.next {
            let owed = self.next;
            self.timer = None;
            self.schedule_after(now);
            return Poll::Ready(owed);
        }

        // Arm a timer for whatever is left, reusing one already armed for this
        // deadline rather than re-arming on every poll.
        let timer = self
            .timer
            .get_or_insert_with(|| Timer::new(self.next.duration_since(now)));

        match Pin::new(timer).poll(cx) {
            Poll::Ready(_fired_at) => {
                let owed = self.next;
                self.timer = None;
                self.schedule_after(Instant::now());
                Poll::Ready(owed)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The future returned by [`Interval::tick`].
#[derive(Debug)]
pub struct Tick<'a> {
    interval: &'a mut Interval,
}

impl Future for Tick<'_> {
    type Output = Instant;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().interval.poll_tick(cx)
    }
}

impl Stream for Interval {
    type Item = Instant;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_tick(cx).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::sleep, LocalExecutor};
    use futures_lite::StreamExt;
    use std::time::{Duration, Instant};

    #[test]
    fn the_first_tick_is_immediate() {
        LocalExecutor::default().run(async {
            let started = Instant::now();
            let mut ticker = interval(Duration::from_secs(30));
            ticker.tick().await;
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "the first tick should not wait for a period"
            );
        });
    }

    #[test]
    fn later_ticks_wait_for_the_period() {
        LocalExecutor::default().run(async {
            let mut ticker = interval(Duration::from_millis(30));
            ticker.tick().await;

            let started = Instant::now();
            ticker.tick().await;
            assert!(
                started.elapsed() >= Duration::from_millis(25),
                "the second tick arrived after {:?}, before its period",
                started.elapsed()
            );
        });
    }

    #[test]
    fn interval_at_waits_for_its_start() {
        LocalExecutor::default().run(async {
            let start = Instant::now() + Duration::from_millis(30);
            let mut ticker = interval_at(start, Duration::from_millis(30));

            let began = Instant::now();
            ticker.tick().await;
            assert!(
                began.elapsed() >= Duration::from_millis(25),
                "interval_at fired before its start instant"
            );
        });
    }

    #[test]
    fn an_interval_is_a_stream_of_ticks() {
        LocalExecutor::default().run(async {
            let ticker = interval(Duration::from_millis(5));
            let ticks: Vec<Instant> = ticker.take(3).collect().await;
            assert_eq!(ticks.len(), 3);
        });
    }

    #[test]
    fn delay_schedules_the_next_tick_from_when_the_slow_one_finished() {
        LocalExecutor::default().run(async {
            let mut ticker = interval(Duration::from_millis(20));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            ticker.tick().await;

            // Miss two periods.
            sleep(Duration::from_millis(50)).await;
            ticker.tick().await;

            // Delay: the schedule restarts from now, so the next tick is a
            // full period away rather than immediate.
            let after_slow = Instant::now();
            ticker.tick().await;
            assert!(
                after_slow.elapsed() >= Duration::from_millis(15),
                "Delay should push the schedule out by a full period, waited {:?}",
                after_slow.elapsed()
            );
        });
    }

    #[test]
    fn burst_catches_up_immediately_on_missed_ticks() {
        LocalExecutor::default().run(async {
            let mut ticker = interval(Duration::from_millis(20));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
            ticker.tick().await;

            // Miss two periods.
            sleep(Duration::from_millis(50)).await;

            // Burst: the missed ticks are still owed, so they arrive back to
            // back without waiting.
            let started = Instant::now();
            ticker.tick().await;
            ticker.tick().await;
            ticker.tick().await;
            assert!(
                started.elapsed() < Duration::from_millis(15),
                "Burst should deliver the owed ticks at once, took {:?}",
                started.elapsed()
            );
        });
    }

    #[test]
    fn skip_drops_missed_ticks_and_keeps_the_original_schedule() {
        LocalExecutor::default().run(async {
            let period = Duration::from_millis(20);
            let mut ticker = interval(period);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let first = ticker.tick().await;

            // Miss two periods.
            sleep(Duration::from_millis(50)).await;
            ticker.tick().await;

            // Skip: no burst, and the schedule stays aligned to the original
            // start rather than to when we got around to it.
            let started = Instant::now();
            let next = ticker.tick().await;
            assert!(
                started.elapsed() < Duration::from_millis(15),
                "Skip should resume on the original grid, waited {:?}",
                started.elapsed()
            );

            let offset = next.duration_since(first).as_millis() % period.as_millis();
            assert!(
                !(5..=15).contains(&offset),
                "ticks drifted off the original grid by {offset}ms"
            );
        });
    }

    #[test]
    fn the_behaviour_can_be_read_back() {
        let mut ticker = interval(Duration::from_millis(1));
        assert_eq!(ticker.missed_tick_behavior(), MissedTickBehavior::Burst);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        assert_eq!(ticker.missed_tick_behavior(), MissedTickBehavior::Skip);
    }

    #[test]
    #[should_panic(expected = "period")]
    fn a_zero_period_is_rejected() {
        interval(Duration::ZERO);
    }
}
