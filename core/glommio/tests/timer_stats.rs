//! Proves the timer population counters are reachable and count what they say.
//!
//! An integration test compiles as a separate crate, so it sees exactly what a
//! consumer sees — which is the only way to catch a type that is public but
//! unreachable, or a counter with no accessor.
#![cfg(feature = "debugging")]

use futures_lite::future::poll_once;
use glommio::{
    timer::{debugging::timer_stats, sleep, Timer},
    LocalExecutor,
};
use std::time::Duration;

#[test]
fn a_timer_that_fires_counts_as_inserted_and_fired() {
    let ex = LocalExecutor::default();
    ex.run(async {
        let before = timer_stats();
        sleep(Duration::from_millis(1)).await;
        let after = timer_stats();

        assert_eq!(after.inserted - before.inserted, 1, "one timer inserted");
        assert_eq!(after.fired - before.fired, 1, "and it fired");
        assert_eq!(
            after.cancelled_before_fire, before.cancelled_before_fire,
            "firing is not cancellation"
        );
    });
}

#[test]
fn a_timer_dropped_before_firing_counts_as_cancelled() {
    let ex = LocalExecutor::default();
    ex.run(async {
        let before = timer_stats();

        // Poll once so the timer registers with the reactor, then drop it
        // unfired — the deadline-churn shape a socket timeout produces.
        let mut timer = Timer::new(Duration::from_secs(3600));
        assert!(poll_once(&mut timer).await.is_none(), "must not fire yet");
        drop(timer);

        let after = timer_stats();
        assert_eq!(after.inserted - before.inserted, 1, "one timer inserted");
        assert_eq!(
            after.cancelled_before_fire - before.cancelled_before_fire,
            1,
            "and it was cancelled, not fired"
        );
        assert_eq!(after.fired, before.fired, "nothing fired");
    });
}

#[test]
fn high_water_tracks_concurrent_live_timers_not_total() {
    let ex = LocalExecutor::default();
    ex.run(async {
        let before = timer_stats();

        // Three live at once.
        let mut timers: Vec<_> = (0..3)
            .map(|_| Timer::new(Duration::from_secs(3600)))
            .collect();
        for t in timers.iter_mut() {
            assert!(poll_once(t).await.is_none());
        }
        let peak = timer_stats();
        assert!(
            peak.live_high_water >= before.live_high_water + 3,
            "high water rose by at least the three live timers: {} -> {}",
            before.live_high_water,
            peak.live_high_water
        );

        drop(timers);

        // Dropping them lowers live, but never lowers the high-water mark.
        let after = timer_stats();
        assert_eq!(
            after.live_high_water, peak.live_high_water,
            "high water is a maximum, not a gauge"
        );
        assert_eq!(after.live, before.live, "all three released");
    });
}
