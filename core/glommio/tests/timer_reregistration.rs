//! A pending timer polled more than once must not accumulate registrations.
//!
//! Upstream passed a stable, caller-owned id into `insert_timer`, so
//! re-registering replaced the entry and was idempotent. The timing wheel
//! generates an id per insert instead, which makes re-registration additive
//! unless the timer explicitly withdraws the old one. Each stray registration
//! fires later and wakes a task that has already finished, which provokes
//! another poll — so the cost compounds rather than staying constant.
#![cfg(feature = "debugging")]

use futures_lite::future::poll_once;
use glommio::{
    timer::{debugging::timer_stats, Timer},
    LocalExecutor,
};
use std::time::Duration;

#[test]
fn repeated_polls_of_one_pending_timer_register_it_once() {
    let ex = LocalExecutor::default();
    ex.run(async {
        let before = timer_stats();

        let mut timer = Timer::new(Duration::from_secs(3600));
        for _ in 0..8 {
            assert!(poll_once(&mut timer).await.is_none(), "must not fire yet");
        }

        let polled = timer_stats();
        assert_eq!(
            polled.inserted - before.inserted,
            1,
            "eight polls of one timer is one registration, not eight"
        );
        assert_eq!(
            polled.live - before.live,
            1,
            "and exactly one timer is live"
        );

        drop(timer);

        let after = timer_stats();
        assert_eq!(
            after.cancelled_before_fire - before.cancelled_before_fire,
            1,
            "dropping it withdraws that one registration"
        );
        assert_eq!(after.live, before.live, "nothing left behind");
    });
}
