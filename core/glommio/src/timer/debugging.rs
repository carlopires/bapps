// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
//! Timer population counters, for deciding what the timer structure has to be
//! good at.
//!
//! The interesting number is not how fast a timer operation is but which
//! operation dominates. A workload that cancels almost everything it inserts
//! wants cancellation to be cheap; one that lets timers fire wants expiry and
//! next-deadline lookup to be cheap. Those pull the implementation in
//! different directions, so the ratio decides the design.
//!
//! Enabled by the `debugging` feature. A default build carries none of this.

use crate::executor;

/// A snapshot of one executor's timer population.
///
/// Counters are per-executor, like the reactor that owns them, and count from
/// the executor's creation. Take two snapshots and subtract to measure a
/// region.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TimerStats {
    /// Timers registered with the reactor since the executor started.
    pub inserted: u64,
    /// Timers that reached their deadline and woke their task.
    pub fired: u64,
    /// Timers removed before they could fire, by a dropped `Timer` future or a
    /// completed socket operation whose timeout no longer applies.
    pub cancelled_before_fire: u64,
    /// Timers registered right now.
    pub live: usize,
    /// The largest `live` has ever been.
    ///
    /// This is what sizes a timer structure, and it is a maximum rather than a
    /// gauge — it never decreases.
    pub live_high_water: usize,
}

impl TimerStats {
    pub(crate) fn record_insert(&mut self) {
        self.inserted += 1;
        self.live += 1;
        self.live_high_water = self.live_high_water.max(self.live);
    }

    pub(crate) fn record_cancel(&mut self) {
        self.cancelled_before_fire += 1;
        self.live = self.live.saturating_sub(1);
    }

    pub(crate) fn record_fired(&mut self, woke: usize) {
        self.fired += woke as u64;
        self.live = self.live.saturating_sub(woke);
    }

    /// Share of inserted timers that were cancelled rather than fired.
    ///
    /// `None` before anything has been inserted. Near 1.0 means cancellation
    /// is the hot path; near 0.0 means expiry is.
    pub fn cancellation_ratio(&self) -> Option<f64> {
        let settled = self.fired + self.cancelled_before_fire;
        (settled > 0).then(|| self.cancelled_before_fire as f64 / settled as f64)
    }
}

/// Timer counters for the executor running this call.
///
/// # Panics
///
/// Panics if called outside a glommio executor.
///
/// # Examples
///
/// ```
/// use glommio::{timer::{debugging::timer_stats, sleep}, LocalExecutor};
/// use std::time::Duration;
///
/// let ex = LocalExecutor::default();
/// ex.run(async {
///     sleep(Duration::from_millis(1)).await;
///     let stats = timer_stats();
///     assert_eq!(stats.fired, 1);
/// });
/// ```
pub fn timer_stats() -> TimerStats {
    executor().reactor().timer_stats()
}
