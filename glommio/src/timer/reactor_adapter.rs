// Copyright 2024 Glommio Project Authors. Licensed under Apache-2.0.

//! Reactor integration layer for StagedWheel.
//!
//! This module provides an adapter that integrates StagedWheel with the Reactor,
//! using TimerId for O(1) cancellation without HashMap overhead.

use super::staged_wheel::StagedWheel;
use super::timer_id::TimerId;
use ahash::AHashMap;
use std::collections::BTreeSet;
use std::task::Waker;
use std::time::{Duration, Instant};

/// Adapter for StagedWheel that integrates with the Reactor.
///
/// This adapter wraps StagedWheel and provides TimerId-based operations
/// for O(1) cancellation. The ID is simply the wheel's internal ID,
/// providing direct access without HashMap overhead.
///
/// # Performance
///
/// - Insert: O(1) - returns ID directly from wheel
/// - Remove: O(1) - direct access via ID
/// - No hashing overhead, no cache misses from HashMap traversal
///
/// # Cache Optimization
///
/// Field ordering optimized for cache locality:
/// - Hot field (wheel) is accessed on every timer operation
/// - Warm field (id_to_expiry) is accessed on insert/remove and duration checks
pub(crate) struct ReactorTimers {
    /// The underlying staged wheel (HOT: accessed every timer operation)
    wheel: StagedWheel,

    /// Maps IDs to their expiry times (WARM: accessed on insert/remove/duration)
    /// TODO: This is the remaining HashMap that could be eliminated by
    /// exposing expiry times from the wheel itself
    id_to_expiry: AHashMap<u64, Instant>,

    /// The same timers ordered by expiry, so the next deadline is the first
    /// element instead of a scan over every live timer on every reactor pass.
    /// With thousands of live timers (per-request deadlines) that scan
    /// dominated a busy executor.
    by_expiry: BTreeSet<(Instant, u64)>,
}

impl ReactorTimers {
    pub(crate) fn new() -> Self {
        Self {
            wheel: StagedWheel::new(),
            id_to_expiry: AHashMap::new(),
            by_expiry: BTreeSet::new(),
        }
    }

    /// Insert a timer and return an ID for O(1) cancellation
    ///
    /// The returned ID provides direct access to the timer's location
    /// in the wheel, avoiding HashMap lookups on removal.
    pub(crate) fn insert(&mut self, expires_at: Instant, waker: Waker) -> TimerId {
        // Insert into the wheel and get its internal ID
        let internal_id = self.wheel.insert(expires_at, waker);

        // Track expiry time (needed for next_timer_duration calculation)
        if let Some(previous) = self.id_to_expiry.insert(internal_id, expires_at) {
            self.by_expiry.remove(&(previous, internal_id));
        }
        self.by_expiry.insert((expires_at, internal_id));

        // Return ID (wrapping the wheel's internal ID)
        // Generation is 0 for now (we don't track reuse yet)
        TimerId::new(internal_id as u32, 0)
    }

    /// Remove a timer by ID (O(1) operation, no hashing)
    ///
    /// Returns true if the timer was found and removed
    pub(crate) fn remove(&mut self, id: TimerId) -> bool {
        let internal_id = id.index() as u64;

        // Remove from expiry tracking
        self.forget(internal_id);

        // Remove from wheel
        self.wheel.remove(internal_id)
    }

    /// Check if a timer exists by ID
    pub(crate) fn exists(&self, id: TimerId) -> bool {
        let internal_id = id.index() as u64;
        self.id_to_expiry.contains_key(&internal_id)
    }

    /// Process expired timers
    ///
    /// Returns (next_timer_duration, num_woke)
    ///
    /// # Re-entrancy Safety
    ///
    /// This method collects all expired wakers BEFORE calling wake() to avoid
    /// re-entrancy panics. If a waker tries to insert/remove timers during
    /// wake(), it won't conflict with our mutable borrow.
    pub(crate) fn process_timers(&mut self) -> (Option<Duration>, usize) {
        let now = Instant::now();

        // Advance the wheel to current time
        self.wheel.advance_to(now);

        // CRITICAL: Collect wakers BEFORE waking to avoid re-entrancy
        // If we wake while iterating, and the waker tries to insert/remove
        // a timer, we'll panic on borrow_mut() in the Reactor
        let expired: Vec<(u64, Waker)> = self.wheel.drain_expired().collect();

        // Clean up expiry time mappings
        for (internal_id, _) in &expired {
            self.forget(*internal_id);
        }

        // Now wake all timers (safe: no longer holding any mutable state)
        let woke = expired.len();
        for (_, waker) in expired {
            waker.wake();
        }

        // Find the next timer expiry
        let next_expiry = self.by_expiry.first().map(|(expires_at, _)| *expires_at);

        let next_duration = next_expiry.map(|expires_at| expires_at.saturating_duration_since(now));

        (next_duration, woke)
    }

    fn forget(&mut self, internal_id: u64) {
        if let Some(expires_at) = self.id_to_expiry.remove(&internal_id) {
            self.by_expiry.remove(&(expires_at, internal_id));
        }
    }

    /// Get the number of active timers
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.id_to_expiry.len()
    }

    /// Check if there are no active timers
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.id_to_expiry.is_empty()
    }
}

impl Default for ReactorTimers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: a waker that does nothing when woken.
    fn dummy_waker() -> Waker {
        Waker::noop().clone()
    }

    #[test]
    fn test_insert_and_process() {
        let mut timers = ReactorTimers::new();
        let now = Instant::now();

        // Insert a timer and get ID
        let _id = timers.insert(now + Duration::from_millis(100), dummy_waker());
        assert_eq!(timers.len(), 1);

        // Process before expiry - should not wake
        let (next, woke) = timers.process_timers();
        assert_eq!(woke, 0);
        assert!(next.is_some());
        assert_eq!(timers.len(), 1);

        // Wait and process after expiry
        std::thread::sleep(Duration::from_millis(150));
        let (_, woke) = timers.process_timers();
        assert_eq!(woke, 1);
        assert_eq!(timers.len(), 0);
    }

    #[test]
    fn test_remove() {
        let mut timers = ReactorTimers::new();
        let now = Instant::now();

        // Insert and get ID
        let id = timers.insert(now + Duration::from_millis(100), dummy_waker());
        assert_eq!(timers.len(), 1);

        // Remove the timer using ID
        assert!(timers.remove(id));
        assert_eq!(timers.len(), 0);

        // Try to remove again with same ID - should return false
        assert!(!timers.remove(id));
    }

    #[test]
    fn test_exists() {
        let mut timers = ReactorTimers::new();
        let now = Instant::now();

        // Insert and get ID
        let id = timers.insert(now + Duration::from_millis(100), dummy_waker());

        // Should exist with correct ID
        assert!(timers.exists(id));

        // Remove and check it no longer exists
        timers.remove(id);
        assert!(!timers.exists(id));
    }

    #[test]
    fn test_multiple_timers() {
        let mut timers = ReactorTimers::new();
        let now = Instant::now();

        // Insert multiple timers
        let id1 = timers.insert(now + Duration::from_millis(100), dummy_waker());
        let id2 = timers.insert(now + Duration::from_millis(200), dummy_waker());
        let id3 = timers.insert(now + Duration::from_millis(300), dummy_waker());

        assert_eq!(timers.len(), 3);

        // Remove one timer
        assert!(timers.remove(id2));
        assert_eq!(timers.len(), 2);

        // Verify correct timers remain
        assert!(timers.exists(id1));
        assert!(!timers.exists(id2));
        assert!(timers.exists(id3));
    }

    #[test]
    fn next_deadline_tracks_insertions_removals_and_expiry() {
        let mut timers = ReactorTimers::new();
        let now = Instant::now();
        let near = timers.insert(now + Duration::from_secs(10), dummy_waker());
        let _far = timers.insert(now + Duration::from_secs(30), dummy_waker());
        let _mid = timers.insert(now + Duration::from_secs(20), dummy_waker());

        let (next, _) = timers.process_timers();
        assert!(next.unwrap() <= Duration::from_secs(10));

        timers.remove(near);
        let (next, _) = timers.process_timers();
        let next = next.unwrap();
        assert!(next > Duration::from_secs(10) && next <= Duration::from_secs(20));

        let _expired = timers.insert(now, dummy_waker());
        let (next, woke) = timers.process_timers();
        assert_eq!(woke, 1);
        assert!(
            next.unwrap() > Duration::from_secs(10),
            "expired timer is forgotten"
        );
        assert_eq!(timers.len(), 2);
    }
}
