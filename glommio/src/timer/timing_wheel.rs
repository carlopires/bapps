// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
//! Hierarchical timing wheel over stable slab handles.
//!
//! Timers live in a [`TimerSlab`]; the wheel's slots hold [`SlotIndex`] values
//! pointing into it. Cascading therefore moves four-byte indices between slots
//! rather than whole entries, and a handle stays valid for a timer's entire
//! life however many times it cascades — which is what makes cancellation a
//! bare array index rather than a hash lookup.
//!
//! Each entry records where in the wheel it currently sits, so removal goes
//! straight there. Removing from a slot is a `swap_remove`, and the entry that
//! moves into the hole has its recorded position corrected; nothing is left
//! behind to be skipped later.

use super::slab::{SlotIndex, TimerId, TimerSlab};
use std::{
    collections::BTreeMap,
    task::Waker,
    time::{Duration, Instant},
};

/// Level 0: 256 slots x 1ms
const LEVEL_0_SLOTS: usize = 256;
/// Level 1: 64 slots x 256ms
const LEVEL_1_SLOTS: usize = 64;
const LEVEL_1_RESOLUTION_MS: u64 = 256;
/// Level 2: 64 slots x 16.384s
const LEVEL_2_SLOTS: usize = 64;
const LEVEL_2_RESOLUTION_MS: u64 = 16_384;
/// Level 3: 64 slots x 17.48min
const LEVEL_3_SLOTS: usize = 64;
const LEVEL_3_RESOLUTION_MS: u64 = 1_048_576;

/// Beyond 18 hours a timer waits in a `BTreeMap` instead of a slot.
const OVERFLOW_THRESHOLD_MS: u64 = 67_108_864;

/// Which slots of one level hold anything.
///
/// Sized for the widest level; the narrow ones simply never set the upper
/// bits. Uniform code costs 24 unused bytes per narrow level and saves a
/// second implementation of the same three operations.
#[derive(Debug, Default, Clone, Copy)]
struct SlotMask([u64; 4]);

impl SlotMask {
    fn set(&mut self, slot: usize) {
        self.0[slot / 64] |= 1 << (slot % 64);
    }

    fn clear(&mut self, slot: usize) {
        self.0[slot / 64] &= !(1 << (slot % 64));
    }

    fn is_empty(&self) -> bool {
        self.0 == [0; 4]
    }

    /// Slots occupied at or after `from`, wrapping once, searched in order.
    ///
    /// Returns how many slots ahead of `from` the first occupied one lies, so
    /// the caller can turn that into a tick without knowing the layout.
    fn distance_to_next(&self, from: usize, slots: usize) -> Option<usize> {
        (0..slots).find(|offset| {
            let slot = (from + offset) % slots;
            self.0[slot / 64] & (1 << (slot % 64)) != 0
        })
    }
}

/// Where an entry currently sits, so that cancelling it goes straight there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WheelPos {
    /// In a wheel slot, waiting for its level to come round.
    Slot {
        level: u8,
        slot: usize,
        index: usize,
    },
    /// Due, and waiting to be drained.
    Expired { index: usize },
    /// Too far out for the wheel; parked in the overflow map under `key`.
    Overflow { key: Instant, index: usize },
}

#[derive(Debug)]
struct TimerEntry {
    expires_at: Instant,
    waker: Waker,
    at: WheelPos,
}

/// Hierarchical timing wheel with stable handles.
#[derive(Debug)]
pub(crate) struct TimingWheel {
    current_tick: u64,
    start_time: Instant,

    /// Owns every entry. Slots below hold indices into this.
    slab: TimerSlab<TimerEntry>,

    /// Due, awaiting drain.
    expired: Vec<SlotIndex>,

    slots_1ms: Box<[Vec<SlotIndex>; LEVEL_0_SLOTS]>,
    slots_256ms: Box<[Vec<SlotIndex>; LEVEL_1_SLOTS]>,
    slots_16s: Box<[Vec<SlotIndex>; LEVEL_2_SLOTS]>,
    slots_17min: Box<[Vec<SlotIndex>; LEVEL_3_SLOTS]>,

    /// Which slots of each level hold anything, so that finding the next
    /// deadline and skipping empty time are both bounded by the number of
    /// levels rather than by the number of timers or by elapsed milliseconds.
    masks: [SlotMask; 4],

    /// Past the wheel's reach. Cold.
    overflow: BTreeMap<Instant, Vec<SlotIndex>>,

    /// Ticks actually processed. The point of the masks is that this stays
    /// unrelated to elapsed time, which is only assertable by counting.
    #[cfg(test)]
    ticks_processed: u64,
}

impl TimingWheel {
    pub(crate) fn new() -> Self {
        Self::new_at(Instant::now())
    }

    pub(crate) fn new_at(start_time: Instant) -> Self {
        Self {
            current_tick: 0,
            start_time,
            slab: TimerSlab::new(),
            expired: Vec::new(),
            slots_1ms: Box::new(std::array::from_fn(|_| Vec::new())),
            slots_256ms: Box::new(std::array::from_fn(|_| Vec::new())),
            slots_16s: Box::new(std::array::from_fn(|_| Vec::new())),
            slots_17min: Box::new(std::array::from_fn(|_| Vec::new())),
            masks: [SlotMask::default(); 4],
            overflow: BTreeMap::new(),
            #[cfg(test)]
            ticks_processed: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slab.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn current_time(&self) -> Instant {
        self.start_time + Duration::from_millis(self.current_tick)
    }

    /// Earliest deadline held, if any.
    ///
    /// Never later than the truth. Sleeping past a deadline is a bug; waking
    /// early and finding nothing due costs one poll.
    pub(crate) fn next_expiry(&self) -> Option<Instant> {
        if !self.expired.is_empty() {
            return Some(self.current_time());
        }

        // Level 0 holds everything due within the next 256 ticks, and a
        // level-0 slot names exactly one tick, so if anything is there it is
        // the earliest deadline and the answer is exact.
        let from = ((self.current_tick + 1) % LEVEL_0_SLOTS as u64) as usize;
        if let Some(offset) = self.masks[0].distance_to_next(from, LEVEL_0_SLOTS) {
            // The slot names a tick, but the entries in it carry the deadline
            // the caller actually asked for -- somewhere in the millisecond
            // ending at that tick. Reporting the tick would round every sleep
            // up to the wheel's resolution, which is the whole millisecond for
            // a caller who asked for a hundred microseconds. The slot is
            // small, so take the real minimum from it.
            let slot = (from + offset) % LEVEL_0_SLOTS;
            return self.earliest_in(&self.slots_1ms[slot]);
        }

        // Otherwise the next thing that must happen is a cascade. The
        // deadlines behind it are later than that boundary, so this is early
        // rather than exact -- which is the safe direction.
        self.next_cascade_tick()
            .map(|tick| self.tick_to_instant(tick))
            .or_else(|| self.overflow.keys().next().copied())
    }

    /// The earliest deadline among the entries a slot holds.
    fn earliest_in(&self, slot: &[SlotIndex]) -> Option<Instant> {
        slot.iter()
            .filter_map(|index| self.slab.id_at(*index))
            .filter_map(|id| self.slab.get(id))
            .map(|entry| entry.expires_at)
            .min()
    }

    fn tick_to_instant(&self, tick: u64) -> Instant {
        self.start_time + Duration::from_millis(tick)
    }

    /// The next tick at which a coarser level must be broken down, if any
    /// coarser level holds anything.
    fn next_cascade_tick(&self) -> Option<u64> {
        [
            (1usize, LEVEL_1_RESOLUTION_MS),
            (2, LEVEL_2_RESOLUTION_MS),
            (3, LEVEL_3_RESOLUTION_MS),
        ]
        .into_iter()
        .filter(|(level, _)| !self.masks[*level].is_empty())
        .map(|(_, resolution)| (self.current_tick / resolution + 1) * resolution)
        .min()
    }

    /// The next tick at or before `limit` at which the wheel has work.
    fn next_work_tick(&self, limit: u64) -> Option<u64> {
        let from = ((self.current_tick + 1) % LEVEL_0_SLOTS as u64) as usize;
        let level_0 = self.masks[0]
            .distance_to_next(from, LEVEL_0_SLOTS)
            .map(|offset| self.current_tick + 1 + offset as u64);

        [level_0, self.next_cascade_tick()]
            .into_iter()
            .flatten()
            .min()
            .filter(|tick| *tick <= limit)
    }

    /// Register a timer, returning a handle valid until it fires or is removed.
    pub(crate) fn insert(&mut self, expires_at: Instant, waker: Waker) -> TimerId {
        let id = self.slab.insert(TimerEntry {
            expires_at,
            waker,
            // Corrected below, once the entry has somewhere to be.
            at: WheelPos::Expired { index: usize::MAX },
        });
        self.place(id);
        id
    }

    /// Withdraw a timer. `false` if it has already fired or already gone.
    pub(crate) fn remove(&mut self, id: TimerId) -> bool {
        let Some(entry) = self.slab.get(id) else {
            return false;
        };
        let at = entry.at;
        self.unlink(at);
        self.slab.remove(id).is_some()
    }

    /// Move time forward, expiring whatever has come due.
    ///
    /// Cost is proportional to the ticks actually crossed. Skipping empty
    /// spans is a separate change; see [`Self::next_expiry`].
    pub(crate) fn advance_to(&mut self, now: Instant) {
        if now <= self.start_time {
            return;
        }

        let target_tick = now
            .duration_since(self.start_time)
            .as_millis()
            .min(u64::MAX as u128) as u64;

        // Step to the next tick that has work rather than through every
        // millisecond between. An executor that idles for ten minutes with a
        // populated wheel would otherwise pay six hundred thousand iterations
        // on its next poll, for time in which nothing was due.
        while self.current_tick < target_tick {
            match self.next_work_tick(target_tick) {
                Some(tick) => {
                    self.current_tick = tick - 1;
                    self.tick();
                }
                None => {
                    self.current_tick = target_tick;
                    break;
                }
            }
        }

        self.expire_due_before(now);
        self.check_overflow();
    }

    /// Expire entries in the next slot whose real deadline has already passed.
    ///
    /// Deadlines round up to a whole tick so nothing fires early, which puts a
    /// timer due at 1.7ms in tick 2 -- and the tick sweep alone would not
    /// reach it until 2.0ms. `next_expiry` reports 1.7ms, so the reactor wakes
    /// then, and this is what finds it. Without it the wheel's resolution
    /// becomes a floor under every sleep.
    fn expire_due_before(&mut self, now: Instant) {
        let slot = ((self.current_tick + 1) % LEVEL_0_SLOTS as u64) as usize;

        // Cheap check first: the slot is usually empty, and when it is not,
        // usually nothing in it is due yet.
        let any_due = self.slots_1ms[slot]
            .iter()
            .filter_map(|index| self.slab.id_at(*index))
            .filter_map(|id| self.slab.get(id))
            .any(|entry| entry.expires_at <= now);
        if !any_due {
            return;
        }

        let held = std::mem::take(&mut self.slots_1ms[slot]);
        let mut retained = Vec::with_capacity(held.len());
        for index in held {
            let Some(id) = self.slab.id_at(index) else {
                continue;
            };
            let due = self
                .slab
                .get(id)
                .is_some_and(|entry| entry.expires_at <= now);

            if due {
                self.expired.push(index);
                let at = WheelPos::Expired {
                    index: self.expired.len() - 1,
                };
                self.record(id, at);
            } else {
                retained.push(index);
                let at = WheelPos::Slot {
                    level: 0,
                    slot,
                    index: retained.len() - 1,
                };
                self.record(id, at);
            }
        }

        if retained.is_empty() {
            self.masks[0].clear(slot);
        }
        self.slots_1ms[slot] = retained;
    }

    /// Take everything that has come due.
    pub(crate) fn drain_expired(&mut self) -> impl Iterator<Item = (TimerId, Waker)> {
        let drained: Vec<_> = std::mem::take(&mut self.expired)
            .into_iter()
            .filter_map(|slot| {
                let id = self.slab.id_at(slot)?;
                let entry = self.slab.remove(id)?;
                Some((id, entry.waker))
            })
            .collect();
        drained.into_iter()
    }

    // ---- placement -------------------------------------------------------

    /// Put an already-stored entry wherever its deadline says it belongs, and
    /// record where that was.
    fn place(&mut self, id: TimerId) {
        let expires_at = self
            .slab
            .get(id)
            .expect("caller just stored this entry")
            .expires_at;

        let deadline_ms = match expires_at.checked_duration_since(self.start_time) {
            // Round up. `as_millis` truncates, which places a timer due at
            // 1.7ms in tick 1 and fires it 0.7ms early -- before its own
            // deadline, so the future polls, finds itself not ready, and has to
            // arm again. A timer may fire late; it may never fire early.
            Some(duration) => duration
                .as_nanos()
                .div_ceil(1_000_000)
                .min(u64::MAX as u128) as u64,
            // Already in the past.
            None => return self.mark_expired(id),
        };

        if deadline_ms <= self.current_tick {
            return self.mark_expired(id);
        }

        let ticks_until_expiry = deadline_ms - self.current_tick;
        let slot = id.slot();

        let at = if ticks_until_expiry >= OVERFLOW_THRESHOLD_MS {
            let bucket = self.overflow.entry(expires_at).or_default();
            bucket.push(slot);
            WheelPos::Overflow {
                key: expires_at,
                index: bucket.len() - 1,
            }
        } else {
            let (level, s) = if ticks_until_expiry < LEVEL_1_RESOLUTION_MS {
                (0u8, (deadline_ms % LEVEL_0_SLOTS as u64) as usize)
            } else if ticks_until_expiry < LEVEL_2_RESOLUTION_MS {
                (
                    1,
                    ((deadline_ms / LEVEL_1_RESOLUTION_MS) % LEVEL_1_SLOTS as u64) as usize,
                )
            } else if ticks_until_expiry < LEVEL_3_RESOLUTION_MS {
                (
                    2,
                    ((deadline_ms / LEVEL_2_RESOLUTION_MS) % LEVEL_2_SLOTS as u64) as usize,
                )
            } else {
                (
                    3,
                    ((deadline_ms / LEVEL_3_RESOLUTION_MS) % LEVEL_3_SLOTS as u64) as usize,
                )
            };

            let bucket = self.bucket_mut(level, s);
            bucket.push(slot);
            let index = bucket.len() - 1;
            self.masks[level as usize].set(s);
            WheelPos::Slot {
                level,
                slot: s,
                index,
            }
        };

        self.record(id, at);
    }

    fn mark_expired(&mut self, id: TimerId) {
        self.expired.push(id.slot());
        let at = WheelPos::Expired {
            index: self.expired.len() - 1,
        };
        self.record(id, at);
    }

    fn record(&mut self, id: TimerId, at: WheelPos) {
        if let Some(entry) = self.slab.get_mut(id) {
            entry.at = at;
        }
    }

    /// Detach an entry from wherever it sits, correcting whatever moves into
    /// the hole it leaves.
    fn unlink(&mut self, at: WheelPos) {
        let (moved_slot, corrected) = match at {
            WheelPos::Slot { level, slot, index } => {
                let bucket = self.bucket_mut(level, slot);
                if index >= bucket.len() {
                    return;
                }
                bucket.swap_remove(index);
                let moved = bucket.get(index).copied();
                let emptied = bucket.is_empty();
                if emptied {
                    self.masks[level as usize].clear(slot);
                }
                (moved, WheelPos::Slot { level, slot, index })
            }
            WheelPos::Expired { index } => {
                if index >= self.expired.len() {
                    return;
                }
                self.expired.swap_remove(index);
                let moved = self.expired.get(index).copied();
                (moved, WheelPos::Expired { index })
            }
            WheelPos::Overflow { key, index } => {
                let Some(bucket) = self.overflow.get_mut(&key) else {
                    return;
                };
                if index >= bucket.len() {
                    return;
                }
                bucket.swap_remove(index);
                let moved = bucket.get(index).copied();
                if bucket.is_empty() {
                    self.overflow.remove(&key);
                }
                (moved, WheelPos::Overflow { key, index })
            }
        };

        // `swap_remove` moved the last element into the hole. Its recorded
        // position now lies, so correct it before anything reads it.
        if let Some(moved) = moved_slot {
            if let Some(id) = self.slab.id_at(moved) {
                self.record(id, corrected);
            }
        }
    }

    fn bucket_mut(&mut self, level: u8, slot: usize) -> &mut Vec<SlotIndex> {
        match level {
            0 => &mut self.slots_1ms[slot],
            1 => &mut self.slots_256ms[slot],
            2 => &mut self.slots_16s[slot],
            3 => &mut self.slots_17min[slot],
            _ => unreachable!("wheel has four levels"),
        }
    }

    // ---- time ------------------------------------------------------------

    fn tick(&mut self) {
        self.current_tick += 1;
        #[cfg(test)]
        {
            self.ticks_processed += 1;
        }

        let slot_0 = (self.current_tick % LEVEL_0_SLOTS as u64) as usize;
        self.expire_slot(slot_0);

        if self.current_tick.is_multiple_of(LEVEL_1_RESOLUTION_MS) {
            let s = ((self.current_tick / LEVEL_1_RESOLUTION_MS) % LEVEL_1_SLOTS as u64) as usize;
            self.cascade_slot(1, s);
        }
        if self.current_tick.is_multiple_of(LEVEL_2_RESOLUTION_MS) {
            let s = ((self.current_tick / LEVEL_2_RESOLUTION_MS) % LEVEL_2_SLOTS as u64) as usize;
            self.cascade_slot(2, s);
        }
        if self.current_tick.is_multiple_of(LEVEL_3_RESOLUTION_MS) {
            let s = ((self.current_tick / LEVEL_3_RESOLUTION_MS) % LEVEL_3_SLOTS as u64) as usize;
            self.cascade_slot(3, s);
        }

        self.check_overflow();
    }

    /// Everything in a level-0 slot is due.
    fn expire_slot(&mut self, slot: usize) {
        let due = std::mem::take(&mut self.slots_1ms[slot]);
        self.masks[0].clear(slot);
        for entry_slot in due {
            self.expired.push(entry_slot);
            let at = WheelPos::Expired {
                index: self.expired.len() - 1,
            };
            if let Some(id) = self.slab.id_at(entry_slot) {
                self.record(id, at);
            }
        }
    }

    /// Re-place a higher level's slot into finer levels.
    fn cascade_slot(&mut self, level: u8, slot: usize) {
        let moving = std::mem::take(self.bucket_mut(level, slot));
        self.masks[level as usize].clear(slot);
        for entry_slot in moving {
            if let Some(id) = self.slab.id_at(entry_slot) {
                self.place(id);
            }
        }
    }

    /// Pull overflow entries that have come within the wheel's reach.
    fn check_overflow(&mut self) {
        let threshold = self.current_time() + Duration::from_millis(OVERFLOW_THRESHOLD_MS);

        let due: Vec<Instant> = self
            .overflow
            .range(..=threshold)
            .map(|(key, _)| *key)
            .collect();
        if due.is_empty() {
            return;
        }

        for key in due {
            if let Some(bucket) = self.overflow.remove(&key) {
                for entry_slot in bucket {
                    if let Some(id) = self.slab.id_at(entry_slot) {
                        self.place(id);
                    }
                }
            }
        }
    }
}

impl Default for TimingWheel {
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
    fn a_sub_tick_deadline_is_reported_and_expired_at_its_real_time() {
        // The wheel's resolution must not become a floor under every sleep.
        // A caller asking for 300us gets 300us, not the millisecond the tick
        // it landed in ends at.
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);
        wheel.insert(start + Duration::from_micros(300), dummy_waker());

        assert_eq!(
            wheel.next_expiry(),
            Some(start + Duration::from_micros(300)),
            "reported the tick boundary instead of the deadline asked for"
        );

        wheel.advance_to(start + Duration::from_micros(300));
        assert_eq!(wheel.drain_expired().count(), 1, "due at 300us");
    }

    #[test]
    fn a_sub_tick_deadline_still_does_not_expire_early() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);
        wheel.insert(start + Duration::from_micros(300), dummy_waker());

        wheel.advance_to(start + Duration::from_micros(299));
        assert_eq!(wheel.drain_expired().count(), 0, "not due at 299us");
    }

    #[test]
    fn crossing_idle_time_costs_nothing_when_there_is_no_work() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // An hour of nothing. Stepping through it a millisecond at a time
        // would be 3.6 million iterations.
        wheel.advance_to(start + Duration::from_secs(3600));

        assert_eq!(wheel.ticks_processed, 0, "no work, so no ticks");
        assert_eq!(wheel.current_tick, 3_600_000, "but time still moved");
    }

    #[test]
    fn crossing_idle_time_costs_the_work_not_the_interval() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);
        wheel.insert(start + Duration::from_secs(3600), dummy_waker());

        wheel.advance_to(start + Duration::from_secs(3540));

        assert!(
            wheel.ticks_processed < 1_000,
            "processed {} ticks to cross 59 minutes holding one timer",
            wheel.ticks_processed
        );
        assert_eq!(wheel.drain_expired().count(), 0, "not due yet");
    }

    #[test]
    fn cancelling_the_last_timer_in_a_slot_empties_the_level() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);
        let id = wheel.insert(start + Duration::from_millis(50), dummy_waker());

        assert!(wheel.next_expiry().is_some(), "a deadline is pending");
        assert!(wheel.remove(id));
        assert_eq!(
            wheel.next_expiry(),
            None,
            "a slot whose last timer was cancelled must stop being reported \
             as occupied, or the reactor wakes for a timer that is not there"
        );

        wheel.advance_to(start + Duration::from_millis(50));
        assert_eq!(wheel.ticks_processed, 0, "and there was nothing to do");
    }

    #[test]
    fn a_deadline_between_ticks_does_not_expire_at_the_earlier_one() {
        // A deadline is rounded up to a whole tick, never down. Truncating
        // places a timer due at 1.7ms in tick 1 and expires it at 1.0ms --
        // before it is due. The future then finds itself not ready and arms
        // again, so one sleep costs several registrations.
        //
        // Checked here rather than through the executor because the reactor
        // sleeps until the true deadline when nothing else is running, which
        // hides it; and because inline storage compares instants exactly, so
        // it only appears once the staged wheel has promoted.
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);
        wheel.insert(start + Duration::from_micros(1_700), dummy_waker());

        wheel.advance_to(start + Duration::from_millis(1));
        assert_eq!(
            wheel.drain_expired().count(),
            0,
            "expired at 1ms a timer that is due at 1.7ms"
        );

        wheel.advance_to(start + Duration::from_millis(2));
        assert_eq!(wheel.drain_expired().count(), 1, "due by 2ms");
    }

    #[test]
    fn test_basic_insert_and_expire() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timer expiring in 100ms
        let id = wheel.insert(start + Duration::from_millis(100), dummy_waker());

        assert_eq!(wheel.len(), 1);

        // Advance past expiry
        wheel.advance_to(start + Duration::from_millis(100));

        // Should have one expired timer
        let expired: Vec<_> = wheel.drain_expired().collect();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, id);

        // Wheel should be empty now
        assert_eq!(wheel.len(), 0);
    }

    #[test]
    fn test_remove_timer() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        let id = wheel.insert(start + Duration::from_millis(100), dummy_waker());
        assert_eq!(wheel.len(), 1);

        // Remove the timer
        assert!(wheel.remove(id));
        assert_eq!(wheel.len(), 0);

        // Advance time - should not expire
        wheel.advance_to(start + Duration::from_millis(100));
        assert_eq!(wheel.drain_expired().count(), 0);
    }

    #[test]
    fn test_multiple_timers_same_slot() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert 3 timers at same expiry
        let id1 = wheel.insert(start + Duration::from_millis(50), dummy_waker());
        let id2 = wheel.insert(start + Duration::from_millis(50), dummy_waker());
        let id3 = wheel.insert(start + Duration::from_millis(50), dummy_waker());

        assert_eq!(wheel.len(), 3);

        wheel.advance_to(start + Duration::from_millis(50));

        let expired: Vec<_> = wheel.drain_expired().map(|(id, _)| id).collect();
        assert_eq!(expired.len(), 3);
        assert!(expired.contains(&id1));
        assert!(expired.contains(&id2));
        assert!(expired.contains(&id3));
    }

    #[test]
    fn test_timer_ordering() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timers in reverse order
        wheel.insert(start + Duration::from_millis(30), dummy_waker());
        wheel.insert(start + Duration::from_millis(10), dummy_waker());
        wheel.insert(start + Duration::from_millis(20), dummy_waker());

        // Advance to 10ms
        wheel.advance_to(start + Duration::from_millis(10));
        assert_eq!(wheel.drain_expired().count(), 1);

        // Advance to 20ms
        wheel.advance_to(start + Duration::from_millis(20));
        assert_eq!(wheel.drain_expired().count(), 1);

        // Advance to 30ms
        wheel.advance_to(start + Duration::from_millis(30));
        assert_eq!(wheel.drain_expired().count(), 1);
    }

    #[test]
    fn test_cascading_level_1_to_0() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timer at 500ms (will be in Level 1)
        let id = wheel.insert(start + Duration::from_millis(500), dummy_waker());

        // Advance to just before Level 1 cascade (255ms)
        wheel.advance_to(start + Duration::from_millis(255));
        assert_eq!(wheel.drain_expired().count(), 0);

        // Advance to 256ms - should cascade from Level 1 to Level 0
        wheel.advance_to(start + Duration::from_millis(256));
        assert_eq!(wheel.drain_expired().count(), 0); // Not expired yet

        // Advance to 500ms - should expire
        wheel.advance_to(start + Duration::from_millis(500));
        let expired: Vec<_> = wheel.drain_expired().collect();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, id);
    }

    #[test]
    fn test_long_duration_timer() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timer at 1 hour (will be in Level 3)
        let id = wheel.insert(start + Duration::from_secs(3600), dummy_waker());

        assert_eq!(wheel.len(), 1);

        // Advance to expiry
        wheel.advance_to(start + Duration::from_secs(3600));

        let expired: Vec<_> = wheel.drain_expired().collect();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, id);
    }

    #[test]
    fn test_overflow_to_btreemap() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timer at 24 hours (should overflow to BTreeMap)
        let id = wheel.insert(start + Duration::from_secs(86400), dummy_waker());

        assert_eq!(wheel.len(), 1);

        // Advance to expiry
        wheel.advance_to(start + Duration::from_secs(86400));

        let expired: Vec<_> = wheel.drain_expired().collect();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, id);
    }

    #[test]
    fn test_past_timer_expires_immediately() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timer in the past
        wheel.insert(start - Duration::from_millis(100), dummy_waker());

        // Should expire immediately
        assert_eq!(wheel.drain_expired().count(), 1);
    }

    #[test]
    fn test_wrap_around() {
        let start = Instant::now();
        let mut wheel = TimingWheel::new_at(start);

        // Insert timer at slot 10
        let id1 = wheel.insert(start + Duration::from_millis(10), dummy_waker());

        // Advance to slot 260 (wraps around Level 0)
        wheel.advance_to(start + Duration::from_millis(260));

        // Insert another timer at slot 10 (should be different from id1)
        let id2 = wheel.insert(start + Duration::from_millis(260 + 10), dummy_waker());

        assert_ne!(id1, id2);

        // First timer should have expired
        assert!(wheel.drain_expired().any(|(id, _)| id == id1));

        // Advance to second timer
        wheel.advance_to(start + Duration::from_millis(270));
        assert!(wheel.drain_expired().any(|(id, _)| id == id2));
    }
}
