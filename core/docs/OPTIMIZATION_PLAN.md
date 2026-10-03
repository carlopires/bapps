# Glommio Performance Optimization Implementation Plan

> ## ⚠️ Partly superseded — read [PERFORMANCE_NEXT.md](PERFORMANCE_NEXT.md) first
>
> Written before any of it was measured. What happened to each priority:
>
> - **P1 Hierarchical Timing Wheel — done.** 17.7 → 10.3 ns at 100k timers,
>   zero unsafe. The plan below was sound.
> - **P2a/P2b RefCell caching and `UnsafeCell` — do not do this.** P2b claims
>   10-15% of a task switch and rates its own unsoundness Critical. A `RefCell`
>   borrow_mut plus drop **measures 0.449 ns** against a 28.72 ns task switch, so
>   removing every borrow on the path cannot reach 10%. Meanwhile 58% of that
>   switch was in atomics removable with no unsafe at all. See
>   [investigations/mechanical-sympathy](investigations/mechanical-sympathy/).
> - **P3 Adaptive I/O submission — premise not supported.** The I/O path was
>   measured and is device-dominated: glommio adds ~2 µs per *blocking*
>   operation, 6% of an NVMe read, and it amortises to nothing at queue depth.
>   See [investigations/io-path](investigations/io-path/).
> - **P4/P5 Soft preemption, cross-thread wakeups — unmeasured.** The wake path
>   was measured separately and is worth ~30% of a wake primitive, which is ~9%
>   of a cross-shard round trip and only when a shard parks.
>
> The timing-wheel sections remain accurate and useful. Treat the rest as a
> record of what was believed before measurement, not as a plan.

**Status:** Planning Phase
**Target:** Close performance gap with Monoio
**Timeline:** 4-6 months
**Last Updated:** 2026-02-14

## Executive Summary

This document provides a detailed implementation plan for five critical performance optimizations identified through analysis of Glommio's architecture and comparison with Monoio. These optimizations target the scheduler, timer management, I/O submission, and memory management systems.

**Expected Overall Impact:** 30-50% performance improvement across key metrics (throughput, P99 latency, timer scaling)

## Related Documents

- [Performance Roadmap](PERFORMANCE_ROADMAP.md) - High-level strategy and phases
- [Implementation Plan](OPTIMIZATION_PLAN.md) - This document - detailed execution plan

---

## Optimization Overview

| Priority | Optimization | Impact | Effort | Risk | Expected Gain | Phase |
|----------|--------------|--------|--------|------|---------------|-------|
| 🔴 P1 | Timing Wheel | High | Medium | Low | 10-20% timer perf | Phase 1 |
| 🟡 P2a | RefCell Cache | Med-High | Low | Low | 5-10% task switch | Phase 1 |
| 🟡 P2b | RefCell Unsafe | Med-High | High | High | 10-15% task switch | Phase 2 |
| 🟡 P3 | Adaptive I/O | Medium | Medium | Low | 10-25% P99 latency | Phase 2 |
| 🟢 P4 | Soft Preempt | Medium | Very High | Very High | 20-40% P99 mixed | Phase 3 |
| 🟢 P5 | Wake Optimize | Low-Med | Medium | Low | 5-10% actor | Phase 2 |

---

# Priority 1: Hierarchical Timing Wheel

## Problem Statement

**Current Implementation:**
- File: `glommio/src/reactor.rs` lines 79-142
- Data Structure: `BTreeMap<(Instant, u64), Waker>`
- Complexity: O(log n) insert/remove, O(k log n) for processing k timers
- Issue: `split_off()` called on every reactor poll to separate ready timers

**Performance Impact:**
- Degrades linearly with concurrent connections (each connection = multiple timers)
- At 100K connections: ~log2(300K) ≈ 18 operations per timer operation
- Monoio uses O(1) timing wheel, giving 5-10x advantage at scale

## Proposed Solution

### Architecture: 4-Level Hierarchical Timing Wheel

```rust
/// Hierarchical timing wheel with O(1) operations for most timers
pub struct TimingWheel {
    /// Level 0: 256 slots × 1ms = 0-255ms range
    slots_1ms: [Vec<TimerEntry>; 256],

    /// Level 1: 64 slots × 256ms = 256ms-16s range
    slots_256ms: [Vec<TimerEntry>; 64],

    /// Level 2: 64 slots × 16s = 16s-17min range
    slots_16s: [Vec<TimerEntry>; 64],

    /// Level 3: 64 slots × 17min = 17min-18hours range
    slots_17min: [Vec<TimerEntry>; 64],

    /// Overflow: BTreeMap for timers > 18 hours (rare)
    overflow: BTreeMap<Instant, Vec<TimerEntry>>,

    /// Current position in wheel
    current_tick: u64,

    /// Base time (when tick 0 started)
    start_time: Instant,

    /// Timer ID allocator
    next_id: u64,

    /// Fast lookup: timer_id → (level, slot, index)
    index: AHashMap<u64, TimerLocation>,
}

struct TimerEntry {
    id: u64,
    expires_at: Instant,
    waker: Waker,
}

struct TimerLocation {
    level: u8,
    slot: usize,
    index_in_slot: usize,
}
```

### Algorithm Details

**Insertion:**
1. Calculate deadline relative to `current_tick`
2. Determine which level based on deadline:
   - < 256ms → Level 0 (1ms resolution)
   - < 16s → Level 1 (256ms resolution)
   - < 17min → Level 2 (16s resolution)
   - < 18hr → Level 3 (17min resolution)
   - Else → Overflow BTreeMap
3. Calculate slot: `(current_tick + deadline_ticks) % slots_in_level`
4. Push to `slots[level][slot]`, store location in index
5. **Complexity:** O(1)

**Removal:**
1. Lookup location in index: O(1)
2. Swap-remove from slot vector: O(1)
3. Update index for swapped element
4. **Complexity:** O(1)

**Processing (called every 1ms tick):**
1. Advance `current_tick`
2. Process Level 0: `slots_1ms[current_tick % 256]`
   - Wake all timers in slot: O(k) for k timers
   - Clear slot
3. Every 256 ticks: cascade from Level 1 → Level 0
   - Move timers from `slots_256ms[tick / 256]` down to Level 0
4. Similarly cascade Level 2 → Level 1, Level 3 → Level 2
5. **Complexity:** O(k) where k = timers expiring this tick

**Cascading:**
When advancing a level boundary, timers from higher level need re-insertion:
```rust
fn cascade(&mut self, from_level: usize) {
    let slot = self.slot_for_level(from_level);
    for timer in self.slots[from_level][slot].drain(..) {
        self.reinsert_at_lower_level(timer);
    }
}
```

### Implementation Plan

#### Step 1: Create New Module (Week 1)
**File:** `glommio/src/timer/timing_wheel.rs`

**Tasks:**
- [ ] Define `TimingWheel` struct
- [ ] Implement `new()`, `insert()`, `remove()`, `tick()`
- [ ] Add comprehensive unit tests:
  - Single timer insertion/removal
  - Multiple timers same slot
  - Cascading between levels
  - Overflow to BTreeMap
  - Edge cases (wraparound, etc.)
- [ ] Property-based testing with `proptest`:
  ```rust
  proptest! {
      fn timers_always_fire_after_deadline(timers: Vec<(u64, Duration)>) {
          // Verify no timer fires early
      }
  }
  ```

**Success Criteria:**
- [ ] All unit tests pass
- [ ] No panics in fuzzing (100M operations)
- [ ] Correct timer ordering maintained

#### Step 2: Integration (Week 2)
**File:** `glommio/src/reactor.rs`

**Tasks:**
- [ ] Add feature flag `timing-wheel` to `Cargo.toml`
- [ ] Wrap existing `Timers` in `#[cfg(not(feature = "timing-wheel"))]`
- [ ] Create new `Timers` implementation using `TimingWheel`:
  ```rust
  #[cfg(feature = "timing-wheel")]
  pub(crate) struct Timers {
      wheel: TimingWheel,
      // Keep HashMap for compat with existing timer ID lookup
      id_to_when: AHashMap<u64, Instant>,
  }
  ```
- [ ] Implement same public API as old `Timers`
- [ ] Update `Reactor::process_timers()` to call `wheel.tick()`

**Success Criteria:**
- [ ] Existing timer tests pass with new implementation
- [ ] No behavioral changes observed in integration tests

#### Step 3: Benchmarking (Week 3)
**File:** `glommio/benches/timer_benchmark.rs`

**Benchmark Suite:**
```rust
#[bench]
fn bench_timer_insertion(b: &mut Bencher) {
    // Measure: insert 10K timers
}

#[bench]
fn bench_timer_removal(b: &mut Bencher) {
    // Measure: insert 10K, remove random 5K
}

#[bench]
fn bench_timer_processing(b: &mut Bencher) {
    // Measure: process 1K expiring timers per tick
}

#[bench]
fn bench_timer_mixed_workload(b: &mut Bencher) {
    // Measure: realistic mix of insert/remove/process
}

#[bench]
fn bench_timer_many_connections(b: &mut Bencher) {
    // Measure: 100K connections, each with 3 timers (300K total)
}
```

**Comparison Metrics:**
- Insert latency: P50, P99, P999
- Remove latency: P50, P99, P999
- Process latency: total time to wake all expired timers
- Memory usage: bytes per timer
- Scaling: performance at 1K, 10K, 100K, 1M timers

**Target Goals:**
- [ ] Insert: <100ns P99 (vs ~500ns BTreeMap at 100K timers)
- [ ] Remove: <100ns P99
- [ ] Process: O(k) vs O(k log n), 5-10x faster
- [ ] Memory: <10% overhead vs BTreeMap

#### Step 4: Production Validation (Week 4)
**Tasks:**
- [ ] Run full Glommio test suite with `--features timing-wheel`
- [ ] Run eventfd_leak test for 24+ hours (stability)
- [ ] Compare against Monoio in timer-heavy benchmark:
  ```rust
  // Benchmark: HTTP server with 10K keepalive connections
  // Each connection: read timeout, write timeout, keepalive timeout
  // Measure: requests/sec, P99 latency, CPU usage
  ```
- [ ] Profile with `perf` to verify hotspot elimination

**Success Criteria:**
- [ ] Zero regressions in test suite
- [ ] 24hr stability test passes
- [ ] Timer operations no longer appear in `perf top`
- [ ] 10-20% improvement in timer-heavy workloads

#### Step 5: Rollout (Week 5)
- [ ] Make `timing-wheel` default in `Cargo.toml`
- [ ] Update documentation
- [ ] Keep old BTreeMap implementation behind `legacy-timers` flag
- [ ] Create migration guide for users (if API changes)

### Risk Mitigation

**Risk:** Cascading overhead at level boundaries
**Mitigation:** Spread cascade work across multiple ticks:
```rust
fn tick(&mut self) {
    // Instead of cascading all at once:
    if self.should_cascade() {
        self.cascade_partial(MAX_CASCADE_PER_TICK);
    }
}
```

**Risk:** Memory overhead from multiple levels
**Mitigation:** Use `SmallVec<[TimerEntry; 4]>` for slots (most slots empty or few timers)

**Risk:** Wraparound bugs at tick boundaries
**Mitigation:** Extensive property-based testing, fuzzing

### Performance Tracking

**Metrics Dashboard:**
- Timer insert P99 latency (before/after)
- Timer removal P99 latency
- CPU cycles per timer operation
- Memory bytes per timer
- Max timers sustainable (before degradation)

**Regression Detection:**
- Automated benchmark in CI
- Alert if P99 latency increases >10%
- Alert if memory usage increases >15%

---

# Priority 2a: RefCell Caching (Phase 1)

## Problem Statement

**Current Implementation:**
- File: `glommio/src/executor/mod.rs` line 1444
- Issue: Inner task loop calls `queue_ref.borrow_mut()` before EVERY task
- Each borrow checks RefCell borrow state at runtime (even though single-threaded)

**Code:**
```rust
loop {
    if self.need_preempt() || queue_ref.yielded() { break; }
    if let Some(r) = queue_ref.get_task() {  // ← RefCell::borrow_mut() here!
        r.run();
    } else { break; }
}
```

**Performance Impact:**
- At 1M tasks/sec: 1M RefCell borrow checks
- Each check: ~5-10ns (atomic read + bounds check)
- Total overhead: 5-10ms per second of wasted CPU

## Proposed Solution (Safe)

### Cache Borrow Across Loop

```rust
// Before: borrow on every iteration
loop {
    if self.need_preempt() || queue_ref.yielded() { break; }
    if let Some(r) = queue_ref.get_task() {
        r.run();
    } else { break; }
}

// After: borrow once, release during task execution
let mut queue = queue_ref.borrow_mut();
loop {
    if self.need_preempt() || queue.yielded() { break; }

    // Get task while holding borrow
    if let Some(r) = queue.get_task() {
        // CRITICAL: Drop borrow before running task
        // (task might need to borrow other queues)
        drop(queue);

        r.run();

        // Re-acquire borrow after task completes
        queue = queue_ref.borrow_mut();
    } else {
        break;
    }
}
```

**Key Insight:** We only need the borrow to call `get_task()`. Release it during `r.run()` to avoid conflicts.

### Implementation Plan

#### Step 1: Refactor Inner Loop (Week 1)
**File:** `glommio/src/executor/mod.rs` lines 1440-1480

**Tasks:**
- [ ] Identify all RefCell borrows in hot path
- [ ] Refactor to cache borrow outside loop
- [ ] Ensure borrow released before calling into user code
- [ ] Add comments explaining safety reasoning

**Code Locations:**
- Line 1444: Main task execution loop
- Line 1417, 1422: Queue selection logic
- Line 1462, 1475: Queue statistics updates

**Changes:**
```rust
fn run_one_task_queue(&self) -> bool {
    // ... queue selection logic ...

    // Cache the borrow for the entire inner loop
    let mut queue_guard = tq.borrow_mut();
    let queue_id = queue_guard.queue_id();  // Get immutable data

    loop {
        if self.need_preempt() || queue_guard.yielded() {
            break;
        }

        match queue_guard.get_task() {
            Some(runnable) => {
                // SAFETY: Drop borrow before executing user task
                // User task might spawn into other queues, causing borrows
                drop(queue_guard);

                runnable.run();

                // Re-acquire borrow for next iteration
                queue_guard = tq.borrow_mut();
            }
            None => break,
        }
    }

    // Update statistics before final drop
    let executed = queue_guard.tasks_executed();
    drop(queue_guard);

    executed > 0
}
```

#### Step 2: Testing (Week 1)
**Tasks:**
- [ ] Run full test suite - verify no deadlocks
- [ ] Run under ThreadSanitizer (detect borrow conflicts)
- [ ] Add stress test: spawn 1M tasks rapidly
- [ ] Verify no panics in "already borrowed" scenarios

**Test Case:**
```rust
#[test]
fn test_cached_borrow_no_conflicts() {
    let ex = LocalExecutor::default();
    ex.run(async {
        // Spawn tasks that spawn other tasks (nested borrows)
        for _ in 0..10000 {
            spawn_local(async {
                spawn_local(async { /* work */ }).detach();
            }).detach();
        }
    });
}
```

#### Step 3: Benchmarking (Week 2)
**File:** `glommio/benches/task_spawn_benchmark.rs`

**Benchmark:**
```rust
#[bench]
fn bench_task_spawn_rate(b: &mut Bencher) {
    let ex = LocalExecutor::default();
    ex.run(async {
        b.iter(|| {
            for _ in 0..1000 {
                spawn_local(async { /* minimal work */ }).detach();
            }
        });
    });
}
```

**Target Goals:**
- [ ] 5-10% improvement in task spawn rate
- [ ] Reduced CPU cycles in `borrow_mut()` (verify with perf)
- [ ] No regression in any test

### Risk Mitigation

**Risk:** Holding borrow during task execution causes "already borrowed" panic
**Mitigation:** Always drop borrow before `runnable.run()`

**Risk:** Re-acquiring borrow fails if queue was dropped
**Mitigation:** Check `Weak::upgrade()` or handle borrow failure gracefully

---

# Priority 2b: RefCell → UnsafeCell (Phase 2)

## Problem Statement

Even with caching, we still pay for RefCell's runtime borrow checking. In a **thread-per-core** architecture, this is provably unnecessary.

**Guarantee:** Each `TaskQueue` is only accessed from one thread (the executor's pinned thread).

## Proposed Solution (Unsafe)

### Replace RefCell with UnsafeCell

```rust
// Current: Runtime borrow checking
pub(crate) struct ExecutorQueues {
    active_executors: BinaryHeap<Rc<RefCell<TaskQueue>>>,
    // ...
}

// Proposed: Raw pointer access
pub(crate) struct ExecutorQueues {
    active_executors: BinaryHeap<Rc<UnsafeCell<TaskQueue>>>,
    // ...
}

impl ExecutorQueues {
    fn run_task_queue(&mut self, tq: &Rc<UnsafeCell<TaskQueue>>) -> bool {
        // SAFETY: Thread-per-core guarantee ensures no concurrent access
        // This executor owns this TaskQueue, no other thread can access it
        unsafe {
            let queue = &mut *tq.get();

            loop {
                if self.need_preempt() || queue.yielded() { break; }

                if let Some(runnable) = queue.get_task() {
                    runnable.run();
                } else {
                    break;
                }
            }

            queue.tasks_executed() > 0
        }
    }
}
```

### Safety Argument

**Invariant:** Each `LocalExecutor` is `!Send`, pinned to a single thread.

**Proof:**
1. `LocalExecutor` contains `Rc<RefCell<ExecutorQueues>>` (not `Arc`)
2. `Rc` is `!Send` → executor cannot move threads
3. `TaskQueue` is owned by `ExecutorQueues` (via `Rc<UnsafeCell<>>`)
4. Only the owning executor can access its `ExecutorQueues`
5. ∴ No concurrent access possible

**Boundary Conditions:**
- ✅ Task spawning: Uses `Weak<UnsafeCell<>>` from closure
- ✅ Cross-thread wakes: Only touches `Waker`, not `TaskQueue`
- ✅ Foreign executors: Check `executor_id()` before any access

### Implementation Plan

#### Step 1: Careful Refactor (Week 1-2)
**Tasks:**
- [ ] Identify ALL RefCell<TaskQueue> locations
- [ ] Create wrapper type to encapsulate safety:
  ```rust
  pub(crate) struct ThreadLocalQueue {
      inner: Rc<UnsafeCell<TaskQueue>>,
  }

  impl ThreadLocalQueue {
      /// SAFETY: Caller must ensure single-threaded access
      #[inline]
      pub(crate) unsafe fn get_mut(&self) -> &mut TaskQueue {
          &mut *self.inner.get()
      }
  }
  ```
- [ ] Replace RefCell with UnsafeCell systematically
- [ ] Add extensive SAFETY comments at every `get()` call

**Documentation:**
```rust
// SAFETY: This is safe because:
// 1. LocalExecutor is !Send (contains Rc, not Arc)
// 2. TaskQueue is only accessed from executor's thread
// 3. Thread-per-core model guarantees no concurrent access
// 4. Waker closures only hold Weak<UnsafeCell<>>, check upgrade()
unsafe {
    let queue = &mut *tq.get();
    // ... use queue ...
}
```

#### Step 2: Extensive Testing (Week 3)
**Tests:**
- [ ] All existing tests pass
- [ ] Miri validation (detects undefined behavior):
  ```bash
  cargo +nightly miri test --package glommio
  ```
- [ ] ThreadSanitizer (detects data races):
  ```bash
  cargo test --target x86_64-unknown-linux-gnu -- --test-threads=1
  ```
- [ ] 48-hour stability test under load
- [ ] Fuzzing with arbitrary task spawn patterns

**Stress Test:**
```rust
#[test]
fn test_unsafe_cell_stress() {
    for _ in 0..1000 {
        let ex = LocalExecutor::default();
        ex.run(async {
            let handles: Vec<_> = (0..10000)
                .map(|_| spawn_local(async { /* work */ }))
                .collect();

            for h in handles {
                h.await;
            }
        });
    }
}
```

#### Step 3: Performance Validation (Week 4)
**Benchmarks:**
- [ ] Task spawn rate: expect 10-15% improvement
- [ ] Task switch latency: expect 10-15% improvement
- [ ] CPU cycles in scheduler: expect 20% reduction

**Measurement:**
```bash
# Before
perf stat -e cycles,instructions cargo bench task_spawn

# After
perf stat -e cycles,instructions cargo bench task_spawn

# Compare IPC (instructions per cycle)
```

**Target Goals:**
- [ ] 10-15% higher task spawn rate
- [ ] Scheduler overhead reduced by 20%
- [ ] No unsafe-related bugs in 48hr test

### Risk Mitigation

**Risk:** Unsound memory access causing UB
**Mitigation:**
- Extensive Miri testing
- Formal proof of safety invariant
- Conservative rollout (behind feature flag initially)

**Risk:** Future refactors break safety invariant
**Mitigation:**
- Document invariant in module-level comments
- Add compile-time assertions where possible:
  ```rust
  const _: () = assert!(!std::mem::needs_drop::<TaskQueue>());
  ```

**Risk:** Difficult to review/maintain
**Mitigation:**
- Create safety abstraction (ThreadLocalQueue wrapper)
- Centralize all unsafe access in one module
- Document every unsafe block thoroughly

---

# Priority 3: Adaptive I/O Submission

## Problem Statement

**Current Implementation:**
- File: `glommio/src/sys/uring.rs` lines 698-733
- Strategy: Submit when ring full OR queue empty
- No workload adaptation
- "Rush dispatch" bypasses batching entirely (lines 796-798)

**Issues:**
- High-latency workloads: Delayed submission hurts P99
- Bursty workloads: First operation waits for full batch
- No learning from observed latencies

## Proposed Solution

### Adaptive Submission Policy

```rust
pub struct AdaptiveSubmissionPolicy {
    /// Target batch size (dynamically adjusted)
    target_batch_size: usize,

    /// Maximum wait time before forcing submission (µs)
    max_wait_us: u64,

    /// Recent completion latencies (ring buffer)
    recent_latencies: RingBuffer<u64, 128>,

    /// Submissions since last wait
    submissions_since_wait: usize,

    /// Current mode
    mode: SubmissionMode,

    /// Last adjustment timestamp
    last_adjustment: Instant,
}

enum SubmissionMode {
    /// Prioritize latency (small batches)
    LowLatency { batch_size: usize },

    /// Prioritize throughput (large batches)
    HighThroughput { batch_size: usize },

    /// Balanced (adaptive)
    Balanced,
}

impl AdaptiveSubmissionPolicy {
    fn should_submit(&mut self, queue_depth: usize, elapsed_us: u64) -> bool {
        match self.mode {
            SubmissionMode::LowLatency { batch_size } => {
                // Submit aggressively
                queue_depth >= batch_size || elapsed_us > 100
            }
            SubmissionMode::HighThroughput { batch_size } => {
                // Batch more
                queue_depth >= batch_size || elapsed_us > 1000
            }
            SubmissionMode::Balanced => {
                // Adapt based on recent latencies
                self.adaptive_decision(queue_depth, elapsed_us)
            }
        }
    }

    fn adaptive_decision(&mut self, queue_depth: usize, elapsed_us: u64) -> bool {
        let avg_latency = self.recent_latencies.average();

        if avg_latency > 10_000 {  // >10ms average
            // System under stress, submit eagerly
            queue_depth >= 1
        } else if avg_latency < 1_000 {  // <1ms average
            // System healthy, batch more
            queue_depth >= 32 || elapsed_us > 500
        } else {
            // Normal operation
            queue_depth >= 8 || elapsed_us > 200
        }
    }

    fn record_completion(&mut self, latency_us: u64) {
        self.recent_latencies.push(latency_us);

        // Adjust every 10ms
        if self.last_adjustment.elapsed() > Duration::from_millis(10) {
            self.adjust_mode();
            self.last_adjustment = Instant::now();
        }
    }

    fn adjust_mode(&mut self) {
        let p99 = self.recent_latencies.percentile(0.99);

        // If P99 latency high, switch to low-latency mode
        if p99 > 50_000 {  // >50ms P99
            self.mode = SubmissionMode::LowLatency { batch_size: 1 };
        }
        // If P99 latency low and throughput mode, increase batch size
        else if p99 < 1_000 && self.submissions_since_wait > 1000 {
            self.mode = SubmissionMode::HighThroughput { batch_size: 64 };
        }
        // Otherwise, stay balanced
        else {
            self.mode = SubmissionMode::Balanced;
        }
    }
}
```

### Integration

**Replace hardcoded logic in:**
- `consume_submission_queue()` (line 729)
- `rush_dispatch()` (line 796)

**New logic:**
```rust
fn consume_submission_queue(&mut self) -> io::Result<bool> {
    let start = Instant::now();
    let mut submitted = false;

    loop {
        let queue_depth = self.submission_queue.len();
        let elapsed_us = start.elapsed().as_micros() as u64;

        if self.submission_policy.should_submit(queue_depth, elapsed_us) {
            // Submit batch
            let submitted_count = self.submit_batch()?;
            self.submission_policy.record_submission(submitted_count);
            submitted = true;
            break;
        }

        if !self.submit_one_event()? {
            break;  // No more events to submit
        }
    }

    Ok(submitted)
}
```

### Implementation Plan

#### Step 1: Create Policy Module (Week 1)
**File:** `glommio/src/sys/submission_policy.rs`

**Tasks:**
- [ ] Implement `AdaptiveSubmissionPolicy`
- [ ] Add `RingBuffer<T, N>` helper for latency tracking
- [ ] Implement percentile calculation (P50, P99, P999)
- [ ] Unit tests for policy decisions

#### Step 2: Integrate with Reactor (Week 2)
**File:** `glommio/src/sys/uring.rs`

**Tasks:**
- [ ] Add `submission_policy: AdaptiveSubmissionPolicy` to Reactor
- [ ] Replace hardcoded submission logic
- [ ] Record completion latencies in CQ processing
- [ ] Add tracing instrumentation:
  ```rust
  tracing::debug!(
      batch_size = batch_size,
      latency_p99 = p99,
      mode = ?self.submission_policy.mode,
      "submission decision"
  );
  ```

#### Step 3: Tuning (Week 3)
**Benchmark Suite:**
```rust
#[bench]
fn bench_latency_sensitive_workload(b: &mut Bencher) {
    // Small random reads: <100 bytes
    // Measure: P99 latency
}

#[bench]
fn bench_throughput_workload(b: &mut Bencher) {
    // Large sequential writes: 1MB blocks
    // Measure: MB/s throughput
}

#[bench]
fn bench_bursty_workload(b: &mut Bencher) {
    // Alternate: 100ms idle, 100ms burst
    // Measure: First-operation latency after idle
}
```

**Tuning Knobs:**
- Latency thresholds (when to switch modes)
- Batch size ranges (min/max)
- Adjustment frequency (how often to recalibrate)

**Comparison Points:**
- Baseline: Current fixed-threshold strategy
- Monoio: Their adaptive strategy
- Tokio: Their submission strategy (if applicable)

#### Step 4: Production Validation (Week 4)
**Tasks:**
- [ ] Run under realistic workloads (HTTP server, DB)
- [ ] Monitor P99 latency improvements
- [ ] Check for throughput regressions
- [ ] 24hr stability test

**Target Goals:**
- [ ] 10-25% P99 latency improvement (latency-sensitive)
- [ ] No throughput regression (throughput-heavy)
- [ ] First-operation latency <500µs (after idle)

### Configuration

Expose tuning via environment variables:
```rust
// Conservative (low latency)
GLOMMIO_SUBMIT_MODE=low-latency

// Aggressive (high throughput)
GLOMMIO_SUBMIT_MODE=high-throughput

// Adaptive (default)
GLOMMIO_SUBMIT_MODE=balanced

// Manual tuning
GLOMMIO_SUBMIT_BATCH_SIZE=16
GLOMMIO_SUBMIT_MAX_WAIT_US=500
```

### Risk Mitigation

**Risk:** Adaptive logic adds overhead
**Mitigation:** Keep policy decisions O(1), cache percentile calculations

**Risk:** Mis-tuned defaults hurt common cases
**Mitigation:** Extensive benchmarking, conservative defaults, easy override

---

# Priority 4: Soft Preemption (Phase 3)

## Problem Statement

**Current:** Purely cooperative scheduling - tasks must yield voluntarily.

**Issue:** Single CPU-bound task can hog executor for milliseconds, blocking high-priority I/O tasks.

**Impact:** Poor P99/P999 latencies in mixed workloads (CPU + I/O).

## Proposed Solution

### Two-Phase Approach

#### Phase A: Compiler-Instrumented Yields (Safer)

Use proc-macro to inject yield points at loop backedges:

```rust
// User writes:
#[glommio::maybe_yield]
async fn cpu_intensive() {
    for i in 0..1_000_000 {
        expensive_work(i);
    }
}

// Macro expands to:
async fn cpu_intensive() {
    for i in 0..1_000_000 {
        if i % 1024 == 0 {
            glommio::yield_if_needed().await;
        }
        expensive_work(i);
    }
}
```

**Pros:**
- Safe (no UB risk)
- Explicit (users can see/control)
- Zero overhead when not needed

**Cons:**
- Requires user annotation
- Doesn't help legacy code
- Only works at explicit yield points

#### Phase B: Signal-Based Preemption (Riskier)

Use Linux signals to force preemption:

```rust
pub struct SoftPreemption {
    /// Signal handler installed
    signal_installed: AtomicBool,

    /// Flag set by signal handler
    need_yield: AtomicBool,

    /// Preemption interval (µs)
    interval_us: u64,
}

impl SoftPreemption {
    fn install(&mut self) -> io::Result<()> {
        // Install SIGALRM handler
        let handler = |_: c_int| {
            NEED_YIELD.store(true, Ordering::Release);
        };

        unsafe {
            signal_hook::low_level::register(SIGALRM, handler)?;
        }

        // Set interval timer
        let interval = libc::itimerval {
            it_interval: timeval_from_us(self.interval_us),
            it_value: timeval_from_us(self.interval_us),
        };

        unsafe {
            libc::setitimer(libc::ITIMER_REAL, &interval, std::ptr::null_mut());
        }

        Ok(())
    }

    fn should_preempt(&self) -> bool {
        self.need_yield.swap(false, Ordering::Acquire)
    }
}
```

**Integration:**
```rust
// In task execution loop:
loop {
    if self.need_preempt() || self.soft_preemption.should_preempt() {
        break;  // Yield to other tasks
    }

    if let Some(r) = queue.get_task() {
        r.run();
    } else {
        break;
    }
}
```

**Pros:**
- Works with existing code (no annotation)
- Forces yield even in tight loops
- Protects against "bad" tasks

**Cons:**
- Signal handlers are VERY tricky in async contexts
- Potential for deadlocks if signal arrives during borrow
- Platform-specific (Linux only)
- Hard to get right

### Implementation Plan

#### Phase A: Compiler Instrumentation (Weeks 1-3)

**Step 1: Create Proc-Macro (Week 1)**
**File:** `glommio-macros/src/lib.rs`

```rust
#[proc_macro_attribute]
pub fn maybe_yield(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let mut func = parse_macro_input!(item as ItemFn);

    // Parse attributes (yield interval)
    let interval: usize = attr.parse().unwrap_or(1024);

    // Inject yield points at loop backedges
    inject_yields(&mut func.block, interval);

    quote! { #func }.into()
}

fn inject_yields(block: &mut Block, interval: usize) {
    // Walk AST, find loop statements
    // Insert yield check every N iterations
}
```

**Step 2: Add yield_if_needed() API (Week 1)**
**File:** `glommio/src/executor/mod.rs`

```rust
/// Cooperatively yield to other tasks if needed
///
/// Checks if preemption is needed and yields if so
pub async fn yield_if_needed() {
    executor().yield_if_needed_impl().await
}

impl LocalExecutor {
    async fn yield_if_needed_impl(&self) {
        if self.need_preempt() {
            yield_now().await;
        }
    }
}
```

**Step 3: Testing (Week 2)**
- [ ] Test with annotated CPU-bound tasks
- [ ] Verify yields actually happen (instrumentation)
- [ ] Measure overhead of yield checks
- [ ] Benchmark latency improvement

**Step 4: Documentation (Week 3)**
- [ ] Document `#[maybe_yield]` usage
- [ ] Add examples
- [ ] Migration guide for existing code

**Target Goals:**
- [ ] Yield overhead <5ns per check
- [ ] P99 latency improvement: 20-30% in mixed workloads
- [ ] No throughput regression

#### Phase B: Signal-Based (Weeks 4-8) - DEFERRED

**Recommendation:** Do NOT implement Phase B unless:
1. Phase A proves insufficient
2. Extensive research into signal safety completed
3. Multiple async experts review design

**Why Defer:**
- Very high complexity
- High risk of subtle bugs
- Signal handlers + async = danger
- Limited benefit over Phase A

### Risk Mitigation

**Risk:** Yield checks add overhead
**Mitigation:** Make checks very cheap (single atomic read), inline aggressively

**Risk:** Users forget to annotate
**Mitigation:** Linting tool to detect CPU-bound loops, suggest annotation

**Risk:** Signal handler corrupts async state
**Mitigation:** DON'T IMPLEMENT Phase B until proven necessary

---

# Priority 5: Optimize Cross-Thread Wakeups

## Problem Statement

**Current Implementation:**
- File: `glommio/src/sys/mod.rs` lines 259-383
- Uses `crossbeam::channel` for waker queue
- Every wakeup: channel send (alloc) + eventfd write (syscall)

**Issues:**
- Allocation on every cross-thread wakeup
- Eventfd write is expensive syscall
- No batching of multiple wakeups

## Proposed Solution

### Lock-Free Waker Ring Buffer

```rust
pub struct WakerQueue {
    /// Lock-free ring buffer
    ring: ArrayQueue<Waker, 256>,

    /// Pending wakers count
    pending: AtomicUsize,

    /// Eventfd for wakeup
    eventfd: RawFd,

    /// Flag: should write eventfd?
    should_notify: AtomicBool,
}

impl WakerQueue {
    pub fn push(&self, waker: Waker, force_notify: bool) -> Result<(), Waker> {
        // Try to push to lock-free ring
        match self.ring.push(waker) {
            Ok(()) => {
                let count = self.pending.fetch_add(1, Ordering::Release);

                // Only write eventfd on first wakeup or if forced
                if count == 0 || force_notify {
                    self.notify();
                }

                Ok(())
            }
            Err(waker) => {
                // Ring full, fall back to eventfd
                self.notify();
                Err(waker)  // Caller must handle overflow
            }
        }
    }

    pub fn drain(&self) -> DrainIter<'_> {
        // Mark as processing
        self.should_notify.store(false, Ordering::Relaxed);

        DrainIter { queue: self }
    }

    fn notify(&self) {
        if self.should_notify
            .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            write_eventfd(self.eventfd);
        }
    }
}

struct DrainIter<'a> {
    queue: &'a WakerQueue,
}

impl Iterator for DrainIter<'_> {
    type Item = Waker;

    fn next(&mut self) -> Option<Waker> {
        self.queue.ring.pop().inspect(|_| {
            self.queue.pending.fetch_sub(1, Ordering::Acquire);
        })
    }
}
```

**Key Optimizations:**
1. **Lock-free ring:** No mutex, just atomic operations
2. **Batched eventfd writes:** Only write on first wakeup in batch
3. **Zero allocation:** Wakers stored directly in ring
4. **Overflow handling:** Fall back to eventfd if ring full

### Implementation Plan

#### Step 1: Implement WakerQueue (Week 1)
**File:** `glommio/src/sys/waker_queue.rs`

**Tasks:**
- [ ] Implement `WakerQueue` with `crossbeam::ArrayQueue`
- [ ] Add overflow handling (fallback strategy)
- [ ] Unit tests:
  - Single producer, single consumer
  - Multiple producers, single consumer
  - Ring overflow scenarios
  - Concurrent push/drain

#### Step 2: Replace Crossbeam Channel (Week 2)
**File:** `glommio/src/sys/mod.rs`

**Replace:**
```rust
// Old:
foreign_wakes: crossbeam::channel::Receiver<Waker>,
waker_sender: crossbeam::channel::Sender<Waker>,

// New:
waker_queue: Arc<WakerQueue>,
```

**Update:**
- `queue_waker()` → `waker_queue.push()`
- `process_foreign_wakes()` → `waker_queue.drain()`

#### Step 3: Benchmarking (Week 3)
**File:** `glommio/benches/cross_thread_wakeup.rs`

**Benchmark:**
```rust
#[bench]
fn bench_cross_thread_wakeup(b: &mut Bencher) {
    // Setup: 2 executors on different threads
    let ex1 = /* ... */;
    let ex2 = /* ... */;

    b.iter(|| {
        // From ex1, wake task on ex2
        // Measure: wakeup latency
    });
}

#[bench]
fn bench_batched_wakeups(b: &mut Bencher) {
    // Send 100 wakeups in rapid succession
    // Measure: eventfd writes (should be ~1, not 100)
}
```

**Target Goals:**
- [ ] 5-10% improvement in actor-style workloads
- [ ] Eventfd writes reduced by 10-100x (batching)
- [ ] Zero allocations in wakeup path

#### Step 4: Validation (Week 4)
**Tasks:**
- [ ] Full test suite passes
- [ ] Stress test: 1M cross-thread wakeups
- [ ] Verify no lost wakeups (correctness)
- [ ] 24hr stability test

### Overflow Handling

**Strategy when ring is full:**
1. Write eventfd to ensure wakeup happens
2. Drop oldest waker (ring is FIFO)
3. Log warning (indicates tuning needed)

**Alternative:** Grow ring dynamically:
```rust
if ring.is_full() {
    let new_ring = ArrayQueue::new(ring.capacity() * 2);
    // Migrate old ring to new ring
    self.ring.store(new_ring);
}
```

But this adds complexity; simpler to just size ring appropriately (256-1024 slots).

---

# Testing Strategy

## Unit Tests
Every optimization must include:
- [ ] Basic functionality tests
- [ ] Edge case tests
- [ ] Property-based tests (where applicable)
- [ ] Concurrent stress tests

## Integration Tests
- [ ] Full Glommio test suite passes
- [ ] No regressions in existing benchmarks
- [ ] New benchmarks for optimized paths

## Stability Tests
- [ ] 24-hour continuous execution
- [ ] Memory leak detection (valgrind, leaksanitizer)
- [ ] No crashes under stress

## Performance Tests

### Benchmark Suite Structure
```
glommio/benches/
├── timer_scaling.rs          # P1: Timing wheel
├── task_spawn_rate.rs         # P2: RefCell optimization
├── io_submission_latency.rs   # P3: Adaptive I/O
├── preemption_latency.rs      # P4: Soft preemption
└── cross_thread_wakeup.rs     # P5: Waker queue
```

### Comparison Baselines
- **Glommio main:** Current performance
- **Glommio optimized:** Each optimization individually
- **Glommio full:** All optimizations combined
- **Monoio:** Competitor baseline

### Metrics to Track
| Metric | Target | Measurement |
|--------|--------|-------------|
| Timer insert P99 | <100ns @ 100K timers | `bench_timer_insertion` |
| Task spawn rate | +10-15% | `bench_task_spawn_rate` |
| I/O submit P99 | +10-25% | `bench_io_submission_latency` |
| Mixed workload P99 | +20-40% | `bench_mixed_cpu_io` |
| Actor wakeup latency | +5-10% | `bench_cross_thread_wakeup` |

---

# Rollout Strategy

## Phase 1: Foundation (Weeks 1-6)
**Focus:** Low-risk, high-impact optimizations

1. **Week 1-4:** Timing Wheel
   - Implement, test, benchmark
   - Behind feature flag: `timing-wheel`
   - Validate in isolated environment

2. **Week 5-6:** RefCell Caching (Safe)
   - Implement borrow caching
   - Test thoroughly
   - Merge to main (low risk)

**Milestone:** 15-25% improvement in timer-heavy workloads

## Phase 2: Refinement (Weeks 7-14)
**Focus:** Medium-risk optimizations

3. **Week 7-10:** Adaptive I/O Submission
   - Implement policy framework
   - Tune defaults
   - Validate on real workloads

4. **Week 11-12:** Cross-Thread Wakeups
   - Implement lock-free queue
   - Benchmark actor patterns

5. **Week 13-14:** RefCell → UnsafeCell
   - Careful refactor
   - Extensive validation (Miri, TSan)
   - Behind feature flag initially

**Milestone:** 30-40% improvement overall

## Phase 3: Advanced (Weeks 15-24)
**Focus:** High-risk, optional optimizations

6. **Week 15-20:** Soft Preemption (Phase A only)
   - Compiler instrumentation
   - Opt-in via macros
   - Limited scope

7. **Week 21-24:** Integration & Polish
   - Comprehensive benchmarking
   - Documentation
   - Migration guides

**Milestone:** 40-50% improvement, competitive with Monoio

---

# Success Criteria

## Performance Targets

### Primary Goals (Must Achieve)
- [ ] **Timing Wheel:** 10-20% improvement in timer-heavy workloads
- [ ] **RefCell:** 10-15% improvement in task spawn rate
- [ ] **Adaptive I/O:** 10-25% improvement in P99 latency
- [ ] **Overall:** 30-40% improvement in composite benchmark

### Stretch Goals (Nice to Have)
- [ ] **Soft Preempt:** 20-40% improvement in P99 for mixed workloads
- [ ] **Waker Queue:** 5-10% improvement in actor patterns
- [ ] **Overall:** 40-50% improvement, match or exceed Monoio

## Quality Targets
- [ ] Zero regressions in existing benchmarks
- [ ] All tests pass (unit, integration, stress)
- [ ] 24hr stability test passes for each optimization
- [ ] No memory leaks (valgrind clean)
- [ ] No undefined behavior (Miri clean)
- [ ] Code review by 2+ maintainers for unsafe code

## Documentation Targets
- [ ] Every optimization has design doc
- [ ] Unsafe code has safety proofs
- [ ] Benchmarking methodology documented
- [ ] Migration guide for breaking changes
- [ ] Performance tuning guide for users

---

# Risk Register

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Timing wheel cascading overhead | Medium | Medium | Spread cascade work, extensive benchmarking |
| RefCell → UnsafeCell unsoundness | Low | Critical | Miri, TSan, formal proof, conservative rollout |
| Adaptive I/O mis-tuned defaults | Medium | Medium | Extensive benchmarking, easy overrides |
| Signal handler corruption | High | Critical | Don't implement Phase B unless necessary |
| Performance regression | Low | High | Comprehensive benchmark suite in CI |
| Breaking API changes | Medium | Medium | Feature flags, migration guides |

---

# Maintenance Plan

## Ongoing Monitoring
- [ ] CI: Run benchmark suite on every PR
- [ ] Alert: >10% regression in any benchmark
- [ ] Weekly: Review performance metrics dashboard
- [ ] Monthly: Re-benchmark against Monoio

## Documentation Maintenance
- [ ] Keep optimization guide up to date
- [ ] Document new tuning parameters
- [ ] Update troubleshooting guide

## Code Health
- [ ] Regular Miri runs (weekly)
- [ ] Regular fuzzing (continuous)
- [ ] Unsafe code audits (quarterly)

---

# Appendix: Benchmark Comparison Targets

## Monoio Baseline Metrics (Target to Match)

### Timer Performance
- Insert: <50ns P99 @ 100K timers
- Remove: <50ns P99
- Process: O(k) expiring timers

### Task Spawn Rate
- 2-3M tasks/sec/core

### I/O Latency
- P99 <1ms for 4KB random reads
- P99 <5ms for 100K concurrent connections

### Resource Usage
- Memory: <100 bytes per task overhead
- CPU: >95% utilization under load

---

# Contact & Review

**Document Owner:** Performance Team
**Last Review:** 2026-02-14
**Next Review:** 2026-03-14 (monthly)

**Reviewers:**
- [ ] Core maintainer approval
- [ ] Performance engineer review
- [ ] Safety review (for unsafe code)

**Questions/Feedback:** File issue with `performance` label
