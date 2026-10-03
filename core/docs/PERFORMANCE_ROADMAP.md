# Glommio Performance Roadmap: Competing with Monoio

> ## ⚠️ Superseded by [PERFORMANCE_NEXT.md](PERFORMANCE_NEXT.md)
>
> This is the strategy document written before the work was done. It is kept for
> the reasoning and the architectural context, and its Tier 1 items are complete.
>
> Its framing — "monoio outperforms glommio" — was never verified here, and four
> measured paths since suggest per-operation cost is not where such a gap would
> come from. glommio's task switch is ~16.5 ns, its DMA read path is within 6% of
> raw io_uring, its shards scale to 8 at 1.11x, and its per-connection cost is
> O(1). What remains is ~2 µs per blocking I/O, which realistic concurrency
> amortises away.

## Executive Summary

Monoio currently outperforms Glommio in benchmarks due to:
1. Better io_uring optimization
2. More flexible runtime (epoll/kqueue fallback)
3. Fewer resource leaks
4. Active maintenance and optimization

**This document outlines the high-level strategy to make Glommio competitive.**

## Related Documentation

- **[Detailed Optimization Plan](OPTIMIZATION_PLAN.md)** ← **START HERE** for implementation details
  - Timing Wheel implementation guide
  - RefCell optimization strategy
  - Adaptive I/O submission design
  - Soft preemption approaches
  - Cross-thread wakeup optimization
  - Complete with timelines, benchmarks, and risk mitigation

- **This Document:** High-level phases, prioritization, and architectural context

## Performance Gaps: Root Cause Analysis

### 1. Random File Read Performance

**Monoio Advantage:** Better random read throughput

**Glommio Issues:**
- Potential suboptimal io_uring SQE submission patterns
- Buffer allocation overhead
- Task scheduling overhead for I/O operations

**Investigations Needed:**
- Profile random read workloads with perf
- Compare io_uring usage patterns with Monoio
- Analyze buffer management strategy

### 2. Resource Exhaustion in Long-Running Apps

**Critical Issue:** #448 Eventfd Leak

**Impact:**
- 12 eventfds leaked per executor create/destroy cycle
- ~36.8GB VSZ after 2723 iterations
- Crashes with ENOMEM

**Performance Impact:**
- File descriptor exhaustion
- Memory bloat
- Degraded performance over time

**Fix Complexity:** Hard (architectural issue with task lifecycle)

**Recommendation:** High priority - this directly impacts production deployments

### 3. Throughput Bottlenecks

**Potential Areas:**

#### A. Task Scheduling Overhead
```rust
// Current: Uses RefCell borrows on hot path
let tq = self.queues.borrow().active_executing...
```

**Optimization Opportunities:**
- Reduce RefCell overhead with unsafe direct access where proven safe
- Cache active queue reference
- Optimize task queue selection

#### B. Allocation Overhead
```rust
// Every spawn allocates
pub fn spawn<T>(&self, future: impl Future<Output = T>) -> Task<T>
```

**Optimization Opportunities:**
- ~~Task object pooling~~ — tried, reverted (worth 0% under mimalloc)
- ~~Arena allocation for tasks~~ — tried, reverted (see
  [post-mortem](investigations/task-arena/))
- Reduce boxing/dynamic dispatch
- Recommend mimalloc to deployments: ~45 ns spawn under glibc, ~24 ns under
  mimalloc, no glommio-side allocator required

#### C. Context Switching
**Current:** Thread-per-core with cooperative scheduling

**Optimization Opportunities:**
- Reduce unnecessary yields
- Better preemption heuristics
- Optimize the need_preempt() check

## Actionable Improvements

### Tier 1: Critical Fixes (Must Have)

#### 1.1 Fix Eventfd Leak (#448)
**Priority:** 🔴 Critical
**Impact:** High - enables long-running production use
**Difficulty:** ⭐⭐⭐⭐ Hard

**Approach:**
- Implement explicit task cleanup on executor drop
- Or: Make eventfd closeable even with Arc references alive
- Requires careful lifecycle management

**Timeline:** 2-3 weeks of focused work

#### 1.2 Fix Task Queue Panics (#689)
**Priority:** 🔴 Critical
**Impact:** High - reliability for production
**Difficulty:** ⭐⭐⭐ Medium

**Approach:**
- Identify panic points in task queue
- Add proper error handling
- Return Results instead of panicking

**Timeline:** 1 week

#### 1.3 Audit io_uring Safety (#686)
**Priority:** 🟡 High
**Impact:** Medium - confidence for production use
**Difficulty:** ⭐⭐⭐⭐ Hard

**Approach:**
- Review all unsafe io_uring code
- Document safety invariants
- Add runtime assertions for debugging

**Timeline:** 2 weeks

### Tier 2: Performance Optimizations (High Value)

#### 2.1 Optimize Hot Path Allocations
**Priority:** 🟡 High
**Impact:** High - throughput improvement
**Difficulty:** ⭐⭐⭐ Medium

**Specific Optimizations:**

> **A and B below were implemented and reverted.** Both the task pool
> (thread-local free lists) and the arena were measured: the pool is worth
> 30-36% under glibc malloc and **0% under mimalloc**, and the arena cost
> 98 MB resident per executor while segfaulting on detached tasks. The
> allocator is the deployment's choice to make; recommend mimalloc and keep
> the free list out of glommio. Full account:
> [task-arena post-mortem](investigations/task-arena/).
>
> The real find in this area was not an allocator change at all —
> `get_sleep_notifier_for` took a process-wide `RwLock` read on every spawn
> (`f28a619`, 4350.9 ns → ~28-37 ns at 64 executors).

```rust
// A. Task Object Pooling  [REVERTED - no gain over mimalloc]
struct TaskPool {
    free_list: Vec<Box<TaskInner>>,
    // Reuse task objects instead of allocating
}

// B. Arena Allocation for Short-Lived Tasks  [REVERTED - see post-mortem]
struct TaskArena {
    arena: bumpalo::Bump,
    // Allocate tasks from arena, reset periodically
}

// C. Reduce RefCell Overhead
// Current:
let tq = self.queues.borrow().active_executing...

// Optimized (where proven safe):
unsafe {
    let tq = (*self.queues.as_ptr()).active_executing...
}
```

**Expected Gain:** 10-20% throughput improvement

**Timeline:** 2 weeks

#### 2.2 Optimize io_uring Submission Patterns
**Priority:** 🟡 High
**Impact:** High - matches Monoio's strength
**Difficulty:** ⭐⭐⭐⭐ Hard

**Areas to Optimize:**

```rust
// A. Batch SQE Submissions
// Instead of: submit() after each operation
// Do: collect operations, submit_and_wait() in batch

// B. Use SQPOLL Mode
// Current: Normal mode
// Optimized: SQPOLL for reduced syscalls

// C. Optimize Buffer Registration
// Pre-register buffers with io_uring
// Reduces setup overhead for I/O operations
```

**Benchmarking Needed:**
- Compare with Monoio's io_uring usage
- Profile SQE submission overhead
- Measure syscall reduction

**Expected Gain:** 15-30% latency improvement for I/O

**Timeline:** 3-4 weeks

#### 2.3 Optimize Task Scheduling
**Priority:** 🟢 Medium
**Impact:** Medium - better CPU utilization
**Difficulty:** ⭐⭐⭐ Medium

**Optimizations:**

```rust
// A. Reduce need_preempt() Overhead
// Current: Checks every N polls
// Optimized: Use timer-based preemption

// B. Better Queue Selection
// Current: Linear search for queue
// Optimized: Cache last-used queue per task type

// C. Reduce Yielding
// Profile where tasks yield unnecessarily
// Optimize yield_if_needed() heuristics
```

**Expected Gain:** 5-10% throughput improvement

**Timeline:** 2 weeks

### Tier 3: Portability & Ergonomics

#### 3.1 Mock io_uring for Non-Linux (#673)
**Priority:** 🟢 Medium
**Impact:** Medium - development experience
**Difficulty:** ⭐⭐⭐⭐ Hard

**Approach:**
- Create mock io_uring implementation
- Falls back to epoll/thread pool
- Allows development on macOS/Windows

**Benefits:**
- Easier development (no Lima VM needed!)
- More contributors
- Better testing

**Timeline:** 3-4 weeks

#### 3.2 Better Error Handling
**Priority:** 🟢 Medium
**Impact:** Medium - API ergonomics
**Difficulty:** ⭐⭐ Easy

**Improvements:**
- Add try_* variants (try_spawn_local, etc.)
- Return Result instead of panicking
- Better error messages

**Timeline:** 1 week

### Tier 4: Ecosystem & Maintenance

#### 4.1 Comprehensive Benchmark Suite
**Priority:** 🟡 High
**Impact:** High - measure improvements
**Difficulty:** ⭐⭐ Easy

**Benchmarks Needed:**
```rust
// 1. Random file reads (Monoio's strength)
bench_random_reads(file_size, block_size, parallelism)

// 2. Sequential I/O throughput
bench_sequential_io(file_size, block_size)

// 3. Network throughput
bench_tcp_echo(connections, message_size)

// 4. Task spawn/destroy rate
bench_task_spawn_rate()

// 5. Long-running stability
bench_stability(duration_hours)
```

**Comparison:**
- Run same benchmarks against Monoio
- Track improvements over time
- Publish results

**Timeline:** 1 week

#### 4.2 Continuous Profiling
**Priority:** 🟢 Medium
**Impact:** High - ongoing optimization
**Difficulty:** ⭐⭐ Easy

**Setup:**
- perf-based profiling in CI
- Flame graphs for hot paths
- Regression detection

**Timeline:** 1 week

## Architectural Improvements

### A. Zero-Copy I/O Paths

**Current:** Multiple buffer copies for I/O

**Proposed:**
```rust
// Use io_uring's buffer ring for zero-copy
struct BufferRing {
    ring: io_uring::BufferRing,
    // Kernel directly writes to user buffers
}
```

**Benefits:**
- Eliminate buffer copies
- Lower latency
- Higher throughput

**Difficulty:** ⭐⭐⭐⭐ Hard
**Timeline:** 4 weeks

### B. Better Memory Management

**Current:** Heavy use of Rc/RefCell

~~**Proposed:** arena allocation for executor-local objects.~~ **Rejected on
measurement** — see the [task-arena post-mortem](investigations/task-arena/).
Bulk deallocation on executor drop is exactly what broke: tasks legally outlive
`run()`, and the arena faulted when they did.

**Still open in this area, and more promising:** `references: AtomicI16` and
`Arc<SleepNotifier>` on paths that never leave their thread. Measured
uncontended and cache-hot, an atomic refcount inc+dec is **8.84 ns** against
~0 for `Cell`, and `Arc` clone+drop is **8.69 ns** against 0.43 for `Rc`. On a
24 ns spawn that is substantial. The blocker is that Rust's `Waker` is
`unsafe impl Send + Sync` unconditionally, so any third-party future may clone
a waker and send it elsewhere. Two routes: `LocalWaker` (unstable, tracking
issue #118959) or biased reference counting, which works on stable today.

**Difficulty:** ⭐⭐⭐⭐ Hard

### C. NUMA-Aware Task Scheduling

**Current:** Basic CPU pinning

**Proposed:**
```rust
// Optimize for NUMA architectures
struct NumaAwareExecutor {
    node: usize,
    local_memory: Vec<u8>,
    // Pin tasks to NUMA-local memory
}
```

**Benefits:**
- Better multi-socket performance
- Lower memory latency
- Higher throughput on big machines

**Difficulty:** ⭐⭐⭐⭐⭐ Very Hard
**Timeline:** 6 weeks

## Measurement & Validation

### Performance Targets

Based on Monoio benchmarks, target improvements:

**Random File Reads:**
- Current: ~X MB/s (need to measure)
- Target: Match or exceed Monoio (typically 1.2-1.5x Glommio)

**Network Throughput:**
- Current: ~Y requests/sec (need to measure)
- Target: Within 5% of Monoio

**Latency:**
- Current: p99 = Z μs (need to measure)
- Target: p99 within 10% of Monoio

**Resource Usage:**
- Fix eventfd leak (0 leaked fds after executor drop)
- Memory: Stable over 24+ hour runs
- CPU: >95% utilization under load

### Benchmarking Methodology

```rust
// Standard benchmark harness
#[bench]
fn bench_vs_monoio(b: &mut Bencher) {
    let workload = WorkloadConfig {
        file_size: 1_000_000_000,  // 1GB
        block_size: 4096,
        random_reads: true,
        concurrency: 100,
    };

    b.iter(|| {
        run_glommio_workload(&workload)
    });
}
```

**Compare against:**
- Monoio (main competitor)
- Tokio (ecosystem baseline)
- Seastar (architectural inspiration)

## Implementation Priority

### Phase 1: Foundation (Weeks 1-4) - **IN PROGRESS**
1. ✅ **DONE:** Fix eventfd leak (#448) - Implemented Mutex<Option<File>> wrapper for explicit cleanup
2. ✅ **DONE:** Fix task queue panics (#689) - Made maybe_activate() panic-safe with LOCAL_EX.is_set() check
3. ✅ **DONE:** Benchmark suite - `examples/alloc_compare.rs`,
   `examples/spawn_scaling.rs`, `benches/timer_benchmark.rs`
4. ✅ **DONE:** Phase 2 begun — timing wheel landed, see below

**Status Update (2026-08-01):**
- #448 additionally fixed at the root by `f28a619`: the task header stores
  `executor_id` instead of an `Arc<SleepNotifier>`, so tasks no longer pin the
  executor's eventfd at all
- Timing wheel complete and measured
- Task arena and thread-local free lists tried, measured and reverted —
  [post-mortem](investigations/task-arena/)
- Beware of benchmarks that construct a `LocalExecutor` per iteration; they
  time io_uring ring setup, not the thing under test

### Phase 2: Performance (Weeks 5-10) - **SEE OPTIMIZATION_PLAN.md**

**Priority-ordered optimizations based on architecture analysis:**

1. ✅ **DONE — 🔴 P1: Hierarchical Timing Wheel**
   - BTreeMap replaced with O(1) timing wheel (`glommio/src/timer/timing_wheel.rs`)
   - **Measured:** 17.7 ns → 10.3 ns at 100K timers (35-42% at scale)
   - Zero unsafe
   - [See detailed plan](OPTIMIZATION_PLAN.md#priority-1-hierarchical-timing-wheel)

2. **🟡 P2: RefCell Optimization** (Weeks 9-10)
   - Phase A: Safe borrow caching (+5-10%)
   - Phase B: UnsafeCell conversion (+10-15%)
   - **Expected:** 15-25% improvement in task switching
   - [See detailed plan](OPTIMIZATION_PLAN.md#priority-2a-refcell-caching-phase-1)

3. **🟡 P3: Adaptive I/O Submission** (Weeks 11-14)
   - Dynamic batching based on observed latency
   - **Expected:** 10-25% P99 latency improvement
   - [See detailed plan](OPTIMIZATION_PLAN.md#priority-3-adaptive-io-submission)

### Phase 3: Advanced Optimizations (Weeks 15-20) - **SEE OPTIMIZATION_PLAN.md**

4. **🟢 P5: Cross-Thread Wakeup Optimization** (Weeks 15-16)
   - Lock-free waker ring buffer
   - **Expected:** 5-10% improvement in actor patterns
   - [See detailed plan](OPTIMIZATION_PLAN.md#priority-5-optimize-cross-thread-wakeups)

5. **🟢 P4: Soft Preemption** (Weeks 17-20, **OPTIONAL**)
   - Compiler-instrumented yield points
   - **Expected:** 20-40% P99 improvement in mixed workloads
   - **High risk** - only implement if P99 issues persist
   - [See detailed plan](OPTIMIZATION_PLAN.md#priority-4-soft-preemption-phase-3)

### Phase 4: Reliability & Ecosystem (Weeks 21-24)
1. Audit io_uring safety (#686) - Week 21-22
2. Comprehensive testing & validation - Week 23
3. Better error handling & portability - Week 24
4. Documentation & migration guides

## Success Metrics

**Quantitative (Updated 2026-02-14):**
- [x] ✅ Zero eventfd leaks (Issue #448 FIXED)
- [x] ✅ Zero panics in task queue operations (Issue #689 FIXED)
- [x] ✅ Timer operations: <100ns P99 @ 100K timers (Timing Wheel — 10.3 ns)
- [x] ✅ Task spawn rate: +15% improvement — 37.5 ns → ~24 ns single executor,
      4350.9 ns → ~28-37 ns at 64 executors (`f28a619`, executor id in header)
- [ ] I/O P99 latency: +20% improvement (Adaptive submission)
- [ ] Overall performance: 30-40% improvement vs baseline
- [ ] Random read throughput within 10% of Monoio
- [ ] Network throughput within 5% of Monoio
- [ ] 24+ hour stability tests passing

**Qualitative:**
- [x] ✅ Critical bugs fixed (eventfd, task panics)
- [ ] Comprehensive optimization plan documented
- [ ] Active maintainer assigned
- [ ] Regular releases (monthly)
- [ ] Growing contributor base
- [ ] Positive community feedback

## Resources Needed

**Engineering:**
- 1 senior engineer (lead) - full time
- 2 engineers (contributors) - part time
- 1 performance engineer - consulting

**Time:**
- Minimum: 4 months for Phase 1-2
- Recommended: 6 months for Phase 1-3
- Ideal: 9 months for all phases

**Infrastructure:**
- Benchmark hardware (consistent results)
- CI/CD for continuous profiling
- Public results dashboard

## Conclusion

**Glommio can compete with Monoio through:**

1. **Fix critical bugs** - eventfd leak, panics, memory corruption
2. **Optimize hot paths** - allocations, io_uring usage, scheduling
3. **Improve reliability** - safety audits, testing, error handling
4. **Maintain momentum** - active development, benchmarks, releases

**Key Insight:** Glommio's thread-per-core architecture is fundamentally sound. The performance gap is due to **implementation bugs and lack of optimization**, not architectural limitations.

With focused effort on these areas, Glommio can match or exceed Monoio's performance while maintaining its unique architectural advantages (pure thread-per-core, CPU pinning, cache optimization).

The biggest challenge is **maintenance bandwidth** - these improvements require sustained engineering effort. But the path forward is clear and achievable.
