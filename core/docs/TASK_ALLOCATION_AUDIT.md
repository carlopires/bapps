# Task Allocation Audit

Analysis of Glommio's current task allocation patterns to evaluate the potential impact of implementing a task arena allocator.

> ## ⚠️ Superseded — the arena was built and reverted
>
> This audit motivated the task arena allocator. The arena was implemented,
> benchmarked and then **removed**; see the
> [post-mortem](investigations/task-arena/).
>
> Two claims below did not survive measurement and are corrected inline:
>
> - **"Global allocator (jemalloc on most systems)"** — false. Glommio declares
>   no `#[global_allocator]`, so it gets whatever the binary uses, normally
>   glibc malloc.
> - **"Worst case: 500ns+ (allocator lock contention)"** — not observed.
>   Allocation cost was flat from 1 to 8 concurrent executors and 1.33x at 64.
>   Task blocks are freed on the thread that allocated them, which every modern
>   allocator has a per-thread fast path for.
>
> The contention that did exist was in glommio's own sleep-notifier registry,
> not in malloc. Fixing it took spawn at 64 executors from 4350.9 ns to
> ~28-37 ns (`f28a619`).
>
> The structural sections — allocation lifecycle, task layout, deallocation
> path — remain accurate and are why this file is kept.

## Executive Summary

**Current State:**
- Tasks use **global heap allocation** via `alloc::alloc::alloc()`
- Each spawned task triggers **1 heap allocation** (Header + Schedule + Future/Output)
- Optional **2nd allocation** for futures >= 2KB (`Box::pin`)
- **No custom arena** or slab allocator currently implemented

**Key Allocation Hot Spots:**
1. **`RawTask::allocate()`** - Primary task allocation (1 per spawn)
2. **`Box::pin(future)`** - Optional for large futures (>= 2048 bytes)
3. **`VecDeque::push_back()`** - Queue growth allocations
4. **`Waker::clone()`** - Reference counting overhead

## Allocation Lifecycle

### 1. Task Spawn Path

```
spawn_local(future)
  ↓
task_impl::spawn_local()
  ↓
[Decision: future size >= 2KB?]
  ├─ YES → Box::pin(future)    [ALLOCATION #1: Box]
  └─ NO  → use future directly
  ↓
RawTask::allocate()             [ALLOCATION #2: Main task]
  ├─ alloc::alloc::alloc()      (Header + Schedule + Future/Output)
  ├─ Write Header
  ├─ Write Schedule closure
  └─ Write Future
  ↓
multitask::spawn_and_schedule()
  ↓
LocalQueue::push(task)          [ALLOCATION #3: VecDeque growth]
```

### 2. Task Structure Layout

```
┌────────────────────────────────────┐
│ Header (32 bytes on 64-bit)       │
│  - Arc<SleepNotifier>             │
│  - state: u8                       │
│  - latency_matters: bool           │
│  - references: AtomicI16           │
│  - awaiter: Option<Waker>          │
│  - vtable: &'static TaskVTable     │
├────────────────────────────────────┤
│ Schedule Function (closure)        │
│  - Weak<RefCell<TaskQueue>>       │
│  - Function pointer                │
├────────────────────────────────────┤
│ Union:                             │
│  ├─ Future (while pending)         │
│  └─ Output (after completion)      │
└────────────────────────────────────┘
```

**Total Size:** ~32 bytes (Header) + closure size + max(Future, Output)

### 3. Task Deallocation Path

```
Task completes / is cancelled
  ↓
references reach 0 AND handle dropped
  ↓
RawTask::destroy()
  ├─ Drop schedule closure
  └─ alloc::alloc::dealloc()    [DEALLOCATION: Free task]
```

## Allocation Hot Spots Analysis

### Hot Spot #1: `RawTask::allocate()` ⭐⭐⭐⭐⭐

**File:** `glommio/src/task/raw.rs:123`

**Code:**
```rust
let raw_task = alloc::alloc::alloc(task_layout.layout) as *mut ();
```

**Frequency:** Once per `spawn_local()` call
**Impact:** **CRITICAL** - This is the main bottleneck
**Size:** Variable (typically 32-256 bytes per task)

**Why It Matters:**
- RMQ workload spawns tasks at **high frequency** (10K-100K/sec)
- Each spawn hits the global allocator
- Even with jemalloc/tcache, this adds **10-50ns overhead**
- **Worst case:** Allocator lock contention under load

**Arena Benefit:** ⚡ **10-50ns → 2-5ns** (pointer bump)

### Hot Spot #2: `Box::pin(future)` ⭐⭐⭐

**File:** `glommio/src/task/task_impl.rs:39`

**Code:**
```rust
let future = if mem::size_of::<F>() >= 2048 {
    alloc::boxed::Box::pin(future)  // Extra allocation!
    // ...
}
```

**Frequency:** Only for futures >= 2KB
**Impact:** **MODERATE** - Depends on workload
**Size:** Size of future (2KB+)

**Why It Matters:**
- Adds **second allocation** for large futures
- RMQ message handling futures are typically **small** (<512 bytes)
- But streaming/batching futures could hit this

**Arena Benefit:** ⚡ Could eliminate by pinning in arena

### Hot Spot #3: `VecDeque::push_back()` ⭐⭐

**File:** `glommio/src/executor/multitask.rs:106`

**Code:**
```rust
self.queue.borrow_mut().push_back(runnable);
```

**Frequency:** Once per task spawn + once per wake
**Impact:** **LOW-MODERATE** - Amortized O(1)
**Size:** VecDeque capacity growth (powers of 2)

**Why It Matters:**
- VecDeque grows dynamically (16 → 32 → 64 → 128...)
- Growth triggers **reallocation + copy**
- Usually amortized away with stable workloads

**Arena Benefit:** ⚡ Could use fixed-size ring buffer

### Hot Spot #4: Reference Counting Overhead ⭐

**File:** `glommio/src/task/raw.rs:270`

**Code:**
```rust
pub(crate) unsafe fn increment_references(ptr: *const ()) {
    let raw = Self::from_ptr(ptr);
    (*raw.header).references.fetch_add(1, Ordering::AcqRel);
}
```

**Frequency:** Every `Waker::clone()` operation
**Impact:** **LOW** - Atomic operation only
**Size:** No allocation, just atomic increment

**Why It Matters:**
- Each task wake clones waker → increments refcount
- **Not an allocation**, but adds cache contention

**Arena Benefit:** ❌ Arena doesn't help (still need refcounting)

## Current Allocator: System Default

**File:** No custom allocator configured

**Evidence:**
```rust
use std::alloc::{alloc, dealloc, Layout};
```

Glommio uses:
- **Whatever global allocator the binary provides** — glommio declares no
  `#[global_allocator]`, so this is glibc malloc unless the application says
  otherwise
- **No thread-local cache** specific to tasks
- **No arena pattern** for task structures

**Comparison (measured, 512 tasks live, `examples/alloc_compare.rs`):**

| Allocator | Spawn |
|-----------|------:|
| glibc malloc (default) | ~45 ns |
| jemalloc | ~30 ns |
| mimalloc | ~24 ns |

> **Corrected.** This table originally read "jemalloc (current) 10-50ns …
> lock contention possible" against a projected arena at "2-5ns". Glommio was
> never on jemalloc, and the arena's advantage over a good allocator did not
> materialize. Choosing mimalloc is the whole win, without any glommio-side
> allocator. See the [post-mortem](investigations/task-arena/).

## Memory Usage Patterns

### Typical RMQ Workload Simulation

**Assumptions:**
- 10,000 active tasks (message handlers)
- Task size: 128 bytes average
- Spawn rate: 50K tasks/sec
- Task lifetime: 200ms average

**Current Memory Usage:**
```
Active tasks: 10,000 × 128 bytes = 1.28 MB
Fragmentation: ~10-20% = 0.13-0.26 MB
Total: ~1.5 MB per shard
```

**With Arena:**
```
Arena capacity: 20,000 tasks × 128 bytes = 2.56 MB
Actual usage: ~1.28 MB
Wasted space: 1.28 MB (50% of arena)
Trade-off: Fixed memory for predictable performance
```

## Performance Impact Estimation

### Spawn Rate Impact

**Current (Global Allocator):**
```
50,000 spawns/sec × 40ns/alloc = 2,000,000ns = 2ms/sec = 0.2% CPU
```

**With Arena:**
```
50,000 spawns/sec × 5ns/bump = 250,000ns = 0.25ms/sec = 0.025% CPU
```

**Savings:** ~1.75ms/sec per shard = **0.175% CPU freed**

### Allocation Jitter Reduction

**Current:**
- **Best case:** 10ns (tcache hit)
- **Worst case:** 500ns+ (allocator lock contention)
- **P99:** ~80ns
- **Variance:** High (10-500ns range)

**With Arena:**
- **Best case:** 2ns (pointer bump)
- **Worst case:** 5ns (boundary check)
- **P99:** 3ns
- **Variance:** **Near-zero** (predictable)

**For Latency-Sensitive Workloads:** This is the real win! ⚡

> **Retracted — none of the above was measured.** The projected contention was
> the arena's entire justification and it does not occur: spawn cost is flat
> from 1 to 8 concurrent executors and 1.33x at 64. The "80ns P99 baseline"
> came from a benchmark that constructed a `LocalExecutor` per iteration and so
> timed io_uring ring setup; real spawn+await was 38 ns. See the
> [post-mortem](investigations/task-arena/).

## Recommendations

### Phase 1: Measurement (Do This First!)

Before implementing an arena, **measure current impact**:

1. **Add allocation counters:**
   ```rust
   static TASK_ALLOCS: AtomicU64 = AtomicU64::new(0);
   static TASK_DEALLOCS: AtomicU64 = AtomicU64::new(0);
   ```

2. **Profile allocation time:**
   - Use `criterion` to benchmark `spawn_local()` alone
   - Compare with/without tasks (just allocation overhead)
   - Measure P50/P99/P999 latencies

3. **Identify workload characteristics:**
   - Average task size
   - Spawn rate
   - Task lifetime
   - Peak concurrent tasks

**Tools:**
- `cargo bench --bench task_spawn` (create new benchmark)
- `perf record -g` for allocation hot spots
- `heaptrack` for allocation frequency

### Phase 2: Arena Design (If Measurement Justifies It)

**Option A: Typed-Arena (Simple)**
```rust
use typed_arena::Arena;

struct TaskSlab {
    arena: Arena<TaskData>,
}
```

**Pros:**
- Easy to integrate
- Type-safe
- Good for short-lived tasks

**Cons:**
- Can't deallocate individual tasks
- Requires bulk reset

**Option B: Slab Allocator (Advanced)**
```rust
use slab::Slab;

struct TaskPool {
    slab: Slab<TaskData>,
    free_list: Vec<usize>,
}
```

**Pros:**
- Can deallocate individual tasks
- Reuses slots immediately
- Good for long-lived + churn pattern

**Cons:**
- More complex
- Requires slot management

**Option C: Hybrid (Best of Both)**
```rust
struct TaskAllocator {
    hot_arena: Arena<SmallTask>,    // <256 bytes, short-lived
    cold_slab: Slab<LargeTask>,     // >=256 bytes, long-lived
}
```

### Phase 3: Implementation Strategy

**Step 1:** Start with **small arena** (1000 tasks) per executor
**Step 2:** Measure impact on RMQ benchmark
**Step 3:** Tune arena size based on workload
**Step 4:** Add bulk reset on idle periods

**Migration Path:**
1. Keep existing heap allocation as fallback
2. Try arena first, fall back if full
3. Gradually increase arena usage
4. Eventually remove heap fallback

## Trade-offs to Consider

### Arena Advantages ✅

1. **Faster allocation:** 2-5ns vs 10-50ns
2. **Zero deallocation cost:** Bulk reset
3. **I-cache locality:** Tasks contiguous in memory
4. **Predictable latency:** No allocator jitter
5. **Zero fragmentation:** Linear layout

### Arena Disadvantages ❌

1. **Fixed memory overhead:** Arena pre-allocated
2. **Wasted space:** Unused capacity
3. **Bulk-only dealloc:** Can't free individual tasks (typed-arena)
4. **Complexity:** Lifetime management
5. **Fallback needed:** What if arena fills up?

## Monoio/Tokio Comparison

**Tokio:**
- Uses `Box` allocation (similar to current Glommio)
- Work-stealing adds overhead
- **No task arena**

**Monoio:**
- Claims "zero allocation" spawning
- Uses object pools + slab allocators
- Thread-per-core like Glommio

**Key Difference:**
Monoio's advantage is **object pooling + slab**, not just arena. They combine:
- Pre-allocated task structures (slab)
- Inline small futures (no Box::pin)
- Immediate slot reuse (not bulk reset)

**To beat Monoio:** Implement hybrid allocator (arena + slab) + inline futures

## Next Steps

### 1. Create Task Spawn Benchmark ⏱️

```bash
# Create glommio/benches/task_spawn.rs
cargo bench --bench task_spawn
```

Measure:
- Spawn latency (P50/P99/P999)
- Spawn throughput (tasks/sec)
- Memory usage per 1000 tasks

### 2. Profile Allocation Hot Spots 🔥

```bash
cargo build --release
perf record -g ./target/release/examples/rmq_bench
perf report
```

Look for:
- `alloc::alloc::alloc` time %
- `RawTask::allocate` time %
- Lock contention in allocator

### 3. Prototype Simple Arena 🧪

Add to `executor/mod.rs`:
```rust
use typed_arena::Arena;

pub struct ExecutorArena {
    task_arena: RefCell<Arena<TaskData>>,
}
```

Measure improvement on spawn benchmark.

### 4. Iterate Based on Data 📊

- If allocation is <5% of spawn time → **Not worth it**
- If allocation is >10% of spawn time → **Proceed with arena**
- If P99 latency improves >20% → **Ship it!**

## References

- [Typed Arena](https://docs.rs/typed-arena/) - Simple arena allocator
- [Slab](https://docs.rs/slab/) - Slot-based allocator
- [Monoio Task Allocation](https://github.com/bytedance/monoio) - Reference implementation
- Current code: `glommio/src/task/raw.rs` (allocation logic)

## Conclusion

**Should we implement a task arena?**

**Answer: no. This was settled by building one.**

The advice this audit closed with — measure first, let data guide the decision —
was right, and was not followed. The arena was written before anyone checked
whether allocator contention existed. It did not. What the arena bought in
allocation speed, mimalloc buys for a one-line `#[global_allocator]`, without
98 MB resident per executor, without a 1 KB ceiling on task closures, and
without segfaulting on detached tasks.

**What measurement actually showed:**

| claim in this audit | measured |
|---|---|
| allocator lock contention on the spawn path | flat 1→8 executors, 1.33x at 64 |
| 80 ns P99 spawn baseline | 38 ns — the old benchmark timed ring setup |
| arena at 2-5 ns is the win | mimalloc at 24 ns; arena's edge over it was noise |
| glommio runs on jemalloc | glommio sets no global allocator at all |

**What to do instead:**

1. Recommend mimalloc to deployments — biggest single lever on this path
2. Look for contention in glommio's own structures, where it turned out to be
   (`f28a619`: process-wide `RwLock` taken on every spawn)
3. Keep the allocator choice with the application

Full account: [task-arena post-mortem](investigations/task-arena/).
