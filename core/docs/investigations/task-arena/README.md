# Task Arena Allocator - Post-Mortem

**Status:** Tried, measured, reverted (`cec3913`)
**Outcome:** No arena. Recommend mimalloc instead.

This records an optimization that was built, benchmarked and removed, so the
reasoning behind it is not repeated. It supersedes the former
`docs/ARENA_IMPLEMENTATION.md` and `docs/phase2-completion.md`, both of which
described the deleted code and asserted results for it.

## What was built

A slot allocator for task blocks, thread-local to each executor
(`glommio/src/task/arena.rs`, 643 lines, added in `287cd74`, extended through
`f98bca4`):

- 100,000 slots of 1024 bytes, allocated up front
- Intrusive free list threaded through the slots, LIFO
- `RawTask::allocate()` took a slot instead of calling the global allocator
- On executor drop, `mprotect(PROT_NONE)` rather than `dealloc`, so that a
  contract violation would fault at the access site

The spawn path did get faster. That was never the problem.

## Why it was reverted

### 1. The premise was not real

The arena was justified by allocator lock contention — see the retraction in
[TASK_ALLOCATION_AUDIT.md](../../TASK_ALLOCATION_AUDIT.md), which projected
"worst case 500ns+ (allocator lock contention)" and a high-variance spawn path.

Measured across concurrent executors, allocation cost was **flat from 1 to 8
threads and 1.33x at 64**. Task blocks are allocated and freed on the same
thread, which is the case every modern allocator has a per-thread fast path
for. There was no contention to remove.

### 2. It cost ~98 MB resident per executor

`SLOT_CAPACITY * SLOT_SIZE` = 100,000 x 1024 = 97.6 MB, and free-list
initialization writes a link into every slot, so the whole mapping is touched —
and therefore resident — at executor creation. Drop then called
`mprotect(PROT_NONE)` instead of freeing, so it was never returned to the OS.

### 3. It panicked on ordinary code

`MAX_TASK_SIZE == SLOT_SIZE == 1024`. Any spawned closure larger than 1 KB
aborted the process. `7f9cec9` had removed the heap fallback specifically to
keep the fast path branch-free, which converted an uncommon-but-fine case into
a crash.

### 4. It segfaulted on detached tasks

A task that outlived `run()` touched the `mprotect`ed region and died. This is
legal glommio usage. Two upstream tests had been marked `#[ignore]` to
accommodate it, and the suite terminated early: **122 tests then SIGSEGV**.
After the revert: **433 passing**, no ignores.

## The follow-up that also failed

A thread-local free list of recently freed task blocks (`ef14579`) was the
narrower version of the same idea, without the memory cost or the contract.
Also removed (`50a34a6`):

| allocator | spawn, 512 tasks live | with free list |
|---|---:|---:|
| glibc malloc | ~45 ns | ~29 ns |
| jemalloc | ~30 ns | — |
| mimalloc | ~24 ns | ~24 ns |

The cache recovered most of the glibc gap and was worth **nothing** under
mimalloc, which is already per-thread heaps with segregated free lists.
mimalloc alone (24 ns) beat glibc-plus-cache (25 ns), so the cache was never
the best available answer — it just partially compensated for a poor allocator
choice while retaining memory and displacing use-after-free diagnostics.

Reproduce:

```bash
RUSTFLAGS='--cfg alloc_mimalloc' cargo run --release --example alloc_compare
```

## What actually made spawn faster

`f28a619`, and it was not an allocator change. `get_sleep_notifier_for` was
called on **every spawn** and took a process-wide `RwLock` read plus a hashmap
lookup plus a `Weak::upgrade`. The task header now stores `executor_id: usize`
and resolves the notifier only on the foreign-wake path.

| | before | after |
|---|---:|---:|
| spawn, 1 executor | 37.5 ns | ~24 ns |
| spawn, 64 concurrent executors | 4350.9 ns | ~28-37 ns |

The 118x figure at 64 executors is the contention the arena was aimed at — it
was in glommio's own notifier registry, not in malloc. It is safe code, and it
also fixes [#448](../issue_448/) at the root, since tasks no longer pin the
executor's eventfd.

## Lessons

1. **Measure the premise, not just the change.** The arena was benchmarked
   extensively and did improve the number it was pointed at. Nobody first
   measured whether allocator contention existed.
2. **A benchmark that builds a `LocalExecutor` per iteration measures
   io_uring setup.** The old `spawn_benchmark` did, which is where the "80 ns
   baseline" came from. Real spawn+await was 38 ns.
3. **Custom allocators inside a library take the choice away from the user.**
   The global allocator dominates this path, and the deployment gets to pick
   one. Document the recommendation instead.

## Recommendation for deployments

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

## Commits

| | |
|---|---|
| `97c80ed` | audit that motivated the work |
| `287cd74` | arena prototype |
| `7f9cec9` | heap fallback removed (source of the >1 KB panic) |
| `06b888f` | `mprotect` on drop, `unsafe_detached` gate |
| `f98bca4` | shared-nothing contract |
| `cec3913` | **arena reverted** |
| `ef14579` | thread-local free lists |
| `50a34a6` | **free lists reverted**, mimalloc documented |
| `f28a619` | executor id in header — the change that worked |
| `c750df0` | public spawn API restored after the gate was dropped |
