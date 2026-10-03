# Glommio Fork Documentation

This fork contains fixes and investigations for critical Glommio issues.

**Upstream is now [glommio/glommio](https://github.com/glommio/glommio)**, a
community fork that took over when `DataDog/glommio` went quiet (last commit
2025-04-21). See [UPSTREAM.md](./UPSTREAM.md) for what of this fork's work is
worth contributing there, and in what order.

## Porting

- [Porting from tokio](./PORTING_FROM_TOKIO.md) — the differences that fail
  *silently*, organised around what compiles and then does the wrong thing.

## Start here for performance work

**[What Is Left, and What It Is Worth](./PERFORMANCE_NEXT.md)** — the current
ranked list, what each is worth, and the measurement traps that cost time.
Supersedes the performance sections of `PERFORMANCE_ROADMAP.md`.

## Fixes Implemented

### ✅ [Issue #700](https://github.com/DataDog/glommio/issues/700) - Memory Corruption in spsc_queue
**Status:** Fixed in PR #703
**Severity:** Critical (heap corruption in safe code)

Removed public `Clone` trait from `Producer` and `Consumer` in SPSC queue to prevent memory corruption when multiple producers/consumers are created.

**Branch:** `fix/issue-700-remove-spsc-clone`

## Investigations

### [Issue #448 - Eventfd Leak on Executor Drop](./investigations/issue_448/)
**Status:** Documented, workarounds available
**Severity:** High (resource exhaustion in long-running apps)

Comprehensive investigation of eventfd file descriptor leak when executors are repeatedly created and destroyed. Includes root cause analysis, potential fix approaches, and practical workarounds.

**Key Finding:** This is an architectural issue that the original maintainer attempted to fix but found "really hard" due to task lifecycle complexity.

**Workarounds:**
- Use long-lived executors (recommended)
- Thread-local executor pattern for tests

### [Issue #695 - Non-Panicking spawn_local()](./investigations/issue_695/)
**Status:** ✅ Implemented
**Severity:** Medium (API design issue)

Investigation of confusing `spawn_local()` API that panics even when called on a `LocalExecutor` instance. The current design ignores `self` and uses thread-local storage instead.

**Key Finding:** The private `spawn()` method actually uses `self` but is not public. Making it public solves the issue without breaking changes.

**Fix:** `LocalExecutor::spawn()` is now public. Additive — thread safety still
comes from `!Send`, and no existing API changed.

### [Task Arena Allocator - Post-Mortem](./investigations/task-arena/)
**Status:** Tried, measured, reverted
**Outcome:** No arena — recommend mimalloc instead

A slot allocator for task blocks was built and benchmarked, then removed. Its
premise (global allocator lock contention on the spawn path) did not survive
measurement: allocation cost is flat from 1 to 8 concurrent executors. The
arena also cost ~98 MB resident per executor, panicked on task closures over
1 KB, and segfaulted on detached tasks.

**Key Finding:** The contention was in glommio's own sleep-notifier registry,
not in malloc — a process-wide `RwLock` taken on every spawn. Fixing that took
spawn at 64 executors from 4350.9 ns to ~28-37 ns, in safe code, and fixed
#448 at the root as a side effect.

### [Where the Cycles Actually Go](./investigations/mechanical-sympathy/)
**Status:** Three candidates, each premise measured before proposal
**Approach:** measure the machine, then the runtime, then propose

Measures what a glommio task switch actually spends its 28.7 ns on, and what
this hardware charges for the primitives involved. Reference counting is 58% of
a task switch, and two of the four atomics per switch exist only because the
schedule closure captures 8 bytes.

**Candidates, with ceilings established by patching and re-benchmarking:**
- Zero-sized schedule closure — **-32%** task switch, low risk, safe code
- Cache-domain-aware placement — **-41%** cross-shard round trip; `CpuLocation`
  has no L3 level, and cross-L3 line transfer costs 11.4x here
- Biased reference counting — **-51%** task switch, high risk, the real prize

**Also records what not to do:** the planned `RefCell` → `UnsafeCell` work
(P2b, "10-15%", Critical unsoundness risk) has an unmeasured premise — a
`RefCell` borrow costs 0.449 ns.

### [The I/O Path](./investigations/io-path/)
**Status:** Measured and resolved to a single number
**Result:** glommio costs **~2 µs per blocking I/O** — 6% of an NVMe read, 53% of a loopback TCP round trip

[synthesis.md](./investigations/io-path/synthesis.md) is the short version, and
[per-io-cost.md](./investigations/io-path/per-io-cost.md) attributes it: a TCP
echo round trip processes **nine io_uring completions to perform one read** —
one real poll, three preempt timers, five cancellations. The
file path, the network path and the reactor loop turned out to be the same
measurement: the executor round trip forced by any operation that blocks. A
ladder implementing glommio's readiness design *by hand* costs −34 ns against
completion-based io_uring, so the network design is not at fault and rewriting it
would gain nothing.

Older sections below, kept for the reasoning:

4 KiB `O_DIRECT` random reads against a raw io_uring floor. The device dominates
at every depth, and glommio's absolute overhead *falls* as concurrency rises —
the shape of one reactor-loop iteration, which overlaps with device time once
several reads are in flight.

**Nothing there is worth optimising**, and it relocates the roadmap's monoio
question: per-operation path cost cannot explain a large gap, because there is
almost none to give back.

**The network path is the opposite** ([network.md](./investigations/io-path/network.md)):
loopback TCP ping-pong runs **+120% over a raw io_uring floor**. glommio's TCP
reads are readiness-based — a `recv` syscall bypassing io_uring with a `PollAdd`
fallback — costing about five SQEs and five kernel enters per side per round
trip where two suffice. First large non-hardware gap found.

An accompanying claim that streaming sends were 10x slower is **retracted**: the
raw side had Nagle on while glommio had `TCP_NODELAY`. Measured fairly, the
write path adds 127 ns over the bare syscall and the executor adds nothing.

### [Shard and Connection Scaling](./investigations/io-path/scaling.md)
**Status:** Measured — scales cleanly on both axes

Eight independent shards cost 1.11x one shard. One shard with 64 connections
costs the same per message as with 4. And per-round-trip cost **drops 2.4x from
one connection to four**, which is the first end-to-end evidence that glommio's
~2 µs per blocking I/O amortises away under realistic concurrency.

### [Replacing the Vendored `iou` / `uring_sys`](./investigations/iou-replacement/)
**Status:** Done, and the C is gone with it

`glommio/src/iou` and `glommio/src/uring_sys` were 3,042 hand-maintained lines
holding 108 of glommio's `unsafe` occurrences -- a copy of two abandoned crates
with nothing to upgrade to. They were replaced by the maintained `io-uring`
crate (`c2e8394`), and the last thing forcing a git dependency, an accessor
`need_preempt` needed, was upstreamed as
[tokio-rs/io-uring#404](https://github.com/tokio-rs/io-uring/pull/404) and
released in 0.7.14.

That left the build compiling five C files and carrying a 3.2 MB `liburing`
submodule that nothing called. Those are now gone too, so the crate needs no C
toolchain, no `make`, and no `configure` -- retiring the packaging failures
that came with them. The investigation is kept for the API mapping and the
reasoning about where the risk sat.

### [Unsafe Code Centralization Analysis](./investigations/unsafe-centralization/)
**Status:** Analysis complete
**Complexity:** High (7-12 weeks refactoring)

Comprehensive analysis of eliminating or centralizing unsafe code in glommio without performance degradation. Identifies 320 unsafe blocks scattered across 43+ files and proposes centralization into 4 core modules.

**⚠️ Partly superseded:** the analysis counts the task arena's 19 unsafe blocks
in its baseline and plans to relocate them. The arena has since been deleted.
See the note at the top of that document.

**Key Findings:**
- Unsafe code cannot be eliminated without 10-100x performance loss
- Can be centralized from 43+ files to 4 core modules (~1000 lines)
- Current scattering makes auditing and maintenance difficult

**Recommended Approach:**
1. Short-term: Document all unsafe with safety comments
2. Medium-term: Add Miri CI for continuous validation
3. Long-term: Incrementally refactor into `core/` modules

## Repository Structure

```
docs/
├── README.md (this file)
└── investigations/
    ├── issue_448/
    │   ├── README.md         # Eventfd leak analysis
    │   └── reproduce.rs      # Test demonstrating the leak
    ├── issue_695/
    │   └── README.md         # API design investigation
    ├── io-path/
    │   ├── README.md         # DMA read path vs the raw io_uring floor
    │   ├── network.md        # TCP path — the first large gap found
    │   ├── reactor-loop.md   # loop attribution; not a constant tax
    │   ├── probe_dma_read.rs
    │   └── probe_net.rs
    ├── iou-replacement/
    │   └── README.md         # Retiring the vendored io_uring wrappers
    ├── mechanical-sympathy/
    │   ├── README.md         # Where the cycles go + three measured candidates
    │   └── probe_*.rs        # Reproducible probes (primitives, topology, shard, switch)
    ├── task-arena/
    │   └── README.md         # Arena allocator post-mortem (built, measured, reverted)
    └── unsafe-centralization/
        └── README.md         # Unsafe code analysis & centralization strategy
```

Top-level documents:

| | |
|---|---|
| `PERFORMANCE_ROADMAP.md` | high-level phases and prioritization |
| `OPTIMIZATION_PLAN.md` | detailed implementation plans |
| `TASK_ALLOCATION_AUDIT.md` | allocation lifecycle — arena conclusions retracted inline |
| `BENCHMARKING.md`, `COVERAGE.md`, `LIMA_TESTING.md` | tooling |
| `CACHE_OPTIMIZATION_ANALYSIS.md` | cache-line layout analysis |

## Contributing

This fork is maintained by [@dahankzter](https://github.com/dahankzter) while awaiting upstream response. If you encounter issues or have fixes, please open an issue or PR.

## Upstream Status

- **Abandoned:** [DataDog/glommio](https://github.com/DataDog/glommio) — last commit 2025-04-21, 16 open PRs
- **Live:** [glommio/glommio](https://github.com/glommio/glommio) — community fork, active, blessed by the original author to take the crates.io name
- **This fork:** merged with the community fork on 2026-08-02; ahead only

See [UPSTREAM.md](./UPSTREAM.md).
