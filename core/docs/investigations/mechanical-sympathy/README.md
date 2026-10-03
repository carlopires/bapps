# Investigation: Where the Cycles Actually Go

**Date:** 2026-08-01
**Method:** measure the machine first, then the runtime, then propose
**Status:** three candidates, each with its premise already tested

The task arena was built on an unmeasured premise and cost four months to
unwind ([post-mortem](../task-arena/)). This investigation inverts the order:
every candidate below has had its premise measured *before* being proposed, and
two of them have had their ceiling established by temporarily patching the
runtime and re-benchmarking. Nothing here is a projection.

---

## 0. The machine

All numbers are from the development box. They will differ elsewhere; the
*method* is what transfers.

```
AMD Ryzen Threadripper PRO 9975WX
32 cores / 64 threads, 1 socket, 1 NUMA node
L1d 32x48K   L2 32x1M   L3 4x32M
cache line 64B
```

The critical structural fact: **one NUMA node, one package, but four L3
domains** — cpus 0-7, 8-15, 16-23, 24-31 (plus SMT siblings). Section 4 shows
glommio cannot see this.

## 1. Primitive costs, measured here

`probe_primitives.rs`, 50M iterations each, uncontended and cache-hot — the
favourable case. The real path is never cheaper than this.

| operation | cost |
|---|---:|
| `AtomicI16` fetch_add + fetch_sub (Relaxed) | 8.822 ns |
| `Cell<i16>` inc + dec | 0.221 ns |
| `Arc` clone + drop | 9.029 ns |
| `Rc` clone + drop | 0.440 ns |
| `Weak::upgrade` + drop | 0.467 ns |
| `RefCell` borrow_mut + drop | 0.449 ns |
| `Mutex` lock + unlock (uncontended) | 9.501 ns |
| `AtomicBool` compare_exchange (Relaxed) | 4.460 ns |
| `AtomicI16` load (Relaxed) | 0.220 ns |

One read-modify-write is **~4.4 ns**. A relaxed atomic *load* is 0.22 ns — on
x86 it is a plain `mov`. The cost is the `lock` prefix, not the atomicity.
**Atomic RMW is 40x a `Cell` operation.** That is the whole story of section 3.

Note what is *not* expensive: `RefCell` and `Weak::upgrade`, at under half a
nanosecond each. Remember that for section 6.

## 2. What a task switch costs today

`probe_task_switch.rs`, one shard pinned to cpu 0:

```
single task, yield loop      28.72 ns/switch
two tasks alternating        28.82 ns/switch
```

28.7 ns per wake → schedule → poll cycle. That is the denominator for
everything below.

## 3. How much of it is reference counting: **58%**

`Header::references` is an `AtomicI16`. I instrumented
`increment_references` / `decrement_references` with a thread-local counter and
ran a spawn-plus-yield workload:

```
spawn+await, no yield :  1.00 RMW/task  (0.00 schedule calls)
spawn+await, 1 yield  : 11.00 RMW/task  (2.00 schedule calls)
spawn+await, 3 yields : 19.00 RMW/task  (4.00 schedule calls)
```

**Four atomic RMWs per wake cycle**, exactly. At 4.4 ns each that is ~17.6 ns
of a 28.7 ns task switch.

Rather than trust that multiplication, I patched the runtime and measured the
real ceiling (both patches reverted; the tree is clean):

| task switch | atomic RMW (today) | non-atomic (probe) |
|---|---:|---:|
| **schedule guard on (today)** | **28.72 ns** | 14.10 ns |
| **schedule guard off** | 19.45 ns | 11.97 ns |

Reference counting is **16.75 ns of a 28.72 ns task switch — 58%**. Glommio is
a thread-per-core runtime in which the overwhelming majority of these
increments and decrements are performed by the thread that owns the task, on a
line no other core is touching. It pays the `lock` prefix anyway.

### 3a. Two of those four atomics are an accident

Look at `RawTask::schedule`:

```rust
let guard = if mem::size_of::<S>() > 0 {
    Some(Waker::from_raw(Self::clone_waker(ptr)))   // atomic RMW
} else {
    None
};
(*raw.schedule)(task);
drop(guard);                                        // atomic RMW
```

`S` is the schedule closure, which lives *inside* the task allocation. If the
task is freed during the call, the closure's captured state dies underneath
it — hence the guard. But the guard is only needed because the closure is
non-zero-sized, and it is non-zero-sized for exactly one reason:

```rust
let tq = Rc::downgrade(&tq);
let schedule = move |runnable: Runnable| {
    let tq = tq.upgrade();                          // <- 8 captured bytes
    ...
};
```

It captures a `Weak<RefCell<TaskQueue>>`. My counter confirms the guard is
taken on **100% of schedule calls** (2 of 2, 4 of 4). Eight bytes of capture
cost two atomic RMWs on every single task switch.

---

## Candidate 1 — make the schedule closure zero-sized

**Status: implemented.** Delivered 28.72 → 19.46 ns/switch, matching the
predicted ceiling of 19.45 ns. Spawn cost unchanged at ~23 ns; `Header` still
40 bytes; 431 lib tests, 14 integration tests, 191 doctests and Miri all green.

**Measured win: 28.72 → 19.45 ns/switch, −32%.**
**Risk: low. Safe code. Directly precedented.**

Store the task-queue index in `Header` and have a ZST closure resolve it from
the thread-local executor, exactly as `f28a619` did for `executor_id`. That
commit removed a process-wide `RwLock` from the spawn path by replacing an
`Arc<SleepNotifier>` in the header with a `usize` id; this is the same move one
level down.

The pleasing detail — `Header` is 40 bytes with **exactly 4 bytes of end
padding**:

```
field .awaiter          16 bytes
field .executor_id       8 bytes
field .vtable            8 bytes
field .references        2 bytes
field .state             1 byte
field .latency_matters   1 byte
end padding              4 bytes   <- a u32 queue index fits here for free
```

A `u32` task-queue index lands in padding that is already being paid for. The
header stays 40 bytes, the task allocation is unchanged, and the cache
behaviour is identical.

**Cost of being wrong:** the `Weak` also handles a real case — a task whose
task queue has been destroyed. The index form must reproduce that (absent
index → drop the runnable), and `upgrade()` returning `None` is the current
signal. This needs care, not cleverness.

**What the implementation turned up.** The worry above was that a schedule
running while no executor is installed on the thread — `LocalExecutor::spawn()`
before `run()` — would fail to resolve the queue and silently drop the task.
It cannot happen: `RawTask::thread_id()` returns `None` in that situation, so
`None != Some(my_id)` sends the wake down the *foreign* path and the notifier
delivers it once `run()` starts. All three schedule call sites (`do_wake`,
`drop_waker`, `run`) sit behind that same check, so **a schedule function only
ever executes on the thread that owns the task, with its executor installed**.
`tests/spawn_public.rs::test_spawn_before_run_with_pending_future` pins this
down.

The optimization is also silently reversible — adding any capture to that
closure gives back a third of a task switch with no test failing — so
`assert_zero_sized` makes it a compile error, verified by injecting a capture.

**Validate before building:** already done — that is the 19.45 ns row above,
produced by forcing the guard condition to `false`.

---

## Candidate 2 — stop paying for atomics on thread-local wakers

**Status: attempted and scrapped.** Re-measured properly, the win is
**1.1-2.5 ns (7-15%)**, not the 51% below, and Miri cannot validate the change.
See "why this was scrapped" at the end of this section.

**~~Measured win: 28.72 → 14.10 ns/switch, −51%. With candidate 1: 11.97 ns, −58%.~~**
**Risk: high. This is the hard one, and it is the real prize.**

The refcount is atomic because Rust's `Waker` is
`unsafe impl Send + Sync` *unconditionally* (`core/src/task/wake.rs`). Any
third-party future may clone a waker and send it to another thread, so the
runtime cannot assume single-threaded access. Seastar avoids this by being a
closed world; Rust's ecosystem contract does not permit that.

Two routes, and they are not equally available:

**(a) Biased reference counting — works on stable today.** The owning thread
uses a non-atomic counter; any other thread uses a separate atomic one; the
task is freed when both reach zero. The owner's path — which is the 4 RMWs per
switch measured above — becomes plain loads and stores. This is a known
technique (Choi et al., "Biased Reference Counting", PACT'18) and needs no
ecosystem change. It costs one branch on the owner-thread check, which
predicts perfectly.

**(b) `LocalWaker`** — `!Send + !Sync`, `#[repr(transparent)]`, exists in core,
still unstable (tracking issue #118959). Cleaner, but it does not help until
the futures ecosystem accepts it, and it cannot be relied on for a library that
must work with arbitrary third-party futures.

Recommendation: (a). Glommio owns its own timers, io_uring sources and
channels, so a large fraction of wakers provably never leave their shard.

**Relationship to candidate 1:** they overlap — candidate 1 removes two of the
four atomics, so doing candidate 2 first would leave candidate 1 worth only
~2.1 ns rather than 9.3 ns. Do candidate 1 first anyway: it is small, safe,
and ships the larger part of its value immediately.

**Validate before building:** already done — the non-atomic column above is a
literal measurement of the ceiling, produced by replacing `fetch_add`/
`fetch_sub` with `load`+`store`. That patch is unsound as a shipping change;
it is sound as a measurement of what is available.

### Why this was scrapped

**The ceiling was measured wrong the first time.** The 2x2 in section 3 compared
cells across a single session but *different builds at different times*; the
baseline drifts by 15% between sessions on this box (28.72, then 19.46, then
16.50 for the same code path), which is enough to swamp the effect being
measured. Redone as an interleaved A/B — alternating the two builds back to back
within one session, five rounds:

| | atomic (today) | non-atomic | delta |
|---|---:|---:|---:|
| single task, yield loop | 16.50 ns | 15.48 ns | **−1.02 ns, −6.2%** |
| two tasks alternating | 16.14 ns | 13.66 ns | **−2.48 ns, −15.4%** |

Variance within each arm was under 0.1 ns, so this is the trustworthy number:
**removing every reference-count atomic from the task path is worth 1.1-2.5 ns.**

**Primitive cost is not path cost.** An atomic read-modify-write measures 4.4 ns
in isolation (section 1), and there are two left on the path, so this "should"
have been ~8.8 ns. It is not, because the isolated benchmark times a dependent
chain while the real path has other work to overlap with — out-of-order
execution hides most of the latency. Section 3's "58% of a task switch is
reference counting" is therefore **wrong**, and the error is instructive:
candidate 1's 9.3 ns came mostly from the work it deleted *around* the atomics
(a `Waker` construction, an `Option`, and a drop), not from the two atomics
themselves.

**And Miri cannot check the result.** Any task-lifecycle test aborts under Miri:

```
error: unsupported operation: syscall: unsupported syscall number 324
  sys::membarrier::syscall::sys_membarrier
```

`make miri-core` covers `channels::spsc_queue` and `free_list` — not the task
refcount. So this change would rewrite the reference counting in the task
lifecycle, with a signed shared counter that may go negative, an
only-the-owner-destroys protocol and hand-reasoned memory ordering, **with no
undefined-behaviour checker able to run over it**. That is the code path the
original maintainer described as "refcount hell in the task structures", and
where the arena's use-after-free lived.

`Header` also has no padding left — candidate 1 consumed it — so a second
counter means shrinking `executor_id` to a `u32` as well.

**1.1-2.5 ns is not worth that.** In a fork whose stated purpose is fixing
memory-safety bugs, taking unverifiable risk in the task lifecycle for 7-15% of
a task switch is the wrong trade. Scrapped.

If it is ever revisited, the prerequisite is making Miri able to run task tests
at all — probably by stubbing `sys_membarrier`. That is worth doing on its own
merits regardless of this candidate.

---

## Candidate 3 — glommio cannot see this machine's topology

**Status: implemented.** `CpuLocation` gained a `cache_domain` field, parsed
from the highest cache level sysfs reports, and the placement tree gained a
`Level::CacheDomain` between `Package` and `Core`. `MaxPack` now touches exactly
one cache domain at 2, 4, 8 and 16 shards on this part, and `MaxSpread` uses all
four as soon as it has four shards to place. See "what it delivered" below.

**Measured: cross-L3 shard placement costs +69% on a shard-to-shard round trip.**
**Risk: low. Safe code. Pure gain on multi-CCX parts.**

`probe_topology.rs`, cache-line ping-pong between pinned threads:

| | round trip |
|---|---:|
| same core, SMT sibling (0↔1) | 58.5 ns |
| same L3 domain (0↔4) | 42.5 ns |
| **cross L3 domain (0↔16)** | **486.6 ns** |
| far cross domain (0↔24) | 516.9 ns |

**11.4x.** Crossing an L3 domain on this part costs an order of magnitude more
than staying inside one. And SMT siblings are *worse* than separate cores in
the same domain — co-locating two shards on one physical core is a pessimisation.

`probe_shard_ping.rs` measures whether that reaches the runtime. Two shards
ping-pong a `u64` over paired `shared_channel`s; only the CPU assignment changes:

| | default (park) | `spin_before_park(10us)` |
|---|---:|---:|
| same L3 (0↔4) | 8002 ns | 4428 ns |
| cross L3 (0↔16) | 13509 ns | 5276 ns |
| **penalty** | **+69%** | +19% |

Now the structural problem. `CpuLocation` is:

```rust
pub struct CpuLocation {
    pub cpu: usize,
    pub core: usize,
    pub package: usize,
    pub numa_node: usize,
}
```

and the placement tree is `SystemRoot → NumaNode → Package → Core → Cpu`
(`placement/pq_tree.rs`, `enum Level`). **There is no cache-domain level.** This
machine reports one NUMA node and one package, so `MaxSpread` and `MaxPack` see
32 undifferentiated cores and cannot distinguish an arrangement that is 69%
slower. The kernel has been telling us all along:

```
$ cat /sys/devices/system/cpu/cpu0/cache/index3/shared_cpu_list
0-7,32-39
```

**The work:** parse `cache/index3/shared_cpu_list` alongside the existing
`physical_package_id` / `core_id` reads, add an `l3_domain` field to
`CpuLocation`, insert a `Level::CacheDomain` between `Package` and `Core`, and
extend the sort key in `hardware_topology.rs` (currently
`(numa_node, package, core, cpu)`). `MaxPack` then packs within an L3 domain
and `MaxSpread` spreads across domains deliberately rather than by accident.
This is a new public field on a public struct, so it is a semver consideration.

### What it delivered

Detection on this part is exact -- four domains matching the kernel's
`shared_cpu_list` byte for byte:

```
domain 0: package 0 numa 0 cpus [0-7, 32-39]
domain 1: package 0 numa 0 cpus [8-15, 40-47]
domain 2: package 0 numa 0 cpus [16-23, 48-55]
domain 3: package 0 numa 0 cpus [24-31, 56-63]
```

Placement is now domain-coherent by construction:

| | domains touched |
|---|---|
| `MaxPack` at n = 2, 4, 8, 16 | **1** |
| `MaxSpread` at n = 2 | 2 |
| `MaxSpread` at n = 4, 8, 16 | **4** |

End to end, on the pairs those policies actually select:

| pair | round trip |
|---|---:|
| same domain, different cores (24↔26) | 8133 ns |
| **what `MaxPack` picks (24↔56)** | **9228 ns** |
| cross domain (24↔0) | 12808 ns |

**An honest wrinkle.** `MaxPack` at n=2 picks CPUs 24 and 56, which are SMT
siblings of one physical core, and that is 13% slower than two separate cores in
the same domain -- consistent with the ping-pong table above, where SMT siblings
(58.5 ns) lose to same-domain separate cores (42.5 ns). This is not a regression
and not something this change should quietly alter: `MaxPack`'s contract is to
minimise the CPU footprint, and it now does that within one cache domain instead
of wherever core numbering happened to land. What it buys is the elimination of
the 12808 ns case. A communication-optimised policy that prefers separate cores
within a domain would be a *new* placement, not a change to this one.

**Guarding the fallback.** A machine that reports no cache topology falls back to
the package id, which reproduces today's behaviour exactly -- and all 431
pre-existing tests passing unchanged is the evidence for that, since they all
construct topologies through the fallback path. One subtlety cost a test: the
fallback is not unique across parents when NUMA nodes nest inside packages, so
`from_topology` remaps cache domains against `(numa_node, package)` before
building the tree, restoring the uniqueness the tree builder requires.

### 3a. `IORING_OP_MSG_RING` — premise measured, mostly falsified

The obvious next thought: 8 µs for a round trip whose data movement costs 42 ns,
and glommio's wake is a `write(2)` into the peer's eventfd plus a read back out.
Since kernel 5.18 `IORING_OP_MSG_RING` posts a CQE straight into a peer's ring
and removes both. Glommio already requires 5.8+, and its vendored liburing
already declares the opcode. It looks like free money.

It is not. `probe_msg_ring.rs` pits the two mechanisms against each other in
isolation — two threads, one ring each, one submit-and-wait per direction, only
the wake mechanism differing:

| | eventfd write + read | `MSG_RING` | change |
|---|---:|---:|---:|
| same L3 (0↔4) | 2236 ns | 2165 ns | **−3.1%** |
| cross L3 (0↔16) | 6214 ns | 4951 ns | −20.3% |

**Same-domain it is noise.** The syscall was never the cost — parking and
unparking is. Both mechanisms still block in `io_uring_enter` and still need a
cross-core wakeup, and that is what the 2.2 µs buys. This is the same thing
`spin_before_park` showed from the other direction.

`MSG_RING` is worth revisiting as a modest cross-domain gain *after* placement
lands, and not before. On its own it does not justify the complexity.

**Superseded — see section 5.** `MSG_RING` alone is worth −3% because the
overhead it would expose is hidden behind io_uring's default completion
delivery. Combined with the taskrun setup flags it is worth −25%. The
conclusion "not worth it" was right about the change measured and wrong about
the change worth making.

### 3b. The number this turned up instead

Set the two measurements side by side:

| | round trip |
|---|---:|
| raw io_uring wake, parked, same L3 | 2236 ns |
| glommio `shared_channel`, same L3 | 8002 ns |
| glommio `shared_channel`, same L3, `spin_before_park(10us)` | 4428 ns |

**Glommio spends ~5.8 µs per round trip above the kernel primitive it sits on**,
and ~2.2 µs of that survives even when it never parks. That is a far larger
target than the wake mechanism, and it is entirely in glommio's own code —
the SPSC ring handoff, `process_foreign_wakes`, the reactor loop's per-iteration
work, and the task wake that follows.

### 3c. Attributed: it is the syscall count

No profiler on this box, so the executor, the three rings and the wake path
were instrumented with counters directly and the shard ping-pong re-run.
Per round trip, both shards combined:

| | parked (default) | `spin_before_park(10us)` |
|---|---:|---:|
| round trip | 7328 ns | 3843 ns |
| **`io_uring_enter`** | **8.00** | **6.00** |
| — on `main` ring | 4.00 | 2.00 |
| — on `latency` ring | 4.00 | 4.00 |
| — on `poll` ring | 0.00 | 0.00 |
| run loop iterations | 2.00 | 2.00 |
| `run_one_task_queue` | 4.00 | 4.00 |
| eventfd writes | 2.00 | 0.00 |
| parks | 2.00 | 0.00 |
| foreign wakes | 0.00 | 0.00 |
| notifier registry lookups | 0.00 | 0.00 |

**The raw primitive needs 2 `io_uring_enter` per round trip. Glommio issues 8.**

### 3d. Correction: the syscall count is not the answer

The obvious reading of 3c is that glommio is paying four times the syscalls and
that is the gap. **That reading is wrong, and measuring it is what showed it.**

A non-sleeping `io_uring_enter` on this box (`probe_msg_ring.rs`, `entercost`
binary):

| | cost |
|---|---:|
| `submit()` with one SQE queued | 99 ns |
| `submit_and_wait(1)`, completion already posted | 110 ns |
| `submit()` with an empty SQ | 69 ns |

**~100 ns.** Six extra enters is ~600 ns, not the ~5 µs gap. The enters that
cost real time are the ones that *sleep*, and glommio issues the same number of
those as the primitive does: two, one per shard.

So 3c identified a true difference that is not the important one. Recorded
rather than quietly fixed, because it is the same mistake the arena made in a
smaller form — a plausible mechanism, a real measurement pointed at it, and the
wrong conclusion drawn because the unit cost was never checked.

### 3e. Where it actually goes

Phase timers inside the run loop, per round trip, both shards combined
(`Instant::now()` around each phase adds roughly 250 ns per iteration of
measurement overhead, visible as the round trip inflating from 7204 to 7529 ns;
treat these as apportionment, not absolutes):

| phase | parked | spinning |
|---|---:|---:|
| round trip | 7529 ns | 4137 ns |
| `parker.poll_io` | 3282 ns | 3189 ns |
| — poll ring | 248 ns | 1236 ns |
| — main ring | 121 ns | 625 ns |
| — latency ring | 118 ns | 607 ns |
| — syscall-thread flush | 53 ns | 275 ns |
| — remainder (park/sleep, preempt timer) | ~2742 ns | ~446 ns |
| `run_task_queues` | 1457 ns | 282 ns |
| polling the main future | 38 ns | 36 ns |

Read the parked column: **most of `poll_io` is the park and the wakeup**, which
the raw primitive pays too (~2.2 µs of its 2236 ns). The three rings' per-
iteration bookkeeping is ~540 ns. `run_task_queues` is ~1.5 µs.

The spinning column looks like the rings got dramatically more expensive; they
did not. `spin_before_park` spins *inside* `poll_io`, so those figures
accumulate many calls per loop iteration. That is the cost of buying latency
with CPU, working as intended.

**The honest conclusion is that there is no single dominant target.** The gap
decomposes roughly into ~2.2 µs of irreducible park-and-wake that any design
pays, ~1.5 µs of `run_task_queues`, ~0.5 µs of ring bookkeeping across three
rings, ~0.6 µs of extra syscalls, and loop overhead. Attacking any one of them
returns a fraction of a fraction.

That is a much less exciting answer than 3c looked like, and it is the answer.

### 3f. One thing that is now ruled out

"Stop installing the preempt timer" is not available. `uring.rs:1769` says the
preempt timer is optional from io_uring's point of view, and it is — but the
eventfd a sleeping shard is woken through is installed on the **latency ring**
(`uring.rs:1346`), and the latency ring only enters the kernel when it has
something to submit. Remove the preempt timer and the eventfd read SQE never
reaches the kernel, so a parked shard is never woken.

Verified the direct way: patching `poll_io(|| Some(...))` to `poll_io(|| None)`
deadlocks the shard ping-pong immediately, in both the parked and the spinning
configuration. The preempt timer is load-bearing for the wake path.

Note also that the topology penalty is *larger* on the raw primitive (+178%,
2236 → 6214 ns) than on glommio's channel (+69%), because glommio's fixed
overhead dilutes it. Placement gets better, not worse, as 3b is addressed.

---

## 5. Candidate 4 — glommio sets no io_uring setup flags

**Measured: −12% to −29% on the raw wake round trip. Not yet attempted.**

glommio's vendored `iou` wrapper exposes `SetupFlags` up to bit 5
(`ATTACH_WQ`) — a 5.5-era view of io_uring. The kernel header vendored in the
same repository defines them to bit 18. Two of the missing ones exist precisely
for a runtime where one thread owns one ring, which is glommio's shape by
construction:

| flag | kernel | effect |
|---|---|---|
| `COOP_TASKRUN` (1<<8) | 5.19 | stop forcing completion task work with an IPI |
| `SINGLE_ISSUER` (1<<12) | 6.0 | one submitter promised, kernel drops locking |
| `DEFER_TASKRUN` (1<<13) | 6.0 | run completion work when the owner waits, not on completion |

`probe_setup_flags.rs`, same two-thread two-ring ping-pong as 3a, varying only
the ring setup. Best of three per cell:

**same L3 (0↔4)**

| setup | eventfd | `MSG_RING` |
|---|---:|---:|
| default (what glommio does) | 2097 ns | 1988 ns |
| `COOP_TASKRUN` | 2120 ns (+1.1%) | 1517 ns (**−23.7%**) |
| `SINGLE_ISSUER` | 2154 ns (+2.7%) | 1527 ns (−23.2%) |
| `SINGLE_ISSUER｜DEFER_TASKRUN` | 1850 ns (**−11.8%**) | 1481 ns (**−25.5%**) |

**cross L3 (0↔16)**

| setup | eventfd | `MSG_RING` |
|---|---:|---:|
| default | 6327 ns | 5096 ns |
| `COOP_TASKRUN` | 6420 ns (+1.5%) | 5045 ns (−1.0%) |
| `SINGLE_ISSUER` | 6416 ns (+1.4%) | 4967 ns (−2.5%) |
| `SINGLE_ISSUER｜DEFER_TASKRUN` | 5986 ns (−5.4%) | 4727 ns (−7.2%) |

Keeping the current eventfd mechanism, `DEFER_TASKRUN` alone is worth −11.8%
same-domain. Taking the whole combination — `MSG_RING` on a `DEFER_TASKRUN`
ring — is **2097 → 1481 ns, −29.4%** same-domain and **6327 → 4727 ns, −25.3%**
across domains.

**Why 3a was misleading.** `MSG_RING` on default rings saves nothing because
io_uring's default completion delivery dominates both mechanisms. Remove that
with the taskrun flags and `MSG_RING` is suddenly worth a quarter of the round
trip. Two changes that each look worthless can be worth a lot together, which no
amount of measuring them one at a time will reveal.

### Before anyone builds this

**Scale it honestly first.** The raw wake is ~2.2 µs of glommio's ~8 µs shard
round trip (3c/3e), so −29% of the wake is roughly −7% of the round trip unless
`DEFER_TASKRUN` also helps the reactor's general completion processing. It might;
that has not been measured and must not be assumed.

**`DEFER_TASKRUN` invalidates a load-bearing assumption.** `sys/uring.rs`:

```rust
fn needs_kernel_enter(&self) -> bool {
    // We only need to enter the kernel to submit SQEs, not to collect CQEs (the
    // kernel posts the CQEs asynchronously for us)
    self.waiting_kernel_submission() > 0
}
```

Under `DEFER_TASKRUN` that comment is false: completions are only reaped inside
an enter with `GETEVENTS`. Adopting it means reworking when glommio enters the
kernel, which is exactly the code 3c/3e says is already responsible for the
syscall count. This is not a flag you can simply turn on.

**Version gating.** glommio supports 5.8+; `DEFER_TASKRUN` and `SINGLE_ISSUER`
need 6.0, `COOP_TASKRUN` needs 5.19. All three have to be probed at runtime and
fall back cleanly.

~~The vendored `iou` wrapper needs the constants added.~~ **No longer true** —
`iou` is gone, see [iou-replacement](../iou-replacement/). The flags are one
builder call away: `IoUring::builder().setup_single_issuer().setup_defer_taskrun()`.

### 5a. Re-measured, and most of section 5 does not survive

The table above is best-of-three with no variance reported. Running the same
probe three times in one session:

| cell, same L3 | run 1 | run 2 | run 3 | spread |
|---|---:|---:|---:|---|
| eventfd, default | 2097 | 2088 | 2401 | ±8% |
| **eventfd, `SI｜DT`** | **1850** | **2433** | **2213** | **±14%** |
| MSG_RING, default | 1988 | 1979 | 2116 | ±4% |
| **MSG_RING, `SI｜DT`** | **1481** | **1464** | **1490** | **±1%** |

**The −11.8% for eventfd plus `DEFER_TASKRUN` is noise.** Three runs give
−11.8%, +16.5%, −7.8% — mean about zero. The "flags first, keep the eventfd"
step this section recommended buys nothing measurable, so there is no cheap
intermediate rung and the phased rollout above should be ignored.

What does reproduce is the endpoint: **MSG_RING on `SINGLE_ISSUER|DEFER_TASKRUN`
rings, 1481 / 1464 / 1490 ns, stable to 1%**, against a baseline that wobbles
±8%. Call it −30% of the wake, and treat it as all-or-nothing.

An attempt to separate the sender's flags from the target's
(`probe_flag_split.rs`) failed: isolating the roles requires MSG_RING on one leg
and an eventfd on the other, which is exactly the mixed configuration the table
shows to be unstable. The question is still open and the rig cannot answer it.

**And scale it before believing it.** The wake primitive is ~2.2 µs of glommio's
~8 µs shard round trip (3c/3e), so −30% of the wake is roughly **−9% of a round
trip** — and only when the peer is parked, since `notify` skips the eventfd
entirely when `should_notify` is clear. On a busy shard the peer is already
awake and none of this executes. That is a much narrower claim than the section
heading suggests, and it is the honest one.

**Recommendation: do not touch the wake path on spec.** Revisit when a real
workload is on glommio and cross-shard wakes appear in a profile.

## 6. What not to do next

`OPTIMIZATION_PLAN.md` has **P2b: RefCell → UnsafeCell**, claiming "10-15% task
switch", rated High risk, with "RefCell → UnsafeCell unsoundness" listed as a
**Critical** consequence in its own risk table.

A `RefCell` borrow_mut + drop costs **0.449 ns** (section 1). The task switch is
28.72 ns. Removing *every* borrow on the path — there are not more than a
handful — cannot reach 10%, and the same measurement shows 58% of that switch
sitting in atomics that can be removed **without any unsafe at all**.

This is the arena's failure mode exactly: a plausible mechanism, a confident
percentage, no measurement, and a proposal to trade safety for it. It should be
struck from the plan or re-scoped to the safe caching half (P2a), and the
effort moved to candidates 1 and 2.

A second negative result, from `probe_topology.rs`:

```
contended fetch_add on one line:
  1 thread                         4.2 ns/op
  8 threads, same L3               5.9 ns/op
  8 threads, spread across L3      7.0 ns/op
  32 threads, all domains          7.3 ns/op
```

Contention on a single hot line degrades by 1.7x across 32 threads and four
cache domains. This is worth keeping in view whenever someone proposes removing
"lock contention" — it independently corroborates the arena post-mortem's
finding, on the same hardware, by a different route.

---

## 7. Order of work

| | candidate | measured win | risk | why this order |
|---|---|---:|---|---|
| 1 | ZST schedule closure | −32% task switch | low | **done** — delivered 19.46 ns |
| 2 | cache-domain placement | avoids a +58% cross-domain penalty | low | **done** — `MaxPack` now touches one domain at every size |
| — | ~~biased reference counting~~ | 1.1-2.5 ns (7-15%) | — | **scrapped**: re-measured far smaller than thought, and Miri cannot cover the task lifecycle |
| — | io_uring setup flags alone | noise (±14%) | — | **dropped** (5a); the cheap intermediate step does not exist |
| 4 | MSG_RING + `SINGLE_ISSUER｜DEFER_TASKRUN` | −30% of wake, ~−9% of round trip | med | **parked** (5a); all-or-nothing, and only when the peer parks. Needs a real workload before it is worth the second wake mechanism |
| — | ~~`IORING_OP_MSG_RING`~~ | −3% same-domain | — | **premise measured, dropped**; see 3a |
| — | ~~latency-ring syscalls~~ | ~600 ns of a ~5 µs gap | — | **dropped**: an enter costs ~100 ns, and removing the preempt timer deadlocks the wake path (3d, 3f) |

Candidate 3's ceiling is already known (14.10 ns), which is the difference
between this list and the roadmap that produced the arena.

## 8. Reproducing

The probes are in this directory. `probe_primitives.rs` and
`probe_topology.rs` are standalone (the latter needs `libc`);
`probe_shard_ping.rs` and `probe_task_switch.rs` depend on `glommio` by path.

```bash
cargo run --release --bin probe      # primitive costs
cargo run --release --bin topo       # cache-domain ping-pong
cargo run --release                  # shard round trip / task switch
```

The two ceiling measurements in section 3 require temporarily patching
`glommio/src/task/raw.rs`:

1. **Candidate 1 ceiling:** change `if mem::size_of::<S>() > 0` to `if false`
   in `RawTask::schedule`.
2. **Candidate 2 ceiling:** replace `fetch_add`/`fetch_sub` in
   `increment_references`/`decrement_references` with `load` + `store`.

Both are unsound as shipping changes and sound as measurements. Revert them.
