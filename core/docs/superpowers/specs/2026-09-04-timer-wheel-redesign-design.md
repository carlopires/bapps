# Design: timer wheel redesign — stable handles over a cascading wheel

**Date:** 2026-09-04
**Status:** approved in chat 2026-09-04. Specifies arm A of a three-arm
comparison — see [Arms of the comparison](#arms-of-the-comparison) — and no
implementation plan should be written for any arm until
[Measurement](#measurement) step 1 has an answer
**Supersedes:** the implementation on `perf/timing-wheel`, offered upstream as
[#33](https://github.com/glommio/glommio/pull/33)
**Review that prompted it:** `vlovich` on #33, 2026-09-04, 14 inline comments

All line references below are to branch `perf/timing-wheel` unless stated.

## Why

#33 replaced the timer `BTreeMap` with a hierarchical wheel and claimed
17.7 → 10.3 ns at 100k timers. Review found the claim rests on an
implementation with four defects, three of which were confirmed by reading the
code and one of which invalidates the measurement.

| Defect | Evidence |
|---|---|
| `generation` is never set | `reactor_adapter.rs:62` mints `TimerId::new(internal_id as u32, 0)`; `timer_id.rs` marks `generation()` `#[allow(dead_code)]`, while its doc describes a slot-reuse guard |
| `TimerId` truncates a monotonic counter | `staged_wheel.rs:121` advances `next_id: u64`; `reactor_adapter.rs:62` casts it `as u32`. At 100k timers/sec that wraps in ~12 hours, after which `remove` cancels a different live timer |
| Next expiry is O(n) | `reactor_adapter.rs:116` is `id_to_expiry.values().copied().min()`, a scan of every live timer on every poll. Upstream gets the same answer from `keys().next()` on a `BTreeMap` in O(1) — so the PR regresses what it set out to improve |
| `advance_to` is O(elapsed ms) | `timing_wheel.rs:217` loops `while self.current_tick < target_tick { self.tick(); }`, unconditionally, regardless of timer count. An executor promoted to wheel mode that then idles ten minutes performs 600,000 iterations on its next poll |

Two further findings, not raised in review:

- **Three implementation modules become public API.** `timer/mod.rs` declares
  `pub mod timing_wheel; pub mod staged_wheel; pub mod timer_id;` where
  upstream has `mod timer_impl;` private. `TimerId` is a `pub` type whose
  fields and constructor are `pub(crate)` — reachable but unconstructable, the
  same shape as the `OwnedRxBuf` bug recorded in CLAUDE.md.
- **The benchmark cannot see defect 4.** `benches/timer_benchmark.rs` spreads
  expiry over `counter % 10000` ms and never advances across an idle gap.

**The measurement is therefore void.** 17.7 → 10.3 ns was taken with the O(n)
scan in the loop and no idle case. Whether a wheel beats the `BTreeMap` at
glommio's real timer counts is currently unknown and must be re-established
before this design is worth building. See [Measurement](#measurement).

## The shape of the fix

Handles fail today because they name a *location*, and a cascading wheel moves
timers between locations. The fix is to make the handle name a *slab slot* that
never moves, and let the wheel store slab indices rather than entries.

That keeps cascading — and therefore full expiry precision — while making
cancellation a genuine O(1) array index. It is the property `bitwheel` offers
by forbidding migration altogether; here it comes from indirection instead, at
the cost of one pointer chase on expiry and none on cancel.

**This design is one of three arms, not a foregone conclusion.** It is built
in parallel with a `bitwheel` integration and measured against upstream's
`BTreeMap`; the direction is chosen from the results. See
[Arms of the comparison](#arms-of-the-comparison).

## Arms of the comparison

| Arm | What it is | What it must prove |
|---|---|---|
| **Control** — upstream `BTreeMap` | `reactor.rs:123-139` on `main`. O(log n) insert and remove, O(1) next expiry, exact firing, no capacity limit | Nothing. It is the incumbent, and if it is not beaten it wins by default |
| **A — slab over a cascading wheel** | This document | That O(1) cancel and O(levels) next-expiry beat O(log n) at the timer counts step 1 finds real, and that cascade cost does not eat the win |
| **B — `bitwheel` integration** | The crate, wired to `Timer`/`TimerDriver` | That its fixed capacity can be sized for glommio without unacceptable memory, and that its firing imprecision is invisible to real users |

### What reading `bitwheel` 0.6.0 established

The API fits glommio better than its README suggests. `TimerDriver<T: Timer>`
is generic over the payload with **no `Send`/`Sync` bound**, which suits a
thread-per-core `!Send` runtime. `poll(now, ctx)` threads a
`&mut T::Context` through firing, so `Context = Vec<Waker>` falls out naturally
— and that shape fixes the `RefMut`-held-across-`wake` problem structurally
rather than by convention. `peek_next_fire()` is O(1) from a cached
`next_fire_tick` with per-gear minima and a dirty mask, which is a better
answer to next-expiry than the bitmap scan arm A proposes.

Four properties decide whether it can host glommio:

1. **Capacity is fixed at compile time and the default is small.**
   `NUM_SLOTS` is a hard `const 64` in `timer/gear.rs`, not a generic, so
   capacity is `64 × SLOT_CAP × NUM_GEARS`. Defaults (`SLOT_CAP = 32`,
   `NUM_GEARS = 5`) hold 10,240 timers. Reaching 100k needs roughly
   `SLOT_CAP = 256, NUM_GEARS = 8` — 131,072 preallocated slots, per
   executor, multiplied by core count.
2. **Overflow falls back to a `BTreeMap`.** `BitWheelWithFailover` holds
   `failover: BTreeMap<(u64, u32), T>`. Undersized, it degrades into the
   control at exactly the scale the wheel was supposed to win.
3. **Firing imprecision is congestion-dependent, not uniform.**
   `DEFAULT_RESOLUTION_MS = 4`, and `acquire_next_available` probes forward up
   to `MAX_PROBES = 3` slots when the target is full, so a timer fires up to
   12 ms late — but only under slot congestion. `delay.max(1)` also means
   `sleep(1ms)` cannot fire before one tick. Both are tunable; finer
   resolution costs gears for the same horizon.
4. **It moves unsafe code into a dependency.** `timer/slot.rs` carries 166
   lines containing `unsafe` out of 965. The zero-unsafe property #33 claimed
   is not preserved by adopting it, and unsafe in a dependency is unsafe we do
   not Miri.

Non-technical, and real: the crate published ten-plus releases between
2025-12-14 and 2025-12-18 and nothing since, is pre-1.0, and is 28%
documented. A project whose own premise is that its upstream went unmaintained
should weigh taking a dependency with that shape — but weigh it against the
code, which is substantial and benchmarked, not against the release cadence
alone.

## Types

The truncation bug exists because two concepts were both spelled as integers
and silently converted: a monotonic identity (`next_id: u64`) and a storage
location. Separating them by type makes the conversion unwritable.

```rust
/// Position in the timer slab. Bounded by live timers, not by insertions.
#[derive(Clone, Copy, PartialEq, Eq)] struct SlotIndex(u32);
/// Bumped when a freed slot is reused.
#[derive(Clone, Copy, PartialEq, Eq)] struct Generation(u64);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerId { slot: SlotIndex, generation: Generation }
```

There is no monotonic counter anywhere, so there is nothing to wrap.
`SlotIndex` is bounded by *concurrently live* timers; four billion of those is
not a case, where four billion insertions was a twelve-hour case.

`generation` becomes structural rather than decorative:

```rust
enum Slot {
    Occupied { generation: Generation, entry: TimerEntry, at: WheelPos },
    Vacant   { generation: Generation, next_free: Option<SlotIndex> },
}
```

Generation lives in **both** states, which is what makes stale-handle detection
sound: it survives the free/reuse cycle. The free list threads through
`Vacant`, so there is no parallel `Vec` to fall out of sync with the slab.
`TimerId` has one constructor, private to the slab module, so a
`generation: 0` cannot be minted by hand the way `reactor_adapter.rs:62` does.
No `#[allow(dead_code)]` survives, because `remove` reads the field on every
call.

Identity and location separate at the type level too. Wheel slots become
`Vec<SlotIndex>` and *cannot* hold a `TimerEntry`, so a cascade physically
cannot invalidate a handle. `TimerLocation` and
`index: AHashMap<u64, TimerLocation>` (`timing_wheel.rs:100`) both delete.

Runtime checks that belong in the type system go with them. `expire_slot`
opens with `debug_assert_eq!(level, 0, "Only Level 0 timers should expire
directly")` — an assertion guarding an argument that could simply have a type.
Levels and slots become newtypes and only level 0 carries an `expire`
operation, so the assertion disappears rather than being kept and doubted.

The three modules go back to `mod`. `TimerId` stays `pub(crate)` and appears in
no public signature. If that ever changes it gets an integration test in
`glommio/tests/`, per the "prove it from outside" rule.

## Layout and mechanical sympathy

**Cancellation is eager, not tombstoned.** The slab entry carries its own
`WheelPos { level, slot, index }`, so `remove` walks straight to the wheel slot
and `swap_remove`s it in O(1); the element swapped into the hole has its slab
entry's `WheelPos` back-patched, one extra store.

The alternative — leaving a stale index behind and skipping it at expiry —
fails on exactly this project's target workload. A proxy that sets a
30-second timeout and cancels it after 2 ms leaves a dead index in a level-2
slot for the full 30 seconds. At 100k ops/sec that is three million stale
entries, about 12 MB, for timers nobody is waiting on. Eager removal makes the
structure's size track live timers exactly.

**The slab is also a cascade optimization, independent of handle stability.**
Wheel slots hold `SlotIndex` — four bytes, 64 to a cache line, against four
entries per line if slots held `(Instant, Waker)` directly. Cascading becomes a
memcpy of `u32`s rather than moving 40-byte entries containing `Waker`s, so the
indirection pays for itself on the most expensive operation.

**Occupancy bitmaps remove both O(n) paths.** Each 256-slot level carries
`[u64; 4]`. Next expiry is `trailing_zeros` over at most sixteen words —
branch-predictable, one or two cache lines, independent of timer count. That
replaces `id_to_expiry.values().copied().min()`.

The same bitmaps fix `advance_to`. Rather than ticking each millisecond,
compute `target_tick`, then ask each level for its next occupied slot at or
before it and process only those. An executor idle for ten minutes performs a
handful of word scans instead of 600,000 iterations. Cost stops being
proportional to elapsed wall-clock time and becomes proportional to work
actually present.

## Overflow audit

Every integer in the design, and what bounds it.

| Value | Type | Bound | Verdict |
|---|---|---|---|
| `SlotIndex` | `u32` | live timers | Safe by memory: 4.29e9 slots × ~48 B is 206 GB, so OOM arrives first |
| `Generation` | `u64` | slot reuses | Safe only at u64 — see below |
| `Tick` | `u64` | ms since wheel start | 5.8e11 years |
| `index_in_slot` | `u32` | live timers in one slot | Same memory bound as `SlotIndex` |
| bitmap word/bit | range-checked | compile-time slot count | Enforced by newtype |
| `tick / res % slots` | `u64` | — | Division and modulo cannot overflow |

**`Generation` must be `u64`.** At `u32` it wraps sooner than the bug being
removed. A slot reused 4.29 billion times returns to a generation a stale
handle still matches, and with a LIFO free list — the normal choice — a hot
slot is reused immediately, so at 1M ops/sec that recurs in about 72 minutes.
`TimerId` becomes 16 bytes (`u32` + `u64`, padded), still `Copy`, still two
registers, and it lives in a `Cell` inside `TcpStream` where eight bytes cost
nothing. At 1e9 reuses/sec a `u64` generation wraps in 584 years.

**Deadline-to-tick conversion is guarded by routing, not by a separate check.**
Today `timing_wheel.rs:222-225` reads
`now.duration_since(self.start_time).as_millis().min(u64::MAX as u128) as u64`
— a saturating cast standing where a range check belongs. In the new design
only timers within `horizon` are converted to ticks at all; anything further
out goes to the overflow map keyed by `Instant` directly, with no millisecond
arithmetic. The conversion is unreachable unless the value already fits, so
there is no guard to forget.

**Config is validated at construction, not at use.** `granularity: 0` divides
by zero when deriving level widths, and `horizon < granularity` yields zero
levels. So `TimerConfig::new(granularity, horizon) -> Result<_, ConfigError>`,
and the wheel accepts only an already-validated `TimerConfig`. The wheel
constructor then has no failure mode, which is the point: nonsense is rejected
where a user can see it, not deep inside a slot calculation.

**One checked `usize → u32` in the subsystem**, at slab growth:
`SlotIndex::new(len: usize) -> Option<SlotIndex>`, with exhaustion a panic
carrying a real message rather than a wrap. Every other integer conversion in
the current implementation — `internal_id as u32`, `id.index() as u64` —
disappears with the monotonic counter.

`current_time()` computes `start_time + Duration::from_millis(tick)`, which
panics on overflow in std; `tick` is derived from an `Instant` delta, so
reaching it needs the 584-million-year case.

Separately, `timing_wheel.rs:176` writes `self.next_id = id + 1` where
`staged_wheel.rs:121` uses `wrapping_add` — an inconsistency that panics in
debug builds. It is listed here for completeness only; the line deletes along
with the counter.

## Config surface

The policy is user-selectable, with defaults good enough that nobody has to
touch it. Exposing *intent* rather than *mechanism* keeps that from
multiplying into configurations we would have to test.

```rust
pub struct TimerConfig {
    granularity: Duration,   // level-0 resolution; smallest firing delta
    horizon: Duration,       // beyond this, timers live in the overflow map
    storage: StorageHint,    // Adaptive (default) | Inline | Wheel
}
```

Level count and slot widths are derived from `granularity`, `horizon` and the
radix, so there is one wheel implementation taking numbers rather than several
strategies behind a trait. `StorageHint` has three variants and everything else
is arithmetic. Illegal states go with it: you cannot request a wheel whose
level-0 resolution is finer than its own granularity, because you never name
level 0.

**Defaults for the database/server/proxy profile:** `granularity: 1ms`,
`horizon: 1h`, `storage: Adaptive`. `DEFAULT_PREEMPT_TIMER` is already 100 ms
(`executor/mod.rs:72`), so 1 ms is two orders finer than the executor's own
scheduling quantum for `Latency::NotImportant` — it is not a precision anyone
perceives. A proxy with 30-second idle timeouts can set `granularity: 10ms`
and get a smaller, cheaper wheel; nobody is obliged to.

Config is per-executor, on `LocalExecutorBuilder`, propagated by
`LocalExecutorPoolBuilder`. Thread-per-core means each reactor owns its wheel,
so there is no shared-state question.

**Radix is a documented tradeoff, not a derived optimum.** The current
implementation uses 256 slots at level 0 and 64 above
(`LEVEL_0_SLOTS: 256`, `LEVEL_{1,2,3}_SLOTS: 64`), giving resolutions of
1 ms / 256 ms / 16.384 s / 17.48 min and a reach of 18.6 h. That mixed radix is
defensible — a fine level 0 where timers cluster, coarse levels above where
they are sparse — but it is written nowhere and was never measured. A uniform
radix of 256 reaches only 4.66 h in three levels, so covering the same 18.6 h
still costs four, overshooting to 49 days and spending 1024 slot vectors
against 448. The design keeps the radix explicit and records
the memory-versus-cascade tradeoff rather than asserting a winner; which radix
to ship is a measurement outcome.

**The bitmaps may make `StorageHint::Inline` moot.** Review asked why the wheel
never demotes back to inline. Demotion mattered because wheel mode had
O(elapsed) `advance_to`, so an idle wheel was expensive. Once `advance_to`
skips empty spans, a wheel holding three timers costs a few word scans. Inline
may still win at very small counts by avoiding the slab indirection, but the
256 threshold was never measured and there may turn out to be nothing worth
switching between. `Adaptive` stays in the API either way; whether it actually
switches is a measurement outcome, not a design commitment.

## Reactor integration

`reactor.rs:761` takes `self.timers.borrow_mut()` and holds the `RefMut` across
the entire call, wake loop included. #33's comment — "safe: no longer holding
any mutable state" — is therefore false, and collecting into a `Vec` before
waking only avoids invalidating an iterator inside `ReactorTimers`. The hazard
is real rather than compile-time-prevented, because `timers` is a `RefCell`
(`reactor.rs:146`): a waker that re-enters the reactor hits a second
`borrow_mut` and panics.

The fix drops the borrow before waking:

```rust
fn process_timers(&self, wakers: &mut Vec<Waker>) -> Option<Duration> {
    let mut timers = self.timers.borrow_mut();
    timers.drain_expired_into(wakers)
}   // RefMut dropped here — caller wakes afterwards
```

The caller owns the `Vec` and reuses it across iterations, which also removes
the per-batch allocation review asked about at `staged_wheel.rs:170`.

**This is upstream's bug too**, and it is separable. Upstream wakes inline at
`reactor.rs:130` under the same borrow, so #33 did not introduce it. It should
go up as its own small PR against `main`, independent of any wheel, rather than
riding along with a large change.

## Comparability protocol

Three implementations are only comparable if the thing measuring them is
identical. That is a structural property, not a matter of care, so it is
arranged rather than intended.

- **Every arm branches from `master` at the merge that carries the harness**
  (`64a0d77`), so all three inherit the same counters, the same ladder, and
  the same deadline-rounding baseline.
- **No arm may modify `timer_ladder.rs` or `timer/debugging.rs`.** A
  measurement the ladder cannot express is a change to `master`, which every
  arm then rebases onto. An arm that edits its own instrument has stopped
  being comparable and its numbers are void.
- **The counters live in the reactor's `Timers` wrapper, not in any wheel**,
  so they mean the same thing whatever is underneath.
- **The control is a branch too.** `arm/control-btreemap` is `master` with the
  wheel swapped back for upstream's `BTreeMap` and the harness kept, rather
  than upstream's tree measured with different instruments.
- **Every run records the commit of its arm, the machine, the toolchain and
  the profile**, and all arms are run in one sitting on one machine. Numbers
  from different sittings are not compared.

Runs are `--release --features debugging`. The `debugging` feature is what
exposes the counters; it does not otherwise change the timer path, and the arms
are measured with it on so that they are measured identically.

## Measurement

The gate, not an afterthought. The control is **upstream's `BTreeMap`**, not
the implementation on `perf/timing-wheel`.

### Step 1 — how many timers, and in what ratio

Everything below is conditional on this. If the answer is "hundreds", the wheel
is solving a problem the executor does not have.

**Derive the ceiling from the code first; it is free and it is most of the
answer.** Only two sites insert into the reactor's timer structure —
`net/stream.rs:331` and `timer_impl.rs:174` — and everything else routes
through them. So live timers are exactly:

- one per stream **per direction**, held only while an operation is pending,
  and only if the application called `set_read_timeout` / `set_write_timeout`.
  The default is `Cell::new(None)` (`net/stream.rs:308`), so a server that
  never sets timeouts contributes **zero**;
- one per live `Timer` / `sleep` / `Interval` future.

The hard ceiling is therefore `2 × concurrent connections with timeouts set`.
The 100k figure #33 was argued from implies a **50,000-connection proxy with
timeouts in both directions, all blocked at once**. That may be a real glommio
target, but it is not the default shape, and it settles what "realistic" means
before anything is run.

**Then measure the ratio, which decides more than the count does.** If timers
are overwhelmingly cancelled before firing, O(1) cancellation is the entire
justification for this design and cascade cost barely matters; if they mostly
fire, cheap next-expiry and cascading dominate and the slab is decoration.
Three counters in `Timers` answer it: high-water live, total inserted, total
cancelled-before-fire.

Those counters belong behind the existing `debugging` feature, which is
declared and empty (`debugging = []` in `glommio/Cargo.toml`). Nothing is paid
in a default build, and `make ci` already compiles `--all-features`, so the
code cannot rot unnoticed.

**Drive it with a ladder**, following the pattern already established by
`accept_ladder.rs`, `recv_ladder.rs`, `send_file_ladder.rs` and
`writev_ladder.rs`: sweep connection count with timeouts set and reads pending,
reporting high-water mark and churn ratio.

`timer_soak_test.rs` is not that shape — it drives 10,000 `sleep` tasks, the
scheduled-work profile, which is the secondary case here. Its header also
instructs `--features timing-wheel`, which is not a feature this crate
declares.

Finally, ask a downstream consumer what they actually run: whether they set
socket timeouts at all, and at what concurrency. One answer from production
outweighs any synthetic figure produced here.

### Step 2 — the comparison, once step 1 has an answer

All three arms run the same cases, at the counts step 1 says are real. Arms A
and B are both built; the direction is chosen from results, not from argument.

1. **Deadline churn** — insert and cancel, never fire. The primary workload for
   the target profile, and `benches/timer_benchmark.rs` does not isolate it
   today.
2. **Expiry** — insert and let fire.
3. **Next-expiry cost per poll**, measured directly, since that is where the
   O(n) regression lives.
4. **Idle gap** — promote to wheel, idle ten minutes, measure the next poll.
   The case the current benchmark structurally cannot express.
5. **End-to-end** — a socket workload with timeouts set. Micro-benchmarks of
   timer operations do not establish that timers sit on anyone's critical
   path.

Two cases exist only for arm B, because only it can fail them:

6. **Resident memory per executor**, at the sizing needed to hold the step-1
   count without failover, multiplied by core count. Arms Control and A grow
   with live timers; arm B preallocates.
7. **Firing lateness distribution**, not its mean — a histogram, under the
   congestion that makes `MAX_PROBES` bite. The relevant question is the tail:
   how late is the worst timer when slots are full, and does anything in the
   target profile notice.

### Choosing

- If the **control** matches the best arm within noise at the step-1 count,
  neither wheel ships. That is `measure-premise-before-perf-work`, and it is
  the trap the task arena fell into: a premise assumed rather than measured,
  months spent, then reverted
  ([task-arena post-mortem](../../investigations/task-arena/README.md)).
- If **arm B** wins on speed but needs sizing whose memory (case 6) or tail
  lateness (case 7) is unacceptable, it loses anyway — and the reason is
  recorded rather than relitigated.
- If **arm A** wins, it ships as code we own, Miri-able, with no dependency.
- If **arm B** wins cleanly, we take the dependency and say so plainly,
  including that it was unmaintained since 2025-12-18 when we chose it.
