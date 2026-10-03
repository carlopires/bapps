# Which timer structure should glommio use?

**Date:** 2026-09-07
**Design:** [2026-09-04-timer-wheel-redesign-design.md](../../superpowers/specs/2026-09-04-timer-wheel-redesign-design.md)
**Prompted by:** review of [#33](https://github.com/glommio/glommio/pull/33)

Three implementations built and measured against each other, after review of
#33 found the hierarchical wheel it proposed had four defects and that the
benchmark arguing for it was taken with one of them in the loop.

**Outcome: the slab wheel wins, narrowly, and not for the reason the original
PR gave.**

## Reproducing it

```bash
make timer-arms                       # every arm
make timer-arms ARMS="a-slab-wheel"   # one of them
```

Needs a clean tree. The script checks out each arm branch in turn, builds it,
runs the benchmark and puts your branch back; results land in
`target/timer-arms/`. An arm that does not contain the benchmark's own commit
is refused rather than measured.

The run is quick by default. For numbers worth quoting, give criterion longer:

```bash
TIMER_ARMS_WARMUP=3 TIMER_ARMS_TIME=10 TIMER_ARMS_SAMPLES=100 make timer-arms
```

The two programs behind it, runnable on any branch:

```bash
cargo run --release --features debugging --example timer_ladder  # populations
cargo bench --bench timer                                        # costs
```

## What the workload actually is

Before comparing structures, count what they hold. Only two sites register a
timer with the reactor, `net/stream.rs` for socket timeouts and
`timer_impl.rs` for `Timer`/`sleep`, glommio registers none of its own, and
socket timeouts are `None` unless an application asks for them.

`timer_ladder`, connections with a read timeout doing request/response cycles
that complete well inside it:

```
  conns    inserted       fired   cancelled   highwater   cancel%
     64        1024           0        1024          64     100.0
    256        4096           0        4096         256     100.0
   1024       16384           0       16384        1024     100.0
   4096       65536           0       65536        4096     100.0
```

**Nothing fires.** Cancellation is the whole hot path, and the live count is
exactly the connection count, one pending read per connection, not two. The
100k figure #33 was argued from implies a 50,000-connection proxy.

Scheduled work (`sleep`, `Interval`) is the mirror image, 100% fired, and is
the secondary case.

## The arms

All three branch from the same commit and carry the same benchmark, the same
counters and the same reactor path. They differ only in how timers are stored,
so a difference in the numbers is a difference in the structure.

| Branch | Structure |
|---|---|
| `arm/control-btreemap` | `BTreeMap<(Instant, u64), Waker>`, the incumbent |
| `arm/a-slab-wheel` | Slab handles over a cascading wheel with occupancy bitmaps |
| `arm/b-bitwheel` | The [`bitwheel`](https://crates.io/crates/bitwheel) crate |

The control differs from upstream in one respect, in its own favour: upstream's
handle is a bare `u64` and a side `AHashMap<u64, Instant>` recovers the deadline
before a cancellation can remove anything. The handle carries its own key here,
so neither is needed. Measuring the other version would charge the incumbent for
a hash lookup the candidates never pay.

## Results

`cargo bench --bench timer`, 64 cores, nanoseconds per operation:

| | arm/64 | arm/4096 | cancel/64 | cancel/4096 | sleep 100us |
|---|---|---|---|---|---|
| control, `BTreeMap` | 45.1 | 56.0 | 41.6 | 49.6 | 105 us |
| arm A, slab wheel | 44.1 | 43.1 | 24.1 | 26.1 | 105 us |
| arm B, `bitwheel` | 40.1 | 67.2 | 23.1 | 49.5 | 1007 us |

**The complexity class is visible, and an earlier version of this document said
it was not.** That claim came from a hand-written timing loop, and it was
wrong: the loop measured a cold allocator, reported 55 to 75ns for arming where
a warmed measurement says 43, and buried a real effect under noise of its own
making. Review upstream asked for criterion rather than a hand-rolled harness
and was right in a way that changed the answer, not the presentation.

Warmed up, with non-overlapping intervals:

- the ordered map **grows** with population: arm 45.1 to 56.0ns, cancel 41.6 to
  49.6ns
- the slab wheel is **flat**: 43ns arm and 26ns cancel at every count measured
- at 4096 the wheel is 47% faster on cancellation, which step 1 measured as
  100% of the workload, and the gap widens with population

**Precision had to be fixed before the wheel was usable at all.** A wheel
rounds deadlines to whole ticks, so a 100us sleep took ~1ms, a floor under
every short sleep, which is exactly the case a low-latency caller reaches for.
Arm A reports the real deadline from the earliest occupied slot rather than the
tick it was rounded into, and expires by deadline rather than by tick, so it
matches the ordered map at 105us. `bitwheel` still has that floor.

**Neither the incumbent nor the slab wheel contains any `unsafe`.** The
vendored `bitwheel` has 166 lines of it in `timer/slot.rs` alone.

## What the defects were

Found by review of #33 and by measurement, all confirmed in the code:

| Defect | Evidence |
|---|---|
| Handle truncates a monotonic counter | `reactor_adapter.rs:62` casts a `u64` to `u32`. At 100k timers/sec, cancellation targets the wrong timer after ~12 hours |
| `generation` never set | Hardcoded `0` and `#[allow(dead_code)]`, while its documentation describes a slot-reuse guard |
| Next expiry is O(n) | A scan of every live timer, on every poll, where the `BTreeMap` answers from its first key in O(1) |
| `advance_to` is O(elapsed ms) | A tick per millisecond regardless of work; ten idle minutes cost 600,000 iterations on the next poll |
| Timers fire early | Deadlines truncated to whole ticks, so a timer due at 1.7ms fired at 1.0ms and the future re-armed. 4,096 sleeps cost 10,749 registrations |
| Sub-tick sleeps rounded up | A 100µs sleep took ~1ms |

The first four are what arm A replaces. The last two are fixed on `master`
already, except the sixth.

## bitwheel

Three defects had to be patched before it could be measured at all, all
reported upstream:

- [#18](https://github.com/Abso1ut3Zer0/bitwheel/issues/18), `cancel`
  performs an unchecked removal justified by "a timer whose deadline is still
  in the future cannot have fired". `poll_tick` fires whole gear slots early,
  so that is false, and cancelling such a timer reaches
  `hint::unreachable_unchecked`, undefined behaviour in release. glommio's
  suite hits it, because cancelling before the deadline is our common case.
- [#19](https://github.com/Abso1ut3Zer0/bitwheel/issues/19), `insert` clamps
  the delay to at least one tick but computes the slot from the unclamped
  deadline, so a timer due inside the current tick lands in the slot the clamp
  was avoiding. `poll` fires it; `duration_until_next` cannot see it for a
  whole gear revolution. A 100µs sleep took 64ms.
- `len` was decremented on cancellation but not on firing.

Two further properties are design rather than defect, and both are wrong for
this workload. Slots have a fixed compile-time capacity, and identical timeouts
give identical deadlines, so a deadline-churn population above `SLOT_CAP` spills
into the `BTreeMap` failover the wheel was meant to replace. And because it
fires slots early by design, a timer can be woken before its deadline and be
gone from the wheel, with no way to ask whether a registration still exists,
so every pending poll must re-arm unconditionally.

Three `TimerActionRepeat` tests also fail on cadence: an action expected to run
eleven times in a window runs five.

## Verdict

Arm A, on the stated order of safety before performance.

**`bitwheel` fails the first test before the second is reached.** It reached
`hint::unreachable_unchecked` from safe API composition, on the pattern glommio
uses most: cancelling a timer before its deadline. It runs here only as a
vendored copy with three patches, both reported issues are unanswered, and the
crate has not been touched since 2025-12-18. Adopting it means depending on
known-unfixed undefined behaviour or maintaining a fork of someone else's crate,
and its `unsafe` is in a dependency, so it is `unsafe` we cannot Miri. It also
lost on performance once measured properly.

**Between the ordered map and the slab wheel, safety is a genuine tie**: both
are zero `unsafe`. Not identical, though. The map has shipped for years; the
wheel is ~800 new lines with invariants that have to hold, back-patched
positions on `swap_remove`, bitmap maintenance at four sites, generations
surviving vacancy. Those failures would be logic bugs rather than undefined
behaviour, and each is covered by a test that fails when the guard is reverted,
but new code is a real risk that "safe" does not cancel.

Performance breaks the tie: 47% on the operation that is the entire workload,
and flat where the incumbent grows.

**The condition, still unmet.** Case 5, the end-to-end socket workload, has not
been run. 24ns per cancellation is real but small beside what a connection
costs. If timers are a rounding error there, then 800 lines we own is the wrong
trade against zero lines we do not, and the ordered map should stay. Run case 5
before treating this as settled.
