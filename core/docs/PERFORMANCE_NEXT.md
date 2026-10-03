# What Is Left, and What It Is Worth

**Date:** 2026-08-02
**Status:** current, replaces the perf sections of `PERFORMANCE_ROADMAP.md`

The easy wins are gone. Four paths were measured on 2026-08-01/02 and three came
back thin. Task switch is down 43% (28.72 → ~16.5 ns) from two landed changes,
and everything else is either device-dominated or already at the floor.

What remains is concentrated in one place, and it only exists in one regime.

## The one number

**glommio costs ~2 µs per I/O that actually blocks.** Not per byte, not per
message. See [io-path/synthesis.md](investigations/io-path/synthesis.md).

It disappears three ways:

- **Queue depth.** Per blocking operation, so N in flight amortises it N ways.
- **Busy shards never park.** `notify` skips the eventfd when the peer is awake;
  `yolo_recv` returns buffered data with one syscall and no io_uring.
- **Denominator.** 6% of a 35 µs NVMe read, 53% of a 3.8 µs loopback round trip,
  a rounding error against a real NIC.

**Before spending anything on items 1 or 2 below, measure how often shards
actually park in a real workload.** That number decides whether either is worth
doing, and it cannot be obtained from a microbenchmark. `spin_before_park` is a
builder knob today: if setting it changes throughput or p99 materially, the park
cost is being paid; if not, it is not.

## The gate was measured, and items 1 and 2 do not pass it

**2026-08-20, slipstream commit `44316f4`.** The instruction below -- "if
setting `spin_before_park` changes throughput or p99 materially, the park cost
is being paid; if not, it is not" -- was carried out on a real workload. 21
interleaved runs per setting on an idle machine, warm-up discarded, medians in
microseconds:

| link | metric | off | 10 us | 100 us |
|---|---|---:|---:|---:|
| direct loopback | p50 | 763.6 | 761.8 | 758.1 |
| | p99 | 1041.5 | 1053.7 | 955.7 |
| relay + 0 us | p50 | 929.7 | 929.7 | 926.0 |
| | p99 | 1174.8 | 1005.0 | 964.5 |
| relay + injected RTT | p50 | 1571.7 | 1576.0 | 1562.7 |
| | p99 | 1788.8 | 1774.1 | 1682.8 |

**p50 does not move** -- under 1% everywhere. At ~760 us between messages the
reactor has nothing to spin for, which is the predicted null.

**p99 looks tempting and does not hold up.** The 100 us setting has a lower
median p99 in all three configurations, by 6-18%. Mann-Whitney U against unset
gives p = 0.094, 0.414, 0.105; the ranges overlap heavily. A consistent
direction across three configurations is suggestive, not evidence, and 21
samples of a tail statistic cannot call a 6% shift.

**Consequence: do not build items 1 or 2.** Both target the ~2 us paid per
blocking operation, and this workload does not pay it -- its shards are not
parking often enough for the cost to reach the numbers anyone cares about.
That is the answer the gate existed to get.

**What would change the answer** is a workload whose inter-event gap falls
inside the spin window, not more analysis of this one. If such a workload turns
up, re-run this experiment before writing any code.

Worth recording that the protocol was written before the data was seen. "p99
median 8% lower in all three configurations" is exactly the sort of finding
that becomes a shipped default when someone goes looking after the fact.

## Ranked

### 1. Preempt-timer churn — real, but the obvious fix does not work

**Update 2026-08-02: the first attempt was made and reverted.** Reusing an
unchanged timer buys nothing, because there is never one to reuse: the park path
cancels it between every pair of polls, correctly, since a timer left armed wakes
a sleeping shard early. The cycle is arm, run, block, cancel, sleep — once per
round trip — so the churn is structural to sleeping, not redundant re-arming.
Details in [per-io-cost.md](investigations/io-path/per-io-cost.md).

What remains is harder: **do not arm a timer for a task queue that is about to
block.** `need_preempt` reads the latency ring, so the arm has to happen before
the task runs, when whether it will block is unknown.

A TCP echo round trip processes **nine io_uring completions to perform one
read**: one real poll, three preempt timers, five cancellations
([per-io-cost.md](investigations/io-path/per-io-cost.md)). That is the 2 µs.

**Cost to fix:** higher than it looks, but lower than first thought. The
cancellation is entangled with the sleep decision — cancelling changes
`can_sleep()`, which feeds `should_sleep`, which is computed after the cancel. So
it is a restructure of the park decision, not a guard clause.

The hang that appeared to gate this is **diagnosed and was self-inflicted**:
`preempt_timer().is_none()` is a *precondition for sleeping*, so patching it to
return `None` told the reactor it was idle while it still held runnable work, and
removed the wakeup at the same time. Not re-arming an unchanged timer does not
change what `preempt_timer()` returns, so it cannot reproduce that hang. See
[per-io-cost.md](investigations/io-path/per-io-cost.md).

**Payoff:** latency and low-queue-depth workloads only. No throughput win.

### 2. `MSG_RING` + `SINGLE_ISSUER|DEFER_TASKRUN`

Targets the *wake* half of the same 2 µs. Measured at ~1,480 ns against
~2,100–2,400 ns for the current eventfd path, stable to 1% across runs — roughly
−30% of the wake primitive. Unblocked now that the vendored `iou` is gone.

All-or-nothing: the flags alone measured as noise (±14%), so there is no cheap
intermediate step. Same regime as item 1, and the same parking caveat applies.

### 3. ~~Many-shard and many-connection scaling~~ — measured, clean

**Done ([scaling.md](investigations/io-path/scaling.md)), and it came back
clean.** Eight independent shards cost 1.11x one shard, with nothing before that.
One shard with 64 connections costs the same per message as with 4.

The useful result is the other axis: **per-round-trip cost drops 2.4x from one
connection to four and then stays flat.** That is the first end-to-end evidence
for the claim the microbenchmarks argued — the ~2 µs is per *blocking* operation,
so a shard with several things in flight stops parking per message and the cost
amortises on its own.

**Which devalues items 1 and 2 further.** They target exactly the cost that four
concurrent connections already remove. Still open: `accept` throughput under
churn, cross-shard fan-out, and connection counts in the thousands.

### 4. Breadth on the I/O path

Writes, buffered (non-DMA) I/O, `read_many`, registered buffers, UDP. Expected
value now lower: three siblings came back thin.

### 5. Upstream the io-uring accessor — done

**Merged as [tokio-rs/io-uring#404](https://github.com/tokio-rs/io-uring/pull/404)
on 2026-08-09.** The maintainer asked for a method rather than the raw pointers
the first draft exposed, which turned out better: `CompletionQueue::status()`
hands back an owned `CompletionStatus`, and `need_preempt` is now safe code.

**Released in `io-uring` 0.7.14 on 2026-08-11**, so `glommio/Cargo.toml` now
carries a plain `io-uring = "0.7.14"`. No git dependency remains, and
`cargo publish` is no longer blocked by this.

## Method, earned the hard way

Five proposed rewrites were killed by measurement on 2026-08-01/02 before anyone
wrote them, and three published claims were retracted the same day. The traps,
all of which cost time:

- **Never compare runs taken minutes apart.** The baseline for unchanged code
  drifted 28.72 → 19.46 → 16.50 ns across sessions on this box. Use
  `investigations/mechanical-sympathy/probe_ab_refcount.sh`, which alternates
  builds back to back in one session.
- **Best-of-three with no variance is not a measurement.**
- **Primitive cost badly overpredicts path cost.** An atomic RMW is 4.4 ns as an
  isolated dependent chain and roughly a quarter of that on a real path.
- **One-factor-at-a-time hides combinations.** `MSG_RING` alone is −3%; with the
  taskrun flags the same change is −25%.
- **Build a ladder.** Isolating a runtime from its own design — implementing the
  design by hand, without the runtime — settled the network question in twenty
  minutes after two wrong conclusions.
- **Check for a benchmark bug before believing a 10x.** One was Nagle left on for
  the control and off for the subject.
