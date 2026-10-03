# One Number: ~2 µs Per Blocking I/O

**Date:** 2026-08-02
**Status:** the I/O investigation resolves to a single figure

Three separate measurements in this directory turn out to be the same
measurement. Reading them together is more useful than any of them alone.

## The read ladder that settled it

A constant blocking echo peer, only the client's read mechanism varying, so
differences come from the client and nothing else. 64-byte messages,
`TCP_NODELAY` throughout.

| client | round trip |
|---|---:|
| 1. blocking `recv` | 3,208 ns |
| 2. io_uring `Recv` (completion-based) | 3,808 ns |
| 3. `yolo_recv` + `PollAdd`, written by hand | 3,774 ns |
| 4. glommio | 5,792 ns |

**The readiness design costs −34 ns.** Row 3 is glommio's design implemented
without glommio, and it is a hair *faster* than completion-based io_uring, not
slower. Switching glommio's TCP reads to `Recv` would gain nothing.

**glommio costs +2,017 ns over its own design.** All of the gap is in the
implementation, and none of it is in the choice of mechanism.

## Which is the same number as everywhere else

| workload | underlying operation | glommio adds | as a share |
|---|---:|---:|---:|
| 4 KiB DMA read, depth 1 | ~35,300 ns | ~2,200 ns | 6% |
| TCP echo round trip | ~3,800 ns | ~2,000 ns | 53% |
| reactor loop, awake portion | — | ~1,800 ns | — |

**glommio costs roughly 2 µs per operation that blocks**, and whether that
matters depends entirely on what the operation costs underneath. Against an NVMe
read it is 6% and invisible. Against a loopback TCP round trip it is more than
half the total.

The network path never had a network problem. It has the same per-I/O cost as
the file path, measured against a denominator eight times smaller.

## What the 2 µs is

Not the network code, and not the readiness trade. It is the executor round trip
that any blocking I/O forces:

- the task registers a waker and returns `Pending`
- the shard runs the reactor loop, and parks if there is nothing else to do
- a completion arrives, the shard wakes
- the source is looked up, the task's waker fires, the task is rescheduled
- the task is polled again and retries the syscall

Row 3 of the ladder does the equivalent wait with one `submit_and_wait`, blocking
the thread directly in the kernel with no scheduler in the middle. That is the
whole difference.

[reactor-loop.md](reactor-loop.md) independently measured the awake portion of a
loop iteration at ~1,800 ns on an I/O-bound shard, which is the same cost seen
from the other side.

## What this means for anyone optimising

**Do not rewrite the network stack.** The design is not the problem, and a
completion-based rewrite would add an SQE and a kernel enter to a write path
that currently costs one syscall.

**Anything that raises queue depth amortises this away**, because the cost is per
blocking operation rather than per byte or per request. A server with many
connections in flight pays it once per batch. A ping-pong pays it every time.
That is why the same runtime looks 6% slower on files and 53% slower on loopback
TCP.

**If it is worth attacking, attack the per-I/O executor round trip**, which is
runtime-wide and would help every path at once. **Now attributed** in
[per-io-cost.md](per-io-cost.md): a TCP echo round trip processes **nine
completions to perform one read**, of which one is the actual poll and eight are
preempt-timer installs and cancellations. The user-space handling is cheap —
`wake_waiters` 93 ns, `consume_source` 70 ns, `Source::new` 30 ns — so the cost
is the kernel round trips those eight extra completions imply.

**And measure against a realistic denominator.** Loopback has no wire time to
hide behind. On a real NIC, a round trip is tens of microseconds and 2 µs is back
to being a rounding error — the same way it is against an NVMe read. Loopback is
the right place to find this cost and the wrong place to size it.
