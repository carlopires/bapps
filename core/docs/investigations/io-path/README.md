# Investigation: The DMA Read Path

**Date:** 2026-08-02
**Status:** measured — **no fat found**
**Method:** raw io_uring floor, then glommio doing the same workload, then attribute

Every previous performance investigation in this fork looked at task switching
and the cross-shard wake. Nobody had measured the I/O path, which is what
glommio is *for*, and where the roadmap's original complaint lived ("monoio
outperforms glommio on random reads").

## Setup

4 KiB `O_DIRECT` random reads over a 1 GiB file. Both sides read the identical
offset sequence from the same deterministic generator, so neither gets a luckier
access pattern. 20,000 operations per cell. `probe_dma_read.rs`.

```
Samsung SSD 990 PRO 2TB, XFS, noatime
```

`O_DIRECT` on both sides, so the page cache is out of the picture for both.

## Result

Four runs. Spread within ±0.5% on every cell except raw at depth 64.

| queue depth | raw io_uring | glommio DMA | overhead |
|---|---:|---:|---:|
| 1 | 35.2–35.5 µs | 37.4–37.7 µs | **~2.2 µs (6%)** |
| 4 | 10.5–10.7 µs | 11.2–11.4 µs | ~0.6 µs (6%) |
| 16 | 4.27–4.34 µs | 4.68 µs | ~0.4 µs (9%) |
| 64 | 3.6–4.4 µs | 3.58–3.80 µs | **~0 (within noise)** |

**The device dominates.** Per-operation software cost is single-digit
microseconds against a device that takes tens, and the *absolute* overhead falls
as concurrency rises — 2.2 µs at depth 1, 0.4 µs at depth 16, indistinguishable
at depth 64.

That last row deserves a caveat rather than a victory lap: the raw harness
submits a batch and drains it fully before submitting the next, a barrier
glommio does not have, so at depth 64 the floor is measuring my loop rather than
the kernel. Read it as "no measurable difference", not "glommio is faster".

## What the shape says

~2.2 µs of added latency at depth 1, shrinking to nothing under concurrency, is
what one reactor-loop iteration looks like. That matches the
[mechanical-sympathy](../mechanical-sympathy/) phase timers, where `poll_io`
costs ~1.6 µs per iteration. At depth 1 every read pays a full loop; with
several in flight that cost overlaps with device time and disappears.

## Consequences

**There is nothing here worth optimising.** Shaving glommio's per-operation cost
in half would move a depth-16 read from 4.68 µs to 4.48 µs — 4%, against a
device floor that is 91% of the total.

**And it relocates the monoio question.** If monoio really does beat glommio on
random reads, per-operation path cost does not explain it, because there is
almost none to give back. Look instead at submission batching policy, at how
many operations the runtime keeps in flight, at buffer management and
registration, or at the benchmark itself. Do not look here.

## Follow-up

[reactor-loop.md](reactor-loop.md) attributes the ~2.2 µs. Short version: the
outer loop does not run at all for CPU-bound work — 200,000 task-queue yields
produced zero iterations — and on an I/O-bound shard it runs once per batch,
where most of its 1.8 µs is an `io_uring_enter` issuing the read, which the raw
floor pays too. Glommio's own bookkeeping is 400-500 ns.

## The monoio question

[monoio-gap.md](monoio-gap.md) covers DataDog/glommio#641, what
`IORING_FEAT_FAST_POLL` actually is, and why the thread's leading explanation —
that glommio's userspace poll-then-read is what costs — does not match what we
measured. Also records the three ways our rig is the wrong shape to call that a
rebuttal.

## It is all one number

[synthesis.md](synthesis.md). glommio costs **~2 µs per blocking I/O** — the
executor round trip of parking a task, running the reactor loop, waking and
rescheduling. Against a 35 µs NVMe read that is 6%. Against a 3.8 µs loopback TCP
round trip it is 53%. Same cost, different denominator.

A ladder that implements glommio's readiness design *by hand* costs −34 ns
against completion-based io_uring, so the network design is not the problem and
rewriting it would gain nothing.

## The network path is a different story

[network.md](network.md). Loopback TCP ping-pong is **+120% over a raw io_uring
floor**. glommio's TCP reads are readiness-based — a direct `recv` syscall bypassing io_uring, falling back to
`PollAdd` on `EAGAIN` — so it issues about five SQEs and five kernel enters per
side per round trip where two suffice.

That is the first large, reproducible, non-hardware gap this fork's performance
work has found, and it is the likelier explanation for the roadmap's monoio
complaint than anything in the file path.

## What this does not cover

One device, one filesystem, one block size, one access pattern, reads only.
Untested: sequential reads, large blocks, writes, buffered (non-DMA) I/O,
`read_many`, registered buffers, and the network path. A finding about 4 KiB
random reads is a finding about 4 KiB random reads.

## Reproducing

```bash
mkdir -p target/ioprobe
dd if=/dev/urandom of=target/ioprobe/data.bin bs=1M count=1024
cargo run --release -- target/ioprobe/data.bin
```

The file must live on a filesystem supporting `O_DIRECT` — not tmpfs, which is
why it goes under `target/` rather than `/tmp`.
