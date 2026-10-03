# Where the Reactor Loop Spends Its Time

**Date:** 2026-08-02
**Status:** attributed — **the loop is not fat, and mostly is not even on the hot path**
**Premise tested:** "the reactor loop taxes everything"

The same number kept appearing across two investigations. In
[mechanical-sympathy](../mechanical-sympathy/) 3e, `poll_io` was 3282 ns of a
7529 ns cross-shard round trip. In [the DMA read path](README.md), glommio added
~2.2 µs at queue depth 1 — about one loop iteration. It looked like a constant
tax on every path.

It is not.

## On a CPU-bound shard the outer loop barely runs

A task that yields its whole task queue, 200,000 times, with counters on the
outer `run()` loop:

```
wall 68 ns per yield, 0.00 loop iterations per yield
run_one_task_queue/iter   200000
tasks run/iter            200000
```

**Zero outer iterations.** `run_task_queues` has an inner loop that keeps
draining runnable queues until `need_preempt` fires or nothing is runnable, so
CPU-bound work never returns to the outer loop at all. The 68 ns per yield is
`run_one_task_queue` plus a task switch, and nothing else.

So the loop is paid **per I/O batch**, not per task. That is exactly why a
depth-1 read carries a whole iteration and a depth-64 read does not.

## On an I/O-bound shard, where it does run

Depth-1 `O_DIRECT` reads, one loop iteration per operation, measured:

```
depth-1 reads: 37678 ns/op wall, 1.00 loop iterations per op

  per reactor-loop iteration:
    whole iteration            1801 ns
      poll_io                  1402 ns
        poll ring               174 ns
        main ring              1404 ns
        latency ring            172 ns
        syscall flush            27 ns
          cancel queue          107 ns
          submission queue     1334 ns
            io_uring_enter     2838 ns
          completion queue       54 ns
      run_task_queues           290 ns
      poll main future           19 ns
```

Two things to read carefully before drawing anything from this.

**The inner three rows aggregate across all three rings**, because they sit in
the shared `poll()`. They do not decompose the main ring's 1404 ns on their own.

**`io_uring_enter` exceeds the submission-queue total** because `sleep()` calls
`submit_sqes` directly, outside `consume_submission_queue`. So that 2838 ns
includes the enter that arms the ring link before parking, not just the one that
issues the read.

## What it means

The loop's 1801 ns is dominated by `io_uring_enter`. That syscall measured
~100 ns when it submits a nop ([probe_enter_cost.rs](../mechanical-sympathy/)),
so an enter costing microseconds here is doing real work: submitting an
`O_DIRECT` read means the kernel runs the block layer inline. **Raw io_uring pays
that too** — which is why the measured gap between glommio and the raw floor at
depth 1 is only ~2.2 µs, and why it vanishes at depth 64.

Glommio's own bookkeeping is the rest: `run_task_queues` 290 ns, completion
queue 54 ns, cancellation queue 107 ns, poll and latency rings ~350 ns together.
**Call it 400-500 ns per iteration of actual runtime overhead**, once per I/O
batch, against a device taking tens of microseconds.

## Conclusion

The premise fails. The reactor loop is not a constant tax:

- CPU-bound work does not enter it at all
- I/O-bound work enters it once per batch, and most of what it costs there is a
  kernel syscall issuing the I/O, which any runtime pays

There is nothing here worth optimising, for the same reason as
[the read path](README.md): the device dominates, and the software that is left
is a few hundred nanoseconds beside it.

The one number still worth remembering is the *shape*: cost per I/O batch, not
per operation. Anything that raises queue depth — batching, more in flight —
amortises it away, and anything that forces depth 1 pays it in full.
