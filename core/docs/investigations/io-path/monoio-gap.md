# The monoio Gap, and What `fast poll` Actually Is

**Date:** 2026-08-02
**Status:** partly answered, and one popular explanation looks wrong
**Context:** [DataDog/glommio#641](https://github.com/DataDog/glommio/issues/641)

The roadmap's founding complaint — "monoio outperforms glommio" — traces to a
real issue, open since 2024. It is worth reading before doing any more network
work, and worth being careful about, because the thread contains a confident
diagnosis from a project collaborator that our measurements do not support.

## What #641 says

monoio's own benchmark page shows glommio roughly **30% worse on a 100-byte echo**.
Seven comments, no measurements taken. The hypotheses offered:

- **vlovich:** "Probably just needs someone to run things under a profiler."
- **bryandmc** (collaborator): *"Their implementation also uses fastpoll which is
  much better than what we do which is poll+read… the way to resolve this now is
  probably to re-do sockets with all the new io_uring features."*
- **bryandmc**, on disk: *"we already do most (if not all?) of the things that
  ensure fast disk reads… I don't think there are features we have left on the
  table."*
- **fsaintjacques:** "it's missing multishot receive."

## What `fast poll` is

`IORING_FEAT_FAST_POLL`, a kernel capability since 5.7 — not something monoio
built. It is declared in the `liburing` this repository vendors:

```c
#define IORING_FEAT_FAST_POLL   (1U << 5)
```

Submit a `Recv` on a socket that is not ready and, instead of punting the
operation to a kernel worker thread (`io-wq`), io_uring arms a poll internally
and retries the read itself once the fd is readable. One SQE, one CQE, no thread
handoff.

The two designs:

| | |
|---|---|
| **monoio** | submits `Recv`; the kernel handles not-ready via fast poll. **1 SQE, 1 CQE.** |
| **glommio** | non-blocking `recv` syscall (`yolo_recv`); on `EAGAIN` submits `PollAdd`; on completion, another `recv`. **Userspace poll-then-read.** |

bryandmc's description of the mechanism is accurate. glommio never references
the feature flag, because it never relies on the behaviour.

## What we measured

**Disk: he is right.** The DMA read path is device-dominated — glommio adds
~2.2 µs at queue depth 1 against a ~35 µs NVMe read (6%), and nothing measurable
at depth 64. Nothing left on the table. See [README.md](README.md).

**Network is indeed where a gap would live.** Loopback TCP ping-pong is +120%
over a raw io_uring floor. See [network.md](network.md).

**But `poll+read` does not appear to be what costs.** A ladder holding the peer
constant and varying only the client's read mechanism
([probe_read_ladder.rs](probe_read_ladder.rs)):

| client | round trip |
|---|---:|
| blocking `recv` | 3,208 ns |
| io_uring `Recv` (completion-based, exercises fast poll) | 3,808 ns |
| **`yolo_recv` + `PollAdd`, written by hand** | **3,774 ns** |
| glommio | 5,792 ns |

Row 3 is glommio's design implemented *without* glommio. It costs **−34 ns**
against completion-based io_uring — the design is free at this shape. glommio
costs **+2,017 ns over its own design**, and that is the same ~2 µs the file path
pays per blocking operation ([synthesis.md](synthesis.md)).

Attributed further: a TCP echo round trip processes **nine io_uring completions
to perform one read** — one real poll, three preempt timers, five cancellations
([per-io-cost.md](per-io-cost.md)). The socket strategy is not the expensive
part; the executor round trip is.

## Why this is not yet a rebuttal

Our rig is the wrong shape for their claim, in three ways:

1. **One operation in flight.** A strict ping-pong. monoio's benchmark uses many
   concurrent connections, which is exactly where "five SQEs and five kernel
   enters per round trip versus two" would compound. Our
   [scaling](scaling.md) numbers are weak evidence against a per-connection
   penalty — cost is flat from 4 to 64 connections — but that is glommio
   measured against itself.
2. **Loopback.** No wire time to hide software cost behind, which exaggerates it;
   their benchmark is over a network.
3. **We never ran monoio.** Every comparison here is against a raw io_uring
   floor we wrote. Nobody has reproduced the 30% claim on this hardware, let
   alone attributed it.

So the defensible statement is narrow: **at queue depth 1 on loopback, the
readiness design costs nothing measurable, and glommio's overhead is the
per-blocking-I/O executor round trip.** Whether fast poll matters at their
benchmark's shape is untested.

## The probe that would settle it

A many-connection echo server, glommio against monoio, on the same hardware,
over a real NIC, with SQE and kernel-enter counts per request on the glommio
side. Until someone runs that, "re-do sockets with the new io_uring features" is
a plausible plan aimed at a cost nobody has demonstrated.

Note also that a completion-based rewrite would **add** an SQE and a kernel enter
to the *write* path, which today costs one `send` syscall and 127 ns of glommio
overhead ([network.md](network.md)). It could improve latency and cost
throughput.

## If this is ever taken upstream

Frame it as measurements and a probe, not as a correction. Everything above is
one machine, one loopback benchmark, one access pattern, and it contradicts a
collaborator who knows this codebase far better than we do. "Here is what I
measured, here is the probe, run it yourself" is both truer and more useful than
a verdict.
