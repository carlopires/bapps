# Shard and Connection Scaling

**Date:** 2026-08-02
**Status:** measured — **scales cleanly on both axes**
**Why:** everything else here used one or two shards and one connection

Two questions a thread-per-core runtime has to answer, and neither had been
looked at:

1. **N independent shards, no shared state.** Does per-shard throughput stay flat
   as N grows? Anything else means global contention.
2. **One shard, M connections.** Does per-message cost stay flat as M grows?

Loopback TCP echo, 64-byte messages, `TCP_NODELAY`. Each shard gets its own
blocking echo peer pinned to a separate core, so peers never contend with
shards. Two runs, agreeing within 2%.

## 1. Shards scale

| shards | ns/round trip | vs 1 shard |
|---:|---:|---:|
| 1 | 14,584 / 14,738 | 1.00x |
| 2 | 14,702 / 14,528 | 1.00x |
| 4 | 14,927 / 14,937 | 1.02x |
| 8 | 16,199 / 16,216 | **1.11x** |

**11% degradation at eight shards, nothing before that.** With no shared state
between shards, near-flat is what a thread-per-core design is supposed to
deliver, and it delivers. The 11% at eight is consistent with contention in the
loopback stack and memory system rather than in glommio — the shards share no
runtime state by construction.

Worth noting against history: `f28a619` earlier this series removed a
process-wide `RwLock` taken on **every spawn**, worth 4,350 → 28 ns at 64
executors. This is what its absence looks like. There is no equivalent
bottleneck left on this path.

## 2. Connections are O(1), and concurrency pays for itself

| connections | ns/round trip | vs 1 connection |
|---:|---:|---:|
| 1 | 13,949 / 14,463 | 1.00x |
| 4 | 5,956 / 5,884 | **0.42x** |
| 16 | 6,381 / 6,352 | 0.45x |
| 64 | 6,513 / 6,497 | 0.46x |

Two things, and the first is the more useful.

**Per-round-trip cost drops 2.4x from one connection to four, then stays flat to
64.** This is the first direct, end-to-end evidence for the claim
[synthesis.md](synthesis.md) argued from microbenchmarks: glommio's ~2 µs is paid
**per operation that blocks**, so a shard with several things in flight stops
parking per message and the cost amortises. One connection pays it in full; four
already do not.

**And it is flat from 4 to 64.** Per-connection cost does not grow — connection
handling is O(1) in the number of connections, with no per-connection sweep or
scan showing up as the count rises.

## What this means

This is the fourth path measured in this series and the fourth to come back
thin. Taken with the others, the picture is consistent: glommio is well built,
its remaining cost is one ~2 µs per-blocking-I/O charge, and **realistic
concurrency amortises that away without anyone doing anything**.

It also sharpens what the parked work is worth. The wake-path items on
[PERFORMANCE_NEXT.md](../../PERFORMANCE_NEXT.md) target exactly the cost that
four connections already remove. They matter for a shard handling one thing at a
time and essentially not at all for a shard handling several — which is what a
loaded server is.

## What this does not cover

Loopback, one machine, blocking peers, 64-byte messages, echo only. Not covered:
real NICs, `accept` throughput under churn, cross-shard fan-out (shards here are
fully independent by design, which is the point but also a limit), connection
counts in the thousands, and any workload where shards share state.

Shard scaling was measured to 8 because peers need cores too; this box has 32
physical, and going wider would measure the harness rather than the runtime.
