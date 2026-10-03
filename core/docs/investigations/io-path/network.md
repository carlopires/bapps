# The Network Path — Where the Fat Is

**Date:** 2026-08-02
**Status:** latency gap confirmed; an accompanying streaming claim retracted
**Method:** raw io_uring floor, glommio over the identical workload, attribute

The task path, the DMA read path and the reactor loop all came back thin: single
digit percentages, device-dominated, nothing worth optimising. The network path
is the first that is not — on latency. A second claim in this document, about
streaming throughput, turned out to be a benchmark bug and is retracted below.

## Latency: loopback TCP ping-pong

64-byte messages, one request in flight, client on cpu 0 and server on cpu 4
(same L3 domain, so the cache-domain effect measured elsewhere is not a
confound). Three runs, spread under 3%.

| stack | round trip |
|---|---:|
| `std::net` blocking | ~3,250 ns |
| raw io_uring | ~3,660 ns |
| **glommio (parks)** | **~8,000 ns (+120%)** |
| glommio, `spin_before_park(50us)` | ~6,600 ns (+82%) |

Blocking sockets beat both. glommio is **more than twice** the raw floor.

Parking explains only about a third of it: never parking still leaves ~2.9 µs.

## What it is doing

Counters on SQE construction, kernel enters and `Source` registration. These are
process-global so they cover **both** shards; raw is quoted per side.

| | SQEs | `io_uring_enter` | `Source`s |
|---|---:|---:|---:|
| raw io_uring, per side | 2 | 2 | — |
| glommio parking, both sides | 20 | 10 | 10 |
| glommio spinning, both sides | 12 | 8 | 6 |

Even halving glommio's figures, it issues roughly **five SQEs and five kernel
enters per side per round trip where two and two suffice**, and allocates and
registers five `Source`s.

`SockRecv` and `SockSend` counts were **zero**. glommio is not using io_uring's
`Recv`/`Send` for TCP at all.

## Why: the read path is readiness-based, not completion-based

`net/stream.rs`:

```rust
if no_pending_poll {
    if let Some(result) = super::yolo_recv(self.stream.as_raw_fd(), buf) {
        ...
        return Poll::Ready(Ok(result));
    }
}
...
self.source_rx = Some(reactor.poll_read_ready(self.stream.as_raw_fd()));
```

A read is a **direct non-blocking `recv` syscall that bypasses io_uring**, and
only on `EAGAIN` does it register a `PollAdd` and wait. That is an epoll-shaped
design wearing io_uring underneath.

**This is a deliberate trade, not an oversight.** When data is already buffered,
`yolo_recv` returns it in one cheap syscall with no io_uring involvement at all
— cheaper than a completion-based `Recv`. The cost lands when the data is *not*
there: a wasted `recv` returning `EAGAIN`, then a poll registration, then a wake,
then a second `recv`. A ping-pong is that case every single time.

## The streaming claim was wrong — retracted

An earlier version of this document reported streaming sends at 100 ns for
blocking sockets against 1,057 ns for glommio and called it a 10x gap in the
regime the design exists for. **That was a benchmark bug, twice over.**

**First, Nagle.** The raw comparisons left `TCP_NODELAY` off while glommio set
it. With Nagle on, small sends coalesce into the existing segment and return
almost immediately; with it off, each 64-byte send pushes a packet and loopback
delivery happens inline. Setting `NODELAY` on all three collapses the difference:

| per send, `TCP_NODELAY` on all three | |
|---|---:|
| bare `send(MSG_DONTWAIT)` | 1,153 ns |
| + glommio executor and `await` | 1,153 ns (**+0**) |
| + glommio net layer (`write_all`) | 1,280 ns (**+127**) |

**glommio's write path costs 127 ns over the bare syscall, not 950**, and the
executor costs nothing measurable.

Timers inside `poll_write` confirm the shape: 1.00 `poll_write` and 1.00
successful `yolo_send` per message, zero `EAGAIN`, 18 ns in everything after the
syscall. The fast path is one syscall and a few cheap operations, exactly as it
reads.

**Second, backpressure.** The two streaming servers are not comparable either:
the blocking one drains with a cheap `read` syscall while glommio's drains
through its async read path, so the client backs up against a slower reader.
That test measures receive throughput indirectly, not send cost.

The write path is fine. A 10x result should have been suspicious rather than
exciting, and the ladder that disproved it took twenty minutes.

## What this means

The latency result stands: a factor of two, reproducible, not explained by
hardware. That comparison was fair — all three stacks set `TCP_NODELAY`, run the
same workload, and use the same stack on both ends. Unlike the read path, where
the device was 91% of the time and there was nothing to give back, here there is
something to give back.

The streaming result does not stand; see the retraction above.

It also **relocates the monoio comparison**, which is where this started.
monoio benchmarks are TCP echo servers, not file reads. If glommio loses to
monoio, this is a far likelier explanation than anything in the task or DMA
paths — and it is consistent with monoio being completion-based throughout.

## Resolved: it is not the design

[synthesis.md](synthesis.md) has the read ladder. Implementing glommio's
readiness pattern by hand, without glommio, costs **−34 ns** against
completion-based io_uring — the design is free. glommio costs **+2,017 ns over
its own design**, and that figure matches the ~2.2 µs the DMA read path pays at
queue depth 1. It is the per-I/O executor round trip, not the network stack.

The section below was written before that was known; keep it for the reasoning,
ignore its conclusion about redesigning.

## Before anyone builds something

The obvious move — switch reads and writes to io_uring `Recv`/`Send` — is a
design change to the core of the network stack, and the current design was
presumably chosen for a reason. Measure first, in this order:

1. ~~Attribute the streaming 950 ns.~~ **Done, and it evaporated** — see the
   retraction. The write path adds 127 ns; the executor adds nothing.
2. **Attribute the latency gap the same way.** Five SQEs and five enters per side
   per round trip is a count, not an attribution. The write ladder worked; the
   read side has had no equivalent. Find out what each op is and what it costs
   before assuming the readiness design is responsible.
3. **Only then** prototype a completion-based read. Note it would *add* an SQE
   and a kernel enter to the streaming path, which today costs one syscall — so
   it could make throughput worse to make latency better.
4. Check `rush_dispatch`. `stream.rs` has two commented-out calls with a note
   referring to issue #458 — someone has already been here.

## Vectored writes: measured, and fixed (2026-08-24)

`TcpStream` did not override `poll_write_vectored`, so it inherited the
futures-io default, which writes **only the first slice**. A server sending a
response as status line, headers and body therefore paid three `send` calls,
and with `TCP_NODELAY` set -- which a latency-sensitive server does set -- put
up to three segments on the wire for one response.

`glommio/examples/writev_ladder.rs`, 100k responses, loopback, `OutSegs` from
`/proc/net/snmp` (host-wide, so it counts the drain server's ACKs too -- the
ratio is the signal, not the absolute):

| 13-byte body | per response | segments |
|---|---:|---:|
| three `write_all` calls | 6,760 ns | 5.50 |
| one `write_vectored` | **1,782 ns** | **1.88** |
| concatenate, then one `write_all` | 1,988 ns | 1.86 |

| 64 KiB body | per response | segments |
|---|---:|---:|
| three `write_all` calls | 9,287 ns | 6.61 |
| one `write_vectored` | **6,726 ns** | 3.00 |
| concatenate, then one `write_all` | 7,906 ns | 3.00 |

**Three writes to one is worth 3.8x on a small response**, and the segment
count drops by the same factor -- which is the part that matters off loopback,
where a packet costs a wire traversal rather than a memcpy.

**Vectoring versus concatenating is a wash on small bodies** (inside run-to-run
noise) and worth ~1,180 ns on a 64 KiB body, which is a 64 KiB copy at memory
bandwidth. That is the honest size of that particular win: it is the copy, and
it scales with the body.

An earlier estimate in conversation put the saving at ~2 µs by multiplying a
per-send ping-pong figure by three. That was wrong in both directions -- it
overstated the syscall component and ignored the segment count, which is the
larger effect.

## Accept: multishot measured and rejected (2026-08-24)

`accept` speculates like the read path: `yolo_accept` calls `accept` and only
falls back to an io_uring `Accept` on `EAGAIN`. So under churn the ring is not
involved, and multishot accept would be replacing a syscall rather than adding
one. Whether that is a win is a measurement, and it went three rounds before
giving a trustworthy answer.

`glommio/examples/accept_ladder.rs`, 10,240 connections, per connection:

| rung | wall | **cpu** |
|---|---:|---:|
| toggle `O_NONBLOCK` per accept (what glommio did) | 17,023 ns | 1,652 ns |
| listener non-blocking once | 7,538 ns | 1,326 ns |
| multishot accept, zero syscalls per connection | 6,492 ns | **1,823 ns** |

**Multishot accept costs more CPU than the plain syscall it would replace.**
The CQE drain and the kernel enters that go with it are not cheaper than one
non-blocking `accept`. There is no case here for the reactor surgery multishot
would need — a `Source` that survives its own completion — and this closes the
"accept throughput under churn" item [scaling.md](scaling.md) left open.

### Two ways the measurement lied first

**The wall column is not the answer.** Wall clock is bounded by the connecting
thread, so every rung finishes at the client's pace no matter what the server
spends. Only `CLOCK_THREAD_CPUTIME_ID` on the accepting thread separates them.

**Timing only the drain loop credited multishot with 3 ns per connection.**
With multishot armed the kernel accepts as connections *arrive*, so the work
happens before the timed section starts. Timing the whole round — fill and
drain — puts it back on the measured path. A rung that looks 500x better than
the next one is a bug in the probe, not a result.

And the toggle rung's wall figure is inflated for a third reason: it spins on
`EAGAIN` at four syscalls a turn where the others spin at one. That inflation
is exactly why the in-situ number below matters more than the ladder.

### What was kept

The per-accept `O_NONBLOCK` toggle is gone: listeners are put in non-blocking
mode when they are constructed, so `yolo_accept` is one syscall rather than
four (`F_GETFL`, `F_SETFL`, `accept`, `F_SETFL`).

In isolation that is worth ~326 ns of CPU per accept. **Inside glommio it is
not measurable**: the accept loop costs 1,838 ns of CPU per connection before
and 1,805 ns after, with the runs overlapping. It is kept because it is
strictly less work and removes three syscalls, not because a number moved.

It does buy an invariant: a listener that reaches `yolo_accept` in blocking
mode would park the whole executor inside `accept`. All three construction
sites set the flag, and a `debug_assert` fails loudly rather than hanging if a
fourth ever appears.

## Reads at 256 connections: the ring wins after all (2026-08-24)

[synthesis.md](synthesis.md) measured the readiness design at **−34 ns**
against completion-based io_uring and concluded that switching reads to `Recv`
would gain nothing. That result was explicitly scoped to **one connection at
queue depth 1**, and [monoio-gap.md](monoio-gap.md) listed the untested regime
as the reason it was not a rebuttal: *"monoio's benchmark uses many concurrent
connections, which is exactly where five SQEs and five kernel enters per round
trip versus two would compound."*

That regime has now been measured, and it does compound.

`glommio/examples/recv_ladder.rs`: 256 connections, one message each per
round, **data arriving after the read is posted** -- a server waiting on many
sockets, not a ping-pong. Server pinned to cpu 0, client to cpu 4, five warm-up
rounds discarded. CPU per message, stable to ~1% across runs:

| rung | cpu/message |
|---|---:|
| readiness: `recv`, `PollAdd`, `recv` (glommio's shape, by hand) | 1,097 ns |
| single-shot `Recv` (1 SQE + 1 CQE) | **783 ns** (−29%) |
| multishot `Recv` + provided buffer ring (0 syscalls) | **631 ns** (−42%) |
| glommio itself | 1,166 ns |

**Both completion-based rungs beat the readiness path at this shape**, and the
ordering is the reverse of the depth-1 result. Nothing about the earlier
measurement was wrong; it answered a different question. The two together say
the design choice is *workload-dependent*, which is a more useful statement
than either alone:

- **one connection, always-ready data**: speculation wins, because the syscall
  returns immediately and the ring costs an SQE and a CQE for nothing.
- **many connections, data that has not arrived**: the ring wins, because the
  speculation is a wasted `recv` returning `EAGAIN` before the poll, and then
  a second `recv` after it. Two syscalls to move one message becomes zero.

glommio costs only 69 ns over its own hand-written design here, against
~2,000 ns at depth 1 -- the per-blocking-operation overhead amortises across
256 concurrent reads exactly as [scaling.md](scaling.md) predicted. **So the
remaining gap is the mechanism, not the executor**, which is the first time
that has been true in this investigation.

### What multishot actually buys over single-shot

Only 152 ns of the 466 ns. Two thirds of the win is available from
**single-shot `Recv`**, which needs no buffer rings, no `IORING_CQE_F_MORE`
handling, no `ENOBUFS` re-arm path, and no kernel newer than 5.6. That
reshapes the work into two stages rather than one leap:

**Stage A — completion reads on the buffered path.** `Recv` into a buffer the
stream owns. The constraint is not performance but lifetime: `poll_read` hands
out a `&mut [u8]` valid for one call, and the kernel needs the buffer until
the CQE arrives, so a completion read is only sound where glommio owns the
buffer -- which is exactly what `RxBuf`/`Preallocated` already provide, and
what a caller wanting zero-copy parsing wants anyway.

**Stage B — multishot plus a provided buffer ring.** Needs a `Source` that
survives its own completion (`process_one_event` consumes it unconditionally
today), a buffer ring per reactor, `ENOBUFS` disarm handling, and kernel 6.0
with a probe and fallback. The extra 152 ns has to pay for all of that.

Stage A is the better first move and Stage B should be judged on its own
numbers afterwards, not on the 42% headline that includes Stage A's share.

## send_file: zero copies cost more CPU than the copies (2026-09-01)

`TcpStream::send_file` splices file -> pipe -> socket on the ring, so the
payload never enters the process. The feature was built without claiming a
speedup, and `glommio/examples/send_file_ladder.rs` was written to find the
size at which it overtakes a plain `read_at` followed by `write_all`.

**There is no such size on the warm path.** Splicing loses at every payload
from 1 KiB to 8 MiB, and by the widest margin at the sizes it was expected to
win.

Measured on a Ryzen Threadripper PRO 9975WX, kernel 7.2.2, file on XFS over
NVMe, server and client pinned to separate cores, three passes, medians below
(run-to-run spread was under 5% on every row except `read + write_all` at
8 MiB, which spanned 380-433 us). `cpu` is `CLOCK_THREAD_CPUTIME_ID` on the
executor thread; `off` is CPU spent for the same rung on any other thread of
the process -- `iou-wrk` kernel workers -- which the executor's own clock
cannot see. `total` is the sum, which is what a machine pays.

### Warm page cache, `BufferedFile`

| payload | read+write cpu | send_file cpu | send_file off | total ratio |
|--------:|---------------:|--------------:|--------------:|------------:|
| 1 KiB   |  3,027 ns |   6,210 ns |   2,041 ns | 2.73x |
| 4 KiB   |  3,275 ns |   6,191 ns |   2,071 ns | 2.52x |
| 16 KiB  |  3,712 ns |   6,203 ns |   2,266 ns | 2.28x |
| 32 KiB  |  4,194 ns |   6,253 ns |   2,542 ns | 2.10x |
| 64 KiB  |  5,569 ns |   6,458 ns |   3,607 ns | 1.81x |
| 128 KiB |  8,240 ns |  11,960 ns |   7,204 ns | 2.33x |
| 256 KiB | 13,762 ns |  23,340 ns |  13,394 ns | 2.67x |
| 512 KiB | 25,595 ns |  46,162 ns |  25,445 ns | 2.80x |
| 1 MiB   | 51,068 ns |  91,167 ns |  50,644 ns | 2.78x |
| 8 MiB   | 418,179 ns | 710,654 ns | 401,672 ns | 2.66x |

`read + write_all` recorded 0-61 ns of off-thread CPU at every size, so its
`cpu` column is its whole cost. `send_file`'s is roughly half of its.

Two shapes in that table say what is going on:

**`send_file` is flat from 1 KiB to 64 KiB** -- 6,210 ns to 6,458 ns while the
payload grows 64-fold. It is paying per operation, not per byte. Above 64 KiB
it becomes linear at about 5,550 ns of executor CPU and 3,140 ns of worker CPU
per chunk (8 MiB is 128 chunks: 710,654/128 and 401,672/128).

**The chunk is 64 KiB because that is the pipe**, and `PIPE_CAPACITY` is the
unmodified default. So 8 MiB is 256 ring operations against the two that
`read_at` plus `write_all` submit for the same bytes. `F_SETPIPE_SZ` was
deferred out of the design; this table is what deferring it costs.

**Every chunk reaches a kernel worker.** The off-thread column is never zero
for `send_file`, not even at 1 KiB, and thread names during a run confirm the
workers: five `iou-wrk-<tid>` threads, all parented to the executor thread. A
warm `read_at` completes inline on the submitting thread and spawns nothing.
Marginal cost per 64 KiB works out at ~3.2 us for copying the bytes through
userspace twice, against ~8.7 us for splicing them zero times.

### Page cache dropped before every response, `BufferedFile`

| payload | read+write cpu | send_file cpu | send_file off | total ratio |
|--------:|---------------:|--------------:|--------------:|------------:|
| 4 KiB   |   9,280 ns |   6,945 ns |   1,993 ns | 0.96x |
| 16 KiB  |  11,031 ns |   7,431 ns |   2,153 ns | 0.87x |
| 64 KiB  |  21,730 ns |   7,974 ns |   3,649 ns | 0.53x |
| 256 KiB |  53,369 ns |  39,065 ns |  38,680 ns | 1.46x |
| 1 MiB   | 130,529 ns | 108,737 ns |  75,286 ns | 1.41x |
| 8 MiB   | 848,396 ns | 825,013 ns | 585,124 ns | 1.66x |

This is the one group `send_file` wins, and only up to 64 KiB. What changes is
not `send_file` -- its numbers barely move from the warm table -- but its
opponent: a cold `read_at` cannot complete inline, so it too pays a park and a
wake, which is most of what `send_file` was paying all along. Read the
executor-CPU column alone and `send_file` wins the whole range down to 0.37x;
add the workers it wakes and the win survives only where the payload is one
pipe-load or less.

### `O_DIRECT`, `DmaFile`

| payload | read+write cpu | send_file cpu | send_file off | total ratio |
|--------:|---------------:|--------------:|--------------:|------------:|
| 16 KiB  |   7,895 ns |    8,714 ns |   4,121 ns | 1.63x |
| 64 KiB  |  10,930 ns |    9,012 ns |   5,823 ns | 1.36x |
| 256 KiB |  27,500 ns |   34,904 ns |  23,235 ns | 2.11x |
| 1 MiB   |  83,549 ns |  137,984 ns |  93,370 ns | 2.77x |
| 8 MiB   | 520,947 ns | 1,091,129 ns | 744,826 ns | 3.52x |

`O_DIRECT` already avoids the page cache, so the copy `send_file` removes is
the only one left, and the chunking overhead swamps it. This is the worst
group for splicing and the one where a caller is likeliest to reach for it.

### This does not contradict nginx

The received wisdom is that `sendfile` is a large win, and the tables above
look like a refutation. They are not, because the wisdom is about
`sendfile(2)` and what is measured here is not `sendfile(2)`.

**io_uring has no sendfile opcode.** Checked against io-uring 0.7.14's
`opcode.rs`: there is `Send`, `SendMsg`, `SendZc`, `SendBundle`,
`MsgRingSendFd` and `Splice` -- and no `SendFile`. `sendfile(2)` is a single
syscall that moves file to socket directly, looping inside the kernel with no
intermediate and no fixed chunk. `send_file` here is `splice` file -> pipe ->
socket, two ring operations per 64 KiB, through a pipe that exists only
because splice refuses that pairing directly. The mechanism the wisdom
describes is not available on the ring; this is the nearest thing that is.

**The baseline also moved.** The classic comparison is against blocking
`read()` plus `write()`: a syscall and a pair of context switches per chunk on
each side, plus the copies. `sendfile` deletes nearly all of it. The baseline
here is io_uring `read_at` plus `write_all` -- already one SQE each, already
asynchronous, no syscall per operation. io_uring had already deleted the part
`sendfile` deletes, so the only prize left for splicing is the copies
themselves.

**And copies got cheap while per-operation overhead did not.** The marginal
cost of copying 64 KiB through userspace twice measures at ~3.2 us here. A
punt to an `iou-wrk` worker plus the park and wake around it costs more than
that. On the hardware the wisdom was formed on, the copy dominated everything;
that ratio has inverted.

Finally, this is loopback and the meter is CPU. On a real 40 or 100 GbE NIC
at line rate, copy bandwidth competes for the memory bus and the calculus
moves back toward zero-copy. Nothing here measures that case.

### What this means for the feature

`send_file` stays. It is correct, it is tested, and it is the only way to
serve a file without the bytes entering the process -- which matters for
address-space pressure and for kTLS offload independently of CPU. But the
rustdoc's original claim, that its cost "does not scale with the size of the
payload the way a read-then-write does", is false as measured and has been
corrected: it scales at 64 KiB granularity, which is worse than a single large
`read_at` scales.

The honest recommendation, now in the doc comment: reach for `send_file` when
the file is not in page cache and the payload is at most one pipe-load; prefer
`read_at` plus `write_all` otherwise.

### What to try next, in order

The measured number is a floor for the technique, not its ceiling: two of the
levers that would move it most were deferred out of the design before any of
this was measured. Nothing below is a promised fix -- each is a hypothesis
with a cheap test.

1. **`F_SETPIPE_SZ`.** The whole deficit lives in per-chunk cost, and the
   chunk is the pipe. At 1 MiB of pipe an 8 MiB payload is 8 chunks instead of
   128. Nothing here shows a bigger pipe stays as cheap *per chunk*, which is
   exactly what the test would establish. Cheapest lever, largest expected
   move, do it first.

2. **`IOSQE_IO_LINK` across the two splices.** Today the file -> pipe splice
   completes, comes back to the executor, and only then is pipe -> socket
   submitted: two round trips per chunk. Linking them submits both at once.
   Halves the round trips without touching the chunk size, and composes with
   (1) rather than competing.

3. **A reactor pipe pool.** Removes a `pipe2` and two reactor-routed closes
   per call, and removes the descriptor budget documented on `send_file`. The
   per-call pipe is currently load-bearing for correctness -- a reused pipe
   could carry one transfer's leftovers into the next -- so a pool has to
   prove it drains, which is the real cost of this one.

4. **A direct `sendfile(2)` probe.** Blocking `sendfile(2)` against blocking
   `read` + `write` against this ring path, same file, same socket. Settles
   whether the pipe is the entire gap or whether the premise does not survive
   an io_uring baseline at all. This is the measurement that tells you whether
   1 through 3 are worth doing.

5. **`UnixStream::send_file`** and **`SpliceSource for ImmutableFile`.** Reach,
   not speed; both are recorded in [FEATURE_GAP.md](../../FEATURE_GAP.md).
   `UnixStream` is wiring. `ImmutableFile` needs the type to hold a `DmaFile`
   rather than a reader builder, so it is a change to what the type is.

Do 4 before 1 through 3 if the question is "should this exist"; do 1 first if
the question is "make it faster".

## What this does not cover

Loopback only, one connection, 64-byte messages, TCP only. Not covered: real
NICs, many connections, large messages, UDP, `accept` throughput, and the
receive side of the streaming test in isolation. Loopback exaggerates software
cost because there is no wire time to hide behind — which makes it the right
place to *find* overhead and the wrong place to size its real-world impact.
