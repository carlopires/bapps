# Design: completion-based reads on the buffered path

**Date:** 2026-08-24
**Status:** approved in chat (Stage A of two)
**Measurements:** [network.md](../../investigations/io-path/network.md),
`glommio/examples/recv_ladder.rs`

## Why

At 256 connections with data arriving after the read is posted, CPU per
message:

| | |
|---|---:|
| readiness: `recv`, `PollAdd`, `recv` — today | 1,092 ns |
| speculate, then `Recv` | 894 ns |
| `Recv` with no speculation | 791 ns |
| multishot + provided buffer ring | 635 ns |

Two thirds of the available win needs no buffer ring, no `IORING_CQE_F_MORE`,
no `ENOBUFS` re-arm, and no kernel past 5.6. That is this stage. Multishot is
Stage B and must re-earn its remaining ~150 ns *against this*, not against the
readiness path it was originally measured against.

## The constraint is lifetime, not speed

`poll_read` receives `&mut [u8]` valid for the duration of one call. A
completion read needs the buffer until the CQE arrives, which may be after the
future is dropped. So a completion read is only sound where **glommio owns the
buffer** — the `Buffered` path (`RxBuf`/`Preallocated`), not the unbuffered
one. The unbuffered path keeps the readiness design, and that is a permanent
split rather than a stage.

Ownership transfer is already the house pattern and is already sound:
`Source::drop` (`sys/source.rs:334`) does not free a dispatched request's
buffers, because "the kernel might be using the buffers right now, so we delay
`consume_source` until we consume the corresponding event from the completion
queue." A buffer held in a `SourceType` therefore outlives the stream that
handed it over. `SockRecv` relies on this today, with a `DmaBuffer` and a copy
at the end; this stage removes the copy by sending the receive buffer itself.

## Shape

```rust
/// A receive buffer on loan to the kernel.
pub struct OwnedRxBuf { memory: Vec<u8>, offset: usize }

pub trait RxBuf {
    // ... existing methods unchanged ...

    /// Lends the buffer for a completion-based read. `None` — the default —
    /// keeps this implementation on the readiness path.
    fn take_kernel_buffer(&mut self) -> Option<OwnedRxBuf> { None }

    /// Takes it back, empty. How much the kernel wrote arrives separately
    /// through `handle_result`, the same call the readiness path uses, so
    /// one place advances the cursor rather than two that must agree.
    fn restore_kernel_buffer(&mut self, buffer: OwnedRxBuf);
}
```

Defaulted, so every existing implementor outside this crate keeps compiling
and keeps its current behaviour. `Preallocated` implements both. The default
`restore` is `unreachable!`, which is honest: it can only be reached if `take`
was overridden to return `Some`, and then the pair must be overridden too.

A new `SourceType::SockRecvInto(Option<OwnedRxBuf>)` carries the loan. It
joins `SockRecv` in `ring_for_source`'s network-Rx arm, so it goes to the
latency ring like every other socket read.

While the loan is out the `RxBuf` has no memory. That is fine and needs no
extra state: a buffer is only lent when it is empty, and only one read per
stream is ever in flight.

## Speculation stays, adaptively

Deleting `yolo_recv` would cost the streaming reader, whose data is already
buffered and for whom one syscall beats an SQE and a CQE. Keeping it
unconditionally costs the idle-then-woken reader a wasted `EAGAIN` on every
message — that is the difference between the 894 ns and 791 ns rungs.

So a one-bit-per-stream heuristic, `speculate`, starting `true`:

- speculative read succeeds → stays `true` (a streaming reader keeps winning);
- speculative read returns `EAGAIN` → `false` (stop paying for it);
- completion read fills the buffer → `true` (more is probably waiting);
- completion read partially fills → `false`.

Converges to one syscall for streaming and to zero wasted syscalls for idle
connections, with no configuration.

## Testing

Existing buffered-stream tests must pass unchanged — that is the evidence the
read path still reads.

New, and each has to fail if the mechanism is removed:

1. **A read in flight survives the stream being dropped.** Drop a
   `TcpStream` with a completion read outstanding, then keep the executor
   alive. Under Miri-less testing the proof is that the buffer is not freed
   before the CQE: assert the source outlives the stream. This is the test
   that would catch the whole design being wrong.
2. **A partially filled buffer reads back correctly** — the byte count comes
   from `handle_result`, so a full buffer must not be reported.
3. **The heuristic flips both ways**, asserted through observed syscall
   behaviour rather than by reading the flag.
4. **Streaming does not regress**: a large transfer over a buffered stream,
   measured, not asserted.

## Measured

Same probe, buffered path, CPU per message:

| | before | after |
|---|---:|---:|
| 256 connections, readers parked before data arrives | 1,247 ns | **1,023 ns** (−18%) |
| streaming, one connection, data always waiting | 3,861 ns/64KiB | 3,795 ns/64KiB |
| unbuffered path (untouched) | 1,227 ns | 1,228 ns |

The streaming figure is the one that had to *not* move, and it did not: the
heuristic keeps that path on the syscall.

**The first version of this measurement was wrong**, in a way worth recording.
The ladder's glommio rung read 256 connections in a loop from one task, so by
the time it reached connection 200 the data had long arrived, the speculation
succeeded, and the rung measured the easy case -- it reported the change as a
50% *regression*. One task per connection, all parked before the writer
speaks, is the shape the hand-written rungs use, and the comparison only means
anything when both sides sit in the same regime.

## Risks

**A lent buffer that is never returned leaks it.** The `Option` in the
`SourceType` makes "returned twice" unrepresentable but not "never returned".
Every completion path must restore, including the error path.

**`OwnedRxBuf` is new public API.** It is inert — no methods a caller needs —
but it is nameable, and it exists only because the trait is public.

**Streaming regression** if the heuristic is wrong in a way the ladder does
not model. Measured, not argued: `writev_ladder`'s drain server gives a
streaming shape to check against.
