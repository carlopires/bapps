# Investigation: Allocations On The Hot Paths

**Date:** 2026-08-19
**Status:** measured — **nothing to chase**
**Probe:** `probe_alloc_per_op.rs`, wired up as `cargo run --release --example alloc_per_op`

"Chase the allocations" is the most reflexive suggestion in runtime performance
work, and it had never been tested here. It is cheap to test: wrap the global
allocator in a counter, run each path, divide.

## Allocations per operation

| path | allocs/op | bytes/op |
|---|---:|---:|
| `spawn_local` + await | 1.00 | 41 |
| `yield_now` | 0.00 | 0 |
| local channel send | 0.00 | 0 |
| TCP round trip (64 B, depth 1) | 12.00 | 3,472 |

Three of the four paths are already at the floor. The task path allocates
exactly once — the task itself, 41 bytes — which is the design, not an
oversight: see [task-arena](../task-arena/) for what happened the last time
somebody tried to remove it.

## The TCP path allocates twelve times, and it does not matter

Size histogram per round trip: **6 x 520 B**, 4 x 40 B, 2 x 96 B.

520 bytes is `Rc<RefCell<InnerSource>>` exactly — `InnerSource` is 496 bytes,
plus strong count, weak count and the borrow flag. So a 64-byte echo round trip
creates **six `Source`s**, which corroborates from a second direction the nine
io_uring completions per read measured in
[io-path/per-io-cost.md](../io-path/per-io-cost.md).

`SourceType` is 280 of those 496 bytes, inflated by two variants nothing on the
TCP path uses: `SockRecvMsg` and `SockSendMsg` carry a `libc::msghdr` and a
`sockaddr_storage` inline. Boxing them would shrink every `Source` by roughly
45%.

**Do not bother.** The premise test is whether allocation cost is on the
critical path at all, and the way to test it is to change the allocator and see
if the path moves. Three alternating pairs, back to back in one session:

| run | System | mimalloc |
|---|---:|---:|
| 1 | 6,540 ns | 6,537 ns |
| 2 | 6,557 ns | 6,599 ns |
| 3 | 6,556 ns | 6,580 ns |

Twelve allocations per round trip, and a materially faster allocator does not
move the round trip — mimalloc is marginally *slower*, inside the spread. At
~6.5 us per round trip the allocations are somewhere under 1%, overlapped by
out-of-order execution and dwarfed by the io_uring submits they accompany.

This is the same shape as the retracted "58% of a task switch is reference
counting" claim: primitive cost badly overpredicts path cost. Twelve
allocations at a nominal ~25 ns each would suggest 300 ns, or 5%. The real
figure is not measurable.

## What this closes, and what it leaves open

**Closed:** allocation count and allocation size on the task, channel and TCP
paths. There is no win here at any queue depth, under any allocator.

**Still open, and unchanged by this:** the *number of sources* per round trip.
Six is a lot for one read and one write, but the cost of a source is the
io_uring submission and completion it represents, not the 520 bytes it lives
in. That is item 1 in [PERFORMANCE_NEXT.md](../../PERFORMANCE_NEXT.md), it is a
restructure of the park decision, and it is unaffected by anything here.

**Worth doing anyway, for reasons other than speed:** boxing the two `msghdr`
variants of `SourceType` would cut resident memory per in-flight operation by
45%. At high queue depth that is a memory footprint argument, not a latency
one, and it should be justified and measured as such rather than smuggled in as
a performance change.

## Method note

The probe counts allocations, not just time, deliberately. Time on these paths
is dominated by io_uring and the device, which is exactly what makes a count
the more honest signal about whether there is anything to find. The timing
column exists only to run the premise test.

The absolute round-trip figure here (~6.5 us) is higher than the ~3.8 us in
[io-path/network.md](../io-path/network.md) because both ends share one
executor on one core. It is a within-probe control, not a headline number.
