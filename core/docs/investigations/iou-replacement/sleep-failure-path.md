# Resolving the `SleepableRing::sleep` Failure Path

**Date:** 2026-08-01
**Status:** resolved — the blocker is not a blocker
**Outcome:** leaving the entry queued is correct; the nop is an optimisation

This was the one thing in the [`iou` replacement](README.md) that could not be
translated. It turns out it does not need to be.

## What the code does today

```rust
if let Some(mut sqe) = self.ring.sq().prepare_sqe() {
    let sqe_ptr = unsafe { sqe.raw_mut() as *mut _ };
    // ... fill with PollAdd on link_fd, user_data = a freshly registered source
    if self.submit_sqes()... != 1 {
        unsafe { crate::uring_sys::io_uring_prep_nop(sqe_ptr) };
        Err(io::Error::from_raw_os_error(libc::EBUSY))
    } else {
        self.ring.cq().wait(1)  // sleep
    }
}
```

`link_fd` is the latency ring's own fd (`uring.rs:1345`). Polling it from the
main ring is how a sleeping main ring gets woken by latency-ring activity.

## Why the nop is there — and it is not what it looks like

The obvious reading is "neutralise the poll". That is only half of it.

`io_uring_prep_nop` calls `io_uring_prep_rw`, which sets exactly `opcode`, `fd`,
`off`, `addr` and `len`:

```c
IOURINGINLINE void io_uring_prep_rw(int op, struct io_uring_sqe *sqe, int fd,
				    const void *addr, unsigned len, __u64 offset)
{
	sqe->opcode = (__u8) op;
	sqe->fd = fd;
	sqe->off = offset;
	sqe->addr = (unsigned long) addr;
	sqe->len = len;
}
```

**It does not touch `user_data`.** So the rewritten SQE is a nop that still
carries the user_data of the `LinkRings` source registered moments earlier. It
completes immediately, and `process_one_event` calls `consume_source`, which
removes that source from the `SourceMap` and drops it.

Without that, the source would sit in the map with nothing left to complete it.
**The nop's real job is reaping the source registration, not neutralising the
poll.**

## What a `LinkRings` completion actually does

```rust
|source| match source.source_type {
    SourceType::LinkRings => Some(()),
    _ => None,
}
```

`try_process` returning `Some(())` makes `process_one_event` **skip** the entire
post-process branch: no `record_stats`, no result stored, no waker woken. A
`LinkRings` completion does one thing and one thing only — `consume_source`
removes it from the map.

That is the whole observable effect, whether it arrives from a nop or from a
poll that fired.

## Therefore

Under `io-uring`, a failed submit can simply **leave the pushed entry queued**.
No rewind, no pop, no repair:

1. The next `submit()` — which the reactor performs in `flush_rings!` before it
   can consider sleeping again — sends the poll.
2. The poll arms on `link_fd`, which is the latency ring, which sees traffic
   constantly (the preempt timer alone guarantees it). It completes promptly.
3. Its completion consumes the source and drops it. **No leak.** Nobody is woken,
   because `LinkRings` short-circuits.
4. The only cost is one spurious CQE on the main ring, which at worst makes a
   later `wait(1)` return early. The reactor loop re-checks its state anyway.

So the nop is an **optimisation and a tidiness measure** — it retires the
registration with an operation that completes instantly instead of arming a real
poll — not a correctness requirement. The invariant that matters, *every
registered source is consumed exactly once*, holds either way.

### Two invariants that were checked, not assumed

- **`assert_eq!(self.waiting_kernel_submission(), 0, "sleeping with pending SQEs")`**
  at the top of `sleep`. A queued entry would trip it — but `flush_rings!` runs
  before the sleep decision on every reactor iteration, so the queue is drained
  in between.
- **`can_sleep()`** requires `waiting_kernel_submission() == 0`, so a leftover
  entry prevents sleeping until it has been submitted. That is the conservative
  direction.

## Recommended implementation

Keep it simple and do not emulate the nop:

```rust
// A failed submit leaves the poll queued rather than being repaired. It will
// go out with the next submit, arm on the latency ring, and complete almost
// immediately, and its completion is what retires the source registration.
// io-uring cannot rewrite a pushed entry, and it does not need to: a
// LinkRings completion wakes nobody, so an extra one is only a spurious CQE.
```

Optionally preflight the common failure: `EBUSY` means the completion queue is
full, which is visible in advance via `completion().len()` against
`completion().capacity()`. Declining to sleep before pushing anything avoids the
spurious poll in the case that actually occurs. `EINTR` cannot be preflighted,
so the queued-entry path has to be correct regardless — which, per the above, it
is.

## What this changes about the migration

The submission path no longer has an untranslatable piece. The remaining work on
`refactor/iou-core` is mechanical: roughly twenty type and API errors, the
registrar, and deleting `glommio/src/iou` and `glommio/src/uring_sys`.
