# Design: making "took wakers, didn't wake them" unrepresentable

**Date:** 2026-08-20
**Status:** approved in chat
**Scope:** a new crate-private module, applied to the five waker stores added this week

## The hazard, and why documenting it is not enough

Every waiting primitive here follows one shape:

```rust
let waker = { let mut state = shared.lock(); state.waker.take() };  // under the lock
if let Some(waker) = waker { waker.wake(); }                        // outside it
```

The lock must be released before waking, or a woken poller blocks on it
immediately. So the wakers necessarily travel, unattached, across a scope
boundary — and a `Vec<Waker>` in flight carries no evidence that anyone still
intends to wake it.

Dropping them instead of waking them is a silent bug: the waiter simply never
runs. Worse, **it does not reliably present as one.** Suppressing the wake in
`ForeignState::cancel` by dropping the wakers left the parked executor
noticing anyway, in about 350 microseconds, because dropping a glommio `Waker`
from a foreign thread routes through the owning executor's notifier and
perturbs it. Only leaking the wakers (`mem::forget`) produced the hang that the
mistake actually deserves.

An implementation with this bug therefore passes its tests, and passes them for
a reason unrelated to its own correctness. That is the case for encoding the
rule in a type rather than a comment.

## The encoding

Two types, in a crate-private module:

```rust
/// Wakers waiting to be woken. The only way to remove them is `take`.
pub(crate) struct WakerList(Vec<Waker>);

impl WakerList {
    pub(crate) const fn new() -> Self;
    pub(crate) fn push(&mut self, waker: Waker);
    pub(crate) fn is_empty(&self) -> bool;

    /// Takes every waker, handing back an obligation to wake them.
    #[must_use = "…"]
    pub(crate) fn take(&mut self) -> PendingWakes;
}

/// Wakers removed from a `WakerList` and not yet woken.
///
/// Exists to be carried out of a lock and discharged. Waking is the only
/// thing you can do with one.
pub(crate) struct PendingWakes(Vec<Waker>);

impl PendingWakes {
    pub(crate) fn wake(self);
}
```

**No method returns a `Waker` or a `Vec<Waker>`.** Once a waker is in a
`WakerList` the only exit is `take()`, and the only thing `PendingWakes` can do
is `wake()`. The correct pattern becomes the *shortest* one to write:

```rust
let pending = { let mut state = shared.lock().unwrap(); state.wakers.take() };
pending.wake();
```

The obligation being a separate value is what lets it cross the lock boundary,
which is exactly the manoeuvre the hazard lives in.

## Where the type system stops, said plainly

Rust has no linear types: nothing can force `PendingWakes` to be consumed. The
design gets two of the three cases and is honest about the third.

| Mistake | Caught by | When |
|---|---|---|
| `state.wakers.take();` — result ignored | `#[must_use]` | compile time |
| `let p = …take(); /* forgotten */` | debug drop guard | test run, loudly |
| `mem::forget(p)` | nothing | — |

The drop guard is `debug_assertions`-only and does not fire while the thread is
already panicking, so it cannot turn a test failure into an abort.

Deliberately not attempted: a `#[must_use]` that survives into release builds
as a runtime cost, or an unsafe trick to make dropping impossible. The residual
gap — someone deliberately leaking the value — is not a mistake anyone makes by
accident, which is the only kind this is meant to catch.

## Where it is applied

The five stores added this week, all of which have the shape above:

- `sync::cancellation_token` — `Node::wakers` and `ForeignState::wakers`
- `channels::broadcast` — `Inner::wakers`
- `channels::watch` — `Inner::wakers`
- `channels::oneshot` — `Inner::waker`
- `executor::BlockingSendShared::waker` (`spawn_blocking_send`)

**Not** applied to `reactor::connection_wakers` or `io::dma_file_stream`, which
predate this work and have not been analysed. Converting them is a separate
change with its own reasoning, and mixing it in would hide behind this one.

## Testing

The type's own behaviour:

- a taken obligation wakes every waker it holds
- `take` on an empty list yields an obligation that is fine to drop
- pushing after a take starts a fresh obligation

Plus the guard itself: a `#[should_panic]` test, `#[cfg(debug_assertions)]`,
that drops a non-empty `PendingWakes` and asserts it complains. Without that,
the guard is a claim rather than a mechanism.

The five converted sites keep their existing tests unchanged. If the conversion
is faithful they all still pass, which is the point — this changes how the
invariant is expressed, not what the code does.
