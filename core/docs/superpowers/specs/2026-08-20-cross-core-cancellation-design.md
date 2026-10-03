# Design: cross-core cancellation

**Date:** 2026-08-20
**Status:** approved in chat
**Scope:** `glommio/src/sync/cancellation_token.rs`

## Why

`CancellationToken` is `Rc`-based and `!Send`, which is the right design and is
not changing. But it cannot express the most common shutdown shape in a
thread-per-core system: a control plane on one core deciding to stop work
running on the others.

A downstream that removed tokio's executor entirely reported this as the single
remaining blocker. Thirty-eight of their thirty-nine `tokio_util` cancellation
sites converted; the thirty-ninth is a job-level token created on the control
core and cloned into chains running on per-vnode executors.

The cost of not having it is not the twenty lines of glue each project writes.
It is that someone concludes "glommio's token cannot do shutdown" and puts
`tokio_util` back — which only has to happen once per project.

## Surface

```rust
impl CancellationToken {
    /// A handle that can cross to another executor.
    pub fn foreign_child(&self) -> ForeignCancellation;
}

/// `Send + Sync + Clone`. Clone one per far-core task.
pub struct ForeignCancellation { /* … */ }

impl ForeignCancellation {
    /// Turns the handle back into an ordinary token. Call it inside the
    /// destination executor.
    pub fn attach(&self) -> CancellationToken;

    /// Whether the origin has cancelled, or gone away.
    pub fn is_cancelled(&self) -> bool;
}
```

`attach()` returns a **fully ordinary** `CancellationToken`: `child_token()`,
`cancelled()` in a `select!` arm, everything. Its parent merely happens to live
on another core, and nothing downstream of it ever learns that.

`Clone` is deliberate, and differs from `ConnectedSender::into_foreign`, which
is `!Clone` because its buffer is single-producer. Cancellation has no such
constraint, and the shape it must fit clones the token once per spawned chain.

## The two races, which are the design

Both come from one real sequence, two lines apart:

```rust
if let Some(rj) = running.remove(&job_id) {   // ownership leaves the map
    rj.shutdown.cancel();                      // cancel fires
}                                              // origin token drops here
```

Microseconds later, chains on other cores may not have attached yet.

**Cancelled, then attached** → `attach()` returns an already-cancelled token.
No watcher task is spawned.

**Dropped without cancelling, then attached** → `attach()` *also* returns an
already-cancelled token. Once the last origin `Rc` is gone nothing can ever
cancel that state, so any other answer is a permanent hang; and semantically
the job is over, so a chain observing late has nothing to wait for.

Collapsing both into one check is why the handle holds an `Arc` to the flag
rather than a `Weak` back to the origin. With a `Weak` the implementation must
distinguish *dropped* from *never-cancelled*, and that distinction is precisely
what gets got wrong.

**`attach()` never returns an error.** A caller that must interpret a failure
would be a second thing to get wrong, in the same window.

## The asymmetry, which must be documented loudly

Dropping a *local* parent token does not cancel its children: they keep their
own `Rc`, and can still be cancelled through their own handle.

Dropping an *origin* token does cancel every attached foreign token, because no
handle to cancel it through survives the crossing. Unreachable and cancelled
are indistinguishable from the far side.

This is a deliberate difference in behaviour between two operations that look
symmetric, so it belongs in the type's own documentation and in the porting
page, not only here.

## Internals

`ForeignState`, held by `Arc`:

```rust
struct ForeignState {
    cancelled: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}
```

`Node` gains `foreign: RefCell<Vec<Arc<ForeignState>>>`.

- `foreign_child()` creates a state seeded with the token's current
  `cancelled` value, registers it on the node unless already cancelled, and
  returns a handle holding an `Arc` to it.
- `Node::cancel()` sets every registered flag and drains its wakers, as it
  already does for local children.
- `Node::drop` does the same, for the reason above. The handle's own `Arc`
  keeps the state alive after the node is gone.
- `attach()` reads the flag once. Cancelled: return an already-cancelled token
  and spawn nothing. Otherwise: create a local token, and `spawn_local` one
  detached task that awaits the state and cancels that token.

The waiting task parks on a `Send` future whose waker is stored in the state's
`Mutex`. Waking it from another thread routes through glommio's existing
foreign-wake path — `raw.rs:211` resolves the owning executor's sleep notifier
— the same machinery `shared_channel` and `spawn_blocking` already use. Nothing
new is required underneath.

**Cancellation is asynchronous across the boundary.** The origin returns from
`cancel()` immediately; the far token becomes cancelled once its executor polls
the watcher. That is inherent to crossing cores and should be stated rather
than implied.

## Testing

The acceptance test is the downstream's shape, not a synthetic one: a token on
one executor, a `foreign_child()` moved to a second, work there observing
`cancelled()`.

- cancellation crosses to another executor at all
- the handle is `Send`, asserted by moving it into another executor's closure
- `attach()` after `cancel()` yields an already-cancelled token
- `attach()` after the origin is **dropped** yields an already-cancelled token
- a token from `attach()` supports `child_token()`, and cancelling the origin
  reaches the grandchild
- one handle, cloned, attaches on several executors and all of them cancel
- the far side is woken rather than merely eventually noticing: the executor is
  parked before the origin cancels

Mutation checks: removing the `Node::drop` rule must fail the
drop-then-attach test, and suppressing the wake must hang the parked test.

**The second mutation has to be written carefully, and the obvious version is
wrong.** Taking the wakers and dropping them without waking still lets the far
executor notice, in about 350 microseconds -- because dropping a glommio
`Waker` from a foreign thread routes through the owning executor's notifier and
perturbs it. Only *leaking* the wakers (`mem::forget`) suppresses the wake
entirely, and then the parked test hangs, which is the real result.

Worth knowing beyond this module: an implementation that stores wakers and
drops them without waking will appear to work while depending on incidental
behaviour of the foreign-drop path.

## Risks

**A watcher task per attach.** One parked task per crossing, released when its
executor shuts down. At job-deploy rates this is free; it would not be at
per-record rates, and the docs should say what it is for.

**`attach()` requires an executor**, since it spawns. Calling it outside one
panics, and that is a runtime requirement the type cannot state — the same
wart `shared_channel::connect` has.
