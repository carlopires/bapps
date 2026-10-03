# Design: one channel, two storages

**Date:** 2026-08-21
**Status:** approved in chat
**Scope:** a crate-private `channels::storage`, applied to `oneshot`, `watch`,
`broadcast`

## Why

Each of the three channels needs a per-core form and a cross-core form. The
per-core form must stay `Rc`-cheap and `!Send`; the cross-core form must be
`Send`. Written twice, that is three duplicated state machines — and `oneshot`
already has one, hand-written in 0.10.15, which is the evidence rather than the
prediction.

The duplication is not where the difficulty is. Ring policy, cursors,
`Lagged(n)`, close rules and waker discipline are what must be right. Whether
the state sits behind a `RefCell` or a `Mutex` is a handful of lines.

## The seam

```rust
pub(crate) trait Storage<S>: Clone {
    fn with<R>(&self, f: impl FnOnce(&mut S) -> R) -> R;

    /// Mutate the state, then wake outside the lock.
    fn with_wakes<R>(&self, f: impl FnOnce(&mut S) -> (R, PendingWakes)) -> R {
        let (value, pending) = self.with(f);
        pending.wake();
        value
    }
}

pub(crate) struct Local<S>(Rc<RefCell<S>>);    // !Send
pub(crate) struct Shared<S>(Arc<Mutex<S>>);    // Send + Sync where S: Send
```

Generic over the state, not over one channel's `Inner`, so all three share it.

### What the types enforce, and what they cannot

`Send`-ness **falls out of the storage** rather than being asserted: `Rc` makes
the local form `!Send`, `Arc<Mutex<_>>` makes the shared form `Send + Sync`. No
`unsafe impl`, no marker types. A reviewer confirms it by reading a type
parameter.

The **closure signature is the guard**, deliberately: `with` hands out `&mut S`
for the duration of a call, so a caller cannot hold the guard across an
`await`. That is the bug people write by hand with `Arc<Mutex<_>>`, and here it
is unrepresentable rather than discouraged.

`with_wakes` makes the *only* expressible order the correct one — take the
wakers under the lock, release it, then wake. Waking under the lock leaves the
woken poller blocking on it immediately, and this shape cannot express that.
It composes with [`PendingWakes`](../../../glommio/src/wakers.rs), which
already makes "took the wakers and dropped them" hard.

What it cannot enforce: nothing stops a caller using `with` directly and
forgetting to wake. `PendingWakes`' own drop guard catches that in debug, which
is the same honest half-measure recorded in the waker-obligation design — Rust
has no linear types, so the compile-time half is `#[must_use]` and the rest is
loud at test time.

There is deliberately **no `fn new`** on the trait: it would foreclose a
storage that needs construction arguments, for no gain.

## What each channel becomes

One state struct and one set of semantics per channel, parameterised:

```rust
pub struct Receiver<T, S = Local<State<T>>> { … }
pub type SharedReceiver<T> = Receiver<T, Shared<State<T>>>;
```

The default parameter keeps `broadcast::Receiver<T>`, `watch::Receiver<T>` and
`oneshot::Receiver<T>` meaning exactly what they mean today. Nothing breaks;
the shared forms are additive.

**`oneshot` converts first, and the conversion deletes code.** Its 0.10.15
shared implementation carries `Mutex<Option<T>>`, `Mutex<WakerList>` and two
`AtomicBool`s; unified, it is one state behind one lock, and the atomics go.
`SharedSender`/`SharedReceiver` remain as type aliases, so 0.11.0's public API
is unchanged.

`watch` and `broadcast` then gain shared forms they do not have today, with no
new semantics written.

## No notifier, no handshake

A glommio task's `Waker` carries its owning executor's id; waking it from any
thread resolves that executor's sleep notifier and writes its eventfd
(`task/raw.rs:272`). So a plain `Waker` in the shared state is enough to wake a
peer parked in `io_uring_enter`, and registration happens at first poll on
whichever executor polls — the ordinary futures contract.

`ConnectedSender::into_foreign` is not a counter-example. What it moves is
placement of *reactor-owned resources* — ring registration, buffer ownership,
free-space accounting — not notification. Reading it as a notification handshake
leads to designing a placement step that nothing here needs.

Proof rather than argument: `oneshot`'s shipped
`a_shared_receiver_is_woken_when_it_was_parked` sends from a plain `std::thread`
to an executor parked 300ms in the kernel, and hangs forever when the wakers are
leaked.

## Contention, stated rather than optimised

N receivers on N cores polling one `Mutex` is a contended cache line per poll,
and "a thread-per-core runtime whose broadcast serialises on a mutex" is the
criticism this will attract. The shared forms are for control-plane fan-out —
checkpoint decisions, route-ready events, hundreds a second — and the
documentation should say so, rather than the mutex being optimised for a
workload nobody has.

Two measurements, both against the existing tests as a baseline: the local
`with()` indirection, which should vanish under inlining against a ~0.4ns
`RefCell` borrow; and the shared path under N-core contention, to have the
number rather than a hand-wave.

## Measured

Taken on this machine, 200k rounds per figure, three runs, `--release`. The
local numbers are the same file run against `821761c`, the revision before the
seam.

| | before | after |
|---|---|---|
| local broadcast send+recv | 11.9 ns | 10.4 ns |
| local watch send+borrow | 3.0 ns | 4.1 ns |
| local oneshot create+send+recv | 15.8 ns | 17.5 ns |

So it is not free, and the prediction that it would vanish under inlining was
wrong: `watch` send costs about 1 ns more, `oneshot` about 1.7 ns, and
`broadcast` about 1.5 ns *less* -- the last because its poll path now decides
and registers under one lock instead of taking two. Marking the seam `#[inline]`
was tried and moved nothing outside the noise, so it is not there.

A nanosecond on a 3 ns operation is a third of it, and worth saying plainly
rather than rounding to "free". It buys deleting one hand-written state machine
and not writing two more.

The shared path, same machine: 26 ns/op uncontended, and about 16 ns per send
with 2, 4 and 8 receivers each on their own executor. Reproduce with
`cargo run --release --example channel_storage_cost`.

## Testing

Every existing test in all three modules must pass **unchanged**. That is the
proof the conversion is faithful: this changes how the state is reached, not
what the code does.

New, per channel: a value crossing cores, a receiver woken while its executor
is parked, and the close/drop races answered as they are locally. Mutation
checks by *leaking* the wakers rather than dropping them — dropping a glommio
`Waker` from a foreign thread perturbs the owning executor into noticing, which
makes a broken implementation look correct.

## Risks

**Generic parameters leak into rustdoc** and into some error messages, even
behind type aliases. Mildly uglier than today, and the reason the aliases exist.

**Monomorphisation compiles each channel twice**, so this is one implementation
in source, not in codegen. That is the point: one place to be correct.
