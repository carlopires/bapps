# Porting from tokio: the differences that fail silently

Organised around **silence**. A compile error is cheap — you fix it and move
on. What costs days is correct tokio code that compiles under glommio and does
something else, with no error anywhere.

Everything here has cost somebody real hours.

## `Task` cancels when dropped; `JoinHandle` does not

The single most expensive difference.

```rust
// tokio: runs to completion. The handle is optional.
tokio::spawn(async { work().await });

// glommio: cancelled immediately. The work never happens.
glommio::spawn_local(async { work().await });

// glommio, intended:
glommio::spawn_local(async { work().await }).detach();
```

No panic, no error, no log. In one real case a table scan spawned its producer
tasks, dropped the handles, and returned **zero results for a table full of
data** — no failure of any kind, just nothing.

`Task` carries `#[must_use]` naming `.detach()`, which catches the direct case.

### It does not survive a newtype wrapper

**This is the case that actually bites, and the lint cannot see it.**

```rust
// A runtime-agnostic library wraps the handle in its own type:
pub struct JoinHandle<T>(glommio::Task<T>);

fn spawn<F>(f: F) -> JoinHandle<F::Output> { /* ... */ }

// The caller discards it. `must_use` is silent: it is on glommio::Task,
// not on your newtype.
spawn(async move { producer().await });
```

Every runtime-agnostic library has that wrapper — it is *what runtime-agnostic
means* — so the people most likely to hit this are structurally the people the
lint cannot reach.

**If you wrap `Task` in your own handle type, do one of these:**

- carry the attribute across:
  `#[must_use = "dropping this cancels the task; call .detach()"]`
- or `.detach()` inside the wrapper, and give your handle
  tokio's drop semantics deliberately.

## `shared_channel` needs `connect()` on both ends, inside an executor

Necessary and correct, but it is a runtime requirement the type does not state,
so the failure lands at runtime, far from the mistake. A sender or receiver
that is never connected simply never carries anything.

If one end has no executor — a thread you spawned yourself — use
[`ConnectedSender::into_foreign`], which yields a `Send` handle you can move
anywhere. It is `try_send`-only and deliberately not `Clone`: the buffer
underneath is single-producer, and cloning it across threads corrupts the heap.

## Wakers already know which core to wake

A glommio task's `Waker` carries its owning executor's id, and waking it from
another thread resolves that executor's sleep notifier and writes its eventfd
(`task/raw.rs:272`). So an ordinary `Waker` stored in an `Arc<Mutex<..>>` and
woken from any thread will wake an executor parked in `io_uring_enter`. You do
not pair a channel with a notifier by hand.

The registration happens at first poll, on whichever executor does the polling
— the ordinary futures contract — so a `Send` future built on one core and
polled on another needs no placement step for its wakeups to arrive.

**`ConnectedSender::into_foreign` is not a counter-example**, and reading it as
one leads to inventing handshakes that nothing needs. What it moves is
*placement of reactor-owned resources* — the ring registration, buffer
ownership, the free-space accounting a local reactor was doing — not
notification. Notification was never the part that needed help.

## `!Send` channels are the default, and each one has a `Send` twin

Every tokio channel is `Send`, so a porting reader reaches for one and finds
`broadcast::Receiver` will not cross a thread. That is deliberate -- the
default keeps a per-core channel per-core, and moving one is a compile error
rather than a convention -- but it is not the whole story: each of
`broadcast`, `watch` and `oneshot` has a `shared()` constructor giving the same
channel over storage that crosses cores.

| per-core | crosses cores |
|---|---|
| `broadcast::broadcast(cap)` | `broadcast::shared(cap)` |
| `watch::watch(initial)` | `watch::shared(initial)` |
| `oneshot::oneshot()` | `oneshot::shared()` |

They are the same implementation, instantiated over `Arc<Mutex<_>>` instead of
`Rc<RefCell<_>>`, so semantics -- `Lagged(n)`, close rules, skip-to-latest --
are identical, and `Send`-ness follows from the storage rather than an
`unsafe impl`.

Two differences worth knowing. `watch`'s `borrow()`, which hands out a
`Ref<'_, T>`, exists on the local channel only; the shared one has
`with_current(|v| ..)` and `get()`, because a borrow outliving the call needs
the value to stay put. And every receiver on a shared channel reads through one
mutex, which suits control-plane fan-out -- a shutdown, a config change, a
route becoming ready -- rather than a per-message data path. For that, give
each core its own channel and fan out with `channels::channel_mesh`.

## Cancellation does not cross cores

`sync::CancellationToken` is `Rc`-based and `!Send` on purpose: it cancels task
trees within one executor. A control plane on one core cannot hold a token that
cancels work on another.

`foreign_child()` is the first-class shape: it hands out a `Send + Sync`
`ForeignCancellation` that another core can `attach()` to a local token, and
which is cancelled both when the parent cancels and when the parent is dropped.
Otherwise the pattern is the same as any cross-core signal: send one (a
`oneshot::shared`, a `watch::shared`) and cancel a *local* token when it
fires.

## `timeout` exists twice

- [`future::timeout`] takes any future. Use this one.
- [`timer::timeout`] takes only futures returning glommio's `Result`, and is
  deprecated.

Both poll the inner future before the timer, so a future that completes exactly
at the deadline reports success rather than racing.

## `use glommio::*` shadows `#[test]`

Re-exporting a macro named `test` at the crate root means a glob import
shadows the built-in attribute. Import the macros by name, or write
`#[glommio::test]`. tokio has the same property.

## Things that are the same, and are worth not worrying about

- `tokio::select!` contains no runtime. It expands to a `poll_fn` over its
  branches and works fine over glommio futures. Keeping it is not a porting
  failure — though `glommio::select!` now exists if you want the dependency
  line gone. It takes plain futures, like tokio's and unlike
  `futures::select!`, which demands `FusedFuture + Unpin`. It has no `if`
  guards, no `else`, and requires irrefutable patterns; and its default
  polling order rotates rather than being random, so a test asserting a fixed
  winner wants `biased;`.
- `tokio::sync` primitives need no reactor either. They work; they are just
  `Send` when your program is per-core, so the type system stops helping you.
  Swapping them for glommio's buys typing, not speed.
- `std::time::Instant` is glommio's clock. There is no paused-time test mode
  and no bespoke `Instant`.

## For library authors: the seam argument

The crates that genuinely block a runtime swap are rarely runtimes. They are
libraries with one hardcoded call — a `tokio::time::sleep` in a retry path, a
sleeper trait with one implementation, a spawn buried in a constructor.

The productive request to those maintainers is never "port to glommio". It is
**a seam, not a rewrite**: an associated type, a trait with a default
implementation, an injectable sleeper. `object_store` already accepts a custom
`HttpConnector` and needs the equivalent for its sleeper. A one-associated-type
DNS change was accepted into `deboa` on exactly that framing.

A seam accepted upstream unblocks *every* runtime, not just this one, which is
why it is an easier argument to win than it looks.

[`ConnectedSender::into_foreign`]: https://docs.rs/glommio-ng/latest/glommio_ng/channels/shared_channel/struct.ConnectedSender.html#method.into_foreign
[`future::timeout`]: https://docs.rs/glommio-ng/latest/glommio_ng/future/fn.timeout.html
[`timer::timeout`]: https://docs.rs/glommio-ng/latest/glommio_ng/timer/fn.timeout.html
