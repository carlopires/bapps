# Completing the Synchronisation Set

**Date:** 2026-08-19
**Status:** all six items shipped, 2026-08-20
**Origin:** slipstream runs entirely on glommio but still imports `tokio::sync`
for primitives glommio does not have. Call-site counts below are from its
`src/`.

## The argument

Not performance. `tokio::sync` needs no reactor — it parks on wakers and is
driven by whatever polls it — which is exactly why slipstream's port worked.
The atomics in those channels sit next to syscalls and network round trips and
do not show up.

The argument is typing. A `!Send` primitive makes "this stays on one core" a
compile error instead of a convention. Anything that does not buy that is not
worth building here.

## Already covered — do not build

- **`ReceiverStream`.** Both channel types already implement `Stream`:
  `ConnectedReceiver` directly (`shared_channel.rs:427`), `local_channel` via
  `.stream()` (`local_channel.rs:731`).
- **`RwLock`** (5 sites), **`Semaphore`**, **`Gate`** — glommio has these.
- **`Barrier`, `Notify`, `Semaphore`** are re-exported by slipstream's facade
  but unused; they should be trimmed there rather than implemented here.

## Shipped

| Item | Commit | Notes |
|---|---|---|
| Async `Mutex` | `133a565` | `Semaphore(1)` + `UnsafeCell`; `Semaphore` gained `is_closed()` |
| `oneshot` | `c362fb0` | receiver is the future; unsent value handed back |
| `watch`, `OnceCell` | `b67355f` | `OnceCell` initialiser runs once under a private semaphore |
| `broadcast` | `f86bb8a` | tokio semantics, bespoke `RecvError`; spec at `docs/superpowers/specs/2026-08-20-broadcast-channel-design.md` |
| `CancellationToken` | `5bf58cd` | `!Send`, hierarchical, per-executor |
| `ForeignSender` | `986213e` | `ConnectedSender::into_foreign`, `Send` + not `Clone`, `try_send` only |

Every suite was mutation-checked. One of those checks earned its keep: three
`ForeignSender` tests passed with the peer notification deleted, proving
delivery but not wakeup, which is the entire mechanism. The fourth test parks
the consumer first and hangs without it.

## Order of work — simplest first

### 1. Async `Mutex` (28 sites)

`Semaphore` already carries the waiter queue. A `Mutex` is that with
`units = 1`, plus a guard giving `Deref`/`DerefMut` over an `UnsafeCell<T>`.
Follow `rwlock.rs` for the guard shape and the poisoning stance.

Open question: whether `lock()` returns `Result` like `RwLock` does here, or
infallible like tokio's. Match `RwLock` for consistency within the crate.

### 2. `oneshot` (14 sites)

Request/response pairing in front of per-core workers. A bounded
`local_channel` of 1 works and says the wrong thing.

Open questions: sender dropped without sending — `RecvError` or `None`;
whether the receiver is cancel-safe on drop.

### 3. `watch` (1 site) and `OnceCell` (4 sites)

Small, and worth doing once the two above establish the module conventions.
`watch` is a `RefCell<T>` plus a waker list and a version counter.

### 4. `broadcast` (30 sites) — the real project, needs a design pass

Multi-consumer fan-out with lagging-receiver semantics: every subscriber sees
every value, and a slow one gets `Lagged(n)` rather than blocking the sender.
No substitute exists — `local_channel` is single-consumer. This is the item
slipstream most wants.

Single-threaded makes the mechanics easier than tokio's — `Rc`/`RefCell`, no
atomics, no `Arc` traffic — but the semantics are the difficulty:

- ring size, and what happens when a receiver lags past the whole buffer
- whether a receiver subscribing later sees backlog or only new values
- what the sender observes when every receiver has gone away
- whether `Lagged` is recoverable, and where the cursor lands after it

Brainstorm before writing.

### 5. `CancellationToken` (78 sites) — last, and for a different reason

The shutdown idiom throughout slipstream, and the one item here that cannot be
`!Send`: cross-shard shutdown fan-out means the root token crosses threads. So
unlike everything above, it buys no typing guarantee.

The reason to build it anyway is vocabulary coherence — a per-core runtime that
offers channels, locks and gates but sends you elsewhere for cancellation has a
hole in its story. What it does **not** buy is shedding the tokio dependency:
slipstream depends on `tokio = { features = ["full"] }` at its workspace root
and uses it directly in `slipstream-kafka`, `slipstream-wasm`,
`cargo-slipstream` and `slipstream-ops`.

Harder than it looks, and `tokio-util`'s version is battle-tested where ours
would start from zero:

- parent/child token trees, and drop-cancels-children
- subscribing to a token that is already cancelled
- cancel-safety of the wait future when the awaiting task is itself dropped
- whether a child outliving its parent is allowed

### 6. `shared_channel` usable from a thread with no executor — investigate first

Both ends currently require `connect()` inside a glommio executor
(`shared_channel.rs:163,328`). Slipstream's two facade sites (object_store's
HTTP connector, the gRPC channel) have a `Send` caller with no executor at all
on one end.

This is a different shape from everything above and the wake path needs
understanding before anything is promised.

## Separate axis, and ranked above all of the above

**`spawn_blocking` returns a `!Send` future** (`executor/mod.rs:2920`). The
closure and result cross threads; the future does not, because it awaits a
reactor source from `LOCAL_EX`. Documented in `e2f2aaf`, not fixed.

It is the only item here with a measured downstream cost: `deboa-glommio`
cannot satisfy a `Pin<Box<dyn Future + Send>>` resolver trait, so it runs a
second thread pool inside a thread-per-core process to wrap `getaddrinfo`.

A channel-backed variant whose future is `Send` looks feasible — a cross-thread
`waker.wake()` is fine — but cancellation on drop and the pool's ownership of
the result need design. Everything else on this page is a stylistic
improvement to code that already works; this one is a capability gap.

## Not built: `ExecutorBound<T>`

A wrapper carrying a `!Send` value through a `Send` bound, `unsafe impl Send`
with an executor-id assertion — a sharper check than `send_wrapper`'s thread
affinity, since the executor is the invariant that matters here.

**Proposed and declined, on the consumer's argument rather than ours.** The
candidate site needs three wrappers, not one: `object_store::HttpService`
requires the service, the future from `call`, and the *body* to be `Send`, and
the body streams frames off a glommio socket and outlives the call under
retry machinery. Three `unsafe impl Send` on the S3 write path, to remove a
queue hop nobody has measured, against an S3 round trip of hundreds of
microseconds.

That is the task-arena mistake in new clothes: trading a compile-time guarantee
for a runtime panic to win what is probably noise.

Build it only when someone has a measured hop. The honest first site would be
the body path in slipstream's `connector.rs` alone — the piece that exists
purely to turn a `!Send` hyper body into a `Send` one — not an end-to-end
facade spike. A measurement was offered: a fourth condition on the
`shuffle_baseline` p50/p99 run.

## Cost to keep in mind

Every item is new public API on a fork whose upstream has six unreviewed PRs
from us. `sync/` additions are additive and conflict-poor, so the rebase cost
is small — but "upstream may never take this" applies to all of it.
