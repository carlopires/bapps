# Design: `spawn_blocking_send`

**Date:** 2026-08-20
**Status:** approved in chat
**Scope:** `glommio/src/executor/mod.rs`, one new method plus its future

## Why

`spawn_blocking`'s closure and result both cross threads — hence the `Send`
bounds on the signature — but the future it returns does not: it awaits a
reactor `Source`, which is `Rc<RefCell<..>>` state owned by the executor that
created it.

That asymmetry has a measured cost. A trait demanding
`Pin<Box<dyn Future + Send>>` — the shape of pluggable DNS resolvers,
connectors and HTTP services — cannot be satisfied by a per-core client, so
`deboa-glommio` runs a **second thread pool** inside a thread-per-core process
purely to obtain a `Send` handle around `getaddrinfo`. It is the only item on
this fork's list with a demonstrated downstream cost rather than a stylistic
one.

## Surface

```rust
impl ExecutorProxy {
    pub fn spawn_blocking_send<F, R>(&self, func: F) -> impl Future<Output = R> + Send
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}
```

Called on the shard — it needs `LOCAL_EX` to reach the pool — but the future it
returns is what crosses the boundary:

```rust
fn resolve(&self, host: String) -> Pin<Box<dyn Future<Output = Addrs> + Send>> {
    Box::pin(glommio::executor().spawn_blocking_send(move || getaddrinfo(&host)))
}
```

## Semantics

**Panics are caught and re-raised at the await point.** The wrapper uses
`catch_unwind`; the pool thread survives and `resume_unwind` at poll time gives
the caller what they would have seen running the closure inline.

This is deliberately better than the existing `spawn_blocking`, where a
panicking closure unwinds the pool thread: the response is never sent, the
awaiting future hangs forever, and the pool loses a worker permanently. That is
a real bug and it is **not** fixed here — changing it silently under a feature
commit would hide it. It wants its own change.

**Dropping the future does not cancel the work.** A blocking call cannot be
interrupted. The closure runs to completion on the pool thread and its result
is dropped. Documented rather than implied.

**The future does not depend on its origin executor.** Nothing in its
completion path touches that reactor, so it resolves even if the executor that
created it is gone. This falls out of the design rather than being engineered,
and it is the sharpest available test that the future is genuinely `Send`
rather than `Send`-shaped.

## Internals

The completion path runs beside the existing one rather than replacing it.

```rust
struct Shared<R> {
    result: Option<std::thread::Result<R>>,
    waker: Option<Waker>,
}
```

held in an `Arc<Mutex<Shared<R>>>`. The user's closure is wrapped so that, on
the pool thread, it:

1. runs the closure inside `catch_unwind`,
2. locks, stores the outcome, takes any waker, unlocks,
3. wakes — never holding the lock across the wake.

Polling locks, takes the result if present (`resume_unwind` on a captured
panic), and otherwise stores the waker.

**The existing plumbing is left alone.** The `Source`, the pool's id map and
the eventfd notify to the origin executor all still happen; a small detached
local task drives the enqueue future and the `Source` await, and the returned
future ignores both. This costs one detached task and one redundant notify per
call, which is nothing against a call that just blocked for milliseconds.

**Why not a `Source`-free enqueue path:** `BlockingThreadPool::flush` does
`waiters.remove(&id).unwrap()`, so every request must have a `Source`
registered or the reactor panics. Making one optional means changing the
request bookkeeping and `flush` itself, which is reactor hot path, to save a
notify on a call that already blocked. Wrong trade.

**Why not a dedicated thread per call:** that is the second thread pool this
exists to eliminate.

## Testing

- a value crosses back from the pool
- the future satisfies a `Send` bound — asserted by moving it into
  `Pin<Box<dyn Future + Send>>`, which is the shape that motivated the feature
- **the future resolves after its origin executor is dropped** — the property
  that distinguishes this from `spawn_blocking`
- a panicking closure unwinds the awaiting side, and the pool still works
  afterwards, proving the worker survived
- dropping the future does not prevent the closure from running
- the work actually happens on another thread, not inline

Each mutation-checked, as with the rest of this week's work: deleting the wake
must hang the waiting test, and deleting `catch_unwind` must change what the
panic test sees.

## Risks

The wrapped closure captures an `Arc<Mutex<..>>` whose lock is taken on a pool
thread. Holding it across the wake would risk the woken poller blocking on the
lock immediately; the order above avoids it, and it is the one place in this
design where sequence matters.
