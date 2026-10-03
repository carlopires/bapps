# Design: `channels::broadcast`

**Date:** 2026-08-20
**Status:** approved in chat
**Scope:** one module, `glommio/src/channels/broadcast.rs`

## Why

Multi-consumer fan-out where every subscriber sees every value, and a slow
subscriber is told it fell behind rather than being allowed to block the
sender. `local_channel` is single-consumer, so there is no substitute in the
crate today; the one consumer we can measure imports `tokio::sync::broadcast`
at 30 call sites for checkpoint decisions and route-ready events.

The argument is typing, not speed. A `!Send` broadcast makes "this fan-out
stays on one core" a compile error rather than a convention.

## Semantics — mirroring tokio deliberately

Verified against `tokio-1.36.0/src/sync/broadcast.rs` rather than from memory.
30 call sites already assume these, so divergence would be a porting trap
rather than an improvement.

- **Capacity is fixed** at construction.
- **Overflow drops the oldest** value to make room.
- **A lagging receiver gets `Lagged(n)`** and its cursor moves to the oldest
  value still retained, so it resumes with as much history as survives.
- **A receiver created by `subscribe()`** sees only values sent after it
  subscribed, never a backlog.
- **`send` with no receivers** hands the value back rather than dropping it.
- **`T: Clone`** — every receiver gets its own value. This is not a limitation
  worth engineering around: `Rc<T>` is itself `Clone`, so `broadcast<Rc<Foo>>`
  gives refcount-cheap fan-out under the same API.

Where a caller wants only the newest value rather than every value, that is
[`watch`](../../../glommio/src/channels/watch.rs), which now exists. Keeping
`broadcast` faithful to "every value, tell me when I miss some" leaves the two
with a clean division.

## API

```rust
pub fn broadcast<T: Clone>(capacity: usize) -> (Sender<T>, Receiver<T>);

impl<T: Clone> Sender<T> {
    pub fn send(&self, value: T) -> Result<usize, GlommioError<T>>;
    pub fn subscribe(&self) -> Receiver<T>;
    pub fn receiver_count(&self) -> usize;
}

impl<T: Clone> Receiver<T> {
    pub async fn recv(&mut self) -> Result<T, RecvError>;
    pub fn try_recv(&mut self) -> Result<T, TryRecvError>;
}

impl<T: Clone> Clone for Receiver<T>;

pub enum RecvError { Closed, Lagged(u64) }
pub enum TryRecvError { Empty, Closed, Lagged(u64) }
```

`Ok` from `send` carries the number of receivers the value went to, as tokio's
does.

## Errors

`send` uses the crate's vocabulary:
`GlommioError::Closed(ResourceType::Channel(value))`, matching `oneshot` and
`watch`.

`recv` uses a **bespoke `RecvError`**, because `Lagged` is neither `Closed` nor
`WouldBlock` and shoehorning it into `ResourceType` would misdescribe what
happened. Adding a variant to `GlommioError` was rejected for two reasons: it
is not `#[non_exhaustive]`, so a new variant breaks exhaustive matches
downstream, and a broadcast-only concept does not belong in a crate-wide error.

This makes broadcast the one channel here whose receiver does not return
`GlommioError`. That cost is accepted; the alternative misreports a recoverable
condition as a terminal one.

## Internals

`Rc<RefCell<Inner<T>>>` shared by both halves. `Inner` holds:

- `values: VecDeque<(u64, T)>` — the ring, never longer than `capacity`
- `next_seq: u64` — monotonic, assigned on send
- `wakers: Vec<Waker>` — receivers currently suspended
- `senders: usize`, `receivers: usize`

A `Receiver` holds `next: u64`, the sequence it wants. Reading:

1. `next` older than the oldest retained sequence → `Lagged(oldest - next)`,
   and `next` jumps to `oldest`.
2. A value at `next` exists → return a clone, `next += 1`.
3. No senders left → `Closed`.
4. Otherwise register a waker and suspend.

**Deliberate divergence:** tokio keeps a per-slot count of receivers that have
yet to read a value, freeing it as soon as the last one does. This design does
not: a value lives until it is overwritten. The retention bound is identical —
at most `capacity` values, which is what the type advertises — and the code is
materially simpler. It costs memory only when payloads are large and consumers
are fast, and is worth revisiting only if that shows up in a real workload.

`Sender` is `Clone`; the channel closes when the last one drops, and receivers
drain what remains before seeing `Closed`.

## Testing

Following the three modules built before it, each with a mutation check that
proves the suite bites:

- `recv` suspends until a value arrives — ordering test, not just a value check
- every receiver sees every value
- a receiver created by `subscribe()` sees only later values
- overflow reports `Lagged(n)` with the correct `n`, and the next `recv`
  returns the oldest retained value
- dropping the last sender wakes a suspended receiver with `Closed`
- a receiver drains retained values before observing `Closed`
- `send` with no receivers hands the value back
- `try_recv` distinguishes `Empty` from `Closed` from `Lagged`

## Risks

New public API on a fork with six unreviewed PRs upstream. Additive and
conflict-poor, but "upstream may never take this" applies.
