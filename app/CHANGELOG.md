# Changelog

## 0.8.0 — a stable public API (breaking)

- `AppError` is an enum (`Config`, `System`, `StartupTimeout`,
  `ShardFailed { shard, reason }`, `NodeStopped`, `Cancelled`) instead of a
  `String`; the CPU helpers, `RpcLimits::validate`, `ReadyGate::wait`, `serve`
  and `run_application` return it.
- Shard factories may return any `Result<(), E: Display>` (trait
  `ShardResult`); existing `Result<(), String>` factories compile unchanged.
- `#[non_exhaustive]` on `CallError`, `AppError`, metrics and snapshots;
  `RpcLimits` and `CallOptions` are built with `with_*` methods.
- `CallError`'s `Display` is a sentence instead of its `Debug` form.
- Every public item is documented; `missing_docs` is enforced.

See [docs/migration-0.8.md](../docs/migration-0.8.md) for each change and its replacement.

## 0.7.0 — calls carry the caller's deadline

`ShardClient::call` caps its timeout at the caller's remaining time (the
scope's effective deadline), and the handler's scope records the request's
deadline, so `bapps_trio::remaining()` works in handlers and nested calls are
capped too. A deadline cancellation of a handler is attributed as
`CancelReason::Deadline`. Test: `tests/deadline_propagation.rs` (lab, 10
seeds). See the RPC contract's "Deadlines" section.

## 0.6.0 — part of bapps

Renamed from `glommio-app` to `bapps-app` (library `bapps_app`) and moved
into the bapps workspace. The default node name is `bapps-app`.

## 0.5.0 — whole nodes in the deterministic lab

Added `lab::run_node(shards, limits, factory)`: `AppBuilder::run` without
threads. Every shard's factory runs as a task of the current
`bapps_trio::testing::Lab`, the readiness barrier and fail-stop host run as
another task, and cross-shard RPC deadlines use the lab's virtual clock. Seeds
then explore interleavings across shards. Tests: `tests/lab.rs` (a two-shard
node with cross-shard calls, a cancelled call and node shutdown, clean under
100 seeds, with more than 10 distinct interleavings). Uses Trio/OTP 0.5.1.

## 0.4.1

Dependency-only release: matched with `bapps-trio`/`bapps-otp` 0.5.0
(deterministic lab). No API or behavior change.

## 0.4.0 — the RPC reply is an obligation

Uses `bapps-trio`/`bapps-otp` 0.4.0. Every started request's `Reply`
holds an `Obligation`: a real reply commits it, the `OutcomeUnknown`
fallback on destruction aborts it, so the executor's obligation ledger shows
how many replies were forced. Protocol tests now assert that every started
request ended in exactly one reply decision (no pending, no leaked
obligations); making the fallback forget its decision fails two of them.

## 0.3.0 — deterministic deadlines and the remaining G3 races

Builds on the carlopires Glommio fork `v0.12.0-ng-cp.2`, which adds a timer
fix: the reactor no longer scans every live timer to find the next deadline
(77% of a busy executor's CPU in a key-value service benchmark;
single-executor throughput ~30.6k -> ~56.4k requests/s).

Uses `bapps-trio` and `bapps-otp` 0.3.0 (generation phases, cancellation
attribution).

- RPC time goes through a crate-private `RpcClock` seam; production behaviour
  is unchanged (monotonic `Instant`, Glommio timers). Tests drive it with a
  virtual clock.
- New race tests: deadline vs. same-step reply, two-hop cancellation, saturated
  re-entry (the capacity-deadlock boundary), and executor loss with a call in
  flight (native integration).
- `docs/rpc-contract.md` documents the capacity-deadlock boundary.

## 0.2.0 — request lifecycle made explicit (G3)

The RPC contract now has written request states, race precedence and an
outcome table (`docs/rpc-contract.md`), pinned by deterministic single-executor
race tests (`src/rpc/protocol_tests.rs`, part of `make validate`).

Fixed:

- **A destroyed handler was reported as `TargetStopped`.** When a generation
  force-abort or a panic destroyed a running handler, its caller saw a closed
  reply channel and got `TargetStopped` ("never executed") although the
  handler may have had effects. A started request now owns a consume-once
  `Reply`; dropped unsent, it reports `OutcomeUnknown`.
- **Pre-admission interruption looked like an uncertain outcome.** A caller
  interrupted while waiting for readiness or queue capacity got
  `Cancelled/Deadline { acknowledged: false }`, which callers must treat as
  "may have run". It now gets `NotAdmitted(Interruption)`: never delivered.

Changed (migration):

- New public `CallError::NotAdmitted(Interruption)` and `Interruption`.
  Exhaustive matches on `CallError` need the new arm.
- Queued requests now belong to the inbox, not to a generation. A restarted
  endpoint serves requests its predecessor never started, instead of
  rejecting them with `TargetStopped`; started requests are still never
  replayed. Queued requests get `TargetStopped` when the shard's inbox itself
  is dropped.
- A destination that is stopping when a request reaches it replies
  `TargetStopped` instead of `Cancelled { acknowledged: true }`.
- After interruption, a `TargetStopped` reply or a closed reply channel counts
  as acknowledgement (the request provably will not run).

## 0.1.2

Matched with `bapps-trio`/`bapps-otp` 0.2.3. Glommio now comes from the carlopires fork, tag `v0.12.0-ng-cp.1`
(glommio-ng 0.12.0 plus one fix): an accept or open that completed without any
caller collecting it leaked the new descriptor. Dropping a listener while an
abandoned `accept()` was in flight left a racing client connected to a socket
nobody served or closed.

## 0.1.1

Dependency-only release: matched with `bapps-trio` 0.2.2 and `bapps-otp`
0.2.2. No API or behavior change in this crate.

## 0.1.0 — multicore teaching implementation

Adds pinned executors, validated CPU selection, all-root readiness gate, node shutdown/joins, typed cross-shard request/reply, separate cancellation channels, admission budgets, per-shard RPC counters, an OTP endpoint, counter example and ignored Linux integration tests.

Uses `async-channel` as an explicit reference transport. Keeps OTP/Trio local. Does not implement automatic executor restart, distributed consistency, dynamic rebalancing, full-mesh SPSC tuning or a generic sharded-service trait.
