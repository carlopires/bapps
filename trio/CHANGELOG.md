# Changelog

## 0.8.0 — a stable public API (breaking)

- One public path per item: `cancel`, `foreign`, `lab`, `nursery`,
  `obligation`, `task_class` and `time` are private; use the root re-exports.
  `sync`, `testing` and `to_thread` stay public. Returned types are nameable:
  `CancelledFuture`, `sync::ConditionWait`, `sync::EventWait`,
  `testing::WaitFor`.
- `#[non_exhaustive]` on `CancelReason`, `TaskClass`, the error enums,
  `Cancelled`, `CancelCause` and the lab report types; `LabConfig` is built
  with `LabConfig::new(seed).with_max_steps(n)`.
- `TaskStatus::started` returns `Result<(), NoWaiter<T>>` instead of exposing
  `glommio::GlommioError`.
- `NurseryError` and `StartError` implement `Display` and `Error`.
- Every public item is documented; `missing_docs` is enforced.

See [docs/migration-0.8.md](../docs/migration-0.8.md) for each change and its replacement.

## 0.7.0 — deadlines in scopes, capacity limiter, blocking work

- Deadlines are part of a cancel scope: `fail_after`, `move_on_after` and the
  new absolute `fail_at`, `move_on_at` record their deadline on the scope
  they create. `CancelScope::effective_deadline` is the earliest deadline in
  the scope's lineage (shields stop it; several owners take the earliest);
  `current_effective_deadline()` and `remaining()` expose it to code.
  `CancelScope::set_deadline` records a deadline its caller enforces.
- `sync::CapacityLimiter`: a cancellable, first-come-first-served token pool;
  a waiter that gives up leaves the queue.
- `to_thread::{run_sync, run_sync_with, ThreadCancel}`: blocking work on the
  executor's blocking thread pool, bounded by a limiter (default one token,
  matching the default pool), cancelled cooperatively; no abandoning.
- Tests: `tests/deadlines.rs`, `tests/limiter.rs` (lab), `tests/to_thread.rs`
  (real executor). Each checked by breaking the code it protects.

## 0.6.0 — part of bapps

Renamed from `glommio-trio` to `bapps-trio` (library `bapps_trio`) and moved
into the bapps workspace, on `bapps-core`. No API change.

## 0.5.1

Added `Lab::is_running()`, so layers above can require a lab.

## 0.5.0 — deterministic lab executor

Added `testing::{Lab, LabConfig, LabReport}`: a single-thread executor for
shard-local code that runs without Glommio. A seeded RNG picks among ready
tasks, so a seed fixes the interleaving and different seeds explore others;
time is a `TestClock` that jumps to the next pending sleep when nothing is
ready; `Lab::explore` sweeps seeds and returns failing reports, which replay
exactly (`LabReport::trace`). Oracles: root finished, deadlock (nothing ready
and no pending timer), tasks left behind, obligations pending or leaked.

Nurseries spawn onto the lab when one is running; `TaskQueues::current()` and
`storage_defaults()` return queue-less sets there (`TaskQueues::queue` panics
under the lab). Also added `TestClock::next_deadline`. Glommio I/O and timers
do not run in the lab. Tests: `tests/lab.rs`, including a lost-update race
that only some seeds expose.

## 0.4.0 — obligations

Added `Obligation`: a value that must be `commit`ted or `abort`ed. Dropping
it unresolved is recorded as a leak, with its label, in an executor-local
ledger (`obligation_stats()`: pending, committed, aborted, leaked, the last 16
leaked labels). Leaks never panic, so drops during a forced stop or a panic
stay safe. Types with a defined fallback resolve their obligation in their
own `Drop`. Tests: `tests/obligation.rs`.

## 0.3.0 — cancellation attribution (G4.1)

Builds on the carlopires Glommio fork `v0.12.0-ng-cp.2`, which adds a timer
fix: the reactor no longer scans every live timer to find the next deadline
(77% of a busy executor's CPU in a key-value service benchmark;
single-executor throughput ~30.6k -> ~56.4k requests/s).

Added `CancelCause` (reason, optional origin, executor-local sequence,
inherited flag), `CancelScope::cancel_by(reason, origin)` and
`CancelScope::cause()`. The first cancellation of a scope is recorded once and
never overwritten; inherited causes resolve to the **earliest** cancelled
ancestor. Nursery failure, nursery drop and deadlines now record origins.
Tests: `tests/attribution.rs`.

Changed: for a multi-parent scope, `reason()` now reports the earliest
cancelled parent instead of the first cancelled parent in list order.

## 0.2.3

Glommio now comes from the carlopires fork, tag `v0.12.0-ng-cp.1`
(glommio-ng 0.12.0 plus one fix): an accept or open that completed without any
caller collecting it leaked the new descriptor. Dropping a listener while an
abandoned `accept()` was in flight left a racing client connected to a socket
nobody served or closed.

## 0.2.2 — wait-registration and stop-accounting fixes

Fixed:

- **Waker retention.** Every wait (`CancelScope::cancelled`, `Event::wait`,
  `Condition::wait_for_change`, `Sequencer::wait_for`, `TestClock` sleeps)
  now owns a tokenized registration: re-polling refreshes it and dropping the
  wait removes it, from the scope *and every ancestor*. Before, each dropped
  wait left its waker on long-lived scopes until they were cancelled, so a
  service generation grew by one waker (and one retained Glommio task
  allocation) per request. Regression: `tests/retention.rs`.
- **Abandoned stop hung the nursery.** `OwnedTask::abort` removed the task
  handle and then awaited the cancellation; if that future was dropped first,
  the task was destroyed but never counted as finished and `with_nursery`
  waited forever. Completion accounting now lives in the spawned future
  itself and runs exactly once when that future returns or is destroyed.
  Regression: `tests/owned_stop.rs`.

Added: `Display` and `std::error::Error` for `Cancelled`, `CancelReason`
(Display), `SpawnError` and `FailAfterError`, so they compose with `?`.

No API removals. Wake order of registered waiters is now registration order.

## 0.2.1 — multicore companion

Retain the user 0.2 API; reap completed task handles on subsequent submissions. Add retention regression, multicore boundary documentation and refreshed validation gates.

## 0.2.0

Built from the validated 0.1 implementation.

### Added

- multi-owner cancellation with `CancelScope::any` and `combined`;
- explicit cancellation adapters for foreign futures: `cancel_on`,
  `cancel_on_current`, and `cancel_on_any`;
- nursery-owned `OwnedTask` handles for framework-level lifecycle policy;
- bounded cooperative shutdown with `OwnedTask::cancel_and_wait`;
- `start_owned` / `start_owned_into` for readiness plus owned shutdown;
- `Nursery::active_tasks` / `NurseryHandle::active_tasks`;
- focused multi-owner and owned-shutdown examples/tests.

### Changed

- nursery task handles now have stable internal IDs and remain retained until
  completion/reap/structured abort;
- force-abort uses Glommio `Task::cancel().await`, so the task future is
  destroyed before shutdown policy reports completion;
- documentation now treats cancellation of foreign futures as an explicit
  runtime boundary.

### Unchanged by design

- no detached-task API;
- no cross-shard cancellation;
- no OTP supervision or restart policy;
- scheduling class remains separate from task lifetime.
