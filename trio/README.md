# bapps-trio

Trio-shaped structured concurrency for Glommio's thread-per-core `io_uring`
executor: every task has an owner, cancellation is cooperative and
hierarchical, and deadlines travel with the work. It is the
transient-concurrency layer of [bapps](../README.md):

```text
your services / bapps-otp
        ↓
Nursery, CancelScope, deadlines, TaskStatus   ← bapps-trio
        ↓
TaskClass → executor task queues
        ↓
bapps-core (Glommio) executor, one per core, io_uring
```

Everything here is shard-local (`Rc`, `!Send`): cancellation across cores or
machines is an explicit message, never shared state.

**Status:** pre-1.0. The API may still change before 1.0.0; every change is in
the [CHANGELOG](CHANGELOG.md).

## Example

```rust
use std::time::Duration;
use bapps_trio::{Nursery, fail_after, remaining, sleep, with_nursery};

glommio::LocalExecutor::default().run(async {
    // Three children; the nursery waits for all of them. Had one failed,
    // the others would have been cancelled and the error returned here.
    let finished = with_nursery::<String, _, _>(|nursery: &mut Nursery<String>| {
        Box::pin(async move {
            for n in 1..=3_u64 {
                nursery
                    .spawn(move |_scope| async move {
                        sleep(Duration::from_millis(n)).await.map_err(|e| e.to_string())
                    })
                    .expect("the nursery is open");
            }
        })
    })
    .await;
    assert!(finished.is_ok());

    // A deadline is part of the scope: code inside can ask how long it has.
    let left = fail_after(Duration::from_millis(50), |_scope| async { remaining() })
        .await
        .expect("finished in time");
    assert!(left.is_some_and(|left| left <= Duration::from_millis(50)));
});
```

## Invariants

1. Every spawned task belongs to a nursery; there is no detached-task API.
2. A child cannot outlive its nursery on the graceful path.
3. Cancellation is cooperative and hierarchical; a shielded scope is a new
   root.
4. One operation may have several owners (`CancelScope::any`); cancelling any
   owner cancels it.
5. A framework may hold an `OwnedTask` only to apply a bounded stop policy to
   that same nursery-owned task.
6. Scheduling (`TaskClass`) is separate from ownership.
7. Waits release their registrations when dropped, so long-lived scopes do not
   accumulate wakers.

## Capabilities

| Capability | API | Tests |
|---|---|---|
| Nurseries: every task owned, children joined, a failure cancels siblings | `Nursery`, `with_nursery`, `TaskStatus` | `tests/semantics.rs` |
| Hierarchical, multi-owner cancellation | `CancelScope::{child, any, shielded}` | `tests/semantics.rs` |
| Cancellation attribution: why a scope was cancelled, by whom | `CancelCause`, `CancelReason`, `cancel_by` | `tests/attribution.rs` |
| Deadlines on an injectable clock | `fail_after`, `move_on_after`, `TestClock` | `tests/semantics.rs` |
| Deadlines are part of the scope: relative and absolute helpers; the effective deadline across parents, shields and several owners; time remaining | `fail_at`, `move_on_at`, `CancelScope::{deadline, effective_deadline, set_deadline}`, `current_effective_deadline`, `remaining` | `tests/deadlines.rs` |
| A cancellable, first-come-first-served token pool | `sync::CapacityLimiter` | `tests/limiter.rs` |
| Blocking work on helper threads, bounded by a limiter, cancelled cooperatively | `to_thread::{run_sync, run_sync_with, ThreadCancel}` | `tests/to_thread.rs` |
| Bounded forced stop of owned tasks; an abandoned stop cannot strand accounting | `spawn_owned`, `OwnedTask` | `tests/owned_stop.rs` |
| Scheduling classes separate from ownership | `TaskClass`, `TaskQueues` | none directly; exercised by applications |
| Local sync primitives that release waits when dropped | `sync::{Event, Condition}` | `tests/retention.rs` |
| Obligations: explicit commit/abort, leaks counted | `Obligation`, `obligation_stats` | `tests/obligation.rs` |
| Deterministic lab: seeded scheduling, virtual time, deadlock/leak oracles, replayable traces | `testing::{Lab, LabConfig, LabReport}` | `tests/lab.rs` |
| Deterministic interleavings by hand | `testing::Sequencer` | unit tests |
| Foreign futures bound to a scope | `cancel_on`, `cancel_on_any` | `tests/semantics.rs` |

## Not in this crate

Supervision and restart budgets, service registries and mailboxes are
[bapps-otp](../otp/README.md); pinned executors and cross-shard calls are
[bapps-app](../app/README.md). There are deliberately no cross-core
nurseries, cross-node cancellation or remote task handles.

## Run

Linux only (`io_uring`); the workspace pins Rust 1.92.0.

```sh
make validate                                    # from the workspace root
cargo run -p bapps-trio --example basic          # also: readiness, deadline,
                                                 # multi_owner, owned_shutdown
```

## Documents

- [Architecture](docs/architecture.md): the semantic model, deadlines,
  blocking work, obligations, the deterministic lab.
- [Developer guide](docs/developer-guide.md): programming rules.
- [Multicore boundary](docs/multicore.md): what stays on one shard.
- [CHANGELOG](CHANGELOG.md); older notes and migration guides in
  [docs/history](docs/history).
