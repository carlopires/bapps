# bapps-trio 0.6.0

`bapps-trio` provides Trio-shaped structured concurrency for Glommio's
Linux `io_uring`, thread-per-core execution model.

It is the **transient-concurrency layer** in this architecture:

```text
application / bapps-otp
        ↓
Nursery / CancelScope / TaskStatus        ← bapps-trio
        ↓
TaskClass → Glommio TaskQueue
        ↓
Glommio executor per core
        ↓
io_uring
```

The crate is deliberately shard-local. Cross-core and cross-node cancellation
must remain explicit messages/protocols.

> Educational framework under active development. The semantics are the point;
> this is not yet a production-hardened runtime.

## Capabilities (0.6.0)

Part of [bapps](../README.md) 0.6.0, on `bapps-core`, the runtime in
`core/`.

| Capability | API | Tests |
|---|---|---|
| Nurseries: every task owned, children joined, a failure cancels siblings | `Nursery`, `with_nursery`, `TaskStatus` | `tests/semantics.rs` |
| Hierarchical, multi-owner cancellation | `CancelScope::{child, any, shielded}` | `tests/semantics.rs` |
| Cancellation attribution: why a scope was cancelled, by whom | `CancelCause`, `CancelReason`, `cancel_by` | `tests/attribution.rs` |
| Deadlines on an injectable clock | `fail_after`, `move_on_after`, `TestClock` | `tests/semantics.rs` |
| Bounded forced stop of owned tasks; an abandoned stop cannot strand accounting | `spawn_owned`, `OwnedTask` | `tests/owned_stop.rs` |
| Scheduling classes separate from ownership | `TaskClass`, `TaskQueues` | none directly; exercised by applications |
| Local sync primitives that release waits when dropped | `sync::{Event, Condition}` | `tests/retention.rs` |
| Obligations: explicit commit/abort, leaks counted | `Obligation`, `obligation_stats` | `tests/obligation.rs` |
| Deterministic lab: seeded scheduling, virtual time, deadlock/leak oracles, replayable traces | `testing::{Lab, LabConfig, LabReport}` | `tests/lab.rs` |
| Deterministic interleavings by hand | `testing::Sequencer` | unit tests |
| Foreign futures bound to a scope | `cancel_on`, `cancel_on_any` | `tests/semantics.rs` |

Older release notes below are kept for history.

## Multicore companion release (0.2.1)

Use this matched release with `bapps_app` 0.1.0. This crate remains local to one executor; the new app crate owns CPU placement, node readiness and cross-shard messages. It does not turn local cancel scopes, registries or handles into thread-safe global state.

See [0.2.1 migration](docs/migration-0.2.1.md), [multicore boundary](docs/multicore.md). Earlier 0.2 feature documentation below is retained. Historical validation output is not validation of this modified release.

## Core invariants

1. Every spawned task belongs to a nursery.
2. A child cannot outlive its nursery on the graceful path.
3. Cancellation is cooperative and hierarchical.
4. One operation may have **multiple owners**; cancellation of any owner ends it.
5. A nursery owns every Glommio task handle; there is no detached-task API.
6. A framework may obtain an `OwnedTask` only to apply bounded shutdown policy
   to that same nursery-owned task.
7. Scheduling policy (`TaskClass`) is separate from lifetime ownership.
8. Cross-shard lifetime propagation is explicit communication, never hidden
   shared state.

## What's new in 0.2

### Multi-owner cancellation

A transient operation often belongs to both a caller and a long-lived service:

```rust
let operation = CancelScope::any([
    caller_scope.clone(),
    service_generation_scope.clone(),
]);
```

Now either owner can cancel it. This is the primitive needed for an outbound RPC
that must die when either its request is cancelled **or** its owning peer service
restarts.

`CancelScope::combined(&other)` is the two-owner convenience form.

### Explicit foreign-future boundary

Third-party/Glommio futures do not automatically know about Trio scopes:

```rust
let n = cancel_on(&scope, stream.read(&mut buf)).await?;
```

`cancel_on`, `cancel_on_current`, and `cancel_on_any` make that boundary visible.
Cancellation is checked before polling the foreign future, and wins if both are
ready in the same poll.

### Structured owned abort

Most application code should still use ordinary `nursery.spawn(...)`. Framework
code such as an OTP supervisor can opt into an owned control handle:

```rust
let task = nursery.spawn_owned(|scope| run_service(scope))?;

match task.cancel_and_wait(Duration::from_secs(5)).await {
    StopOutcome::Graceful => {}
    StopOutcome::Forced => {}
}
```

The task never becomes detached. `OwnedTask` controls a task that remains owned
by its nursery:

```text
cancel
  ↓
cooperative grace period
  ↓
still running?
  ├─ no  → graceful
  └─ yes → cancel this retained Glommio Task and await destruction → force cancel
```

`start_owned` / `start_owned_into` combine the same ownership with the existing
`TaskStatus` readiness handshake.

## v0.1 semantics retained

- `Nursery` / `NurseryHandle`
- `CancelScope`, child scopes and shielding
- `TaskStatus<T>` / `Nursery::start`
- cancellation-aware `Event` and `Condition`
- `fail_after`, `move_on_after`, shielded deadlines
- `RealClock` and deterministic `TestClock`
- `Sequencer`
- `TaskClass` → Glommio task queues
- `!Send` / shard-local task model

## A typical service-owned RPC

The higher OTP layer can now express the intended ownership without manual
`select!` plumbing:

```text
caller scope ─────┐
                  ├── CancelScope::any ── outbound RPC
service generation┘
```

The socket operation itself is a foreign Glommio future, so the final boundary
still uses `cancel_on`.

## Platform

Glommio is Linux-only and uses `io_uring`. This crate targets the sibling
`glommio-ng` 0.12 API used by the validated 0.1 project and Rust 1.92+, built
from the carlopires fork (tag `v0.12.0-ng-cp.2`: 0.12.0 plus an accepted/opened
descriptor-leak fix and a timer next-deadline performance fix).

For architecture/semantic tests a Linux VM is adequate. For I/O, task-queue,
CPU-affinity, or latency measurements use native Linux.

## Run

```bash
make validate

cargo run --example basic
cargo run --example readiness
cargo run --example deadline
cargo run --example multi_owner
cargo run --example owned_shutdown
```

## Layer boundary

`bapps-trio` intentionally does **not** implement:

- OTP supervisors/restart budgets
- service registries
- distributed actors
- cross-core nurseries
- cross-node cancellation
- transparent remote task handles

Those would blur the distinction between task lifetime and failure topology.
`bapps-otp` is the layer that consumes `OwnedTask` and multi-owner cancellation
for long-lived services.

## Documents

- [`docs/developer-guide.md`](docs/developer-guide.md) — programming rules and pre-commit checks
- [`CHANGELOG.md`](CHANGELOG.md) — version changes
- [`docs/architecture.md`](docs/architecture.md) — semantic model and invariants
- [`docs/migration-0.2.md`](docs/migration-0.2.md) — changes from 0.1
- Validation: `make validate` and `make integration` at the workspace root
