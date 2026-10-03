# bapps-otp 0.6.0

`bapps-otp` is an OTP-shaped service and supervision framework for Glommio,
built on the sibling `bapps-trio` crate.

The goal is not to reproduce BEAM. The goal is to encode the production
architecture decisions that make OTP compelling while preserving Rust's
ownership model and Glommio's Seastar-style execution model.

```text
application                  domain semantics
    ↓
bapps-otp                  failure topology / services / mailboxes
    ↓
bapps-trio                 transient lifetime / cancellation / deadlines
    ↓
Glommio                      shard-per-core scheduling + io_uring
    ↓
Rust                         memory/resource ownership
```

The framework is intentionally **shard-local**. Cross-core and cross-node
communication remains explicit.

> Educational framework under active development; not yet a production OTP
> runtime.

## Capabilities (0.6.0)

Part of [bapps](../README.md) 0.6.0, on `bapps-core`, the runtime in
`core/`.

| Capability | API | Tests |
|---|---|---|
| Supervision trees: one-for-one, one-for-all, rest-for-one; restart policies and intensity windows | `SupervisorSpec`, `ChildSpec`, `Strategy`, `Restart` | `tests/supervision.rs`, `tests/lab.rs` |
| Ordered start with readiness; bounded ordered stop | `TaskStatus::started`, `Shutdown` | `tests/lifecycle.rs` |
| Service generations: a phase per incarnation; operations bound to the caller *and* the generation | `ServiceGeneration`, `GenerationPhase`, `operation_scope` | `tests/lifecycle.rs`, `tests/lab.rs` |
| Service-owned transient work, bounded and stopped with the generation | `ServiceTasks`, `ChildContext::tasks` | `tests/v021.rs` |
| Bounded local mailboxes; reserve-then-send permits | `LocalMailbox`, `LocalSender::{send_in, reserve_in}`, `SendPermit` | `tests/permits.rs`, `tests/v02.rs` |
| Typed service registry, per generation and global | `Registry`, `ServiceKey`, `ChildContext::{register, service}` | `tests/registry.rs` |
| Introspection of the live tree | `RuntimeTree`, `Application::tree` | `tests/supervision.rs` |
| Runs under the trio deterministic lab (virtual time, seeded interleavings) | `bapps_trio::testing::Lab` | `tests/lab.rs` |

Older release notes below are kept for history.

## Multicore companion release (0.2.1)

Use this matched release with `bapps_app` 0.1.0. This crate remains local to one executor; the new app crate owns CPU placement, node readiness and cross-shard messages. It does not turn local cancel scopes, registries or handles into thread-safe global state.

See [0.2.1 migration](docs/migration-0.2.1.md), [multicore boundary](docs/multicore.md). Earlier 0.2 feature documentation below is retained. Historical validation output is not validation of this modified release.

## What 0.2 adds

The first application built on it proved several patterns twice (storage and peer services). 0.2 extracts
those patterns into the framework.

### `LocalMailbox<T>`

A bounded, shard-local, cancellation-aware mailbox:

```rust
let (tx, rx) = ctx.mailbox::<Command>("commands", 256);
```

Properties:

- bounded capacity and backpressure;
- cloneable sender, single-owner receiver;
- cancellation-aware `send`/`recv`;
- explicit `send_in`/`recv_in` for a chosen scope;
- clean close semantics;
- live depth/capacity/sender count in the runtime tree.

Application command enums remain application-specific. The framework supplies
the mailbox, not a giant generic `GenServer` trait.

### Service-owned Trio tasks

Every service generation gets a `ServiceTasks` group:

```rust
ctx.tasks().spawn_into(TaskClass::Replication, |scope| async move {
    replicate(scope).await
})?;
```

These tasks are still Trio children, but they also belong to the current
service generation. When the service ends/restarts, its transient work cannot
survive it.

Unexpected `Err`/panic in a service-owned task participates in the service
failure boundary rather than becoming an unrelated detached failure.

### Framework-owned service generations

`ChildContext::generation()` returns a liveness token for the exact supervised
generation:

```rust
#[derive(Clone)]
struct StorageHandle {
    tx: LocalSender<StorageCommand>,
    generation: ServiceGeneration,
}

impl StorageHandle {
    async fn get(&self, key: String) -> Result<Option<String>> {
        self.generation.ensure_alive()?;
        // send command...
        # Ok(None)
    }
}
```

The supervisor marks the token stopped on **normal exit, failure, panic, and
force-abort**. Applications no longer need a load-bearing `ServiceLifetime`
`Drop` guard to publish death.

Registry entries owned by the generation are removed by the same framework
exit path.

### Bounded shutdown policy

`bapps-trio` 0.2 provides `OwnedTask`. OTP consumes it as policy:

```text
cancel service
     ↓
wait child-specific grace period
     ↓
returned?
  ├─ yes → Shutdown
  └─ no  → force-abort exact owned task → Killed
```

Per-child policy:

```rust
ChildSpec::worker("storage", run_storage)
    .shutdown_after(Duration::from_secs(10));
```

or, rarely:

```rust
.shutdown(Shutdown::BrutalKill)
```

This closes the major v0.1 shutdown gap without exposing raw Glommio task
handles to application code.

### Richer runtime tree

Child snapshots now include:

- status / generation / restart count / last exit;
- restart and shutdown policy;
- `TaskClass`;
- active service-owned task count;
- live mailbox depth/capacity/closed state/sender count;
- bounded recent exit history.

Supervisor snapshots include active child count and recent exit history.

This is intended to power application admin surfaces without each application
inventing lifecycle instrumentation.

## Supervision retained from 0.1

- sequential startup with `TaskStatus` readiness;
- reverse-order shutdown;
- `OneForOne`, `OneForAll`, `RestForOne`;
- `Permanent`, `Transient`, `Temporary`;
- supervisor restart intensity;
- ordinary Rust panic → `ExitReason::Panic`;
- nested supervisors;
- typed shard-local registry;
- automatic generation-owned registry cleanup;
- explicit runtime supervision tree;
- task classes preserved per child.

## The long-lived/transient boundary

Do not supervise every request.

```text
LONG-LIVED                       TRANSIENT

StorageService                  TCP connection
RouterService                   outbound RPC
PeerService                     distributed scan
TcpApiService                   replica read
MaintenanceService              repair fan-out

     ↓                               ↓
bapps-otp                    bapps-trio
Supervisor                     Nursery
Restart policy                 CancelScope
LocalMailbox                   deadlines / race
ServiceGeneration              multi-owner cancellation
```

A request failure normally must not restart a service. A service failure must
not leave its transient operations alive.

## Recommended service shape

```rust
let storage = ChildSpec::worker("storage", |ctx, started| async move {
    let generation = ctx.generation();
    let (tx, rx) = ctx.mailbox::<StorageCommand>("commands", 256);

    ctx.register(
        STORAGE,
        StorageHandle { tx, generation },
    )?;

    let mut state = recover_storage().await?;
    started.started(())?;

    loop {
        let command = rx.recv().await?;
        state.handle(command)?;
    }
});
```

The domain still defines `StorageCommand` and state-transition rules. The
framework standardizes lifetime, backpressure, readiness, supervision,
observability, and transient task ownership.

## Workspace layout

This crate lives in the bapps workspace as `otp/`, next to `core/`, `trio/`
and `app/`; the workspace builds and validates them together.

## Run

```bash
make validate
cargo run --example restart
cargo run --example registry
cargo run --example service
```

## What remains deliberately explicit

0.2 still does **not** implement:

- distributed actors / transparent remote PIDs;
- cross-shard `CancelScope`;
- cross-node supervision;
- arbitrary OTP links;
- hot code upgrades;
- a universal `GenServer` trait;
- persistence/consensus semantics;
- work stealing or a global executor.

Those would either blur architectural boundaries or are not yet justified by
validated application patterns.

## Documents

- [`docs/developer-guide.md`](docs/developer-guide.md) — programming rules and pre-commit checks
- [`CHANGELOG.md`](CHANGELOG.md) — version changes
- [`docs/architecture.md`](docs/architecture.md) — framework layers/invariants
- [`docs/service-pattern.md`](docs/service-pattern.md) — how to write a service
- [`docs/migration-0.2.md`](docs/migration-0.2.md) — changes from 0.1
- Validation: `make validate` and `make integration` at the workspace root
