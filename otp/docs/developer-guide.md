# bapps-otp 0.2 developer guide

`bapps-otp` is for **long-lived failure topology**. Use `bapps-trio` for
transient request/task topology.

## 1. A service owns state

A service should normally own its mutable invariants in one task and expose a
typed handle backed by a bounded mailbox:

```rust
enum Command {
    Get { key: String, reply: Reply<Option<Value>> },
    Put { key: String, value: Value, reply: Reply<()> },
}

let (tx, rx) = ctx.mailbox::<Command>("commands", 256);
```

The command enum belongs to the application. The mailbox/lifecycle does not.

## 2. Publish a generation-aware handle

Handles that may be cached across call sites should retain the current
`ServiceGeneration` and start each operation with
`generation.operation_scope(caller)?`, which checks liveness and ties the
operation to both the caller and the generation.
After a restart, old handles fail fast instead of silently targeting a dead
service generation.

## 3. Service-owned background work uses `ctx.tasks()`

```rust
ctx.tasks().spawn_into(TaskClass::Replication, |scope| async move {
    replication_loop(scope).await
})?;
```

This work remains a Trio child and cannot outlive the supervised generation.
Unexpected failure of such a task is a service failure, not a detached error.
If an individual job error is expected, handle it inside the job and return
`Ok(())` from the service task.

## 4. Requests are not OTP children

TCP connections, individual RPCs, scans, replica reads, and repair fan-out are
transient operations. Structure them with Trio nurseries/scopes. OTP supervises
the long-lived service that owns or initiates them.

## 5. Choose restart topology deliberately

- `OneForOne`: siblings are independent;
- `OneForAll`: children form one coherent unit;
- `RestForOne`: child order encodes a dependency chain.

Child startup is sequential. Readiness is explicit. Shutdown is reverse order.

## 6. Shutdown is policy

Default child shutdown is bounded cooperative cancellation. If the child does
not return within the grace period, the exact nursery-owned task is
force-cancelled and recorded as `ExitReason::Killed`.

Use `BrutalKill` only when immediate destruction is an intentional invariant.
A force-abort is **not** transactional rollback; durable state machines still
need WAL/commit/recovery protocols.

## 7. Registry ownership follows generations

Register service handles through `ChildContext::register`. The framework ties
those entries to the generation and removes them on every exit path: normal,
error, panic, shutdown, or force-abort.

## 8. Observability cannot own application resources

`RuntimeTree` exposes lifecycle and mailbox/task metrics. Instrumentation uses
weak mailbox references so a snapshot facility cannot keep queued messages
alive after the owning service has gone away.

## 9. Failure boundary

A supervised service converts:

```text
normal return          → Normal / Shutdown
Err                     → Failure
panic                   → Panic
shutdown grace expires → Killed
service task Err        → Failure
service task panic      → Panic
```

Those are events for the supervisor. They must not escape first as a Trio
nursery error that blindly cancels unrelated OTP siblings.

## 10. Pre-commit check

- long-lived mutable state has one obvious owner;
- command mailbox is bounded;
- cached handles are generation-aware;
- background service work uses `ctx.tasks()`;
- transient request work uses Trio directly;
- supervisor strategy matches dependency semantics;
- startup uses readiness, never "spawn means ready";
- shutdown policy is explicit for nontrivial workers;
- registry entries are generation-owned;
- runtime-tree instrumentation does not extend resource lifetimes;
- cross-shard/cross-node behavior is an explicit protocol.


## Multicore companion

This document describes shard-local semantics. See [the multicore boundary](multicore.md) and [0.2.1 migration](migration-0.2.1.md) for the new application layer and release-specific additions. The current validation gate is in [VALIDATION](../VALIDATION.md).
