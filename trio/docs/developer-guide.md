# bapps-trio 0.2 developer guide

This document is the short operational guide for application and framework
code. `README.md` explains the model; this file states the rules to follow
while writing code.

## 1. Every transient task has a nursery

Application code should not call raw Glommio spawn APIs. Prefer:

```rust
with_nursery::<Error, _, _>(|nursery| Box::pin(async move {
    nursery.spawn(|scope| do_work(scope))?;
    Ok(())
})).await?;
```

Use `OwnedTask` only in framework/lifecycle code that must stop one exact task
with a bounded grace period. Dropping `OwnedTask` itself does not detach or
cancel anything; ownership stays with the nursery.

## 2. Cancellation is cooperative until policy explicitly aborts

Normal code observes a `CancelScope`. Foreign Glommio or third-party futures
must be wrapped at the boundary:

```rust
let n = cancel_on(&scope, stream.read(&mut buf)).await?;
```

Do not assume dropping a random future is equivalent to structured shutdown.

## 3. One operation can have several owners

For work owned by both a request and a service generation:

```rust
let scope = CancelScope::any([
    request_scope.clone(),
    service_scope.clone(),
]);
```

Cancellation of either owner ends the operation. Keep this shard-local.
Cross-core cancellation is an explicit message.

## 4. Readiness is not spawning

Use `Nursery::start` / `start_owned` when a parent cannot proceed until a child
is actually usable. `TaskStatus::started(value)` is the boundary between
initialization and steady state.

## 5. Scheduling policy is independent of ownership

Use `spawn_into(TaskClass::..., ...)` when the task has a meaningful resource
class. A foreground read and background repair can have different scheduler
policy while obeying identical nursery/cancellation rules.

## 6. Shield only bounded cleanup/durability transitions

A shielded scope intentionally stops inheriting outer cancellation. Pair it
with a fresh deadline. Do not shield ordinary request work.

## 7. Test concurrency with signals, not sleeps

Wait for explicit states (readiness, queue positions, events, channels), never
for time; bound every wait; prefer the deterministic lab. The full rules, each
learned from a flaky test this stack had, are in the workspace
[testing guide](../../docs/testing.md).

## 8. Pre-commit check

- no raw detached Glommio task in application code;
- every long-running operation observes cancellation;
- every foreign blocking/IO future has an explicit cancellation boundary;
- multi-owner work names all of its owners;
- readiness uses `TaskStatus`, not polling;
- task class describes resource intent, not lifecycle;
- force-abort exists only behind a bounded lifecycle policy;
- cross-shard lifetime propagation is explicit communication;
- tests synchronize on explicit states and every wait in a test is bounded.


## Multicore companion

This document describes shard-local semantics. See [the multicore boundary](multicore.md) and [0.2.1 migration](history/migration-0.2.1.md) for the new application layer and release-specific additions. Validation: `make validate` and `make integration` at the workspace root.
