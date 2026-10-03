# Migrating from 0.1 to 0.2

0.2 is additive for ordinary application code. Existing `Nursery`,
`CancelScope`, deadlines, readiness, test clocks, and task-class APIs remain.

## New: multiple cancellation owners

Before, applications manually raced a caller scope and a service scope. Prefer:

```rust
let scope = CancelScope::any([caller, service]);
```

or:

```rust
let scope = caller.combined(&service);
```

## New: foreign future adapters

Instead of repeating hand-written `poll_fn`/`select` logic around Glommio I/O:

```rust
let response = cancel_on(&scope, rpc()).await?;
```

Use `cancel_on_current` when the active task-local scope is the intended owner.

## New: `OwnedTask`

Do **not** replace normal `spawn` calls with `spawn_owned`. It is intended for
frameworks such as `bapps-otp` that implement shutdown policy.

New APIs:

- `spawn_owned`
- `spawn_owned_into`
- `start_owned`
- `start_owned_into`
- `OwnedTask::cancel_and_wait`
- `OwnedTask::abort` (async; waits until the task future is destroyed)
- `OwnedTask::reap`

Dropping `OwnedTask` does not affect the task; the nursery remains the owner.

## Internal change

The nursery retains task handles by stable ID rather than a simple vector. This
allows one owned child to be reaped/force-cancelled without detaching any other
child.
