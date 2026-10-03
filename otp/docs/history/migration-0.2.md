# Migrating from 0.1 to 0.2

0.2 keeps the 0.1 supervisor/registry concepts but moves several patterns from
applications into the framework.

## Replace application mailboxes

Before:

```text
app-local BoundedSender / BoundedReceiver
```

After:

```rust
let (tx, rx) = ctx.mailbox::<Command>("commands", 256);
```

Standalone code may use `LocalMailbox::<T>::bounded(capacity)`.

## Replace `ServiceLifetime` guards

Do not maintain an application `Drop` guard solely to tell handles the service
has died. Capture:

```rust
let generation = ctx.generation();
```

and embed that token in published handles. The framework marks it stopped on
all service exit paths, including force-abort.

## Replace service-local nurseries

For transient tasks owned by the service generation:

```rust
ctx.tasks().spawn(...)
```

rather than creating a separate nursery merely to make work die with the
service.

Application-level nurseries remain correct for transient operations whose owner
is a request/operation rather than the service itself.

## Add shutdown policy where needed

0.1 shutdown was cooperative forever. 0.2 defaults to a bounded five-second
grace period and then structured force-abort.

Customize with:

```rust
.shutdown_after(Duration::from_secs(30))
```

or explicitly choose `Shutdown::BrutalKill` for a deliberately
non-cooperative worker.

## Runtime-tree changes

`ChildSnapshot` now includes `shutdown`, `active_tasks`, `mailboxes`, and
`recent_exits`. `SupervisorSnapshot` includes `active_children` and
`recent_exits`.
