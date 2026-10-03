# Writing a service

## 1. Own mutable state in one long-lived service task

Prefer:

```text
StorageService
    owns BTreeMap / WAL / metadata state
```

over spreading a multi-step invariant across shared locks.

This does **not** mean every expensive operation belongs inside the mailbox
loop. CPU/I/O work that can be performed from an immutable snapshot or explicit
request state should be delegated to a Trio task and returned as a typed result.

## 2. Define a domain command enum

The framework intentionally does not erase your protocol behind boxed closures:

```rust
enum StorageCommand {
    Get { key: String, reply: Reply<Option<String>> },
    Put { key: String, value: String, reply: Reply<()> },
    Scan { start: String, limit: usize, reply: Reply<Vec<Pair>> },
}
```

Reviewers can see the complete mutation surface.

## 3. Use a bounded framework mailbox

```rust
let (tx, rx) = ctx.mailbox::<StorageCommand>("commands", 256);
```

The mailbox is automatically visible in runtime-tree snapshots.

## 4. Publish a typed handle with the generation token

```rust
#[derive(Clone)]
struct StorageHandle {
    tx: LocalSender<StorageCommand>,
    generation: ServiceGeneration,
}
```

Each method starts with:

```rust
let scope = self.generation.operation_scope(caller)?;
```

`operation_scope` fails fast when the handle's generation already exited, so
a stale handle cloned before the registry entry was removed never starts new
work. Otherwise it returns `CancelScope::any([caller, generation])`: the
operation ends when either its caller gives up or the service generation stops.
Send the command and await the reply under that scope:

```rust
self.tx.send_in(&scope, StorageCommand::Get { key, reply }).await?;
cancel_on(&scope, value).await?
```

## 4a. Keep ingress resources outside the generation

A service generation borrows resources whose teardown has kernel-level
effects; it does not own them. A network service should bind each shard's `TcpListener` once and
lends it to every `tcp-api` generation. With io_uring an accept is a kernel
operation: closing the listener while one is in flight can complete it into a
socket no generation serves, and the client waits until its own timeout. A
listener that outlives restarts simply delivers that connection to the next
generation. Connections themselves stay generation-owned transient work.

## 5. Publish readiness only after invariants hold

```text
open resources
recover durable state
create mailbox
register handle
READY
serve commands
```

`started.started(())` is the boundary between initialization and steady state.

## 6. Spawn transient service work through `ctx.tasks()`

Do not call raw Glommio spawn from service code:

```rust
ctx.tasks().spawn_into(TaskClass::Replication, |scope| async move {
    replicate(scope).await
})?;
```

The task cannot outlive the service generation.

## 7. Give external operations all of their owners

For an RPC owned by both a request and a peer service:

```rust
let scope = CancelScope::any([
    request_scope.clone(),
    ctx.generation().scope(),
]);
```

Normally a service helper should package this so callers see a domain verb, not
cancellation wiring.

## 8. Keep critical durable transitions explicit

Do not treat force-abort as transactional rollback. WAL append/fsync/publish
protocols still need explicit crash consistency and appropriately shielded,
bounded commit regions.
