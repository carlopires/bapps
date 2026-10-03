# bapps-otp architecture

## Four topologies

The framework deliberately assigns one responsibility to each layer:

```text
bapps-otp      failure topology
                 What happens when a long-lived service dies?

      ↓

bapps-trio     lifetime topology
                 Who owns transient work and when must it end?

      ↓

Glommio          execution topology
                 Which core/queue runs the work?

      ↓

Rust             resource topology
                 Who owns memory/resources?
```

Correctness depends on *not* collapsing these layers.

## Who owns what

The same separation, stated as concrete owners:

| Question | Owner |
|---|---|
| Who owns long-lived state? | The service task: one task owns the state and is its only mutator (no shared mutex). |
| Who handles service failure and restarts? | The supervisor: `Strategy`, `restart_intensity` and `ExitReason`. |
| Who decides whether a handle is still valid and still admits work? | The `ServiceGeneration` token. It is local to this executor and is **not** a distributed fencing token. |
| Who owns transient operation work? | A Trio nursery for request-scoped work; `ChildContext::tasks()` for generation-scoped work. |
| Which CPU executes it? | The shard's own pinned Glommio executor. `TaskClass` chooses a queue inside that executor; it never moves work to another CPU. |
| Who owns executor threads, readiness and node stop? | `bapps_app` (`AppBuilder`, `ReadyGate`, `NodeControl`). A shard-root exit stops the whole node. |

State ownership, restart policy, admission validity and CPU placement are four
different jobs. The table assigns each to a concrete type so a review can check
them separately.

## Service generation

A supervised worker is not just a future. Each start creates a generation
with an observable `GenerationPhase`:

```text
Starting   init / recover / register        accepts work, advertised
   │ TaskStatus::started
   ▼
Ready                                       accepts work, advertised
   │ stop requested: the generation's scope is cancelled
   ▼            (supervisor stop, parent shutdown, failed service task)
Draining   cleanup only                     rejects new work, NOT advertised
   │ Normal / Failure / Panic / Shutdown / Killed
   ▼
Stopped
```

The admission linearization point is the cancellation of the generation's
scope. From that instant `ServiceGeneration::operation_scope` returns
`ServiceUnavailable { phase: Draining, .. }` and `Registry::get` no longer
returns the generation's entries, although the generation is still alive and
cleaning up. A replacement generation cannot register the same key until the
old one has exited, and exit removes only the old owner's entries.

A parent stop that arrives while a child is still `Starting` is forwarded to
that child (children are otherwise shielded so they can be stopped in reverse
order). The supervisor then returns cleanly instead of waiting for a readiness
that may never come. Pre-readiness stop is cooperative: there is no task handle
to force yet.

`ServiceGeneration` is owned by the framework. Stale application handles may
clone it, but cannot keep it alive after the supervisor has ended the
generation.

Generation exit synchronously performs three critical publications:

1. mark generation `Stopped`;
2. remove generation-owned registry entries;
3. publish the `ChildExit` to the supervisor and runtime tree.

A drop guard *inside the framework task boundary* preserves those publications
when structured force-abort destroys the service future. Runtime-tree mailbox
metrics hold only weak references, so observability cannot extend queued-message
lifetimes after the service itself is gone.

## Service-owned transient work

A service may create transient tasks through `ctx.tasks()`:

```text
Service generation
       │
       └── ServiceTasks
            ├── RPC A
            ├── RPC B
            └── repair probe
```

Internally each service task has two owners:

```text
Trio nursery scope ───┐
                      ├── operation scope
service-task lifetime ┘
```

A nursery failure still has Trio semantics; ending the service also cancels the
whole service-task group.

## Mailboxes

`LocalMailbox<T>` is intentionally local to one shard and one address space.
It is not a distributed actor transport.

```text
many handles
    │
    ▼
bounded LocalMailbox<Command>
    │
    ▼
one service state owner
```

Boundedness is a correctness property: overload becomes backpressure instead of
unbounded memory growth.

### Reserve, then send

`LocalSender::send_in` waits for capacity with the value in hand: if the
wait is cancelled, the value is dropped with it. That is wrong when the
message carries something that must not be lost (a reply channel, a byte
reservation, a claim). `reserve_in(scope)` waits for a slot *before* the
message is built and returns a `SendPermit`; `SendPermit::send(value)` never
waits and fails only if the receiver has closed the mailbox, handing the
value back. Cancellation while reserving loses nothing, because nothing was
built yet; dropping an unused permit releases the slot. Snapshots report
reserved slots alongside depth. `tests/permits.rs` covers cancellation during
reservation, closed receivers, and slot release.

## Supervision

Startup is sequential and readiness-based. Shutdown is reverse order. Restart
strategies remain OTP-shaped:

- `OneForOne`
- `OneForAll`
- `RestForOne`

Every child also has:

- `Restart::{Permanent, Transient, Temporary}`
- `Shutdown::{Graceful(duration), BrutalKill}`
- `TaskClass`

A graceful shutdown is implemented by the Trio-owned task handle; OTP only
selects policy.

## Introspection

The runtime tree is part of the framework contract, not debug-only application
state. A child snapshot should answer:

- which generation is running?
- how often has it restarted?
- why did recent generations exit?
- what task class does it use?
- how many generation-owned tasks are active?
- which command mailboxes exist and how full are they?

This makes overload and lifecycle failures observable in the same vocabulary as
the architecture.


## Multicore companion

This document describes shard-local semantics. See [the multicore boundary](multicore.md) and [0.2.1 migration](history/migration-0.2.1.md) for the new application layer and release-specific additions. Validation: `make validate` and `make integration` at the workspace root.
