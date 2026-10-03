# bapps-otp

OTP-shaped supervision and service lifecycle for one shard of a
thread-per-core [bapps](../README.md) application, built on
[bapps-trio](../trio/README.md). It keeps what makes OTP worth having
(supervision trees, restart policies, services that own their state behind a
mailbox) without a BEAM: services are Rust values owned by one executor.

```text
your application            domain logic
        ↓
bapps-otp                   failure topology, services, mailboxes, registry
        ↓
bapps-trio                  transient tasks, cancellation, deadlines
        ↓
bapps-core (Glommio)        one executor per core, io_uring
```

Everything here is shard-local; across cores use
[bapps-app](../app/README.md), across machines your own protocol.

**Status:** pre-1.0. The API may still change before 1.0.0; every change is in
the [CHANGELOG](CHANGELOG.md).

## Example

A counter service owns its state behind a bounded mailbox and advertises a
handle for its current generation; a client started after it looks the
handle up and sends to it.

```rust
use std::error::Error;
use bapps_otp::{
    Application, CancelScope, ChildSpec, LocalSender, ServiceKey, Strategy, SupervisorSpec,
    TaskQueues,
};

#[derive(Clone)]
struct Counter(LocalSender<u64>);
static COUNTER: ServiceKey<Counter> = ServiceKey::new("counter");

glommio::LocalExecutor::default().run(async {
    let shutdown = CancelScope::new();
    let stop = shutdown.clone();
    let counter = ChildSpec::worker("counter", move |ctx, started| {
        let stop = stop.clone();
        async move {
            let (adds, inbox) = ctx.mailbox::<u64>("adds", 64);
            ctx.register(COUNTER, Counter(adds))?;
            started.started(())?;
            let mut total = 0;
            // Ends when the supervisor stops this generation.
            while let Ok(n) = inbox.recv_in(&ctx.scope()).await {
                total += n;
                if total == 6 {
                    stop.cancel(); // done: stop the application
                }
            }
            Ok::<(), Box<dyn Error>>(())
        }
    });
    let client = ChildSpec::worker("client", |ctx, started| async move {
        let counter = ctx.service(COUNTER).ok_or("counter is not registered")?;
        started.started(())?;
        for n in 1..=3 {
            counter.0.send(n).await?;
        }
        ctx.scope().cancelled().await;
        Ok::<(), Box<dyn Error>>(())
    });
    // Children start in order; RestForOne restarts the client too if the
    // counter fails, because it depends on it.
    let root = SupervisorSpec::new("root", Strategy::RestForOne)
        .child(counter)
        .child(client);
    Application::new("example", root)
        .run(shutdown, TaskQueues::current())
        .await
        .expect("a clean stop");
});
```

## Supervision

- Sequential start with readiness (`TaskStatus::started`), reverse-order stop.
- Strategies `OneForOne`, `OneForAll`, `RestForOne`; restart policies
  `Permanent`, `Transient`, `Temporary`; restart intensity per supervisor.
- A panic is an exit reason (`ExitReason::Panic`), not a crash of the shard.
- Nested supervisors; a typed shard-local registry whose per-generation
  entries disappear with their generation; a runtime tree to inspect it all.
- Bounded stops: cooperative cancellation, then a forced abort after the
  child's grace period (`Shutdown`).

## Long-lived versus transient

Do not supervise every request. Long-lived services (storage, routing, a TCP
listener, maintenance) are supervised children; transient work (one
connection, one outbound call, one scan) runs in their generation's
`ServiceTasks` or a nursery. A request failure must not restart a service; a
service failure must not leave its transient work running.

## Capabilities

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

## Not in this crate

No distributed actors or remote PIDs, no cross-shard cancel scopes, no
cross-node supervision, no arbitrary links, no hot code upgrade, no universal
`GenServer` trait, no persistence or consensus. These either blur the
boundaries above or are not yet justified by an application.

## Run

```sh
make validate                                   # from the workspace root
cargo run -p bapps-otp --example service        # also: restart, registry
```

## Documents

- [Architecture](docs/architecture.md): supervision, generations, mailboxes.
- [Service pattern](docs/service-pattern.md): how to write a service.
- [Developer guide](docs/developer-guide.md); [multicore boundary](docs/multicore.md).
- [CHANGELOG](CHANGELOG.md); older notes and migration guides in
  [docs/history](docs/history).
