# bapps-app

Pinned shard-per-core applications on [bapps](../README.md): one executor per
CPU, each running its own [bapps-otp](../otp/README.md) application, joined by
bounded cross-shard calls with explicit cancellation and deadlines. Services
own state, nurseries own concurrent work, shards own execution.

```text
host thread: start pinned executors -> readiness barrier -> stop/join
  |
  +-- CPU A / shard 0: Application -> services -> tasks -> local state
  +-- CPU B / shard 1: Application -> services -> tasks -> local state

between shards:   bounded, typed Send request/reply + cancellation
between machines: your application's protocol (not this crate)
```

A shard ID is its index in the configured CPU list, not the CPU number:
`.cpus(vec![4, 7])` puts shard 0 on CPU 4 and shard 1 on CPU 7. Automatic
selection takes the lowest CPUs the calling thread may use; it does not pick
physical cores or optimize NUMA.

**Status:** pre-1.0. The API may still change before 1.0.0; every change is in
the [CHANGELOG](CHANGELOG.md).

## Example

Every shard runs the same factory, on its own executor: here, a counter
endpoint, and on shard 0 a driver that calls every shard and stops the node.

```rust
use std::{cell::Cell, error::Error, rc::Rc};
use bapps_app::{AppBuilder, CallOptions, ShardId};
use bapps_otp::{Application, ChildSpec, Strategy, SupervisorSpec};

AppBuilder::new()
    .shards(1) // or .shards(n), .cpus(vec![...]): every shard runs this factory
    .run::<u64, u64, _, _>(|shard| {
        // Runs on the shard's own executor: Rc and !Send state are fine here.
        let (id, inbox, client) = (shard.shard_id(), shard.inbox(), shard.client());
        let (gate, node) = (shard.ready_gate(), shard.node_control());
        async move {
            let endpoint = ChildSpec::worker("counter", move |ctx, started| {
                let counter = Rc::new(Cell::new(0_u64));
                bapps_app::serve(ctx, started, inbox.clone(), move |amount, _scope| {
                    let counter = counter.clone();
                    async move {
                        counter.set(counter.get() + amount);
                        Ok(counter.get())
                    }
                })
            });
            let mut root = SupervisorSpec::new("root", Strategy::OneForOne).child(endpoint);
            if id == ShardId(0) {
                root = root.child(ChildSpec::worker("driver", move |ctx, started| {
                    let (client, gate, node) = (client.clone(), gate.clone(), node.clone());
                    async move {
                        started.started(())?;
                        let scope = ctx.scope();
                        gate.wait(&scope).await?; // every shard is ready
                        for target in 0..client.shard_count() {
                            let total = client
                                .call(&scope, ShardId(target), 7, CallOptions::default())
                                .await?;
                            assert_eq!(total, 7);
                        }
                        node.shutdown();
                        scope.cancelled().await;
                        Ok::<(), Box<dyn Error>>(())
                    }
                }));
            }
            shard.run_application(Application::new("counter", root)).await
        }
    })
    .expect("the node ran and stopped cleanly");
```

## Capabilities

| Capability | API | Tests / docs |
|---|---|---|
| Pinned executors per allowed CPU, readiness barrier, node-wide stop/join, fail-stop on a lost executor | `AppBuilder`, `ShardContext`, `NodeControl`, `ReadyGate` | `tests/multicore.rs`; [architecture](docs/architecture.md) |
| Typed, bounded cross-shard RPC with explicit cancellation | `ShardClient::call`, `ShardInbox`, `serve`, `CallOptions` | `tests/multicore.rs`, `src/rpc/protocol_tests.rs` |
| Outcome classes: not admitted (never ran) vs outcome unknown (may have run) | `CallError::{NotAdmitted, OutcomeUnknown, ..}` | `src/rpc/protocol_tests.rs`; [RPC contract](docs/rpc-contract.md) |
| RPC deadlines on an injectable clock (crate-private seam; the lab uses virtual time through it) | `RpcClock` (internal) | `src/rpc/clocked_tests.rs` |
| One value for every transport bound | `RpcLimits`, `AppBuilder::limits` | [architecture: limits](docs/architecture.md#limits) |
| A call carries the caller's deadline: its timeout is capped at the caller's remaining time, and the handler sees the same deadline | `ShardClient::call`, `bapps_trio::remaining` | `tests/deadline_propagation.rs`; [RPC contract](docs/rpc-contract.md#deadlines) |
| Whole node in the deterministic lab | `lab::run_node` | `tests/lab.rs`; [architecture](docs/architecture.md#deterministic-node) |

## Guarantees and boundaries

- One distinct pinned logical CPU per configured executor; no task migration between them.
- Local OTP owns service restart/generation semantics; local Trio owns transient work.
- All local OTP roots must initialize before the node opens normal traffic.
- Per-destination queued requests, inbound active handlers and caller outbound calls are separately bounded.
- Caller timeout/cancellation sends a per-request cancellation signal and waits a bounded interval for a terminal response.
- Acknowledgement concerns handler termination, **not rollback**. Inconclusive termination is reported rather than retried.
- A failed shard root/executor stops all shards; the host joins every executor thread. Automatic executor resurrection/rebalancing is out of scope.

No safe Rust API can preempt an OS thread stuck in blocking/CPU work; an external watchdog is required to force-kill a wedged process.

## Transport choice

Cross-core IPC uses **`async-channel` bounded MPSC queues**, plus per-call cancellation and reply channels. It passes owned Rust values without serialization. This is deliberately a replaceable, easy-to-audit reference transport; it is not Glommio's tuned SPSC/shared-channel mesh. Allocations, synchronization and wakeups must be measured before making Seastar-like performance claims. No global database mutex exists.

Stop/readiness signals use separate channels/control state, so a full data queue cannot prevent the host from requesting shutdown. Application admin RPCs do share the data-plane endpoint and can wait behind saturation.

## Run

```sh
make validate                                            # from the workspace root
make integration                                         # multicore suite on real threads
cargo run -p bapps-app --example sharded_counter -- 2    # two pinned shards
```

## Documents

- [Architecture](docs/architecture.md): host threads, per-executor worlds,
  limits, the deterministic node.
- [RPC contract](docs/rpc-contract.md): admission, outcomes, race precedence,
  deadlines.
- [Developer guide](docs/developer-guide.md); [performance plan](docs/performance.md).
- [CHANGELOG](CHANGELOG.md); older notes in [docs/history](docs/history).
