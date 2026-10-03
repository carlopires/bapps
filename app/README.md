# bapps-app 0.6.0

An **opinionated native-Rust application framework** for shard-per-core services: services own state, nurseries own concurrent work, pinned CPU shards own execution, and bounded cross-shard RPC connects them. It sits above `bapps_otp` and `bapps_trio` and is an educational systems framework, not an OTP, BEAM or Seastar compatibility implementation.

```text
Host thread: start pinned executors -> readiness barrier -> stop/join
  |
  +-- CPU A / shard 0: Application -> OTP services -> Trio tasks -> local state
  +-- CPU B / shard 1: Application -> OTP services -> Trio tasks -> local state
  +-- CPU C / shard 2: Application -> OTP services -> Trio tasks -> local state

Between shards: bounded explicit Send request/reply and cancellation messages
Between machines: application-owned network protocol (not this crate)
```

A shard ID is its index in the configured CPU list, not the CPU number. `--cpus 4,7` means shard 0 on CPU 4, shard 1 on CPU 7. Automatic selection respects `/proc/thread-self/status` affinity and selects the lowest allowed IDs. It does **not** detect physical/P-cores, optimize NUMA or reserve a host core.

## Capabilities (0.6.0)

Part of [bapps](../README.md) 0.6.0, on `bapps-core`, the runtime in
`core/`.

| Capability | API | Tests / docs |
|---|---|---|
| Pinned executors per allowed CPU, readiness barrier, node-wide stop/join, fail-stop on a lost executor | `AppBuilder`, `ShardContext`, `NodeControl`, `ReadyGate` | `tests/multicore.rs`; [architecture](docs/architecture.md) |
| Typed, bounded cross-shard RPC with explicit cancellation | `ShardClient::call`, `ShardInbox`, `serve`, `CallOptions` | `tests/multicore.rs`, `src/rpc/protocol_tests.rs` |
| Outcome classes: not admitted (never ran) vs outcome unknown (may have run) | `CallError::{NotAdmitted, OutcomeUnknown, ..}` | `src/rpc/protocol_tests.rs`; [RPC contract](docs/rpc-contract.md) |
| RPC deadlines on an injectable clock (crate-private seam; the lab uses virtual time through it) | `RpcClock` (internal) | `src/rpc/clocked_tests.rs` |
| One value for every transport bound | `RpcLimits`, `AppBuilder::limits` | [architecture: limits](docs/architecture.md#limits) |
| Whole node in the deterministic lab | `lab::run_node` | `tests/lab.rs`; [architecture](docs/architecture.md#deterministic-node) |

## First experiment

Unpack alongside `bapps_trio` 0.5.1 and `bapps_otp` 0.5.1, then:

```sh
make validate
make integration
cargo run --example sharded_counter -- 2
```

The example constructs an independent `Rc<Cell<u64>>` counter on each executor, exposes a typed RPC endpoint as an OTP child, and runs a demo driver only on shard 0. It shuts down and joins the whole node when done. See [the example](examples/sharded_counter.rs).

## Real API

```rust,ignore
AppBuilder::new()
    .cpus(vec![4, 7]) // actual allowed Linux CPU IDs; or .shards(2)
    .run::<Request, Reply, _, _>(|shard| {
        // This factory runs inside its destination executor.
        // Create Rc, local handles and !Send futures here.
        async move {
            let application = build_local_otp_application(&shard);
            shard.run_application(application).await
        }
    })?;
```

`build_local_otp_application` is application code, not a framework API. The executable counter example shows the complete version.

`ShardContext` gives the local ID/CPU, CPU list, typed `ShardClient`, `ShardInbox`, readiness gate and `NodeControl`. `bapps_app::serve` runs the inbox as an OTP child. `ShardClient::call` sends one owned request to one explicit `ShardId`; `M` and `R` must be `Send + 'static`. The call handle itself is `!Send`.

There is intentionally no `spawn_remote`, generic `ShardedService` trait, cluster discovery, node-to-node transport, or magically distributed nursery. The per-shard factory is the first sharded-service composition primitive. A node-wide service is explicitly added on one selected shard, as in the example. Future APIs must earn their complexity from working applications.

## Guarantees and boundaries

- One distinct pinned logical CPU per configured executor; no task migration between them.
- Local OTP owns service restart/generation semantics; local Trio owns transient work.
- All local OTP roots must initialize before the node opens normal traffic.
- Per-destination queued requests, inbound active handlers and caller outbound calls are separately bounded.
- Caller timeout/cancellation sends a per-request cancellation signal and waits a bounded interval for a terminal response.
- Acknowledgement concerns handler termination, **not rollback**. Inconclusive termination is reported rather than retried.
- A failed shard root/executor stops all shards; the host joins every executor thread. Automatic executor resurrection/rebalancing is out of scope.

These are implementation contracts awaiting the new Linux test gate, not production certifications. No safe Rust API can preempt an OS thread stuck in blocking/CPU work; an external watchdog is required to force-kill a wedged process.

## Transport choice

Cross-core IPC uses **`async-channel` bounded MPSC queues**, plus per-call cancellation and reply channels. It passes owned Rust values without serialization. This is deliberately a replaceable, easy-to-audit reference transport; it is not Glommio's tuned SPSC/shared-channel mesh. Allocations, synchronization and wakeups must be measured before making Seastar-like performance claims. No global database mutex exists.

Stop/readiness signals use separate channels/control state, so a full data queue cannot prevent the host from requesting shutdown. Application admin RPCs do share the data-plane endpoint and can wait behind saturation.

Read [architecture](docs/architecture.md), [CHANGELOG](CHANGELOG.md), [developer guide](docs/developer-guide.md), [RPC contract](docs/rpc-contract.md), and [performance plan](docs/performance.md).
