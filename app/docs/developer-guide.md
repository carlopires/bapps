# Multicore developer guide

## Choose the owner before writing the function

Inside one shard, use `bapps_otp` for long-lived services and `bapps_trio` for transient concurrent work. Keep the database/cache/WAL state owned by a service rather than hidden behind a shared global mutex. Channels crossing cores carry an enum or another explicit owned `Send` value; do not smuggle a local handle or a closure across.

Separate the owners you are choosing between: the service task owns the state, the supervisor owns failure and restarts, the `ServiceGeneration` token owns admission validity (local only — not a distributed fencing token), a nursery or `ctx.tasks()` owns transient work, the shard's pinned executor owns CPU placement (`TaskClass` only picks a queue), and `bapps_app` owns executor threads, readiness and node stop. The full mapping is the *Who owns what* table in `bapps_otp/docs/architecture.md`.

Use domain verbs around framework calls. `StorageHandle::get` is local. A typed `ShardClient::call(scope, target, request, options)` is visibly cross-core. A peer RPC is visibly network traffic. These three operations have different failure and performance costs.

## Compose a local root per CPU

Read the complete [counter example](../examples/sharded_counter.rs). Add each local child in dependency order. `ShardContext::run_application` owns the local root and bridges its readiness/shutdown to the host. Root readiness is not permission to perform network consensus or claim data durability.

To run a singleton, explicitly add it only when `shard.shard_id() == ShardId(0)`. This does not turn its local registry entry into a shared global entry. Other shards address it through a typed endpoint.

## Use bounded service tasks

A bounded mailbox alone does not bound the number of active tasks. `ServiceTasks::wait_below` plus immediate spawn provides local admission when a single dispatcher submits tasks. Do not await another operation between a capacity reservation/check and submission, and do not claim this pattern is an atomic multi-producer semaphore. `bapps_app::serve` has its own synchronous reservation counter.

CPU-heavy handlers must yield or split into continuations. Scheduler shares are not preemptive CPU interrupts. Disk direct I/O, buffer pools and NUMA placement are not supplied by this application layer.

## Shutdown policy

Use `NodeControl::shutdown` to stop all shards. Do not drop the app future as the normal shutdown API. A normal local service failure should be handled by local OTP; an exhausted root is a node failure. Do not automatically restart an executor containing unknown storage state.

The node currently has no built-in signal handler. An application's admin shutdown command should use the explicit graceful path; a default OS signal termination is not equivalent. Integrate signal handling at the application host boundary when needed, with tests for ownership and joins.

## Code-review checklist

1. Is every task local to a nursery/service generation, with no raw detached spawning?
2. Are CPU ID, shard ID and machine ID kept distinct?
3. Are capacity, payload bytes, fan-out, timeouts and uncertainty all explicit?
4. Does every cross-core call target a known shard with owned messages?
5. Does startup signal local ready before awaiting cross-shard readiness?
6. Can failure injection reach the service under saturation, and is the control path isolated where required?
7. Do tests verify cleanup/termination rather than merely observing a Cancel message?
8. Are performance statements backed by native-Linux measurements, not the number of threads alone?
