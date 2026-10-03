# Architecture: three separate ownership domains

## 1. The host owns OS threads

`AppBuilder::run` validates CPU selection and RPC limits, creates the transport endpoints, starts one OS thread per CPU, creates the Glommio executor **on that thread**, then constructs its `ShardContext` and factory future. Factories may return `!Send` futures. Only the factory and transport endpoint values need to cross the OS-thread boundary.

The host uses blocking standard channels for low-frequency readiness/exit notifications. It does no database work. Shared atomics are limited to node control state; per-shard counters, registries and state use local ownership.

`NodeControl::shutdown` wakes dedicated per-shard stop channels. Each `ShardContext::run_application` owns a Trio stop watcher and the local OTP root. Shutdown drains local children; the host joins the thread. The factory must not return before that local application ends.

## 2. Each executor owns one local OTP world

Applications construct ordinary `Application` and `SupervisorSpec` values locally. Services, registry entries, generations, `ServiceTasks`, mailboxes and cancellation scopes never migrate. Use Rust `Rc` and ordinary owned state, without borrowing local state into messages to another executor.

Normal service errors and Rust unwinding panics are handled within the configured OTP restart topology. Exhausting that topology returns from the local root; `bapps_app` treats this as a node-level failure and stops sibling executors. A native abort/segfault/OOM kill can kill the entire process. No BEAM-style process isolation is claimed.

## 3. Cross-core operations are a protocol

`ShardClient::call` -> bounded destination queue -> `bapps_app::serve` -> destination service-owned Trio task -> terminal reply. Each request includes a `(source shard, monotonic sequence)` ID, local-process absolute monotonic deadline, scheduling class, cancellation endpoint and reply endpoint.

The service owns the handler. The caller owns only the local request/reply wait. A dropped caller cannot synchronously prove remote cleanup; it can only signal best-effort cancellation. An explicitly awaited cancellation path can wait for a terminal reply. See [RPC contract](rpc-contract.md).

## Startup barrier

A local root uses OTP's additive `run_started` API. Sequential child readiness completes before the root reports ready. The host opens the global gate only after **every** root is ready. TCP listeners bind during initialization but accept application traffic after the gate opens.

Children must signal `TaskStatus::started` before waiting for the global gate. Do not perform a cross-shard RPC inside initialization before signalling readiness: that dependency cycle is a startup deadlock. Init should do local work and publish capabilities; cross-shard synchronization belongs after readiness.

Startup timeout requests whole-node shutdown; it is not an OS-thread kill deadline. The inherited local supervisor can still be waiting for uncooperative initialization. An external process watchdog is necessary for that case. Do not interpret a requested timeout as a guarantee that `run()` returned by that instant.

## Placement

Explicit order defines stable shard IDs for this process run. `AppBuilder::cpus` sets count from the list; duplicate, unavailable and empty selections are rejected. CPU lists are logical IDs respecting current affinity, not physical-core topology. Do not oversubscribe several independent nodes onto the same CPUs and interpret the result as a scaling experiment.

No default assumes every available CPU should be consumed. `AppBuilder` defaults to one shard; choose the count or CPU list explicitly.

## Endpoint lifetime

Each typed inbox has one active receiver lease. A restarted endpoint may acquire it after the previous owner releases it. Pending **queued** calls are failed when the old lease ends, and its active handler tasks belong to the old service generation. Senders still awaiting queue admission are not generation-fenced and can be admitted by a subsequent generation. There is no transparent retry/replay promise.

This API is a process-local message system, not a transactional transport. Durability, fencing, epochs, idempotency and machine-to-machine consistency belong above it.

## Limits

Every bound of the cross-core transport is one `RpcLimits` value, set with
`AppBuilder::limits` (or passed to `lab::run_node`) and validated at start:

| Field | Default | Bounds |
|---|---|---|
| `queue_capacity` | 256 | requests queued per destination shard |
| `max_in_flight` | 128 | handlers active at once on a shard's inbox |
| `max_outbound` | 256 | calls a shard has outstanding as a caller |
| `max_call_duration` | 30 s | ceiling on any call, whatever its own timeout |
| `handler_cancel_grace` | 1 s | how long a cancelled handler may run on |

Per call, `CallOptions` sets the timeout, the extra `cancellation_grace` to
await a terminal reply after cancelling, and the handler's `TaskClass`.
What each outcome means is in the [RPC contract](rpc-contract.md).

## Deterministic node

`lab::run_node(shards, limits, factory)` is `AppBuilder::run` without
threads, for `bapps_trio::testing::Lab`: every shard's factory runs as a
task of the lab executor, the host's readiness barrier and fail-stop logic as
another, and cross-shard calls use the same bounded channels with deadlines
on the lab's virtual clock. Seeds then explore interleavings *across shards*,
which real threads never repeat. Shard CPU ids are fake and nothing is
pinned; Glommio I/O is unavailable, so applications put network and disk
behind their own seams (a simulated network and disk, for example). `tests/lab.rs` runs a
two-shard node clean under many seeds and checks the seeds interleave.
