# Migrating OTP 0.2 -> 0.2.1

This is additive to the corrected user-supplied 0.2 APIs; it requires sibling Trio 0.2.1.

## Root readiness

`Application::run_started(shutdown, queues, TaskStatus<()>)` and `SupervisorSpec::run_root_started(...)` report readiness after the local root finishes sequential child initialization. `bapps_app` combines these local handshakes into a node-wide gate. Ordinary `Application::run` remains available.

Do not await a node-wide gate inside child initialization before calling `started`. A scope timeout requests cooperative shutdown; it does not kill an OS thread hung in initialization.

## Reliable service-task admission

`ServiceTasks::active_tasks()` now includes tasks that have been submitted but not yet polled. `wait_below(limit)` waits for completions, using a local condition. Reserve/submit immediately after it returns; do not insert an await. A single dispatcher such as a TCP accept loop can use this pattern. It is not an atomic multi-producer permit API.

An active-count guard is reserved before spawning, and released when the task future or failed submission is dropped. Waiting dispatchers are notified when it drops. The test verifies the count **before yielding**, which the old first-poll accounting did not guarantee.

## Mailbox cancellation fast path

`send_in` and `recv_in` check active cancellation before accepting an immediately ready operation. A pre-cancelled caller must not enqueue a new command merely because capacity is free. This does not roll back an already accepted command; application commands can carry an operation scope to skip work that has not started.

## Application extraction

The first application ported to 0.2.1 uses `ctx.mailbox`, `ctx.tasks`, `ServiceGeneration` and these admission APIs. Its old mailbox implementation and application lifetime Drop guard are deleted. `CrashControl` remains application-specific injection policy, not framework liveness bookkeeping.
