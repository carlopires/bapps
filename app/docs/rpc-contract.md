# Cross-shard RPC contract

## Admission

Three counts are independent: destination queue capacity (default 256), destination active handlers (128) and outbound calls shared by local client clones (256). An exhausted outbound budget returns `Overloaded`; a full destination queue asynchronously waits within the original request deadline. The queue does not run unbounded detached tasks.

Capacity is measured in messages/tasks, **not bytes**. Bound request/response payloads and resource reservations at the application boundary. Small messages can be high-frequency; large values can exhaust memory even with bounded queue length. A destination serial service may impose additional limits.

Control-plane shutdown has its own queue. Per-request Cancel uses a separate capacity-one channel retained by that request. It cannot be blocked behind more RPC requests. A request still queued must be dequeued or the service must stop before a terminal cancellation acknowledgement is available.

## Execution

The destination checks cancellation and deadline before first polling the handler. The handler receives a local scope owned by its service generation. The `CallOptions` scheduling class selects the local Trio/Glommio task queue. Ordinary handler `Err(String)` becomes `CallError::Remote`; it does not intentionally fail sibling calls. A panic reaches OTP via the service nursery.

Do not have a saturated handler call synchronously into the same saturated endpoint. Bound fan-out and avoid cycles in service dependency graphs. Message passing does not eliminate capacity deadlocks.

## Cancellation and deadlines

The deadline covers readiness wait, queue admission and response wait. Source and destination use `std::time::Instant` in the same process; the deadline is not serialized across machines. No distributed-clock claim is made.

After caller cancellation/deadline, the source sends Cancel and waits up to `cancellation_grace` for the response. The destination cancels its handler scope, continues polling it for `handler_cancel_grace` so cooperative asynchronous cleanup can finish, then destroys a non-cooperative handler future if necessary.

## Request states

```text
caller:       Validate -> WaitingForReadiness -> WaitingForAdmission -> Admitted -> Terminal
                              |                      |                     |
                              +--- interrupted ------+--> NotAdmitted      +--> CancelRequested -> Terminal
destination:  Queued -> Started -> Replied
                 |         |
                 |         +--> handler destroyed (force-stop, panic) --> OutcomeUnknown
                 +--> never started (inbox dropped, owner stopping) ----> TargetStopped
```

- **Admission** is the instant `send` enqueues the request; it happens inside
  one poll, so an interrupted wait for capacity means the request was never
  delivered.
- **Started** is the instant the destination's handler task first runs. From
  then on the destination owes exactly one reply, enforced by a consume-once
  `Reply` value; if it is dropped unsent (generation force-abort, panic) it
  reports `OutcomeUnknown`.
- Queued requests belong to the **inbox**, not to a service generation. When a
  generation stops, requests it had not started are served by the next
  generation. Requests a generation *started* are never replayed. When the
  shard itself goes away, everything still queued gets `TargetStopped`.

## Outcomes

| Result | Meaning | Effects |
|---|---|---|
| `Ok(r)` | Handler completed. | Applied. |
| `Remote(e)` | Handler returned an application error. | Whatever the handler did. |
| `InvalidShard`, `InvalidOptions`, `Overloaded`, `NodeStopping` | Refused before sending. | None. Safe to retry. |
| `NotAdmitted(Cancelled \| Deadline)` | Caller gave up while waiting for readiness or queue capacity. | None: never delivered. Safe to retry. |
| `TargetStopped` | Destination dropped the request before starting it. | None. Safe to retry. |
| `Cancelled { acknowledged: true }` / `Deadline { acknowledged: true }` | Interrupted after admission; within `cancellation_grace` the destination reached a terminal state it could vouch for (finished, cleaned up cooperatively, or never started). | **Possible.** Not a rollback: a completed write stays completed. |
| `Cancelled { acknowledged: false }` / `Deadline { acknowledged: false }` | Interrupted after admission; no vouchable terminal state within grace. | **Possible, and the handler may still be running.** |
| `OutcomeUnknown` | Destination destroyed the handler before it finished. | **Unknown.** Async cleanup and nested work not certified. |

Never map a possible or unknown outcome to "not applied". The transport never
retries; reconcile at the application level (idempotent operations, request
IDs, reads that confirm state).

## Race precedence

- **Caller side.** While waiting (readiness, capacity, reply), caller
  cancellation beats the deadline, and both beat an operation that becomes
  ready in the same poll. A reply that loses that race is still collected
  during the cleanup grace and counts as acknowledgement, but the caller
  reports `Cancelled`/`Deadline`, not the reply.
- **Destination entry.** Before the handler starts: a stopping owner gives
  `TargetStopped`; a cancelled or abandoned caller gives
  `Cancelled { acknowledged: true }`; an expired deadline gives
  `Deadline { acknowledged: true }`. None of them runs the handler.
- **Destination running.** Owner stop or caller cancellation beats the
  deadline. After either, the handler keeps being polled for
  `handler_cancel_grace`; if it finishes, the reply is the interruption (its own
  result is discarded, its effects are not); if the grace expires, the handler
  is destroyed and the reply is `OutcomeUnknown`.
- **Source mapping of a forced reply.** A caller that was itself interrupted
  and then receives `OutcomeUnknown` reports `acknowledged: false`. A caller
  that was not interrupted receives `OutcomeUnknown` directly.

A dropped call future sends best-effort Cancel but **cannot await
acknowledgement from Drop**. No asynchronous cleanup guarantee can be inferred
from synchronous destruction.

Both sides must still follow structured concurrency. Handlers must not launch detached work. Awaited handler completion does not undo writes already accepted by another service, finish a foreign blocking call, flush disks, or establish distributed transaction atomicity. Application messages should carry cancellation or a deliberate commit protocol where appropriate.

## Shutdown and restart

The transport does not retry requests. A lost connection/receiver, interrupted generation, or uncertain result requires application-level reconciliation. Framework shutdown can force-drop owned futures; this is an escape hatch, not graceful cleanup. Never map uncertain write outcomes to "not applied".

No cancellation mechanism can preempt an executor thread currently running blocking code without returning to the scheduler. Avoid that code in handlers and use an external watchdog for node hangs.

## Testing

`src/rpc/protocol_tests.rs` stages each race on one executor with explicit
events, no timing guesses: effect kept after caller cancellation, forced
handler not acknowledged, panic and generation force-stop reported as
`OutcomeUnknown`, queued work carried to the next generation, `NotAdmitted` by
cancellation and by deadline, shared outbound budget released by a dropped
call.

`src/rpc/clocked_tests.rs` runs RPC deadlines on a virtual clock (Trio's
`TestClock` behind the crate-private `RpcClock` seam): a deadline beats a reply
released in the same step (the reply then counts as acknowledgement), a reply
one tick earlier wins, cancellation propagates across two hops when each
handler passes its scope on, and re-entering a saturated endpoint ends by
deadline plus grace, unacknowledged, with the queued nested request never run.
Both files run in `make validate`.

`tests/multicore.rs` (`make integration`) runs real executor threads: state
isolation, cross-core calls, cancellation acknowledgement, whole-node shutdown,
a failed startup, and **executor loss**: a shard root fails while a call from
another shard is running on it; the node fail-stops, all threads are joined,
and the caller receives `OutcomeUnknown`, never success or `TargetStopped`.

## Capacity deadlocks

Message passing is not deadlock prevention. A handler that synchronously calls
an endpoint that is saturated, including its own, waits for capacity that only
its own completion can free. The protocol bounds the damage rather than
preventing it: the nested call ends at its deadline plus cleanup grace with
`Deadline { acknowledged: false }`, and its queued request is refused when it
is finally dequeued. Avoid cycles in service dependency graphs, and give nested
calls deadlines shorter than their caller's.
