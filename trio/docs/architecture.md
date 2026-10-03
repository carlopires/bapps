# bapps-trio architecture

## Responsibility

`bapps-trio` answers one question:

> Who owns this transient concurrent operation, and when must it end?

It does not answer what should restart after a long-lived service failure. That
belongs to `bapps-otp`.

## Lifetime tree

```text
Nursery
├── task A
│   └── CancelScope
├── task B
│   └── CancelScope
└── task C
    └── CancelScope
```

A normal task has exactly one nursery owner. A cancellation scope may have
several cancellation parents:

```text
request                 service generation
   │                           │
   └──────────┐      ┌─────────┘
              ▼      ▼
          operation scope
                │
                ▼
             socket I/O
```

This is deliberately **not** a cross-core abstraction. A cancellation request
for another shard must be sent as a message to that shard, which then cancels
its local scope.

## Cancellation attribution

Each cancelled scope records one `CancelCause`: reason, optional origin
(`cancel_by(reason, "who and why")`), an executor-local sequence number, and
whether the queried scope inherited it. `CancelScope::cause()` returns the
scope's own cause, or else the earliest cause among the scopes it inherits
from, so a multi-owner operation reports whichever owner was cancelled first,
not whichever was listed first. The first cancellation of a scope is never
overwritten. Records are bounded: one per cancelled scope, no chains, no
payloads. "Why cancellation started" is separate from how a task stopped
(`StopOutcome::{Graceful, Forced}`) and from remote-effect uncertainty.

Framework origins: nursery failure ("a sibling task returned an error" /
"panicked"), nursery drop, and `fail_after`/`move_on_after` deadlines.

## Cooperative versus forceful cancellation

Normal cancellation is cooperative. Cancellation-aware waits return and code
gets an opportunity to restore invariants and release resources.

`OwnedTask` exists for one narrow reason: a higher-level owner may need a
bounded shutdown policy. It keeps the task inside the nursery while exposing:

1. cooperative cancel;
2. bounded grace wait using the nursery clock;
3. force-cancel the exact retained Glommio task and await its destruction.

This is safer than exposing raw `spawn_local` or detached `Task` handles to
application code.

## Foreign futures

A future from Glommio or another crate is not magically scope-aware. The rule is:

```rust
cancel_on(&scope, foreign_future).await
```

At this boundary Rust cancellation is drop-based. Therefore only use it where
dropping that future is safe. Durability-critical transitions should instead be
structured as explicit shielded regions with their own deadline.

## Scheduling

Task lifetime and CPU scheduling are orthogonal:

```text
Nursery ownership
      │
      ▼
TaskClass::ForegroundRead / Repair / Compaction / ...
      │
      ▼
Glommio TaskQueue
```

An application should express *why* work exists and its scheduling class; it
should not manually select raw Glommio queues throughout domain code.

## Obligations

An `Obligation` names work that must end in an explicit decision: a reply
owed, a reservation held, a claim to publish or release. `commit()` records
that the effect happened, `abort()` that it deliberately did not. Dropping it
unresolved is a *leak*: it never panics (drops during a forced stop or a panic
must stay safe) but is counted, with its label, in an executor-local ledger
read through `obligation_stats()`. A type with a defined fallback (for example
"reply `OutcomeUnknown` if the handler is destroyed") resolves its obligation
in its own `Drop`, so the fallback is a decision rather than a leak. The lab
report lists obligations left pending or leaked, so a test can fail on a
forgotten promise without instrumenting the code under test.

## Wait-registration retention

Waiting on a scope, `Event` or `Condition` registers a waker. A dropped wait
(its future cancelled or raced away) removes that registration from the scope
and from every ancestor it was linked into, so a long-lived service scope
that is repeatedly waited on does not accumulate wakers. `tests/retention.rs`
repolls and drops waits in loops and checks the registration counts return to
zero; application churn soaks should also check that process RSS plateaus.

## Deterministic lab

`testing::Lab` runs a future and everything its nurseries spawn on one thread
without Glommio: seeded choice among ready tasks, virtual time that jumps to
the next pending sleep, and a report with deadlock, leftover-task and
obligation oracles plus the poll trace for replay. It covers this crate and
layers built only on it (OTP trees, mailboxes, registries); Glommio I/O and
timers need the real executor. Unlike Asupersync's lab, it runs `!Send`,
executor-pinned tasks, which is the whole programming model here.

## Testing contract

Semantic tests should prove at least:

- child failure cancels siblings;
- nursery waits for cooperative cleanup;
- shield breaks cancellation inheritance;
- any parent cancels a multi-owner scope;
- pre-cancelled foreign operations are never polled;
- owned tasks exit gracefully when cooperative;
- owned tasks are force-cancelled when grace expires;
- `TestClock` drives deadlines without wall-clock sleeps.
- a dropped wait releases its registration on the scope and every ancestor;
- a dropped `abort`/`cancel_and_wait` future cannot strand nursery accounting,
  because completion is recorded by the task future itself.


## Multicore companion

This document describes shard-local semantics. See [the multicore boundary](multicore.md) and [0.2.1 migration](migration-0.2.1.md) for the new application layer and release-specific additions. The current validation gate is in [VALIDATION](../VALIDATION.md).
