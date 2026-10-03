# Migrating Trio 0.2 -> 0.2.1

No public nursery/cancellation API is renamed. Existing shard-local callers keep their signatures.

A long-lived nursery now records finished task IDs and reaps their retained Glommio handles at the next submission, rather than keeping every historical completed task until the entire nursery exits. Active task ownership is unchanged. At most the last completion batch remains retained until another submission or nursery exit. A regression test repeatedly submits and completes tasks and inspects retained-handle count.

This is not a complete allocation audit: some inherited Event/Condition/CancelScope waiter registrations persist until notification/cancellation. Verify RSS and waiter behavior under long-lived-root churn before production use.

For multicore, use `bapps_app`, not a cross-thread `Nursery`. `CancelScope::any` combines local lifetime owners only. Keep `OwnedTask` for explicit framework shutdown policy; force-dropped futures do not run asynchronous cleanup. Shared CPU/cross-core tests are in the new app crate.
