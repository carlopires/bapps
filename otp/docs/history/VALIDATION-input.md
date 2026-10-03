# Validation — bapps-otp 0.2

This version is based on the user-validated 0.1 supervisor and targets sibling
`bapps-trio` 0.2.

Expected workspace layout:

```text
experiments/
├── bapps_trio/
└── bapps_otp/
```

Run on Linux/io_uring:

```bash
cargo fmt --check
cargo check --all-targets
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

Or:

```bash
make validate
```

## New 0.2 checks

Tests should prove:

1. `LocalMailbox` is bounded and reports live depth;
2. a service generation is marked stopped by the framework;
3. stale generation tokens fail after service exit;
4. a non-cooperative service is force-aborted after its configured grace;
5. forced exit is recorded as `ExitReason::Killed`;
6. mailboxes created through `ChildContext` appear in runtime-tree snapshots;
7. service-owned Trio tasks end with their service generation;
8. all 0.1 restart strategies/readiness/panic tests still pass.

## Suggested manual experiment

Create a worker with:

```rust
.shutdown_after(Duration::ZERO)
```

and deliberately wait on a raw Glommio timer that ignores its cancellation
scope. Stop the application and confirm the child snapshot records `Killed`.

Then switch the worker to `ctx.scope().cancelled().await`; the same shutdown
should record `Shutdown` instead.
