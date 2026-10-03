# Validation — bapps-trio 0.2

The source is based on the user-validated 0.1 crate and extends the same
`glommio-ng` 0.12 substrate.

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

Then run the new focused examples:

```bash
cargo run --example multi_owner
cargo run --example owned_shutdown
```

## 0.2 semantic checks

The test suite should demonstrate:

1. cancelling either parent cancels `CancelScope::any`;
2. a pre-cancelled scope prevents a foreign future from being polled;
3. cooperative owned tasks return `StopOutcome::Graceful`;
4. non-cooperative owned tasks can be force-cancelled after the grace period;
5. existing nursery failure/cancellation semantics remain unchanged;
6. deterministic clocks still drive timeout behavior.

## Deliberate limits

- waiter storage still uses teaching-grade waker vectors;
- no cross-shard scope exists;
- force-abort is local to a nursery-owned Glommio task;
- deterministic *time* exists, deterministic executor scheduling does not yet;
- no blocking-work abstraction is included until a real application requires it.
