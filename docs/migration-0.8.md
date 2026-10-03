# Migrating to bapps 0.8.0

0.8.0 settles the public API of `bapps-trio`, `bapps-otp` and `bapps-app` for a
long-term release. Runtime behaviour is unchanged; apart from `CallError`'s
message text, every item below is a compile error you fix once. `bapps-core`
(`glommio`) has no API changes.

## Every crate: `#[non_exhaustive]`

Enums that will grow are `#[non_exhaustive]`: the error enums,
`CancelReason`, `ExitReason`, `Strategy`, `TaskClass`, and the phase and status
enums. A `match` on one outside its crate needs a wildcard arm:

```rust,ignore
match reason {
    ExitReason::Normal => {}
    ExitReason::Shutdown => {}
    other => tracing::warn!("unexpected exit: {other:?}"),
}
```

Enums that are complete by definition stay exhaustive: `Restart`, `ChildType`,
`TrySendError`.

Output structs you only read (reports, snapshots, metrics, `Cancelled`,
`CancelCause`) are non-exhaustive too: read their fields, but you can no longer
build them with a struct literal or destructure them without `..`.

Config structs are non-exhaustive and have builders instead of literals:

| 0.7 | 0.8 |
|---|---|
| `LabConfig { seed, max_steps, .. }` | `LabConfig::new(seed).with_max_steps(n)` |
| `RestartIntensity { max_restarts, within }` | `RestartIntensity::new(max_restarts, within)` |
| `RpcLimits { queue_capacity: n, ..Default::default() }` | `RpcLimits::default().with_queue_capacity(n)` (also `with_max_in_flight`, `with_max_outbound`, `with_max_call_duration`, `with_handler_cancel_grace`) |
| `CallOptions { timeout: t, ..Default::default() }` | `CallOptions::default().with_timeout(t)` (also `with_cancellation_grace`, `with_task_class`) |

## bapps-trio

- **One public path per item.** The implementation modules (`cancel`,
  `foreign`, `lab`, `nursery`, `obligation`, `task_class`, `time`) are private;
  their items were already re-exported at the root, so
  `bapps_trio::time::sleep` becomes `bapps_trio::sleep`,
  `bapps_trio::nursery::Nursery` becomes `bapps_trio::Nursery`, and so on.
  `sync`, `testing` and `to_thread` stay public modules.
- Return types are now nameable: `CancelledFuture` at the root,
  `sync::ConditionWait` and `sync::EventWait`, `testing::WaitFor`.
- `TaskStatus::started` returns `Result<(), NoWaiter<T>>` (the value handed
  back when nobody waits for it) instead of a `glommio::GlommioError`.
- `NurseryError` and `StartError` implement `Display` and `std::error::Error`,
  so `?` into `Box<dyn Error>` or `anyhow` works. Not breaking.

## bapps-otp

- `ChildSpec` getters are consistent: `task_class_value()` is now
  `scheduling_class()`, `child_type_value()` is now `child_kind()`; with
  `restart_policy()` and `shutdown_policy()`.
- `SupervisorSpec::run_root` and `run_root_started` are crate-private: start a
  tree through `Application`. `ServiceFactory` and `ServiceFuture` are no
  longer exported.
- `OtpError::Registry` is removed (nothing constructed it).

## bapps-app

- **`AppError` is an enum**, not a `String`:
  `Config(String)`, `System(String)`, `StartupTimeout(Duration)`,
  `ShardFailed { shard, reason }`, `NodeStopped`, `Cancelled` (and
  non-exhaustive). The CPU helpers, `RpcLimits::validate`, `ReadyGate::wait`,
  `serve` and `run_application` return it. Code that formatted the error keeps
  working (`AppError: Display + Error`); code that compared or built strings
  matches variants instead.
- **Shard factories** may finish with any `Result<(), E: Display>` (trait
  `ShardResult`), so the turbofish of `AppBuilder::run` and `lab::run_node`
  is unchanged and existing `Result<(), String>` factories compile as they are.
- `CallError`'s `Display` is a sentence instead of its `Debug` form. Logs and
  string comparisons on it change; match the variant instead.
- Handlers still return `Result<R, String>`.

## Checking a downstream crate

Point the dependencies at a local checkout and build every target:

```toml
[patch."https://github.com/carlopires/bapps"]
bapps-trio = { path = "../bapps/trio" }
bapps-otp = { path = "../bapps/otp" }
bapps-app = { path = "../bapps/app" }
glommio = { package = "bapps-core", path = "../bapps/core/glommio" }
```

Then move the lock file onto the patched crates (a patch with a newer version
is otherwise ignored, with a "was not used in the crate graph" warning) and
build every target:

```sh
cargo update -p bapps-app -p bapps-otp -p bapps-trio
cargo check --all-targets
```

That lists every site above. For rudb it was four: two `AppError` results
converted with `.map_err(|e| e.to_string())` where a function keeps
`Result<(), String>`, and wildcard arms for `TreeNodeSnapshot` and
`FailAfterError`.
