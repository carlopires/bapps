# bapps — backend apps

A Rust platform for shard-per-core backend services on Linux `io_uring`:
pinned executors, services that own their state, structured concurrency for
transient work, OTP-style supervision, and bounded messages between cores.

```text
your application                      domain logic, network protocol, storage
  bapps-app   (app/)                  pinned shards, readiness, bounded cross-shard RPC, lab node
  bapps-otp   (otp/)                  supervision trees, service generations, mailboxes, registry
  bapps-trio  (trio/)                 nurseries, cancel scopes, deadlines, obligations, lab executor
  bapps-core  (core/)                 io_uring thread-per-core runtime (from Glommio)
```

| Crate | Directory | Version | Capabilities and docs |
|---|---|---|---|
| `bapps-core` (lib `glommio`) | `core/glommio` | 0.10.0 line, release `v0.12.0-ng-cp.4` | [README](core/README.md), [CHANGELOG](core/CHANGELOG.md) |
| `bapps-trio` | `trio/` | 0.7.0 | [README](trio/README.md), [architecture](trio/docs/architecture.md) |
| `bapps-otp` | `otp/` | 0.7.0 | [README](otp/README.md), [architecture](otp/docs/architecture.md) |
| `bapps-app` | `app/` | 0.7.0 | [README](app/README.md), [architecture](app/docs/architecture.md), [RPC contract](app/docs/rpc-contract.md) |

## Use

```toml
[dependencies]
bapps-app = { git = "https://github.com/carlopires/bapps", tag = "v0.7.0" }
bapps-otp = { git = "https://github.com/carlopires/bapps", tag = "v0.7.0" }
bapps-trio = { git = "https://github.com/carlopires/bapps", tag = "v0.7.0" }
glommio = { package = "bapps-core", git = "https://github.com/carlopires/bapps", tag = "v0.7.0", default-features = false }
```

The runtime crate is published as `bapps-core` but its library keeps the name
`glommio` (`use glommio::...`): its tests, examples and the code its macros
generate refer to `::glommio`, and keeping it makes upstream merges cheap.

## Develop

```sh
make validate      # fmt, clippy -D warnings, all tests, docs
make integration   # multicore suite on real pinned threads
make bench         # core benchmarks
```

## History and upstream

`core/` is where the runtime is maintained. Its history is Glommio's:
[DataDog/glommio](https://github.com/DataDog/glommio), the community fork
[glommio/glommio](https://github.com/glommio/glommio), and
[dahankzter/glommio](https://github.com/dahankzter/glommio), plus the fixes
made for bapps (release line `v0.12.0-ng-cp.1` to `cp.4`). `trio/`, `otp/`
and `app/` were developed as separate repositories (`glommio-trio`,
`glommio-otp`, `glommio-app` 0.5.x); their CHANGELOGs carry that history.

Fixes to the runtime, tests included, are made in `core/` directly. Upstream
changes are pulled straight into it after review, for example:

```sh
git subtree pull --prefix=core https://github.com/dahankzter/glommio master
```

and the review of every sync is recorded in [core/CHANGELOG.md](core/CHANGELOG.md).
To offer a fix upstream, `git subtree split --prefix=core` gives a branch
with upstream's paths to open a pull request from.

Workspace-only changes to `core/`: Glommio's root `Cargo.toml` is replaced by
this workspace, the packages are renamed `bapps-core` and
`bapps-core-macros`, and its bench profile moved to the root.

## License

MIT OR Apache-2.0. `core/` is derived from Glommio (DataDog and
contributors); its LICENSE and NOTICE files are kept in `core/`.
