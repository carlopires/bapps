# AGENTS

Guidance for anyone, human or agent, changing this repository.

## Layout

| Directory | Crate | Notes |
|---|---|---|
| `core/` | `bapps-core` (library `glommio`) | the runtime, from Glommio; upstream changes are pulled with `git subtree pull --prefix=core` after review, recorded in `core/CHANGELOG.md` |
| `trio/` | `bapps-trio` | nurseries, cancel scopes, deadlines, lab |
| `otp/` | `bapps-otp` | supervision, services, mailboxes, registry |
| `app/` | `bapps-app` | pinned shards, cross-shard calls |

`core/AGENTS.md` is upstream Glommio's guide for its own repository; inside
bapps, use the commands below.

## Commands

```sh
make validate      # fmt check, clippy -D warnings, all tests, docs -D warnings
make integration   # multicore suite on real pinned threads
make bench         # core benchmarks
```

The toolchain is pinned in `rust-toolchain.toml` (Rust 1.92.0). Linux only:
everything runs on io_uring.

## Rules

- **Tests:** follow [docs/testing.md](docs/testing.md): explicit states
  instead of sleeps, bounded waits, the lab first, and every new test checked
  by breaking the code it protects.
- **Commit only after both gates pass on the whole workspace.** Make the
  commit depend on the gates (`make validate && make integration && git
  commit ...`), never chain it with `;`.
- **Public API:** every public item is documented (`missing_docs` is enforced);
  enums that may grow and output structs are `#[non_exhaustive]`; config
  structs have `with_*` setters; errors implement `Display` and
  `std::error::Error`. Breaking changes are allowed before 1.0 and recorded in
  the crate's CHANGELOG.
- **Docs follow the code:** README examples are doctests; a new capability
  gets a row in its crate's capability table, with the test that proves it.
- **No sleeps as synchronization, no retries for flaky tests:** see the flaky
  tests section of the testing guide.
- **`core/`:** keep changes minimal and reviewable against upstream; record
  each in `core/CHANGELOG.md`.

## Guides

- [Testing](docs/testing.md)
- bapps-trio: [architecture](trio/docs/architecture.md), [developer guide](trio/docs/developer-guide.md)
- bapps-otp: [architecture](otp/docs/architecture.md), [service pattern](otp/docs/service-pattern.md), [developer guide](otp/docs/developer-guide.md)
- bapps-app: [architecture](app/docs/architecture.md), [RPC contract](app/docs/rpc-contract.md), [developer guide](app/docs/developer-guide.md)
