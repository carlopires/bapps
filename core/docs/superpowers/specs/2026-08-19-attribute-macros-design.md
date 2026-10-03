# Design: `#[glommio::main]` and `#[glommio::test]`

**Date:** 2026-08-19
**Status:** approved in chat, not yet implemented
**Scope:** two attribute macros, one new workspace member, one release-script change

## Why

Every glommio binary and every async glommio test opens with executor
construction:

```rust
fn main() -> Result<()> {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0)).make()?;
    ex.run(async move { /* the actual program */ })
}
```

In a test suite that boilerplate repeats once per test, and downstream projects
hand-roll it because glommio's own `test_executor!` is `#[cfg(test)]` and
crate-private. `#[tokio::test]` set the expectation; glommio has no equivalent.

**What this is not.** No `spawn!`, no `run!`. `glommio::spawn_local(async move
{ … })` is already the short form and a macro around it saves one word while
costing a layer in every backtrace and IDE jump. Nor is there a
`spawn!(FIXED, …)`: placement is a property of the executor, fixed when the
thread is created, so there is nothing to select at spawn time. The spawn-time
axis is the task queue, which `spawn_local_into` already covers.

## Surface

```rust
#[glommio::main]
#[glommio::main(placement = Fixed(0))]
#[glommio::main(placement = Fixed(0), name = "server")]

#[glommio::test]
#[glommio::test(placement = Fixed(2))]
```

Both accept `crate = <ident>` — see [Naming under the republish](#naming-under-the-republish).

**Arguments are deliberately thin.** `placement`, `name`, `crate`. Everything
else the builder offers — `spin_before_park`, `record_io_latencies`,
`preempt_timer`, pool placement — stays on the builder. An attribute argument
for each would be a second, worse spelling of an API that already reads well,
and the builder remains available inside a macro-annotated function for anyone
who needs it.

**`placement` takes a variant, not an expression.** The tokens after
`placement =` are emitted with `::glommio::Placement::` prepended, so
`Unbound`, `Fixed(0)` and `Fenced(cpu_set)` all work and arbitrary expressions
do not. A caller who needs a computed placement builds the executor by hand.
This rule is worth stating in the error message, not just the docs.

**Return types.** The annotated function may be `async fn` returning `()` or
any `T`. `main` returns whatever `run` returns, so `Result` works through
`std::process::Termination` exactly as it does for a plain `fn main`. `test`
emits a plain `#[test]` fn, so `Result` returns, `#[should_panic]` and
`#[ignore]` all compose without the macro knowing about them.

## Structure

```
glommio-macros/          new workspace member, proc-macro = true
  src/lib.rs             #[main], #[test], shared argument parser
glommio/
  Cargo.toml             glommio-macros as an optional dependency
  src/lib.rs             pub use behind the `macros` feature
```

Dependencies: `syn`, `quote`, `proc-macro2`. All three are ubiquitous and
already in the dependency tree of anything that pulls in `serde_derive` or
`thiserror`, so this adds no new compile-time cost for most consumers.

**Feature gate.** `macros` is a default-on feature of `glommio`:

```toml
[features]
default = ["macros"]
macros  = ["dep:glommio-macros"]
```

Adding a default feature is semver-compatible. `default-features = false`
consumers keep a proc-macro-free build.

**Expansion.** `#[glommio::main(placement = Fixed(0), name = "server")]` on
`async fn main() -> Result<()>` becomes:

```rust
fn main() -> Result<()> {
    ::glommio::LocalExecutorBuilder::new(::glommio::Placement::Fixed(0))
        .name("server")
        .make()
        .expect("failed to create the glommio executor")
        .run(async move { /* original body */ })
}
```

`make()` builds the executor on the current thread rather than spawning one, so
there is no thread to join and no handle to drop. `expect` is the right call
here and not laziness: a `main` that cannot build its executor has nothing to
fall back to, and the panic message carries the underlying error, which now
names the missing opcode or the `kernel.io_uring_disabled` value.

`#[glommio::test]` emits the same body wrapped in `#[test] fn`, defaulting to
`Placement::Unbound` so tests do not fight over cores when cargo runs them in
parallel.

## Naming under the republish

The expansion has to name the runtime crate, and this fork is published under
two names. `::glommio` resolves for anyone using the documented dependency
line:

```toml
glommio = { package = "glommio-ng", version = "0.10" }
```

It does not resolve for anyone who writes `glommio-ng = "0.10"` plain, because
their crate is then `glommio_ng`. Hence `crate = <ident>`:

```rust
#[glommio::main(crate = glommio_ng)]
```

tokio carries the same escape hatch for the same reason. Adding it later would
be a breaking change for anyone who had already hit the problem, so it ships in
the first version.

## Release and publishing

`glommio-macros` is a second crates.io name: `glommio-ng-macros`.
`scripts/prep-ng-release.sh` grows accordingly:

1. Rewrite `glommio-macros/Cargo.toml`: `name = "glommio-ng-macros"`, version.
2. Rewrite the dependency in `glommio/Cargo.toml` to
   `glommio-macros = { version = "…", package = "glommio-ng-macros" }`, so the
   re-export path is unchanged in source.
3. Publish `glommio-ng-macros` first, then `glommio-ng`. crates.io rejects a
   package whose dependency version does not yet exist, so the order is not
   optional.
4. The path dependency needs a `version` key alongside `path` or
   `cargo publish` rejects the manifest outright.

The macro crate carries the same version as the runtime crate, moved in
lockstep, including the `-ng` patch numbers.

## Testing

- **Behaviour.** Integration tests under `glommio/tests/` using
  `#[glommio::test]` on real async bodies — including one returning `Result`,
  one `#[should_panic]`, and one with `placement = Fixed(0)`. These run as
  ordinary cargo tests, which is also the proof that the attribute composes
  with the standard harness.
- **Expansion errors.** `trybuild` for the cases where the macro must fail
  well: an unknown argument key, `placement` given an expression rather than a
  variant, the attribute on a non-`async fn`, and the attribute on a function
  with arguments. Each has a specific message; the tests assert the message,
  not just the failure.
- **Both names.** One test compiled against the crate under its real name with
  `crate = …`, so the escape hatch is exercised rather than assumed.

Note that `glommio`'s own integration tests can use `#[glommio::test]`
directly: from `glommio/tests/`, the crate is an external crate and `::glommio`
resolves. Unit tests inside `src/` cannot, and should keep using the existing
`test_executor!`.

## Upstream posture

This is fork-only for now. It is new public API on a crate whose upstream has
been quiet since 2026-06-22, and `docs/UPSTREAM.md` says not to add more pull
requests while six sit unreviewed. It lands on `master`, ships in the next
`glommio-ng` release, and is offered upstream when there is evidence anyone is
reviewing — at which point it is a clean, self-contained PR: a new crate, new
test files, and two-line changes to `lib.rs` and `Cargo.toml`. No existing
code is modified, which is what keeps it mergeable however long it waits.

## Risks

- **A second crate name is permanent.** `glommio-ng-macros` cannot be renamed
  later, only transferred, exactly like `glommio-ng`.
- **Proc-macro compile time.** `syn` is not free for a consumer who has no
  other proc-macro dependency. `default-features = false` is the answer and is
  documented.
- **Divergence surface.** Two macros is a small, stable surface, but it is
  surface upstream does not have, and every future merge from `glommio/glommio`
  has to leave it alone. Confining the change to `lib.rs` and `Cargo.toml`
  keeps that cost near zero.
