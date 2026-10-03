# `#[glommio::main]` and `#[glommio::test]` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give glommio two attribute macros that build the executor for a binary's `main` and for an async test, removing the boilerplate every downstream currently hand-rolls.

**Architecture:** A new `proc-macro` workspace member, `glommio-macros`, parses a three-key attribute (`placement`, `name`, `crate`) and emits a synchronous function that builds a `LocalExecutor` and calls `run` on the original async body. `glommio` re-exports both macros behind a default-on `macros` feature, so `#[glommio::main]` works with no second dependency.

**Tech Stack:** Rust 2021, `syn` 2 with the `full` feature, `quote`, `proc-macro2`, `trybuild` for compile-failure tests.

**Spec:** `docs/superpowers/specs/2026-08-19-attribute-macros-design.md`

## Global Constraints

- MSRV is **1.92**; edition **2021** for `glommio` and `glommio-macros`.
- License headers are not used in this repo's `src` files — do not add any.
- Run `make fmt` before every commit. CI warns on unformatted code.
- Commits are signed off (`git commit -s`) and never mention AI assistance. Format: `type: imperative subject under 72 chars`, blank line, body explaining **what and why**.
- Work on a branch: `feat/attribute-macros`. Do not commit to `master` directly, and do not touch any branch that feeds an upstream PR.
- The macro crate version tracks the runtime crate exactly: **0.10.0**.
- `glommio-macros` must never depend on `glommio`. The dependency runs one way only, or the workspace will not build.
- No existing glommio source file is modified except `glommio/src/lib.rs` and `glommio/Cargo.toml`, plus the two workspace files. This keeps the change mergeable upstream however long it waits.

---

### Task 1: Create the `glommio-macros` crate with a working `#[glommio::main]`

**Files:**
- Create: `glommio-macros/Cargo.toml`
- Create: `glommio-macros/src/lib.rs`
- Create: `glommio-macros/tests/expand_main.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Consumes: nothing.
- Produces: `#[proc_macro_attribute] pub fn main(args: TokenStream, item: TokenStream) -> TokenStream`. Emits a sync `fn` of the same name, visibility and return type, whose body is `::glommio::LocalExecutorBuilder::new(::glommio::Placement::Unbound).make().expect("failed to create the glommio executor").run(async move { <original body> })`.

- [ ] **Step 1: Create the crate manifest and register it in the workspace**

`glommio-macros/Cargo.toml`:

```toml
[package]
authors = [
  "DataDog <info@datadoghq.com>",
  "Glauber Costa <glommer@gmail.com>",
  "Hippolyte Barraud <hippolyte.barraud@datadoghq.com>",
]
categories = ["asynchronous", "concurrency"]
description = "Attribute macros for glommio: #[glommio::main] and #[glommio::test]."
edition = "2021"
homepage = "https://github.com/dahankzter/glommio"
keywords = ["async", "iouring", "linux", "macros", "thread-per-core"]
license = "Apache-2.0 OR MIT"
name = "glommio-macros"
repository = "https://github.com/dahankzter/glommio"
rust-version = "1.92"
version = "0.10.0"

[lib]
proc-macro = true

[dependencies]
proc-macro2 = "1"
quote       = "1"
syn         = { version = "2", features = ["full"] }

[dev-dependencies]
futures-lite = "2.6"
trybuild     = "1"
```

`futures-lite` is for the stub runtime the tests use in place of `glommio`;
`trybuild` is for the compile-failure cases in Task 4.

Then change the workspace member list in the root `Cargo.toml` to:

```toml
[workspace]
members  = ["examples", "glommio", "glommio-macros"]
resolver = "2"
```

- [ ] **Step 2: Write the failing test**

`glommio-macros/tests/expand_main.rs`. This test does not depend on `glommio`; it checks the macro compiles and the emitted function runs, using a stub module named `glommio` so the `::glommio` path in the expansion resolves.

```rust
//! The expansion names `::glommio` paths, so these tests provide a stub crate
//! of that name rather than depending on the real runtime — which would make
//! the dependency circular.

extern crate self as glommio;

pub struct LocalExecutorBuilder(Placement);
pub struct LocalExecutor;

#[derive(Debug, PartialEq)]
pub enum Placement {
    Unbound,
    Fixed(usize),
}

impl LocalExecutorBuilder {
    pub fn new(placement: Placement) -> Self {
        LocalExecutorBuilder(placement)
    }

    pub fn name(self, _name: &str) -> Self {
        self
    }

    pub fn make(self) -> Result<LocalExecutor, ()> {
        PLACEMENT.with(|p| *p.borrow_mut() = Some(self.0));
        Ok(LocalExecutor)
    }
}

impl LocalExecutor {
    pub fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        futures_lite::future::block_on(future)
    }
}

thread_local! {
    static PLACEMENT: std::cell::RefCell<Option<Placement>> =
        const { std::cell::RefCell::new(None) };
}

#[glommio_macros::main]
async fn runs_the_body() -> u32 {
    7
}

#[test]
fn main_runs_the_body_and_defaults_to_unbound() {
    assert_eq!(runs_the_body(), 7);
    PLACEMENT.with(|p| assert_eq!(*p.borrow(), Some(Placement::Unbound)));
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p glommio-macros --test expand_main`
Expected: FAIL — `could not find 'main' in 'glommio_macros'`.

- [ ] **Step 4: Write the minimal implementation**

`glommio-macros/src/lib.rs`:

```rust
//! Attribute macros for glommio.
//!
//! The expansion names the runtime crate by path, so everything here emits
//! `::glommio::…` unless the caller overrides it with `crate = <ident>`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Builds a `LocalExecutor` and runs an async `main` on it.
#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);

    let attrs = &function.attrs;
    let vis = &function.vis;
    let name = &function.sig.ident;
    let output = &function.sig.output;
    let body = &function.block;

    quote! {
        #(#attrs)*
        #vis fn #name() #output {
            ::glommio::LocalExecutorBuilder::new(::glommio::Placement::Unbound)
                .make()
                .expect("failed to create the glommio executor")
                .run(async move #body)
        }
    }
    .into()
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p glommio-macros --test expand_main`
Expected: PASS, 1 test.

- [ ] **Step 6: Commit**

```bash
make fmt
git add Cargo.toml glommio-macros
git commit -s -m "$(cat <<'EOF'
feat: add a glommio-macros crate with #[main]

Every glommio binary opens by building an executor and calling run on
it. The attribute does that, leaving the body as the program.

The crate deliberately does not depend on glommio: the expansion names
::glommio by path, so the dependency runs one way and the tests stub the
runtime rather than pulling it in.
EOF
)"
```

---

### Task 2: Parse the attribute arguments

**Files:**
- Create: `glommio-macros/src/args.rs`
- Modify: `glommio-macros/src/lib.rs`
- Modify: `glommio-macros/tests/expand_main.rs`

**Interfaces:**
- Consumes: `main` from Task 1.
- Produces: `pub(crate) struct Args { pub placement: proc_macro2::TokenStream, pub name: Option<syn::LitStr>, pub krate: syn::Path }` and `pub(crate) fn parse(args: proc_macro::TokenStream, default_placement: &str) -> syn::Result<Args>`. `Args::placement` is already fully qualified (`::glommio::Placement::Fixed(0)`); `Args::krate` is the runtime crate path, `::glommio` unless overridden.

- [ ] **Step 1: Write the failing tests**

Append to `glommio-macros/tests/expand_main.rs`:

```rust
#[glommio_macros::main(placement = Fixed(3))]
async fn pinned() {}

#[glommio_macros::main(placement = Fixed(1), name = "worker")]
async fn pinned_and_named() {}

#[test]
fn placement_argument_reaches_the_builder() {
    pinned();
    PLACEMENT.with(|p| assert_eq!(*p.borrow(), Some(Placement::Fixed(3))));
}

#[test]
fn name_argument_compiles_alongside_placement() {
    pinned_and_named();
    PLACEMENT.with(|p| assert_eq!(*p.borrow(), Some(Placement::Fixed(1))));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p glommio-macros --test expand_main`
Expected: FAIL — the attribute currently ignores its arguments, so `PLACEMENT` holds `Unbound` and both new assertions fail.

- [ ] **Step 3: Write the argument parser**

`glommio-macros/src/args.rs`:

```rust
//! Attribute argument parsing, shared by `#[main]` and `#[test]`.
//!
//! Three keys, deliberately: `placement`, `name` and `crate`. Anything the
//! builder does better stays on the builder.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Ident, LitStr, Path, Token,
};

pub(crate) struct Args {
    /// Fully qualified, e.g. `::glommio::Placement::Fixed(0)`.
    pub(crate) placement: TokenStream,
    pub(crate) name: Option<LitStr>,
    pub(crate) krate: Path,
}

enum Arg {
    Placement(TokenStream),
    Name(LitStr),
    Crate(Path),
}

impl Parse for Arg {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        // `crate` is a keyword, so it does not parse as an Ident.
        if input.peek(Token![crate]) {
            input.parse::<Token![crate]>()?;
            input.parse::<Token![=]>()?;
            return Ok(Arg::Crate(input.parse()?));
        }

        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;

        match key.to_string().as_str() {
            "placement" => {
                let variant: syn::Expr = input.parse()?;
                match &variant {
                    syn::Expr::Path(_) | syn::Expr::Call(_) => {}
                    _ => {
                        return Err(syn::Error::new_spanned(
                            &variant,
                            "placement takes a Placement variant such as `Unbound`, `Fixed(0)` \
                             or `Fenced(cpus)`, not an arbitrary expression. Build the executor \
                             with LocalExecutorBuilder if you need a computed placement",
                        ))
                    }
                }
                Ok(Arg::Placement(quote!(#variant)))
            }
            "name" => Ok(Arg::Name(input.parse()?)),
            other => Err(syn::Error::new_spanned(
                &key,
                format!("unknown argument `{other}`; expected `placement`, `name` or `crate`"),
            )),
        }
    }
}

/// Parses the attribute arguments, defaulting the placement to
/// `default_placement` when the caller did not name one.
pub(crate) fn parse(args: proc_macro::TokenStream, default_placement: &str) -> syn::Result<Args> {
    let parsed =
        syn::parse::Parser::parse(Punctuated::<Arg, Token![,]>::parse_terminated, args)?;

    let mut placement = None;
    let mut name = None;
    let mut krate = None;

    for arg in parsed {
        match arg {
            Arg::Placement(value) => placement = Some(value),
            Arg::Name(value) => name = Some(value),
            Arg::Crate(value) => krate = Some(value),
        }
    }

    let krate = krate.unwrap_or_else(|| syn::parse_quote!(::glommio));
    let placement = match placement {
        Some(variant) => quote!(#krate::Placement::#variant),
        None => {
            let default: Ident = Ident::new(default_placement, proc_macro2::Span::call_site());
            quote!(#krate::Placement::#default)
        }
    };

    Ok(Args {
        placement,
        name,
        krate,
    })
}
```

- [ ] **Step 4: Use the parser in `main`**

Replace `glommio-macros/src/lib.rs` with:

```rust
//! Attribute macros for glommio.
//!
//! The expansion names the runtime crate by path, so everything here emits
//! `::glommio::…` unless the caller overrides it with `crate = <ident>`.

mod args;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Builds a `LocalExecutor` and runs an async `main` on it.
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, "Unbound", false)
}

fn expand(
    args: TokenStream,
    item: TokenStream,
    default_placement: &str,
    is_test: bool,
) -> TokenStream {
    let args = match args::parse(args, default_placement) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };

    let function = parse_macro_input!(item as ItemFn);

    let attrs = &function.attrs;
    let vis = &function.vis;
    let ident = &function.sig.ident;
    let output = &function.sig.output;
    let body = &function.block;

    let krate = &args.krate;
    let placement = &args.placement;
    let named = args.name.map(|name| quote!(.name(#name)));
    let test_attr = if is_test { quote!(#[::core::prelude::v1::test]) } else { quote!() };

    quote! {
        #test_attr
        #(#attrs)*
        #vis fn #ident() #output {
            #krate::LocalExecutorBuilder::new(#placement)
                #named
                .make()
                .expect("failed to create the glommio executor")
                .run(async move #body)
        }
    }
    .into()
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p glommio-macros --test expand_main`
Expected: PASS, 3 tests.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio-macros
git commit -s -m "$(cat <<'EOF'
feat: parse placement, name and crate attribute arguments

Three keys, and no more. Everything else the builder offers reads better
on the builder, and an attribute argument for each would be a second,
worse spelling of an API that already works.

placement takes a variant rather than an expression so the emitted path
can be qualified for the caller; the error says so, since the
restriction is not guessable.
EOF
)"
```

---

### Task 3: Add `#[glommio::test]`

**Files:**
- Modify: `glommio-macros/src/lib.rs`
- Create: `glommio-macros/tests/expand_test.rs`

**Interfaces:**
- Consumes: `expand` and `args::parse` from Task 2.
- Produces: `#[proc_macro_attribute] pub fn test(args: TokenStream, item: TokenStream) -> TokenStream`. Emits `#[test] fn <name>() <output> { … }`, defaulting to `Placement::Unbound`.

- [ ] **Step 1: Write the failing test**

`glommio-macros/tests/expand_test.rs`, with the same stub runtime as Task 1 (repeated deliberately — the test files are independent compilation units):

```rust
//! `#[test]` composes with the standard harness because the macro emits a
//! plain `#[test] fn`. These tests prove that, including the cases where the
//! harness attributes have to survive: Result returns and should_panic.

extern crate self as glommio;

pub struct LocalExecutorBuilder(Placement);
pub struct LocalExecutor;

#[derive(Debug, PartialEq)]
pub enum Placement {
    Unbound,
    Fixed(usize),
}

impl LocalExecutorBuilder {
    pub fn new(placement: Placement) -> Self {
        LocalExecutorBuilder(placement)
    }

    pub fn name(self, _name: &str) -> Self {
        self
    }

    pub fn make(self) -> Result<LocalExecutor, ()> {
        Ok(LocalExecutor)
    }
}

impl LocalExecutor {
    pub fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        futures_lite::future::block_on(future)
    }
}

#[glommio_macros::test]
async fn plain() {
    assert_eq!(1 + 1, 2);
}

#[glommio_macros::test]
async fn returning_result() -> Result<(), std::io::Error> {
    Ok(())
}

#[glommio_macros::test]
#[should_panic(expected = "boom")]
async fn panicking() {
    panic!("boom");
}

#[glommio_macros::test(placement = Fixed(0))]
async fn pinned() {}

#[glommio_macros::test]
#[ignore]
async fn ignored() {
    unreachable!("this test is ignored and must not run");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p glommio-macros --test expand_test`
Expected: FAIL — `could not find 'test' in 'glommio_macros'`.

- [ ] **Step 3: Write the minimal implementation**

Add to `glommio-macros/src/lib.rs`, directly below `main`:

```rust
/// Builds a `LocalExecutor` and runs an async test on it.
///
/// Emits a plain `#[test]` function, so `#[should_panic]`, `#[ignore]` and
/// `Result` returns compose without this macro knowing about them.
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, "Unbound", true)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p glommio-macros --test expand_test`
Expected: PASS — 4 passed, 1 ignored.

- [ ] **Step 5: Commit**

```bash
make fmt
git add glommio-macros
git commit -s -m "$(cat <<'EOF'
feat: add #[glommio::test]

Downstreams hand-roll an executor per async test because glommio's own
test_executor! is cfg(test) and crate-private. The attribute emits a
plain #[test] fn around the executor, so should_panic, ignore and Result
returns keep working without the macro knowing they exist.

Defaults to Placement::Unbound so tests do not fight over cores when
cargo runs them in parallel.
EOF
)"
```

---

### Task 4: Make the failure modes fail well

**Files:**
- Modify: `glommio-macros/src/lib.rs`
- Create: `glommio-macros/tests/compile_fail.rs`
- Create: `glommio-macros/tests/ui/not_async.rs`
- Create: `glommio-macros/tests/ui/not_async.stderr`
- Create: `glommio-macros/tests/ui/takes_arguments.rs`
- Create: `glommio-macros/tests/ui/takes_arguments.stderr`
- Create: `glommio-macros/tests/ui/unknown_argument.rs`
- Create: `glommio-macros/tests/ui/unknown_argument.stderr`
- Create: `glommio-macros/tests/ui/placement_expression.rs`
- Create: `glommio-macros/tests/ui/placement_expression.stderr`

**Interfaces:**
- Consumes: `expand` from Task 2, `test` from Task 3.
- Produces: no new public API. `expand` gains two rejections: a non-`async fn` and a function taking arguments.

- [ ] **Step 1: Write the failing tests**

`glommio-macros/tests/compile_fail.rs`:

```rust
//! The macro's error messages are the interface a caller meets when they get
//! it wrong, so they are asserted rather than left to drift.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
```

`glommio-macros/tests/ui/not_async.rs`:

```rust
#[glommio_macros::main]
fn not_async() {}

fn main() {}
```

`glommio-macros/tests/ui/takes_arguments.rs`:

```rust
#[glommio_macros::main]
async fn takes_arguments(_x: u32) {}

fn main() {}
```

`glommio-macros/tests/ui/unknown_argument.rs`:

```rust
#[glommio_macros::main(flavour = "current_thread")]
async fn unknown_argument() {}

fn main() {}
```

`glommio-macros/tests/ui/placement_expression.rs`:

```rust
#[glommio_macros::main(placement = if true { 1 } else { 2 })]
async fn placement_expression() {}

fn main() {}
```

Leave the four `.stderr` files absent for now; trybuild writes them on first run.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p glommio-macros --test compile_fail`
Expected: FAIL. `not_async.rs` and `takes_arguments.rs` currently **compile**, which trybuild reports as "expected test case to fail to compile, but it succeeded". The other two already fail, and trybuild reports the missing `.stderr` files as `wip`.

- [ ] **Step 3: Add the two missing rejections**

In `glommio-macros/src/lib.rs`, insert immediately after `let function = parse_macro_input!(item as ItemFn);`:

```rust
    if function.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            &function.sig.fn_token,
            "expected an `async fn`; this attribute builds an executor to run one on",
        )
        .to_compile_error()
        .into();
    }

    if !function.sig.inputs.is_empty() {
        return syn::Error::new_spanned(
            &function.sig.inputs,
            "expected a function taking no arguments; the executor has nothing to pass one",
        )
        .to_compile_error()
        .into();
    }
```

- [ ] **Step 4: Generate and review the expected stderr**

Run: `TRYBUILD=overwrite cargo test -p glommio-macros --test compile_fail`

Then **read each generated `.stderr` file** and confirm the message is the one a confused caller needs. `unknown_argument.stderr` must name `placement`, `name` and `crate`. `placement_expression.stderr` must point at `LocalExecutorBuilder`. If a message reads badly, fix the message in `src/`, delete the `.stderr`, and regenerate.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p glommio-macros`
Expected: PASS — all four ui cases match, plus the tests from Tasks 1-3.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio-macros
git commit -s -m "$(cat <<'EOF'
test: assert the macro's error messages

The message a caller meets when they annotate the wrong thing is part of
the interface, so it is pinned by trybuild rather than left to drift.

Rejects a non-async fn and a fn taking arguments, neither of which the
expansion could do anything sensible with.
EOF
)"
```

---

### Task 5: Re-export from `glommio` behind a default-on feature

**Files:**
- Modify: `glommio/Cargo.toml`
- Modify: `glommio/src/lib.rs`
- Create: `glommio/tests/attribute_macros.rs`

**Interfaces:**
- Consumes: `main` and `test` from `glommio-macros`.
- Produces: `glommio::main` and `glommio::test` at the crate root, available when the default `macros` feature is on.

- [ ] **Step 1: Write the failing test**

`glommio/tests/attribute_macros.rs`. From an integration test the crate is external, so `::glommio` resolves and the real runtime is exercised:

```rust
//! The attribute macros against the real runtime. Unit tests inside `src/`
//! cannot use these — there `::glommio` does not resolve — and should keep
//! using `test_executor!`.

use std::io::Result;

#[glommio::test]
async fn runs_on_a_real_executor() {
    let id = glommio::executor().id();
    glommio::executor().yield_task_queue_now().await;
    assert_eq!(id, glommio::executor().id());
}

#[glommio::test]
async fn spawns_and_awaits() {
    let answer = glommio::spawn_local(async { 42u32 }).await;
    assert_eq!(answer, 42);
}

#[glommio::test]
async fn returns_result() -> Result<()> {
    Ok(())
}

#[glommio::test(placement = Fixed(0))]
async fn honours_placement() {
    assert!(glommio::executor().id() < usize::MAX);
}

#[glommio::test]
#[should_panic(expected = "deliberate")]
async fn propagates_panics() {
    panic!("deliberate");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p glommio --test attribute_macros`
Expected: FAIL — `could not find 'test' in 'glommio'`.

- [ ] **Step 3: Wire up the dependency and the re-export**

In `glommio/Cargo.toml`, change the `[features]` block to:

```toml
[features]
default   = ["macros"]
bench     = []
debugging = []
macros    = ["dep:glommio-macros"]
```

and add to `[dependencies]`, in the existing alphabetical order:

```toml
glommio-macros        = { version = "0.10.0", path = "../glommio-macros", optional = true }
```

The `version` key is required alongside `path`, or `cargo publish` rejects the manifest.

In `glommio/src/lib.rs`, add immediately above `pub mod prelude`:

```rust
#[cfg(feature = "macros")]
#[doc(inline)]
pub use glommio_macros::{main, test};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p glommio --test attribute_macros`
Expected: PASS, 5 tests.

- [ ] **Step 5: Verify the feature actually gates**

Run: `cargo build -p glommio --no-default-features`
Expected: builds, and `glommio-macros` does not appear in the output.

Run: `cargo tree -p glommio --no-default-features -i syn`
Expected: `syn` is not in the tree (or the command reports nothing matching).

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio/Cargo.toml glommio/src/lib.rs glommio/tests/attribute_macros.rs
git commit -s -m "$(cat <<'EOF'
feat: re-export the attribute macros from glommio

#[glommio::test] is what a caller expects to write, so the macros are
re-exported rather than left as a second dependency to remember.

The macros feature is on by default and gates the proc-macro dependency
entirely, so a consumer who does not want syn in their build can turn it
off. Adding a default feature is semver-compatible.
EOF
)"
```

---

### Task 6: Support consumers who do not rename the crate

**Files:**
- Create: `glommio/tests/attribute_macros_unrenamed.rs`
- Modify: `glommio-macros/src/args.rs` (only if the test reveals a gap)

**Interfaces:**
- Consumes: `crate = <ident>` parsing from Task 2.
- Produces: no new API. Proves the escape hatch works end to end.

- [ ] **Step 1: Write the failing test**

`glommio/tests/attribute_macros_unrenamed.rs`:

```rust
//! Anyone depending on `glommio-ng` without renaming it in Cargo.toml has a
//! crate called `glommio_ng`, and the default `::glommio` path in the
//! expansion does not resolve for them. `crate = …` is their escape hatch.
//!
//! This test simulates that by aliasing the crate under a different name and
//! pointing the macro at the alias.

extern crate glommio as glommio_ng;

#[glommio_ng::test(crate = glommio_ng)]
async fn runs_under_an_aliased_crate_name() {
    let answer = glommio_ng::spawn_local(async { 7u32 }).await;
    assert_eq!(answer, 7);
}

#[glommio_ng::test(crate = glommio_ng, placement = Fixed(0))]
async fn accepts_crate_alongside_other_arguments() {}
```

- [ ] **Step 2: Run the test to verify it behaves as expected**

Run: `cargo test -p glommio --test attribute_macros_unrenamed`
Expected: PASS if Task 2's parser is correct. If it FAILS, the failure is in `args.rs` — most likely `crate` not being accepted before other keys, or the placement path not being built from `krate`. Fix `args.rs` and re-run. Do not add a new code path; the parser was written for this case.

- [ ] **Step 3: Commit**

```bash
make fmt
git add glommio/tests/attribute_macros_unrenamed.rs
git commit -s -m "$(cat <<'EOF'
test: cover the crate = escape hatch end to end

Anyone depending on glommio-ng without renaming it has a crate called
glommio_ng, and the expansion's default ::glommio path does not resolve
for them. The test aliases the crate to prove the override works rather
than assuming it.
EOF
)"
```

---

### Task 7: Document the macros and teach the release script to publish two crates

**Files:**
- Modify: `glommio/src/lib.rs` (crate-level docs)
- Modify: `README.md`
- Modify: `scripts/prep-ng-release.sh`

**Interfaces:**
- Consumes: everything above.
- Produces: a release script that publishes `glommio-ng-macros` before `glommio-ng`.

- [ ] **Step 1: Document the macros in the crate docs**

In `glommio/src/lib.rs`, add to the module documentation immediately before the `## Examples` heading:

```rust
//! ## Attribute macros
//!
//! `#[glommio::main]` and `#[glommio::test]` build the executor for you:
//!
//! ```no_run
//! #[glommio::main(placement = Fixed(0))]
//! async fn main() {
//!     glommio::spawn_local(async { println!("hello") }).await;
//! }
//! ```
//!
//! Both accept `placement = <Placement variant>` and `name = "…"`. Anything
//! else the executor offers stays on [`LocalExecutorBuilder`], which remains
//! available inside an annotated function.
//!
//! `placement` takes a variant — `Unbound`, `Fixed(0)`, `Fenced(cpus)` — not
//! an arbitrary expression. Build the executor by hand if you need a computed
//! placement.
//!
//! Both are available under the default `macros` feature. Turning it off drops
//! the proc-macro dependency entirely.
//!
//! If you depend on this crate under a name other than `glommio`, name it:
//! `#[glommio::main(crate = glommio_ng)]`.
```

- [ ] **Step 2: Verify the doc example compiles**

Run: `cargo test -p glommio --doc`
Expected: PASS. The example is `no_run`, so it is compiled but not executed.

- [ ] **Step 3: Add the same note to the README**

In `README.md`, directly after the existing usage example, add:

```markdown
### Attribute macros

```rust
#[glommio::main(placement = Fixed(0))]
async fn main() {
    glommio::spawn_local(async { println!("hello") }).await;
}

#[glommio::test]
async fn it_works() {
    assert_eq!(glommio::spawn_local(async { 2 + 2 }).await, 4);
}
```

Both take `placement` and `name`; everything else lives on
`LocalExecutorBuilder`. Turn off the default `macros` feature to drop the
proc-macro dependency.
```

- [ ] **Step 4: Teach the release script about the second crate**

In `scripts/prep-ng-release.sh`, extend `restore()` to include the new manifest:

```bash
restore() {
  git checkout -- glommio/Cargo.toml examples/Cargo.toml README.md \
    glommio-macros/Cargo.toml 2>/dev/null || true
  rm -f glommio/README.md
}
```

Add to the python block that rewrites `glommio/Cargo.toml`, after the `examples/Cargo.toml` rewrite:

```python
# --- glommio-macros: its own name, and the dependency that points at it ----
p = "glommio-macros/Cargo.toml"
s = open(p).read()
s = re.sub(r'(?m)^name = "glommio-macros"$', f'name = "{name}-macros"', s, count=1)
s = re.sub(r'(?m)^version = ".*"$', f'version = "{version}"', s, count=1)
open(p, "w").write(s)

p = "glommio/Cargo.toml"
s = open(p).read()
s = re.sub(
    r'(?m)^glommio-macros(\s*)= \{ version = "[^"]*", path = "\.\./glommio-macros"',
    f'glommio-macros\\1= {{ version = "{version}", package = "{name}-macros", '
    f'path = "../glommio-macros"',
    s,
    count=1,
)
open(p, "w").write(s)
```

And replace the publish step so the macro crate goes first:

```bash
elif [ "$MODE" = "--publish" ]; then
  echo "== cargo publish: $NG_NAME-macros $NG_VERSION =="
  cargo publish -p "$NG_NAME-macros" --allow-dirty
  echo "== cargo publish: $NG_NAME $NG_VERSION =="
  cargo publish -p "$NG_NAME" --allow-dirty
fi
```

- [ ] **Step 5: Verify the release script still packages cleanly**

Run: `NG_VERSION=0.10.2 ./scripts/prep-ng-release.sh --dry-run`
Expected: both crates package; `glommio-ng` verifies against `glommio-ng-macros`.

**If the dry run fails with an unresolved `glommio-ng-macros` dependency, that is expected and not a bug in the script:** crates.io does not yet have that crate, so the verification build cannot resolve it. Note the failure, report it, and stop — the first real publish must push the macro crate first, after which dry runs resolve normally. Do not work around it with `--no-verify`.

- [ ] **Step 6: Run the full suite and commit**

```bash
make all
git add glommio/src/lib.rs README.md scripts/prep-ng-release.sh
git commit -s -m "$(cat <<'EOF'
docs: document the attribute macros and publish two crates

The release script now rewrites both manifests and publishes the macro
crate first: crates.io rejects a package whose dependency version does
not exist yet, so the order is not optional.
EOF
)"
```

---

### Task 8: Convert one example, as proof the ergonomics actually land

**Files:**
- Modify: `examples/hello_world.rs`

**Interfaces:**
- Consumes: `glommio::main` from Task 5.
- Produces: nothing.

- [ ] **Step 1: Rewrite the example's `main`**

`examples/hello_world.rs` currently builds two executors by hand to demonstrate both construction styles. Keep that demonstration — it is the point of the example — but add the macro form at the top so a reader meets the short version first:

```rust
use futures::future::join_all;
use glommio::prelude::*;
use std::io::Result;

async fn hello() {
    let mut tasks = vec![];
    for t in 0..5 {
        tasks.push(glommio::spawn_local(async move {
            println!("{}: Hello {} ...", glommio::executor().id(), t);
            glommio::executor().yield_task_queue_now().await;
            println!("{}: ... {} World!", glommio::executor().id(), t);
        }));
    }
    join_all(tasks).await;
}

// The shortest way to start an executor. Everything below shows what this
// expands to, and when you would want to write it out by hand instead.
#[glommio::main(placement = Fixed(0))]
async fn main() -> Result<()> {
    hello().await;

    // You can still build executors by hand inside an annotated main --
    // spawning one on another thread, for instance.
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    let handle = builder.name("hello").spawn(|| async move {
        hello().await;
    })?;
    handle.join().unwrap();

    Ok(())
}
```

- [ ] **Step 2: Run it**

Run: `cargo run -p examples --example hello_world`
Expected: ten lines of hello/world output from two executor ids, exit 0.

- [ ] **Step 3: Commit**

```bash
make fmt
git add examples/hello_world.rs
git commit -s -m "$(cat <<'EOF'
docs: show the attribute macro in the hello_world example

A reader meets the short form first and the manual construction second,
which is the order they will want them in.
EOF
)"
```

---

## Definition of Done

- `make all` passes: fmt, clippy, and the full test suite.
- `cargo build -p glommio --no-default-features` builds without `syn` in the tree.
- `cargo test -p glommio-macros` passes, including the four trybuild ui cases.
- `cargo test -p glommio --test attribute_macros --test attribute_macros_unrenamed` passes.
- The branch `feat/attribute-macros` is pushed to `origin`, and **not** to `fork`. No upstream pull request — see the spec's upstream posture.
