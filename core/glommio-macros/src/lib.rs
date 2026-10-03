//! Attribute macros for glommio.
//!
//! The expansion names the runtime crate by path, so everything here emits
//! `::glommio::…` unless the caller overrides it with `crate = <ident>`.

mod args;
mod select;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Builds a `LocalExecutor` and runs an async `main` on it.
#[proc_macro_attribute]
pub fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, "Unbound", false)
}

/// Builds a `LocalExecutor` and runs an async test on it.
///
/// Emits a plain `#[test]` function, so `#[should_panic]`, `#[ignore]` and
/// `Result` returns compose without this macro knowing about them.
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, "Unbound", true)
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

    if function.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            function.sig.fn_token,
            "expected an `async fn`; this attribute builds an executor to run one on",
        )
        .to_compile_error()
        .into();
    }

    if let Some(unsafety) = function.sig.unsafety {
        return syn::Error::new_spanned(
            unsafety,
            "expected a safe `async fn`; a `main`/`test` entry point cannot be \
             `unsafe` because the executor is what calls it, not the caller. \
             Move the unsafe code inside the body instead",
        )
        .to_compile_error()
        .into();
    }

    if let Some(constness) = function.sig.constness {
        return syn::Error::new_spanned(
            constness,
            "expected a non-`const` `async fn`; `const` and `async` cannot be \
             combined, so a `main`/`test` entry point cannot be `const` either",
        )
        .to_compile_error()
        .into();
    }

    if let Some(abi) = &function.sig.abi {
        return syn::Error::new_spanned(
            abi,
            "expected a plain `async fn`; an explicit ABI is not meaningful on a \
             `main`/`test` entry point, which the executor calls directly",
        )
        .to_compile_error()
        .into();
    }

    if !function.sig.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &function.sig.generics,
            "expected a non-generic `async fn`; `main`/`test` entry points cannot be \
             generic because nothing instantiates the parameters. Build the \
             executor with `LocalExecutorBuilder` inside a non-generic wrapper if \
             you need one",
        )
        .to_compile_error()
        .into();
    }

    if let Some(where_clause) = &function.sig.generics.where_clause {
        return syn::Error::new_spanned(
            where_clause,
            "expected a `where`-clause-free `async fn`; `main`/`test` entry points \
             cannot be generic because nothing instantiates the parameters. Build \
             the executor with `LocalExecutorBuilder` inside a non-generic wrapper \
             if you need one",
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

    let attrs = &function.attrs;
    let vis = &function.vis;
    let ident = &function.sig.ident;
    let output = &function.sig.output;
    let body = &function.block;

    let krate = &args.krate;
    let placement = &args.placement;
    // `name` is rejected rather than emitted: the expansion builds the executor
    // with `make()`, which runs it on the CALLING thread and never reads the
    // builder's name -- only `spawn()` does, where the name becomes the name of
    // the thread it creates. Threading the argument through anyway would accept
    // an option and silently drop it.
    if let Some(name) = &args.name {
        return syn::Error::new_spanned(
            name,
            "`name` cannot be honoured here: this attribute builds the executor on the calling \
             thread with `make()`, which has no thread of its own to name. Use \
             `LocalExecutorBuilder::new(placement).name(..).spawn(..)` if you want a named \
             executor thread",
        )
        .to_compile_error()
        .into();
    }
    let test_attr = if is_test {
        quote!(#[::core::prelude::v1::test])
    } else {
        quote!()
    };

    quote! {
        #test_attr
        #(#attrs)*
        #vis fn #ident() #output {
            #krate::LocalExecutorBuilder::new(#placement)
                .make()
                .expect("failed to create the glommio executor")
                .run(async move #body)
        }
    }
    .into()
}

/// Races several futures, running the body of whichever finishes first.
///
/// ```ignore
/// glommio::select! {
///     biased;                                   // optional: poll top to bottom
///     () = shutdown.cancelled() => break,
///     maybe = upstream.recv()   => handle(maybe).await,
/// }
/// ```
///
/// Each future is evaluated once and pinned on the stack, so branches need
/// neither `Unpin` nor `FusedFuture` and no `.fuse()` wrappers. The winning
/// branch's output binds to its pattern, its body runs in the caller's async
/// context -- `.await` inside a body works -- and `select!` evaluates to that
/// body's value.
///
/// # The losing futures are dropped
///
/// Not suspended: dropped. A branch that had already taken an item from a
/// channel and was about to return loses it.
///
/// **All** the branch futures are dropped before any handler runs, as tokio
/// does, so a handler may use whatever its own branch borrowed:
///
/// ```ignore
/// msg = upstream.recv() => process(&mut upstream, msg).await,
/// ```
///
/// The compiler ends a borrow at its last use unless the borrower has drop
/// glue -- any field that needs dropping, not a hand-written `Drop` impl. That
/// covers nearly every `async fn` future capturing a borrow alongside anything
/// droppable, so holding the futures across handlers rejects ordinary code
/// while leaving contrived examples compiling.
///
/// A handler cannot bind a reference *into* a dropped future's output, but that
/// is unrepresentable anyway: stable Rust has no lending futures, so an
/// `Output` can only borrow from something that outlives the future.
///
/// Where a future is not cancel-safe, hold it in a variable outside the loop
/// and poll it by `&mut` rather than recreating it each iteration.
///
/// # Polling order
///
/// By default the starting branch rotates between invocations, so a branch
/// that is always ready cannot starve the ones after it. `biased;` polls top
/// to bottom instead, which a test asserting a fixed winner will want.
///
/// # Not supported
///
/// No `if` guards, no `else`, and patterns must be irrefutable. Nothing in the
/// codebases that asked for this uses them, and each is additive later.
#[proc_macro]
pub fn select(input: TokenStream) -> TokenStream {
    select::expand(input.into()).into()
}
