# Design: `glommio::select!`

**Date:** 2026-08-20
**Status:** approved in chat
**Scope:** `glommio-macros`, plus one hidden helper and a re-export in `glommio`

## Why

For many projects `select!` is the **last** line of `tokio` in `Cargo.toml`
after everything else is ported. The downstream that drove this week's work
still has 44 sites and has decided to keep them, because `tokio::select!`
expands to a `poll_fn` over its branches and contains no runtime at all — it
costs them nothing functionally.

So this is bought for positioning, and that should be said plainly: the belief
it argues against is "tokio is part of Rust", and `cargo tree` is what settles
that argument.

`futures::select!` is not a substitute. It requires every branch to be
`FusedFuture + Unpin`, which means `.fuse()` on each and often `Box::pin`;
tokio's pins internally and requires neither.

## Surface

```rust
glommio::select! {
    biased;                                    // optional, first
    () = shutdown.cancelled() => break,
    maybe = upstream.recv()   => handle(maybe).await,
}
```

Each future expression is evaluated once and pinned on the stack. **No
`Unpin`, no `FusedFuture`, no `Fuse` wrappers.** The first branch to return
`Ready` wins: its output binds to the pattern, the body runs in the enclosing
async context — so `.await` inside a body works — and `select!` evaluates to
the body's value. Every other future is dropped.

## Scope, from the 44 real sites

Measured rather than assumed: 30 sites with two branches, two with three, one
with five; `biased;` twice; **no `else`, no `if` guards, every pattern
irrefutable**.

Supported: `pat = future => body` branches of any arity, and `biased;`.

Deliberately excluded, each additive later:

- **`if` guards.** Nothing uses them.
- **`else`.** With no guards, no branch can be disabled, so it would be
  unreachable.
- **Refutable patterns.** tokio disables a branch whose pattern does not match
  and keeps polling the others. That is a surprising rule to reimplement for
  zero current users; a non-matching pattern here is a compile error.

## Polling order

`biased;` polls top to bottom.

The default **rotates the starting branch** on each invocation, via a
thread-local counter, so a hot first branch cannot starve a later one. This
differs from tokio, which randomises: same goal, no RNG, and reproducible
between runs, which matters more in a test suite than the randomness does.

The counter lives in `glommio` as a hidden helper, since a proc macro cannot
own runtime state:

```rust
#[doc(hidden)]
pub mod __private {
    pub fn next_select_start() -> usize;
}
```

## Expansion

For two branches, the generated code is what a careful person writes by hand:

```rust
{
    let mut f0 = ::core::pin::pin!(shutdown.cancelled());
    let mut f1 = ::core::pin::pin!(upstream.recv());
    let mut o0 = ::core::option::Option::None;
    let mut o1 = ::core::option::Option::None;

    let which = ::glommio::__private::poll_fn(|cx| {
        // starting offset from the rotation, or 0 when biased
        …
        if let Poll::Ready(v) = f0.as_mut().poll(cx) { o0 = Some(v); return Poll::Ready(0usize) }
        if let Poll::Ready(v) = f1.as_mut().poll(cx) { o1 = Some(v); return Poll::Ready(1usize) }
        Poll::Pending
    })
    .await;

    match which {
        0 => { let () = o0.take().unwrap(); break }
        1 => { let maybe = o1.take().unwrap(); handle(maybe).await }
        _ => ::core::unreachable!(),
    }
}
```

**Why it splits in two:** the futures must be polled inside a closure that
borrows them, but handlers contain `.await` and a non-async closure cannot
await. So the poll phase reports only *which* branch fired and stashes its
output; the handler runs afterwards, in the caller's async context. Every
`select!` has this shape — tokio uses a generated enum where this uses one
`Option` per branch, which avoids naming a type per call site.

Generated identifiers are `__glommio_select_f0`-style rather than `f0`, so
nothing at the call site can collide.

## Why a proc macro

`macro_rules!` cannot synthesise identifiers, so each branch's binding would
need a hand-written arm per arity — roughly 2..=8 expansions, doubled for
`biased`, with the repetition being exactly where a subtle bug would hide.
`glommio-macros` already exists and is already published in lockstep, and
`#[glommio::main]` already sits behind the default-on `macros` feature.

The cost, stated: `select!` is unavailable under `default-features = false`.

## Cancellation safety

Losing futures are **dropped**, not suspended. A branch that had already
consumed from a channel and was about to return loses that work.

This is the classic trap and gets documentation next to the macro rather than
a footnote: prefer branches whose futures are cancel-safe, and where they are
not, hold the future in a variable outside the loop and poll it by `&mut`
instead of recreating it each iteration.

## Testing

- the ready branch wins, and its output binds to the pattern
- a body containing `.await` compiles and runs — the reason for the split
- losing futures are dropped, observed with a `Drop`-counting future
- `biased;` polls top to bottom, shown by two always-ready branches
- the default rotates: two always-ready branches are both observed across
  repeated invocations, which is the starvation the rotation exists to prevent
- arity: a five-branch `select!`, matching the largest real site
- `select!` evaluates to its body's value
- patterns bind: `()`, `_`, and a named binding

`trybuild` for the failures that should be legible: a refutable pattern, and a
branch missing its `=>`.

## Risks

**A macro is a permanent surface.** Every future addition — guards, `else` —
is a compatibility question. The exclusions above are chosen to keep that
surface as small as the evidence allows.

**Rotation is observable.** Two consecutive identical `select!`s may poll in
different orders. That is the point, but a test asserting a fixed order will
need `biased;`, and the documentation should say so.
