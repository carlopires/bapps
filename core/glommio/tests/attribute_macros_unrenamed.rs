#![cfg(feature = "macros")]
//! Anyone depending on `glommio-ng` without renaming it in Cargo.toml has a
//! crate called `glommio_ng`, and the default `::glommio` path in the
//! expansion does not resolve for them. `crate = …` is their escape hatch.
//!
//! This test verifies that the `crate = …` argument parses and compiles.
//! The proof that the argument is actually *honoured* (i.e., reaches the
//! expansion and changes the emitted path) lives in the trybuild suite:
//! `glommio-macros/tests/ui/crate_override_is_honoured.rs` emits a path
//! referencing a non-existent crate and fails to compile — proving the
//! override is in effect rather than ignored.

extern crate glommio as glommio_ng;

#[glommio_ng::test(crate = glommio_ng)]
async fn runs_under_an_aliased_crate_name() {
    let answer = glommio_ng::spawn_local(async { 7u32 }).await;
    assert_eq!(answer, 7);
}

#[glommio_ng::test(crate = glommio_ng, placement = Fixed(0))]
async fn accepts_crate_alongside_other_arguments() {}

#[glommio_ng::test(crate = glommio_ng)]
async fn select_resolves_the_crate_it_is_told_to() {
    // The same macro-boundary problem as the attributes: a caller reaching
    // glommio under another name, or through a facade, cannot resolve a
    // hardcoded `::glommio` path in the expansion.
    let value = glommio_ng::select! {
        crate = glommio_ng;
        v = async { 5u32 } => v,
        _ = glommio_ng::timer::sleep(std::time::Duration::from_secs(30)) => 0,
    };
    assert_eq!(value, 5);
}

#[glommio_ng::test(crate = glommio_ng)]
async fn select_takes_crate_and_biased_together() {
    let value = glommio_ng::select! {
        crate = glommio_ng;
        biased;
        v = async { 1u32 } => v,
        _ = async {} => 2,
    };
    assert_eq!(value, 1);
}
