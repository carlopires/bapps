#![cfg(feature = "macros")]
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

// Reads the calling thread's currently-allowed CPU list from procfs. See the
// comment in `honours_placement` for why `/proc/thread-self` and not
// `/proc/self`.
fn cpus_allowed_list() -> String {
    let status = std::fs::read_to_string("/proc/thread-self/status").unwrap();
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .expect("Cpus_allowed_list missing from /proc/thread-self/status")
        .trim()
        .to_string()
}

#[test]
fn honours_placement() {
    // On a single-CPU runner or a cpuset-limited container, the harness
    // thread's inherited affinity mask is already "0" before the executor is
    // ever built. In that case `Fixed(0)` reaching the builder and `Fixed(0)`
    // being silently ignored are indistinguishable from the test's own
    // vantage point, so the assertion below would hold either way -- exactly
    // where CI runs. Check the pre-existing mask first and bail out rather
    // than pass vacuously.
    if cpus_allowed_list() == "0" {
        eprintln!(
            "skipping honours_placement: this thread is already pinned to CPU 0 \
             before the executor is built, so Fixed(0) cannot be distinguished \
             from Unbound here"
        );
        return;
    }

    // Built by hand rather than through `#[glommio::test]` here: the mask
    // check above has to happen on the harness thread, before any executor
    // thread exists, and an inner item cannot be a `#[test]` in its own
    // right (rustc: "cannot test inner items"). The macro expansion this
    // mirrors is exercised by the rest of this file.
    glommio::LocalExecutorBuilder::new(glommio::Placement::Fixed(0))
        .make()
        .expect("failed to create the glommio executor")
        .run(async {
            // Placement::Fixed(0) binds this executor's thread to CPU 0, and
            // the test body runs on that thread -- so the kernel's own view
            // of the affinity mask is the check. An ignored placement would
            // leave every CPU allowed.
            let allowed = cpus_allowed_list();
            assert_eq!(allowed, "0", "executor thread is not pinned to CPU 0");
        });
}

#[glommio::test]
#[should_panic(expected = "deliberate")]
async fn propagates_panics() {
    panic!("deliberate");
}
