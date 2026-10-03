# Testing bapps

How tests are written, checked and run in this workspace. It applies to every
crate; `core/` follows it for code we change and keeps upstream's tests
otherwise.

## The gates

| Command | Runs | When |
|---|---|---|
| `make validate` | `cargo fmt --check`; Clippy with warnings denied on every target; every unit, integration, lab and doc test; rustdoc with warnings denied | before every commit |
| `make integration` | the multicore suite on real pinned threads (`bapps-app`, `--ignored`) | before every commit that touches runtime, RPC or startup code; always before a release |
| CI | both, on every push to `main` and every pull request (`.github/workflows/ci.yml`) | automatically |

Commit only after both gates pass on the whole workspace, not only on the
crate you changed: a change in one crate breaks another's tests more often
than its own. The toolchain is pinned (`rust-toolchain.toml`, Rust 1.92.0)
because compile-fail snapshots and lints depend on it.

## Kinds of tests

| Kind | Where | Use it for |
|---|---|---|
| Deterministic lab | `bapps_trio::testing::Lab`, `bapps_app::lab::run_node` | anything about ordering, cancellation, deadlines, supervision or cross-shard calls. Virtual time, seeded interleavings, deadlock and leak oracles; failing seeds replay exactly. The default. |
| Real executor | `glommio::LocalExecutor` in a `#[test]` | what the lab cannot run: io_uring I/O, Glommio timers, helper threads (`to_thread`), file descriptors. |
| Multicore | `app/tests/multicore.rs`, `#[ignore]`d, run by `make integration` | real pinned threads: startup barrier, executor loss, node stop. |
| README doctests | each crate's README, included with `#[cfg(doctest)]` | the README example compiles and runs, so it cannot drift from the API. |
| Unit tests | `#[cfg(test)]` modules | pure logic: data structures, parsing, accounting. |

Prefer the lab. A lab test runs a scenario under many seeds in milliseconds
and reports a deadlock or a leftover task as a failure; a real-executor test
of the same thing samples one interleaving and needs real time.

## Rules

Each rule comes from a flaky or misleading test this stack actually had.

1. **Wait for an explicit state, never for time.** Readiness through
   `TaskStatus`/`start`; a queue position through `CapacityLimiter::waiting`;
   progress through an `Event`, a `Sequencer` step, a channel or a counter. Not
   `sleep(20ms)` "so it is queued by now": that passes on a quiet machine and
   fails on a loaded one.
2. **Across threads, the other side signals first.** A job on a helper thread
   sends on a channel when it is running before the test acts on it. An
   executor that hands a channel endpoint to another thread waits until the
   peer has connected before it exits (four `shared_channel` tests raced on
   exactly this).
3. **Prove absence structurally.** "It never ran" is shown by its closure
   having been dropped (`Arc::strong_count` back to one), a channel closed or
   a slot released, not by watching for a while and seeing nothing.
4. **Time is virtual where it can be.** In the lab, sleeps cost nothing and
   the clock jumps to the next timer; `TestClock` drives time by hand. Real
   sleeps belong only inside work the test means to cancel.
5. **Every wait is bounded.** Timeouts and loop limits in tests are deadlock
   guards, not synchronization: a broken invariant must fail the test, not
   hang it.
6. **Wait for convergence instead of sampling once** when a value legitimately
   lags (open descriptors after executors exit, a controller's shares): poll
   with a bound; a real leak never converges and still fails. Only do this
   when you know why the value lags; otherwise investigate first.
7. **Explore interleavings.** Run lab scenarios over a range of seeds (10 to
   40) and require every run to be clean (`LabReport::is_clean`), not just
   to return the right value.
8. **Break the code each new test protects.** Before committing a test,
   change the code it guards (skip the call, invert the condition, remove the
   cleanup) and watch the test fail; record what was broken in the commit
   message. If nothing fails, the test protects nothing. Check that the
   breakage was actually applied: formatting can make a textual edit miss.
9. **Test the contract, not the implementation.** Assert what callers rely on
   (outcomes, ordering, what is released), so refactors keep the tests.

## Flaky tests

A flaky test is a bug, in the test or in the code, and it is investigated, not
retried. Run it alone and in the full suite repeatedly to measure the rate;
capture the failing values; check whether it fails at the previous release on
the same machine (load from other processes makes timing tests fail). Fix the
synchronization when the test is wrong; leave the test unchanged and record it
as open when the cause is not understood (see `test_runtime_stats` in
core/CHANGELOG.md). Never raise a timeout or add a retry to make it pass.

## Writing a test for a new capability

1. Write the test first and see it fail for the expected reason.
2. Implement until it passes; then break the implementation (rule 8).
3. Add a row to the crate README's capability table naming the test file.
4. Run both gates; commit with what the test proves and what breaking it did.
