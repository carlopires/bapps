# Upstreaming: Where, What, and Whether

**Date:** 2026-08-02, last updated 2026-09-07
**Short answer:** yes, and the first one has landed.

**2026-09-07: `#35` merged.** The io-uring migration went in as `8c881a5`, the
first of ours to land upstream. That satisfies the gate this document set, which
was one PR merged rather than one reviewed, so **opening further PRs is allowed
now**, two at a time, smallest first, in the staged order below.

Three people are active on our PRs: `utilitydelta`, `humb1t`, and `vlovich`, who
has asked to be made a maintainer and has reviewed #33 and #32 in detail. Both
of those reviews were right and both cost less to accept than to argue. See
[What review changed](#what-review-changed).

`main` moves now, so PR branches need rebasing rather than leaving.

## The landscape changed

**`DataDog/glommio` is abandoned.** Last commit 2025-04-21. Sixteen open pull
requests, the oldest from 2021. Issue
[#707 "Call for glommio maintainers"](https://github.com/DataDog/glommio/issues/707)
is where the community organised itself.

**`glommio/glommio` is the live fork.** A new org, active through June 2026,
already merging the DataDog backlog (musl CI, dependency updates, a stdin fix
from 2022). Glauber Costa, the original author, has said in #707 that he is happy
to move the crates.io name to the fork "as long as there is a clear leader".

## Upstream activity

**Checked 2026-09-07, and it is active.**

| | |
|---|---|
| last commit to `main` | 2026-09-07 (`8c881a5`, our #35) |
| merged from us | #35 |
| open from us | #29, #30, #31, #32, #44 |
| closed | #33, superseded by #44 |
| reviewers engaged | `utilitydelta`, `humb1t`, `vlovich` |

Practical consequences, revised:

- **The gate is open.** It was one PR merged, not one reviewed. #35 merged.
  Open further PRs in the staged order below, two at a time.
- **This fork is still the artifact.** One merge is not five. Consumers should
  keep depending on `dahankzter/glommio`.
- **Rebase, do not wait.** `main` moves now, and four of our branches needed a
  rebase the day #35 landed.
- **Answer reviews, and concede when they are right.** Both substantive reviews
  we have had found real defects. Arguing would have cost more than fixing.

## What review changed

Two reviews by `vlovich`, both correct, both worth recording because they
overturned things this document previously asserted.

**#33, the timing wheel.** The handle named a wheel position, and a cascading
wheel moves timers between positions. That one mistake produced the hash map,
the dead `generation` field, and a next-expiry that scanned the population.
Rather than patch it, three implementations were built and measured. The wheel
that survived is [#44](https://github.com/glommio/glommio/pull/44); #33 is
closed. See
[docs/investigations/timer-comparison](investigations/timer-comparison/README.md).

Worth carrying forward: **the asymptotic argument for a wheel did not appear**
at any timer population glommio actually holds. The win is a constant. Claiming
a complexity class we could not demonstrate is what made #33 easy to attack.

**#32 and issue #448.** We claimed `f28a619` fixed #448 at the root. It does
not. The root is that the task header is never deallocated, so the
`Arc<SleepNotifier>` in it cannot drop, and `vlovich` fixed that in
[#40](https://github.com/glommio/glommio/pull/40). Ours stops tasks holding the
notifier, which relieves the fd symptom and leaves the memory leak alone. #32 is
reframed as the performance change it is, and
`docs/investigations/issue_448/README.md` still needs the same correction.

## Divergence

**Merged 2026-08-02.** Their 15 commits are now in; `upstream` points at
`glommio/glommio`. We are ahead only.

```
before the merge:
  ours ahead of community/main:  131 commits
  community/main ahead of ours:   15 commits
```

Their 15 included some that touched the same code we do:

| commit | why it matters to us |
|---|---|
| `4424815` Bump waker refcount from i16 to i32 | **Conflicts with our `Header` layout** |
| `59f5f55` Make the gate closure more robust | executor internals |
| `1981af4` `TcpStream::into_accepted` | new API we do not have |
| musl CI, dependency updates | infrastructure |

### The one real conflict, resolved

They widened `references` to `AtomicI32`. We had put `task_queue_index: u32` into
`Header`'s four bytes of end padding to keep it at 40 bytes. Combined, the header
grew to 48.

**Fixed** by narrowing `executor_id` from `usize` to `u32` (`ac43600`), it is a
registry index that is only ever compared or looked up, so four billion
executors is ample. `Header` is back to 40 bytes carrying their wider refcount.
Kept as a separate commit from the merge so the type change is reviewable on its
own.

Other resolutions: their `nightly` cfg predicate with our `LOCAL_EX.is_set`
guard and `spawn_internal` rename; their manifest formatting (they use `taplo`,
config now in the tree) with our `io-uring` dependency and allocator
`check-cfg`s; their rewritten CI wholesale, with the branch filter widened to
cover `master`; and the deletion of `tests/linters.rs`, which they replaced with
direct fmt and clippy steps in CI.

## What we have that they do not

Verified against `community/main`, not assumed:

| | what | value | self-contained? |
|---|---|---|---|
| 1 | **`3f4d113`**, no process abort when a task panics in a custom queue | closes DataDog **#689** | yes, small |
| 2 | **`f28a619`**, `executor_id` in the header instead of `Arc<SleepNotifier>` | fixes **#448** at the root; 4,350 → 28 ns spawn at 64 executors | yes, but conflicts as above |
| 3 | **Timing wheel** (`timer/timing_wheel.rs`, `staged_wheel.rs`) | 17.7 → 10.3 ns at 100k timers, **zero unsafe** | large but standalone |
| 4 | **Public `LocalExecutor::spawn`** | closes DataDog **#695** | yes, small, additive |
| 5 | **`8fc2a8e`**, zero-sized schedule closure | −32% task switch | depends on 2 |
| 6 | **`f15c68c`**, cache-domain-aware placement | avoids a +69% cross-L3 penalty | yes; adds a public field |
| 7 | **Miri coverage of the task lifecycle** | first UB checking on `task::raw` | yes |
| 8 | **`32dd7dd`**, 17 broken doctests repaired | hygiene | ours to fix; the break was ours |
| 9 | **`docs/investigations/`** | answers DataDog **#641** ("what's the problem with performance") with measurements | docs only |
| 10 | **iou/uring_sys retirement** | −3,042 lines, −100 `unsafe` | **blocked**, see below |

They still vendor `iou` and `uring_sys`, and still have the old `timer_impl`
without a wheel, so 3 and 10 are not duplicated effort.

**Added since this plan was written:**

| | what | value | where |
|---|---|---|---|
| 11 | **`TcpStream::send_file`**, `IORING_OP_SPLICE` through a per-call pipe | the only way to serve a file without the bytes entering the process. **Measured, and the measurement is unflattering:** 1.8x-2.8x more total CPU than `read_at` + `write_all` on a warm cache, 3.5x on `O_DIRECT`; it wins only on a cold cache at one pipe-load or less | on `master` since 2026-09-02 |
| 12 | ~~`fd43b32`~~, **superseded**; see below |, | deleted |

Item 11 is not upstream material yet, and possibly not at all: a feature whose
own ladder says it costs more CPU than the thing it replaces needs the
`F_SETPIPE_SZ` and `IOSQE_IO_LINK` work first, or an honest doc comment
carrying the numbers, it currently has the latter.

**Item 12 is gone, and so is what it fixed** (`147aa0e`, 2026-09-02). Porting
the eventfd change out of #35 meant first asking whether it still did anything,
and it does not: [#32](https://github.com/glommio/glommio/pull/32) puts
`executor_id` in the task header instead of an `Arc<SleepNotifier>`, which is
what actually fixes #448, nothing outlives the executor holding its notifier
alive, so the refcount reaches zero on its own. Measured, not argued: twenty
executor lifecycles grow the descriptor count by twenty before either change,
and by nothing with the header change alone and `close_eventfd` deleted.

`close_eventfd` was therefore a workaround that outlived its premise, and it
was not inert while it did, a `shared_channel` peer on another executor can
legitimately still hold that `Arc`, and closing the descriptor under it is what
created the use-after-close. Deleting the mechanism removed the race instead of
locking around it. **So there is no separate #448 PR**, contrary to what was
said on #35; the correction is posted there. #448 rides on #32.

## What has been submitted

Opened 2026-08-02, smallest and most obviously correct first, so review trust
could be established before anything large arrived. Status as of 2026-09-07:

| PR | What | Status |
|---|---|---|
| [#29](https://github.com/glommio/glommio/pull/29) | don't abort the process when a task panics in a custom queue | open, rebased, mergeable. Closes their #689 |
| [#30](https://github.com/glommio/glommio/pull/30) | non-panicking `LocalExecutor::spawn` | open, rebased, mergeable. Closes their #695 |
| [#31](https://github.com/glommio/glommio/pull/31) | cache-domain-aware placement | open, rebased, mergeable. New public field on `CpuLocation` is a semver consideration for them |
| [#32](https://github.com/glommio/glommio/pull/32) | indices in the task header, not owned references | open, reviewed, answered. **Not** the #448 fix, see [What review changed](#what-review-changed) |
| [#33](https://github.com/glommio/glommio/pull/33) | timing wheel replacing the timer `BTreeMap` | **closed**, superseded by #44 |
| [#35](https://github.com/glommio/glommio/pull/35) | retire the vendored iou / uring_sys | **merged 2026-09-07** as `8c881a5` |
| [#44](https://github.com/glommio/glommio/pull/44) | slab-backed wheel replacing the timer `BTreeMap` | open, mergeable. Replaces #33 |

The history of #35, kept because the pattern is worth remembering:

**#35 was blocked and no longer is.** It depended on an `io-uring` accessor that
lived on a personal fork; that **merged upstream as
[tokio-rs/io-uring#404](https://github.com/tokio-rs/io-uring/pull/404) on
2026-08-09** as `CompletionQueue::status()`. Taken out of draft 2026-08-10; the
dependency points at `tokio-rs/io-uring` directly.

**Fully unblocked as of 2026-08-11**, when `io-uring` released 0.7.14 carrying
the accessor. The dependency is now a plain `io-uring = "0.7.14"`, no git
dependency, nothing preventing publication. See
[investigations/iou-replacement](investigations/iou-replacement/).

**Reviewed 2026-08-31, and it needs surgery before it can land.** Two blockers,
both fair:

1. **`f3b8bc5` carries a `SleepNotifier` change that has nothing to do with
   retiring `iou`**, and does not mention it in the commit message. It replaces
   `eventfd: std::fs::File` with `Mutex<Option<File>>` and adds
   `close_eventfd()`, that is the **#448 fix**, smuggled into a refactor. It
   also introduced a use-after-close: `notify()` read the descriptor number
   under the lock, released it, then wrote, so a concurrent `close_eventfd()`
   could free the number and let the kernel hand it to the next `open` before
   the write landed. Fixed on `fix/notify-eventfd-race` (`fd43b32`), with a
   test that reproduces it by pinning every participant to one CPU.
   **The resolution for #35 is removal, not repair:** upstream has no
   `close_eventfd` at all, so taking the eventfd change out of `f3b8bc5`
   leaves the PR with no race to fix and restores their plain `File`. #448 then
   goes as its own PR carrying the `Mutex`, the `close_eventfd`, the locked
   `notify()` and the regression test, which is the split the reviewer asked
   for.
2. **The vendored C is not actually retired.** `build.rs` on the PR branch
   still clones the submodule and compiles `liburing` plus `rusturing.c`, which
   nothing links any more, and `.gitmodules` is still there. Our `master`
   fixed this long ago, 14-line `build.rs`, no `cc` dependency, no
   `.gitmodules`, no `rusturing.c`, so this is a port from `master`, not new
   work. Note that `master` itself still carries `submodules: recursive` in
   `.github/workflows/ci.yml` in three places; dead, and worth removing in the
   same pass.

Six non-blocking follow-ups came with it: orphaned `#[allow]` stacks on
`mod parking`/`mod nop`, a README that still says kernel 5.8 while the code says
5.6, a missing const size/align assert on the `KernelTimespec` cast, a
discarded `submit_and_wait` count that deserves a `debug_assert_eq!`, a dangling
doc reference, and an opcode-probe failure string memoized even for a transient
`EMFILE`.

### Not yet submitted

- **Miri task-lifecycle tests**, plus the `test_executor_id` hook they require.
  Genuinely depends on #32 landing first, it uses the new `spawn_local`
  signature.
- **The investigations**, or a condensed version. DataDog **#641** has been open
  since 2025 asking exactly what these answer. Deliberately held: see the note
  about not piling on above, and
  [investigations/io-path/monoio-gap.md](investigations/io-path/monoio-gap.md)
  for why that one needs framing as measurement rather than correction.

## Found by sweeping outward (2026-09-04)

Everything above was verified by walking the queue and checking each entry still
exists on `master`. That says nothing about the reverse, whether every commit on
`master` has a home here, and the catch-all row that claimed the rest was "fork
infrastructure" was carrying six upstream-applicable bug fixes.

Each was confirmed present in upstream code, not assumed:

| Commit | Defect upstream | Size | Verdict |
|---|---|---|---|
| `57ca3c6` | `MSG_ZEROCOPY` set on every socket send, still there in `sys/uring.rs` on `main` today. It needs `SO_ZEROCOPY` to do anything, so it is inert until a caller sets it, and then it arms zero-copy against a send path that frees its buffer at the send CQE | 13/6 lines | **Queue first.** Small, a real bug, one file |
| `4f6d506` | a peer whose executor is gone is not treated as disconnected | 82 lines | Queue. Self-contained, no API change |
| `639af85` | `accept` toggles `O_NONBLOCK` on every call, `net/mod.rs` | 35 lines of src | Queue. Strip the example and the measurement doc first |
| `81a93eb` | `GlommioError` is not `Sync`: it carries the panic payload as `Box<dyn Any + Send>` | 69 lines | **Issue first.** Breaking, so the semver call is theirs |
| `2c57d3c` | `to_socket_addrs()?.next().unwrap()` on the executor thread: blocking DNS, plus a panic on empty resolution | 342 lines | Queue, but feature-sized |
| `49537c6` | `CpuIter` and `CpuSetGenerator` are `pub` but not re-exported, so they are reachable and unnameable | mixed | **Split.** Only the placement half applies; the rest is ours |

`57ca3c6` also survived the 2026-09-07 merge of upstream `main` into `master` as
a deliberate conflict resolution: upstream's version of that match arm still sets
`MSG_ZEROCOPY`, ours does not, and ours won.

**The lesson is about method.** Verifying inward, that every commit the queue
names still exists, is not the same as verifying outward, that every commit has a
home. Only the second one finds work you forgot you did.

## Plan for the 2026-08-19/20 additions

Everything below was written after the first PRs above were opened, and none of
it has been offered upstream. The constraint has not changed: **their scarce
resource is review capacity, not code.** Six of ours have sat unreviewed since
2026-08-02. Arriving with nine more would make that worse, not better.

So this is staged, and each stage has a gate that must open before the next
begins.

### Stage 0, three bug reports, no patches (do this first, unconditionally)

Issues cost a maintainer minutes to read and nothing to review. All three are
defects in **their** code, found while working here, and two of them block
things they want.

| Report | Why it matters to them |
|---|---|
| A panicking `spawn_blocking` closure hangs its caller forever and permanently costs the pool a worker | Live in canonical 0.9.0 and their `main`. Four panics exhaust a four-thread pool. The obvious fix is worse than the bug: the awaiting side calls `assume_init` on memory a panicked closure never wrote |
| `build.rs` runs `./configure` inside the vendored `liburing`, mutating the crate's own source directory | `cargo package` rejects the tarball outright, so **they cannot publish 0.10 under any name.** DataDog's `configure()` runs it in `OUT_DIR`; the regression is theirs alone |
| `LocalExecutorBuilder::name` is silently ignored by `make()` | Only `spawn()` reads it. Every caller building with `make()` sets a name that goes nowhere |

Report the `configure` one as a bug, **not** as our patch: ours copies the whole
tree because the liburing revision we vendored needs `Makefile.common`, and
theirs may not. Tell them what breaks and let them pick the fix.

**Gate to Stage 1: any human reply on any of the three.** That is the signal
that review capacity exists. Without it, nothing else is worth queueing.

**Status 2026-09-02: filed, and the gate had already opened by another
route**, on #35, not on an issue. See
[2026-08-31: a review arrived](#2026-08-31-a-review-arrived-and-the-gate-is-open).

| Issue | What | Verified |
|---|---|---|
| [#36](https://github.com/glommio/glommio/issues/36) | `cargo package` fails verification; `build.rs` runs `./configure` in-tree | first-hand on `1981af4`: `error: failed to verify package tarball` |
| [#37](https://github.com/glommio/glommio/issues/37) | a panicking `spawn_blocking` closure hangs its caller **and** breaks the pool | both reproduced; the second is worse than this plan recorded |
| [#38](https://github.com/glommio/glommio/issues/38) | `LocalExecutorBuilder::name` ignored by `make()` | reproduced; `make()` reads every builder field except `name` |

**#37 was under-stated here.** The old entry said the panic "permanently costs
the pool a worker". It does, but the default pool is `PoolPlacement::Unbound(1)`
one thread, so the first panic drops the last receiver and the request
channel disconnects. Every later `spawn_blocking` on that executor then panics
the *executor* thread at `blocking.rs:255` with `failed to enqueue blocking
operation: "SendError(..)"`. Not a degraded pool: a dead one, and it takes the
caller with it.

### Stage 1, one small bug fix, and it is not open yet

| PR | Size | Notes |
|---|---|---|
| `spawn_blocking` panic fix (`dcab422`) | +52/−12, 1 file | Deletes an `unsafe` block, an `assume_init` and an `Arc::try_unwrap(..).expect("leak")`. Best review-to-value ratio we have. **Code applies cleanly to their `main`**; only its tests need re-homing, since upstream has no `glommio/tests` directory at all, everything there is an inline `#[cfg(test)]` module |
| ~~Kernel probe returns an error instead of calling `exit(1)`~~ |, | **Already in #35** as `d873c54`, and by patch-id identical to `cd7c9d3` |

**The kernel probe fix resolved itself.** An earlier draft said it had to be
rewritten against their `iou` probe, or gated behind #35. Neither happened: it
rode along inside #35 when that branch was reworked on 2026-09-02, written
against the `io-uring` `Probe` the same PR introduces. One less thing to carry.

**And the remaining fix has not been offered.** Its rationale is filed as
[#37](https://github.com/glommio/glommio/issues/37), which is the cheap half;
the patch waits on the gate below rather than adding a seventh open PR to a
queue of six.

**Gate to Stage 2: something merged.** A merge proves the pipeline works end to
end. A review does not, #35 has had a good one and is still open. Anything
larger before a merge is speculative.

### Stage 2, API additions, smallest commitment first

Each is additive, each stands alone, and each is sized to be reviewable in one
sitting. Ordered by how much a maintainer is being asked to take on, not by how
much we want it.

1. **`spawn_blocking_send`** (~150 lines). One method. No new module.
2. **`future::timeout`** (~120 lines). Carries a question they must answer, and
   the PR should ask it plainly: by the `try_` convention the general form
   should own the name `timeout` and the `Result`-aware one should be
   `try_timeout`. We split by module to avoid breaking their callers. Their
   call whether to take the rename at their next breaking release.
3. **`Interval` + `MissedTickBehavior`** (~400 lines). Rebase risk if #33
   lands first, since both touch `timer/`.
4. **`sync::Mutex` + `Semaphore::is_closed`** (~400 lines). `Mutex` needs
   `is_closed`, so they ship together.
5. **`sync::OnceCell`** (~250 lines).
6. **`channels::oneshot` + `channels::watch`** (~600 lines). Two small channels
   with one review context.
7. **`channels::broadcast`** (~700 lines). Its own PR: the semantics are the
   substance, and the design doc travels with it.
8. **`CancellationToken`** (~350 lines). Last of the additions because it is
   the one with an argument attached, `!Send` means it cannot carry a
   cross-shard shutdown, and a maintainer may reasonably want the `Send` one
   instead.
9. **`ConnectedSender::into_foreign`** (~250 lines). Touches `shared_channel`,
   so it wants a rested reviewer rather than a ninth-in-a-row one.

**Never more than two of these open at once.** The failure mode to avoid is
recreating today's queue of six.

### Stage 3, the structural ones

- ~~**Deleting the vendored `liburing` and the five C files.**~~ **Done, and
  not as a separate stage.** It went into #35 itself on 2026-09-02, because a
  reviewer pointed out that a PR titled "retire the vendored iou" which leaves
  `build.rs` compiling liburing has not done what it says. The finding that
  made it safe held up: no `extern "C"` block and no reference to a C symbol
  survives in `glommio/src` after the migration, checked before deleting
  anything. `cargo package -p glommio` verifies as a result, which it could not
  before, see [#36](https://github.com/glommio/glommio/issues/36).
- **`#[glommio::main]` / `#[glommio::test]`.** Raise as an **issue**, never as
  an unsolicited PR. Merging it obliges them to publish and maintain a second
  crate on crates.io under their name, permanently. That is a policy decision,
  not a code review, and no amount of test coverage makes it for them. The
  branch is ready (`upstream/macros-proposal`) if they say yes.

### The six already in flight come first, mostly

They are older, they are already queued, and asking for attention on new work
while six sit unreviewed is how a contributor becomes noise. They are also
load-bearing for what follows:

| Open PR | Touches | Consequence for the queued series |
|---|---|---|
| [#35](https://github.com/glommio/glommio/pull/35) retire vendored `iou` | `sys/uring.rs`, **+474/−258** | Now *carries* the kernel-probe fix and the C deletion outright, so it gates neither |
| [#33](https://github.com/glommio/glommio/pull/33) timing wheel | `timer/mod.rs`, **+8** | Barely interacts with queued #8 and #9 |
| [#29](https://github.com/glommio/glommio/pull/29), [#30](https://github.com/glommio/glommio/pull/30), [#32](https://github.com/glommio/glommio/pull/32) | `executor/mod.rs` | Same file as queued #1 and #2, different regions |
| [#31](https://github.com/glommio/glommio/pull/31) cache-domain placement | `executor/placement/` | No overlap |

**The exception used to be that the two bug fixes need not wait.** One of them
the kernel probe, is in #35 already, and it was rewritten against the
`io-uring` probe rather than their `iou` one, so the fork in the road that this
section used to describe is closed. Queued #1, the `spawn_blocking` panic fix,
is the only opener left, and its rationale is now filed as
[#37](https://github.com/glommio/glommio/issues/37) rather than asserted in a
PR body.

**Whether even that should be offered before something merges is a real
question, and as of 2026-09-02 the answer is no.** The gate below is *one
merged*, not one reviewed. Six PRs are open, one of them has just been reworked
in response to a review, and the reviewer received a force-push, two comments
and three issues inside half an hour on 2026-09-02. Adding a seventh PR on top
of that is the failure mode this document was written to avoid, whatever the
merits of the patch.

Everything else in the series waits for the existing six to move. If they never
do, the queue was never the problem.

### The series

**Re-verified 2026-09-02 against `master`, commit by commit.** Every commit
named below still exists there; two that this table used to queue turned out to
be in flight already, so the series is shorter by two than it was.

Numbered in the order they should be offered. The number is the order, **not
permission to open them all**, the two-at-a-time rule below still holds.

Branches are `upstream/NN-name`, cut from `upstream/main`, pushed to the
`fork` remote (`dahankzter/glommio-community`), PR'd against `glommio/glommio`
`main`.

#### Already with them, and complete

Nothing further to send for any of these. What each carries from `master`:

| PR | from `master` | contains |
|---|---|---|
| [#29](https://github.com/glommio/glommio/pull/29) | `3f4d113` | no process abort when a task panics in a custom queue |
| [#30](https://github.com/glommio/glommio/pull/30) | `fix/issue-695-public-spawn` lineage | non-panicking `LocalExecutor::spawn` |
| [#31](https://github.com/glommio/glommio/pull/31) | `f15c68c` | cache-domain-aware placement |
| [#32](https://github.com/glommio/glommio/pull/32) | `f28a619` | `executor_id` in the task header, **and this is the #448 fix** |
| [#33](https://github.com/glommio/glommio/pull/33) | `81769d6` + follow-ups | timing wheel replacing the timer `BTreeMap` |
| [#35](https://github.com/glommio/glommio/pull/35) | migration + `cd7c9d3` + `322fb8d` | iou retirement, kernel-probe error, the C deletion, `test_spin` |

**Two former series entries live here now.** Old #2 (kernel probe, `cd7c9d3`)
and #35's `d873c54` are the *identical patch*, same patch-id, `a92e93a…`. Old
#14 (delete the vendored C, `322fb8d`) went in as `3db5be3` on 2026-09-02,
minus the fork-only `scripts/prep-ng-release.sh`. Neither needs a branch of its
own any more, and #14's "hard-gated on #35" note is obsolete because it is now
*part of* #35.

#### Queued

| # | PR | from `master` | Size | Apply after | What a reviewer must decide |
|---|---|---|---|---|---|
| 1 | fix: don't hang the caller when a `spawn_blocking` closure panics | `dcab422`, tests re-homed | +52/−12, 1 file |, | Nothing. A bug fix that deletes an `unsafe`; rationale filed as [#37](https://github.com/glommio/glommio/issues/37) |
| 2 | feat: add `spawn_blocking_send` | `61bb142` | +228, 1 file | 1 (shares a test module) | Whether the pool should hand back a `Send` future at all |
| 3 | feat: add an async `Mutex` | `133a565`, incl. `Semaphore::is_closed` | +257, 3 files |, | Whether `lock()` returns `Result`, as their `RwLock` does |
| 4 | feat: add `sync::OnceCell` | split of `b67355f` | ~250, estimated | 3 (`sync/mod.rs`) | Nothing beyond wanting it |
| 5 | feat: add `channels::oneshot` | `c362fb0` | +226, 2 files |, | Nothing beyond wanting it |
| 6 | feat: add `channels::watch` | split of `b67355f` | ~350, estimated | 5 (`channels/mod.rs`) | Nothing beyond wanting it |
| 7 | feat: add `channels::broadcast` | `f86bb8a` + its design doc | +482, 2 files | 6 | The semantics. Mirrors tokio deliberately; `recv` returns a bespoke `RecvError` because `Lagged` is neither closed nor would-block |
| 8 | feat: add `future::timeout` | split of `8a867b9` | ~120, estimated |, | **The naming.** By the `try_` convention the general form should own `timeout` and theirs should become `try_timeout`. The rename is theirs to take at a breaking release |
| 9 | feat: add `timer::Interval` and `MissedTickBehavior` | split of `8a867b9` | ~400, estimated | 8, and #33 | Whether all three missed-tick behaviours are wanted |
| 10 | feat: add a hierarchical `CancellationToken` | `5bf58cd` | +293, 2 files | 4 (`sync/mod.rs`) | **The scope argument.** `!Send` means it cannot carry a cross-shard shutdown |
| 11 | feat: send to a shared channel from a thread with no executor | `986213e` | +302, 1 file |, | That the handle is deliberately not `Clone`, because the buffer is strictly single-producer |
| 12 | test: stop the eventfd leak tests racing each other | idea from `eff2c49`, rewritten | ~30 |, | Nothing. Their tests count process-wide descriptors in parallel |
| 13 | `#[glommio::main]` / `#[glommio::test]` | `upstream/macros-proposal` | ~900 | **an issue first** | Whether to publish and maintain a second crate on crates.io. A policy decision, not a review |

`b67355f` and `8a867b9` are each **one commit carrying two unrelated features**,
so each must be split before it goes. The per-half sizes above are estimates;
the measured combined figures are +509 (4 files) and +543 (4 files).

**1, 5, 8, 11 and 12 depend on nothing** and can be offered in any order. The
"apply after" column is almost entirely about two branches appending to the
same `mod` / `pub use` list; merged out of order, each costs a two-line rebase
and nothing more.

**Start with 1.** It is a bug fix, it asks for no design decision, it deletes an
`unsafe`, and it now arrives with its own filed issue rather than as an
assertion. Old #2 was the other opener and is already in #35.

#### Deliberately not going up

| What | Why |
|---|---|
| `TcpStream::send_file`, on `master` since 2026-09-02, 16 commits | its own ladder says it costs more CPU than the read-plus-write it replaces at every size except a cold cache under one pipe-load. Offering a feature upstream whose measurement argues against it needs the `F_SETPIPE_SZ` and `IOSQE_IO_LINK` work first |
| Miri task-lifecycle tests (`c96e9a1`) | genuinely blocked on #32, uses the new `spawn_local` signature |
| `docs/investigations/` | held; answers DataDog #641 but needs framing as measurement rather than correction |
| doctest repairs (`32dd7dd`), spawn-API restoration (`c750df0`) | these fix our own breakage and are not upstream value |
| the rest of the 235 commits | fork infrastructure, benchmarks, release scripts |

### Verified porting mechanics

Every row below was tested by cherry-picking onto `upstream/main` on
2026-08-20, not estimated.

| Item | Commit | Onto their `main` alone | Note |
|---|---|---|---|
| `spawn_blocking_send` | `61bb142` | **clean**, 1 file, +228 |, |
| `sync::Mutex` + `Semaphore::is_closed` | `133a565` | **clean**, 3 files, +257 |, |
| `channels::oneshot` | `c362fb0` | **clean**, 2 files, +226 |, |
| `channels::broadcast` | `f86bb8a` | **clean**, 2 files, +482 | design doc travels with it |
| `ConnectedSender::into_foreign` | `986213e` | **clean**, 1 file, +302 |, |
| `watch` + `OnceCell` | `b67355f` | module-list conflict alone; clean when stacked | **split into two PRs**, one commit, two unrelated features |
| `CancellationToken` | `5bf58cd` | `sync/mod.rs` list conflict; clean when stacked |, |
| `future::timeout` + `Interval` | `8a867b9` | `timer/mod.rs` list conflict, 3 lines | **split into two PRs**; their `timeout` sits at `timer/mod.rs:56`, unchanged |
| `spawn_blocking` panic fix | `dcab422` | code clean; test module conflicts | re-home the tests |
| Kernel probe error | `cd7c9d3` | ~~conflicts on `sys/uring.rs`~~ | **landed inside #35** as `d873c54`; identical patch to `cd7c9d3` by patch-id, so no rewrite against their `iou` probe was needed after all |
| eventfd leak test race | `eff2c49` | conflicts | their test file differs; port the mutex idea, not the diff |
| Delete the vendored C | `322fb8d` | n/a | ~~hard-gated on #35~~, **landed inside #35** as `3db5be3` on 2026-09-02, once it was verified that no `extern "C"` block or C symbol reference survives in `glommio/src` |

**Applied in order, the seven additive commits stack onto `upstream/main` with
zero conflicts and the result compiles.** The conflicts in that table are
almost all one kind, two branches adding lines to the same `mod` / `pub use`
list, and they evaporate when the PRs land in sequence. That is worth knowing
before anyone budgets rebase time for this.

### Rules that apply to every stage

- **Strip the fork.** No `glommio-ng` in any diff. The macros branch already
  uses a generic alias in its `crate = ` examples for this reason.
- **One coherent change per PR**, rebased on their `main`, with tests, and
  under ~600 lines including them.
- **Two open at a time, and the gate is now merges rather than silence.** The
  original rule was that nothing new opens while the existing PRs sit untouched.
  #35 merged on 2026-09-07, so that gate is open. The replacement is a throttle
  rather than a stop: keep no more than two of ours awaiting first review, and
  send the smallest first.
- **If the canonical `glommio` name transfers to us, this plan is void.**
  Upstream becomes us and the mergeability constraint dissolves, including the
  one that shaped several decisions this week.

## Honest caveats

**131 commits is not a pull request.** It is roughly seven, and several need
rebasing onto their 15. Expect real conflict work in `executor/mod.rs`,
`task/header.rs` and `task/raw.rs`. *(As of 2026-09-02 the count is 232 commits
ahead of `upstream/main` at the time. The shape of the argument
is unchanged; only the number grew.)*

**The six open PRs did not go stale while they waited.** Checked 2026-09-02:
their distinctive files are byte-identical to `master`, the tests on #29 and
#30, `header.rs`/`raw.rs`/`task_impl.rs`/`multitask.rs` on #32,
`timing_wheel.rs`/`staged_wheel.rs`/`timer_id.rs` on #33. Where they differ from
`master` it is entirely in high-churn shared files (`executor/mod.rs`,
`net/stream.rs`, `reactor.rs`, `lib.rs`) carrying 232 commits of unrelated work.
Since `upstream/main` has not moved either, all six still apply to it. **Only
#35 needs work, and its work is subtraction plus a port.**

**Some of our commits fix our own breakage** and should not be presented as
upstream value: the doctest repairs undo a rename we made, and the spawn-API
restoration undoes a gate we added. Squash or drop those parts when porting.

**The performance work is measured on one machine.** Every figure in
`docs/investigations/` comes from a single Threadripper. The method transfers;
the numbers may not, and the write-ups say so.

**A maintainer with review capacity is the scarce resource**, not patches. #707
suggests they are short of reviewers rather than short of code. Landing 1 and 2
quickly is worth more than arriving with everything at once.
