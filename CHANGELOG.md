# Changelog

`glommio-ng` is a republish of the community [glommio](https://github.com/glommio/glommio)
fork. Versions track the fork's own `0.10.0`; the patch number belongs to this
republish and is applied at release time, so a bump here does not imply a
change upstream.

Dependency line, unchanged across every version below:

```toml
glommio = { package = "glommio-ng", version = "0.12" }
```

**Correct this if you copied it before 2026-09-02.** This line read
`version = "0.10"` from the first release until now, which was right through
0.10.15 and wrong from 0.11.0 onward: cargo reads `"0.10"` as
`>=0.10.0, <0.11.0`, so anyone who pasted it has been pinned below every
0.11.x release and has none of the fixes in them.

`glommio-ng-macros` is published in lockstep from `0.10.2` onward and is pulled
in automatically by the default `macros` feature. You do not depend on it
directly.

## 0.12.0-cp.3 — carlopires fork, synced with dahankzter/glommio

Merges the 29 commits dahankzter/glommio `master` gained after `v0.12.0-ng`
(2026-09-04..07), each read before merging, plus one fix found in review.

- **Timers: a slab-backed hierarchical wheel** replaces the staged wheel and
  our 0.12.0-cp.2 `BTreeSet` index (the merge takes theirs; the wheel solves
  the same scan with per-level occupancy bitmaps). Handles carry a
  generation, so a stale handle cannot cancel a reused slot; removal fixes up
  the entry `swap_remove` moves. Deadlines round up (a timer never fires
  early; before, one sleep could register several times), sub-millisecond
  sleeps are no longer rounded to a whole tick, a sleep polled again with the
  same waker does not re-register, and due wakers are woken after the timer
  structure is released, so a waker that arms a timer cannot double-borrow it.
- **Fix found in review (ours):** `next_expiry` returned the earliest level-0
  deadline without comparing it with the next cascade. Level 0 is filled
  relative to now but levels cascade on aligned boundaries, so a timer 300
  ticks out (level 1 until tick 256) was hidden by one inserted at tick 100
  for tick 350: an idle reactor slept to 350 and fired the first 50 ms late
  (up to ~255 ms). Regression test
  `a_level_one_timer_is_not_hidden_by_a_later_level_zero_timer`.
- **Cancellation tokens** no longer keep one waker per poll: registrations
  take a reusable slot, are refreshed on re-poll and withdrawn on drop (local
  and foreign tokens).
- **`OnceCell`**: an initializer no longer overwrites a value another task
  published while it awaited.
- **Sockets**: a read times out when its deadline has passed, not when its
  timer is merely absent (it is also absent after a cancellation).
- **io_uring support check**: EMFILE/ENFILE/ENOMEM while probing is no longer
  cached as "unsupported" forever; compile-time check that the timespec
  layout matches `io-uring`'s. (The io-uring crate switch itself, `8c881a5`,
  was already in our base; it is the same change as merged upstream.)
- Dev only: criterion benchmarks for spawn and timers, a timer ladder
  example, timer counters behind the `debugging` feature.

Not merged: the 30 commits on the community fork `glommio/glommio` after
2026-09-07 (eventfd leak #448, task-lifecycle leak #51, executor parking).
They conflict with this fork in 27 files of the task and executor core,
where the two forks implemented the same PRs differently.

## 0.12.0-cp.2 — carlopires fork

Perf: the reactor found the next timer deadline by scanning every live timer
on every pass (`values().min()` over the expiry map). With per-request
deadlines there are thousands of live timers, and a profile of a busy
two-executor RUDB node spent 77% of user CPU in that scan. Timers are now also
kept in a `BTreeSet` ordered by expiry, so the next deadline is its first
element. On the same RUDB benchmark one executor went from ~30.6k to ~56.4k
requests/s. Upstream master fixes the same scan differently (wheel occupancy);
this is the minimal change on 0.12.0.

Regression: `timer::reactor_adapter::tests::next_deadline_tracks_insertions_removals_and_expiry`.

## 0.12.0-cp.1 — carlopires fork, based on 0.12.0

Fix: an accept or open that completed without any caller collecting its
result leaked the new descriptor. The common way to hit it is dropping a
`TcpListener` (or `UnixListener`) whose abandoned `accept()` future left an
io_uring accept in flight: when a client connects before the listener goes
away, the kernel accepts the connection into a socket nobody serves or
closes, and that client waits until its own timeout. A too-late cancellation
of a dispatched accept/open, or dropping an open future after its completion
was reaped, leaked the same way.

`InnerSource` now records whether its result was handed to a caller; on drop,
an `Accept`/`Open` source whose descriptor was never observed closes it.
Regression: `net::tcp_socket::tests::dropped_listener_closes_an_accepted_socket_nobody_collected`.

## 0.12.0 — 2026-09-02

Two fixes and one new API. Upgrade for the fixes: both are memory-safety
problems reachable from ordinary use, and both are live in 0.11.5.

The minor bump rather than a patch is for `TcpStream::send_file`, which is new
public API. Nothing is removed and nothing changes shape, so the upgrade is a
version-line edit.

### Fixed

- **`MSG_ZEROCOPY` is no longer set on every socket send.** The flag went on
  every `Send` and `SendMsg` while nothing ever set `SO_ZEROCOPY`, without
  which the kernel copies and ignores it — so it had been inert since it was
  written, confirmed with a positive-control probe.

  Inert is not harmless. `AsRawFd` is public on `TcpStream` and `UdpSocket`,
  and `recv_tls_record`'s own documentation tells callers to reach through it
  and `setsockopt` to turn on kernel TLS. One `SO_ZEROCOPY` from a user would
  have armed a send path that frees its buffer at the send CQE while the
  kernel still holds those pages and reports their release on an error queue
  nothing in this crate reads: **a use-after-free reachable without touching
  glommio**, transmitting freed memory. Removing the flag changes no
  behaviour, since the kernel was already ignoring it.

- **The wake-up eventfd is no longer force-closed when an executor drops.**
  `Reactor::drop` called `close_eventfd()`, which shut the descriptor even
  while other threads still held an `Arc<SleepNotifier>` — and a
  `shared_channel` peer on another executor legitimately does. `notify()` read
  the descriptor number under a lock, released it, then wrote, so the close
  could free the number and the kernel hand it to the next `open` before the
  write landed: eight bytes into an unrelated descriptor, or a panic out of
  `write_eventfd`'s unwrap.

  The mechanism turned out to be unnecessary as well as unsafe. It existed so
  that a dropped executor released its eventfd even while tasks held the
  notifier, and tasks stopped holding it when the task header began carrying
  an `executor_id` instead. Measured rather than assumed: twenty executor
  lifecycles grow the process descriptor count by twenty before either change,
  and by nothing with the header change alone and the close removed. So the
  close is gone rather than locked, and the race with it.

### Added

- **`TcpStream::send_file`** — sends a file to a socket without the bytes
  entering the process, built on `IORING_OP_SPLICE` through a per-call pipe.
  Takes any `SpliceSource`, a sealed trait implemented for `BufferedFile` and
  `DmaFile`. A `DmaFile`'s `O_DIRECT` alignment requirement on the offset is
  checked before submission and refused with an error naming the offset and
  the alignment it needed, rather than returning a bare kernel `EINVAL`.

  **Read the cost table before reaching for it.** Measured against `read_at`
  plus `write_all` on total process CPU:

  | file state | payload | vs. read + write_all |
  |---|---|---|
  | in page cache | any size measured | 1.8x – 2.8x **more** CPU |
  | not in page cache | up to 64 KiB | 0.53x – 0.96x, a win |
  | not in page cache | above 64 KiB | 1.4x – 1.7x more CPU |
  | `O_DIRECT` (`DmaFile`) | any size measured | 1.4x – 3.5x more CPU |

  The pipe holds 64 KiB, so a payload is spliced in 64 KiB chunks of two ring
  operations each and the file-to-pipe leg reaches an `iou-wrk` worker every
  time, while a single `read_at` submits one operation whatever the size.
  Prefer this when the bytes must not enter the process — address-space
  pressure, not CPU, is what it buys — or when the file is cold and the
  payload fits one pipe-load. Prefer `read_at` plus `write_all` otherwise.

  Note it opens two descriptors per call and closes them through the reactor,
  so budget accordingly at high concurrency. Numbers, method and the ordered
  list of what would make it faster are in
  `docs/investigations/io-path/network.md`.

## 0.11.5 — 2026-08-26

Two crash fixes and five capabilities. Lead with the fixes: anyone running
shared channels under load has been hitting a panic, and anyone connecting by
hostname has been stalling a whole core per connection.

### Fixed

- **A shared channel no longer panics when its peer's executor is gone.**
  Connecting resolved the peer's executor id to its sleep notifier and
  unwrapped the result — but the two are not dropped together: an executor
  drops its notifier on its own schedule, while its id stays in the buffer
  until the peer's channel half is dropped. So the surviving side could hold
  an id whose notifier had already gone. About one full-suite run in five,
  which is why it read as a flake rather than as a bug.

  An executor that no longer exists can neither be woken nor send, which is
  what disconnected means, so the peer is now marked disconnected as well as
  resolved to a placeholder notifier. Marking it is not optional: the
  placeholder alone stops the panic and leaves this side waiting forever for a
  peer that cannot arrive, trading a crash for a hang.

- **`TcpStream::connect` no longer resolves DNS on the executor thread.** It
  called `to_socket_addrs` inline; on a hostname that is `getaddrinfo`, which
  blocks — and on a thread-per-core runtime it blocks every task on that core,
  for single-digit milliseconds warm and seconds when a resolver is
  unreachable. glommio's own stall detector reported it as a stalled task
  queue.

  std's trait could not carry the fix: moving the address to the blocking pool
  needs it `Send + 'static`, and `connect(&host_string)` is neither. So
  `glommio::net` now has its own sealed `ToSocketAddrs` that hands back owned
  data before anything blocking happens — addresses when it already has them,
  a `String` when a lookup is genuinely needed. The shapes callers pass are
  unchanged, including the borrow of a local `String`, and only a real
  hostname crosses to the pool. A literal address never reaches the resolver,
  which is why this never showed up in a benchmark that dials an IP.

  The same line turned an address resolving to nothing into a panic, on a path
  where every other failure is a `Result`. The two `bind` calls are not async
  and still resolve inline, which is defensible once at startup, but they no
  longer panic on a resolver error.

### Added

- **`io::PollableFd`** — await readiness on a descriptor glommio did not
  create. Everything glommio owns already goes through the reactor; an inotify
  watch, a timerfd, a device node or a C library's descriptor had no way in,
  because `poll_read_ready` and `poll_write_ready` are crate-private. That is
  the difference between a runtime an ecosystem can build on and one that
  supports only what it ships.

  It registers through the same `IORING_OP_POLL_ADD` the socket paths use, so
  a foreign descriptor parks the executor in the kernel alongside everything
  else. It owns what it is given, so the descriptor cannot be closed while a
  registration is outstanding, and hands it back through `into_inner`.

  Readiness here is not edge-triggered: each call registers a fresh one-shot
  poll, so there is no readiness state to clear and no guard to return. The
  `AsyncFd`/`clear_ready` dance epoll forces on tokio has no counterpart here,
  which is worth saying out loud because its absence looks like an omission.

- **`signal::Signals`** — signals as readable events on the reactor, so a
  server can drain on `SIGTERM`. A signal handler is a bad fit twice over: it
  runs on whichever thread the kernel picks, and almost nothing in a per-core
  runtime is safe to touch from there. signalfd avoids both — signals become a
  readable descriptor, that descriptor goes on the reactor through
  `PollableFd`, and the code that reacts is ordinary async code on a core the
  caller chose.

  Two properties of signals do not go away and are documented rather than
  papered over. They must be blocked before signalfd can see them, and the
  mask is per thread and inherited across spawn — so `block()` belongs in
  `main` before any executor exists; `Signals::new` blocking its own thread is
  enough for one executor and not for a pool. And a signal goes to one
  signalfd, so two executors watching `SIGTERM` race for it; fan out with a
  shared channel or a `ForeignCancellation`.

- **`task_local!`** — storage scoped to a task rather than to a core.
  `thread_local!` is almost right on a runtime whose tasks never migrate, and
  wrong in the one way that matters: every task on a core shares the slot, so
  a request id written there is visible to the next request and to everything
  running between the two.

  A task-local is set around each poll of the future it is scoped to and taken
  out again afterwards, so two tasks interleaving on one core each read their
  own. That makes it a future combinator rather than a change to the task
  structures: nothing is allocated per task, the task header is untouched, and
  reading one costs a thread-local access. Deliberately not inherited by a
  task spawned inside the scope — the child is a separate task, and a value
  that followed it would outlive the future it belongs to.

- **`TcpStream::recv_tls_record`** and **`net::tls_record`** — read one record
  off a socket with kernel TLS enabled and report the type the kernel attached
  to it. glommio does no TLS and should not. What it has to get right is that
  a record which is *not* application data can be read at all: once `TLS_RX`
  is installed, the kernel refuses a plain `recv` with such a record at the
  head of the queue and returns `EIO`. A TLS 1.3 key update is such a record,
  so a long-lived kTLS connection that only ever calls `read` eventually fails
  with an opaque I/O error that looks like a runtime bug rather than a key
  update.

  The record type is `None` on a socket without kernel TLS, so the call is an
  ordinary read there and a caller need not know which it has. Enabling kernel
  TLS stays the caller's business — `TCP_ULP` and the keys a handshake
  produced, on the descriptor `AsRawFd` already hands over; the `ktls` crate
  does that against rustls. This is only the part that cannot be done from
  outside, because it is glommio's read path that turns a control record into
  `EIO`.

- **Six types callers met and could not name** are exported: `NonBuffered`,
  the default receive buffer of `TcpStream` and `UnixStream`; `Tick`, what
  `Interval::tick` returns; `CpuSetGenerator` and `CpuIter`, which come back
  from `Placement::generate_cpu_set`; and `ReadManyArgs` with
  `ScheduledSource`, the item type of the stream `read_many` returns. Each was
  usable inline and impossible to store, wrap, or implement a trait for.

  Found by diffing rustdoc's output against every `pub` the source declares —
  470 declared, 177 reachable — because `#![deny(unreachable_pub)]` does not
  catch this class: a public impl or a re-exported supertrait keeps rustc
  quiet while the type stays unnameable. That is the same mechanism that hid
  `RxBuf` for years.

  `Statx` went the other way. It was the argument of a public `From` impl
  while staying unnameable, so no caller could ever invoke the conversion, and
  exporting the raw kernel structure would commit this crate to its twenty
  fields forever, for nobody. The conversion is a crate-private constructor
  now.

### Changed

- **The `native-tls` feature is now `native-thread-local`.** It selects the
  nightly `#[thread_local]` attribute for executor-local storage instead of
  `scoped_tls`. The "tls" was always thread-local storage and never Transport
  Layer Security, but it collides with the well-known `native-tls` crate and
  has now been read as an empty TLS placeholder more than once — reasonably,
  since the feature has no dependencies and glommio has no TLS of its own.
  `native-tls` remains as an alias that enables it, so no existing build
  breaks and nobody has to migrate.

## 0.11.4 — 2026-08-24

> These notes and those for 0.11.1 through 0.11.3 were written retroactively
> on 2026-08-26. Those four versions shipped without entries; what follows was
> reconstructed from the commits published before each version's crates.io
> timestamp.

### Fixed

- **`OwnedRxBuf` can be constructed and unwrapped.** It shipped public in
  0.11.2 with a `pub(crate)` constructor and no accessor, so a foreign `RxBuf`
  could neither hand its buffer over nor take it back: `take_kernel_buffer`
  could not return `Some`, and the completion read path was in practice
  reserved for `Preallocated`. The 0.11.2 notes said external implementations
  keep the readiness path "until they opt in" — there was no way to opt in.
  `new` and `into_vec` fix that.

  `new` panics on an empty vector rather than accepting it. The kernel fills
  `memory[..len]`, so a vector with capacity and no length lends it nowhere to
  write, and the resulting zero-byte read is indistinguishable from the peer
  hanging up — a healthy connection would silently appear closed.
  `Vec::with_capacity(n)` is exactly how that mistake gets written, so it is
  refused where it is made rather than one layer down.

### Added

- An integration test that drives the public API from outside the crate, plus
  `deny(unreachable_pub)` as hygiene, narrowing 36 internal items to
  `pub(crate)`. Two releases in a row shipped API a dependent crate could not
  use and no unit test could see, because from inside every module is in scope
  and every constructor visible.

## 0.11.3 — 2026-08-24

### Fixed

- **`RxBuf` is exported, so `buffered_with` can be satisfied at all.** It
  takes any `B: Buffered`, and `Buffered` requires `RxBuf` — but `RxBuf` lived
  in a private module and was never re-exported, so no type outside this crate
  could implement it. The generic parameter was real and its only possible
  argument was `Preallocated`. That also made `OwnedRxBuf`, added in 0.11.2,
  useless to the callers it exists for: the trait whose methods hand it out
  could not be named.

  Its methods are documented now that they are public, including the two rules
  an implementation has to know: `is_empty` must not claim to hold bytes that
  have not arrived, and `unfilled` is called before the read completes.
  Getting that pair wrong hands the caller uninitialised space instead of
  data.

## 0.11.2 — 2026-08-24

### Changed

- **Buffered reads go through the ring.** The buffered read path now hands its
  receive buffer to the kernel and takes it back filled, rather than polling
  for readiness and then reading. Only the buffered path can do this, and the
  reason is lifetime rather than speed: `poll_read` receives a borrowed slice
  good for one call while the kernel needs the buffer until the completion
  arrives. Where glommio owns the buffer it can lend it. **The unbuffered path
  keeps the readiness design permanently** — that split is not a stage.

  Buffered reads at 256 connections, every reader parked before data arrives:
  1,247ns of CPU per message to 1,023ns. Streaming, where the data is always
  waiting and the risk of a regression lives, is unchanged at 3,795ns per
  64KiB. The unbuffered path does not move.

  Speculation stays, because deleting it would cost the streaming reader one
  syscall against an SQE and a CQE. A bit per stream follows the workload
  instead of choosing for it.

### Added

- **`poll_write_vectored` on `TcpStream` and `UnixStream`.** Neither
  overrode it, so both inherited the futures-io default, which writes only the
  first slice. A server sending a response as status line, headers and body
  paid three `send` calls — and with `TCP_NODELAY` set, which a
  latency-sensitive server does set, put up to three segments on the wire for
  one response. Measured over 100k loopback responses: 6,760ns to 1,782ns on a
  small response, with segments per response dropping by the same factor. Off
  loopback the segment count is the larger effect.

### Performance

- `accept` no longer toggles `O_NONBLOCK` on every call.

## 0.11.1 — 2026-08-21

### Added

- **Cross-core `watch` and `broadcast` channels.** `watch` carries a latest
  value to observers that only need the current state; `broadcast` fans each
  message out to every receiver. Both are written over a shared storage seam
  that `oneshot`'s two separate implementations were folded into, so the
  local and cross-core variants are one implementation over two storages
  rather than two codebases.

## 0.11.0 — 2026-08-21

The first release that asks anything of downstreams. Both changes are
mechanical; the whole migration is below.

### Migrating

**`GlommioError::BuilderError(BuilderErrorKind::ThreadPanic(..))` now carries a
`String`** instead of `Box<dyn Any + Send>`. Only code that matched that
variant and inspected its payload is affected — and since `Display` printed
`"thread panicked"` and discarded it, there was nothing useful to inspect.

**`timer::timeout` is now `timer::try_timeout`.** If you were using it you have
had a deprecation warning since 0.10.9. Two ways forward:

- Take `future::timeout` — it accepts any future and hands the output back
  untouched. This is what you want in almost every case.
- Or rename the call to `timer::try_timeout` to keep the `Result`-flattening
  behaviour.

Nothing else changes.

### Fixed

- **`GlommioError` is now `Send + Sync`.** It never was, for any `T`, because
  `ThreadPanic` held a `Box<dyn Any + Send>`, and `Box<dyn Any + Send>` is not
  `Sync`. That made even the `GlommioError<()>` returned by every file and
  socket operation unusable with `anyhow`, which requires
  `Send + Sync + 'static` — so a glommio error could not be `?`'d into an
  `anyhow::Result`, and downstream code grew flattening shims purely to get
  `Sync` back.

  The panic message is now captured where the panic is caught. That is `Sync`,
  and strictly more useful than what it replaced: a panic carries a `&str` or
  `String` in essentially every real case, and that text now reaches `Display`
  rather than being dropped.

### Changed

- `timer::timeout` renamed to `timer::try_timeout`, completing the split begun
  in 0.10.9. `future::timeout` keeps the plain name, so exactly one public
  function is called `timeout` and the `try_` prefix marks the `Result`-aware
  variant, as it does for `try_join` and `try_select`.

- The test-directory notice no longer claims glommio needs an NVMe volume
  generally. **Poll io** (`IORING_SETUP_IOPOLL`) does and always will.
  `DmaFile` needs only `O_DIRECT`, which tmpfs has supported since Linux 6.1 —
  so plain DMA file tests run anywhere recent.

### A note on the version

Releases so far were `0.10.x`, mirroring the fork's own `0.10.0` with the patch
number belonging to this republish. This release breaks that: `0.11.0` is
required by cargo's compatibility rules, since every `0.10.x` is treated as
interchangeable and this one is not. **It does not mean this crate is a minor
version ahead of glommio proper** — the upstream fork is still at `0.10.0`.

## 0.10.15 — 2026-08-21

Two asks from a downstream's second field report, both of which unblock real
call sites and neither of which had open design questions.

### Added

- **`channels::oneshot::shared()`** — a one-shot reply channel whose halves can
  be sent between executors. The existing `oneshot` is `Rc`-based and stays on
  one core, which is the right default; this is for the ask-and-reply idiom
  across cores, where the sender travels to the service that will answer and
  the receiver is awaited where the question was asked.

  `shared_channel` can already cross, but it is mpsc-shaped, bounded, and needs
  a `connect()` handshake — a lot of machinery to carry one value back.

  Dropping the sender wakes the receiver with an error rather than leaving it
  waiting for something that can no longer arrive, and sending to a departed
  receiver hands the value back.

- **`LocalReceiver::into_stream()`** — an owned, `'static` stream.
  [`stream()`](https://docs.rs/glommio-ng/latest/glommio_ng/channels/local_channel/struct.LocalReceiver.html#method.stream)
  borrows the receiver, which is right for a loop in the same scope and useless
  for handing the stream to a library that wants
  `Pin<Box<dyn Stream<Item = T>>>`. The receiving end closes when the stream is
  dropped, exactly as when the receiver is.

## 0.10.14 — 2026-08-20

### Fixed

- **`select!` no longer requires a comma after a block-bodied branch**, the
  same rule `match` arms and tokio's `select!` follow:

  ```rust
  select! {
      _ = interval.tick() => {
          work().await;
      }                                  // no comma needed
      () = token.cancelled() => break,
  }
  ```

  The cause was not the comma rule itself. `{ … }` followed by `(` parses as a
  **call expression** — `{block}()` — so the general expression parser swallowed
  the next branch, and the error surfaced against *that* branch's `=>` with a
  list of expected pattern tokens. It read as "unit patterns are unsupported",
  which is why it cost a first pass looking at the wrong branch entirely. A
  braced body is now parsed as a block, as syn's own `Arm` does.

  Where a comma genuinely is required, the error names the branch that needs
  it rather than the next one.

- A single-branch `select!` no longer emits a degenerate loop and modulo.

### Testing

The three `select!` defects so far were each invisible to the tests that
existed: two were semantic and one was purely syntactic. There is now a syntax
corpus — block body with and without the trailing comma, bare expressions,
`if` and `match` bodies, unit patterns, mixed forms, `biased;` — because real
call sites produce that spread naturally and a hand-written behavioural suite
does not.

## 0.10.13 — 2026-08-20

### Fixed

- **`select!` now drops the branch futures before running a handler**, as
  tokio's does. They were held across the handler, so a handler could not use
  anything its own branch had borrowed:

  ```rust
  select! {
      msg = upstream.recv() => process(&mut upstream, msg).await,  // borrow error
  }
  ```

  The compiler ends a borrow at its last use unless the borrower has **drop
  glue** — any field needing drop, not a hand-written `Drop` impl — which
  covers nearly every `async fn` future that captures a borrow alongside
  anything droppable. So this rejected ordinary code while contrived examples
  kept compiling. A compile error rather than a silent divergence, but it
  refused code that is correct under tokio.

## 0.10.12 — 2026-08-20

### Fixed

- **`select!` now accepts `crate = <path>`**, like `#[glommio::main]` and
  `#[glommio::test]`. Its expansion hardcoded `::glommio::…`, so a caller
  reaching glommio under another name — `glommio-ng` without a rename, or
  through a facade that re-exports it — could not compile it.

  ```rust
  my_runtime::select! {
      crate = ::my_runtime::glommio;
      biased;
      () = shutdown.cancelled() => break,
      maybe = upstream.recv()   => handle(maybe).await,
  }
  ```

  This is the second time the same defect has shipped: `#[main]` had it, it
  was fixed, and the new macro repeated it. `glommio-macros` now says in its
  own crate documentation that a macro is not finished until it takes
  `crate = <path>` **and** carries a trybuild case naming a crate that does not
  exist — which fails to resolve only if the override actually reaches the
  expansion. Parsing the argument proves nothing; the negative control does.

## 0.10.11 — 2026-08-20

### Added

- **`glommio::select!`** — races several futures and runs the body of whichever
  finishes first. Branches are pinned on the stack, so they need neither
  `Unpin` nor `FusedFuture` nor `.fuse()` wrappers, which is what made
  `futures::select!` unusable as a replacement for tokio's.

  ```rust
  glommio::select! {
      biased;                                   // optional: poll top to bottom
      () = shutdown.cancelled() => break,
      maybe = upstream.recv()   => handle(maybe).await,
  }
  ```

  Branch bodies run in the caller's async context, so `.await` inside one
  works. Losing futures are **dropped**, not suspended — the usual
  cancellation-safety caveat applies and is documented on the macro.

  By default the starting branch **rotates** between invocations, so a branch
  that is always ready cannot starve the ones after it. tokio randomises for
  the same reason; counting is cheaper and reproduces between runs. A test
  asserting a fixed winner wants `biased;`.

  Not supported, each additive later and none with a user today: `if` guards,
  `else`, and refutable patterns.

  Requires the default-on `macros` feature.

### Changed

- Internal: the waker stores added over the last two releases now hand back an
  obligation that can only be discharged by waking, rather than a bare
  `Vec<Waker>` that can be dropped on the floor. No API change; it removes a
  class of bug whose symptom was silence.

## 0.10.10 — 2026-08-20

### Added

- **Cross-core cancellation.** `CancellationToken::foreign_child()` returns a
  `Send + Clone` handle; `ForeignCancellation::attach()` turns it back into an
  ordinary token on the destination executor. The token itself stays `!Send`,
  and an attached token supports `child_token()` and `cancelled()` like any
  other, so nothing downstream of it need know another core exists.

  This closes the shape a control plane on one core needs to stop work on the
  others, which previously had no answer and pushed projects back to
  `tokio_util`.

  Two cases are settled deliberately, both drawn from a real shutdown path
  where they sit two lines apart:

  - attaching after the origin cancelled yields an already-cancelled token;
  - attaching after the origin was **dropped** does too. Once the last origin
    handle is gone nothing can ever cancel that state, so anything else is a
    permanent hang.

  Note the asymmetry with local tokens, which is deliberate: dropping a local
  parent leaves its children alive and cancellable through their own handles.
  Dropping the origin of a `ForeignCancellation` cancels everything attached
  from it, because no handle to cancel it through survived the crossing.

  Cancellation is asynchronous across the boundary: `cancel()` returns
  immediately, and the attached token becomes cancelled once its executor polls
  the task watching for it.

## 0.10.9 — 2026-08-20

Asks from a downstream that has removed tokio's executor entirely and reported
what it hit afterwards.

### Added

- `OnceCell::get_or_try_init` — for lazily-initialised resources that can fail,
  which is most of them. **A failed initialiser does not poison the cell:** the
  next caller, queued or later, runs its own. One transient failure must not
  leave a permanently dead cell.
- `Stream` for `broadcast::Receiver` and `watch::Receiver`, which
  `local_channel` and `shared_channel` already had. `broadcast` yields
  `Result<T, RecvError>` rather than skipping quietly — a stream that hid
  `Lagged(n)` would turn a detectable gap into a silent one.

### Deprecated

- `timer::timeout`, in favour of `future::timeout`, which accepts any future
  rather than only one returning glommio's `Result`. Two public functions with
  the same name and different contracts is a trap for whoever greps first. At
  the next breaking release the name moves and this becomes `try_timeout`.

### Documentation

- [Porting from tokio](docs/PORTING_FROM_TOKIO.md), organised around
  differences that fail **silently**. It leads with `Task` cancelling on drop,
  and with the case `#[must_use]` cannot catch: the attribute does not survive
  a newtype wrapper, so a runtime-agnostic library that wraps `Task` in its own
  handle type gets no warning at all. That cost one downstream more time than
  anything else in their port.

## 0.10.8 — 2026-08-20

### Fixed

- **The crate did not compile for musl targets.** `libc` does not define
  `AT_STATX_SYNC_AS_STAT` there; it is `0` in the kernel UAPI and is now
  spelled out, so both libcs take the same path. Verified by building and
  running on Alpine: with the C removal in 0.10.6, glommio itself needs no C
  toolchain, so `musl-dev` is required only for the linker that any Rust build
  script needs.
- Two paths that only `--all-features` compiles: `schedule_runnable` gated its
  raw-pointer access on the `native-tls` feature without the `nightly` cfg, and
  `task::debugging` compared the header's `u32` executor id against a `usize`
  accessor. Neither affects a default build.

## 0.10.7 — 2026-08-20

### Fixed

- **A panicking `spawn_blocking` closure no longer hangs its caller.** The
  closure wrote its result straight into a `MaybeUninit` on the pool thread; a
  panic skipped that write and unwound the worker, so no response was ever
  sent. The awaiting future hung forever and the pool lost a thread
  permanently — four panics exhausted a four-thread pool. The panic now
  surfaces where the caller awaited. **This bug is present in every earlier
  glommio release, including the canonical 0.9.0.**

### Added

- `ExecutorProxy::spawn_blocking_send` — same pool, but the returned future is
  `Send` and does not depend on the executor that created it. Lets a per-core
  client satisfy a trait demanding `Pin<Box<dyn Future + Send>>` without
  running a second thread pool. Panics are caught and re-raised at the await
  point.

## 0.10.6 — 2026-08-20

### Changed

- **No C toolchain, `make`, `configure` or submodule required.** The vendored
  `liburing` and the five C files it fed were deleted: nothing in the crate
  referenced them once the `io-uring` crate migration completed. The package
  went from 630 files and 3.9 MB to 110 files and 1.6 MB.

## 0.10.5 — 2026-08-20

### Added

- `future::timeout` — races any future against a deadline, whatever it returns.
  `timer::timeout` only accepts futures already returning a glommio `Result`.
  Both now document that the inner future is polled before the timer, so one
  ready exactly at the deadline reports success.
- `timer::interval`, `timer::interval_at`, `Interval`, `MissedTickBehavior` —
  a pollable stream of ticks, first one immediate, usable in a `select!` arm.
  `Burst`, `Delay` and `Skip` are three genuinely different schedules.

### Note

At the next breaking release `timer::timeout` becomes `try_timeout` and
`future::timeout` takes the plain name, matching the ecosystem's `try_`
convention for `Result`-aware combinators.

## 0.10.4 — 2026-08-20

### Added

- `sync::Mutex` — an async mutex for state held across an `await`. Prefer
  `RefCell` where no borrow crosses one.
- `sync::OnceCell` — initialised at most once by an initialiser that may
  itself await; concurrent callers wait for the first rather than running
  their own.
- `sync::CancellationToken` — hierarchical cancellation within one executor.
  `!Send` by design: cross-shard shutdown is one root token per executor.
- `channels::oneshot` — a channel carrying exactly one value; an unsent value
  is handed back rather than dropped.
- `channels::watch` — keeps only the latest value; slow receivers skip rather
  than queue.
- `channels::broadcast` — multi-consumer fan-out with `RecvError::Lagged(n)`
  for receivers that fall behind. Semantics mirror tokio's deliberately.
- `ConnectedSender::into_foreign` — a `Send`, non-`Clone`, `try_send`-only
  handle usable from a thread with no executor. Not `Clone` on purpose: the
  buffer underneath is strictly single-producer.
- `Semaphore::is_closed`.

## 0.10.3 — 2026-08-19

### Fixed

- **`#[glommio::main(name = "…")]` is now rejected rather than silently
  ignored.** `LocalExecutorBuilder::name` is only read by `spawn()`; the
  macros build with `make()`, which never reads it, so the argument was
  accepted and discarded. It now fails to compile with a message pointing at
  `spawn()`.

### Documentation

- `crate = …` accepts a path, not just an identifier, so a crate reaching
  glommio through a facade can point at the re-export.

## 0.10.2 — 2026-08-19

### Added

- `#[glommio::main]` and `#[glommio::test]`, from the new `glommio-ng-macros`
  crate, behind a default-on `macros` feature. Arguments: `placement` and
  `crate`. `#[glommio::test]` emits a plain `#[test]`, so `#[should_panic]`,
  `#[ignore]` and `Result` returns compose.

### Note

Re-exporting a macro named `test` means `use glommio::*` shadows the built-in
`#[test]`. Import the macros by name, or write `#[glommio::test]`.

## 0.10.1 — 2026-08-19

### Fixed

- **A kernel that cannot run glommio now returns an error instead of calling
  `exit(1)` on your process.** The failure names the missing `IORING_OP_*`, or
  reports `kernel.io_uring_disabled` by value, or points at a container seccomp
  policy — the three ways this fails on RHEL and in containers.
- The documented kernel floor was 5.8; the newest operation glommio submits
  landed in 5.6.

## 0.10.0 — 2026-08-19

First release. The canonical `glommio` crate last published 0.9.0 in March
2024, and 0.9.0 no longer compiles against current kernel headers, leaving
downstreams pinned to git revisions.

### Fixed

- **`cargo package` was impossible.** `build.rs` ran `./configure` inside the
  vendored `liburing`, mutating the crate's own source directory, which cargo
  rejects outright. Configuring outside the source tree made the crate
  publishable at all. (Superseded in 0.10.6, which removed the C entirely.)
