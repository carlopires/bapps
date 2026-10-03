# What glommio is missing, and which of it matters

**Date:** 2026-08-25
**Method:** read the public surface of this fork item by item, then asked of
each tokio feature whether a thread-per-core server actually needs it.

glommio is not trying to be tokio, so a bare feature diff is misleading — half
of tokio exists to manage a work-stealing multi-threaded runtime that glommio
deliberately does not have. The useful question is narrower: **what does a
server on this runtime need that it cannot get?**

One entry below is a live bug, three are real gaps, and a long tail is
correctly absent.

## 1. ~~`connect` resolves DNS on the reactor thread~~ — fixed 2026-08-25

```rust
// net/tcp_socket.rs:489
pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<TcpStream> {
    let addr = addr.to_socket_addrs()?.next().unwrap();
```

`to_socket_addrs` on a hostname is `getaddrinfo`, which blocks. Called inline,
it stalls **the whole core** — every task on that executor — for the length of
a DNS lookup: single-digit milliseconds when warm, seconds when a resolver is
down. On a thread-per-core runtime that is the most expensive place in the
system to block, and glommio's own stall detector will report it as a stalled
task queue.

tokio does not have this problem: `tokio::net::lookup_host` runs the resolver
on the blocking pool.

The same line has a second defect: `.next().unwrap()` panics when resolution
yields no addresses, where every other failure on this path is a `Result`.

`TcpListener::bind` (`:127`) resolves the same way. It matters less — binding
happens at startup — but it is the same call.

**Fixed.** Resolution now goes through `spawn_blocking`, and an address that
resolves to nothing is an error rather than a panic.

It could not be done with std's trait: moving the address to the pool needs it
`Send + 'static`, and `connect(&host_string)` is neither. So `glommio::net`
has its own sealed `ToSocketAddrs` that hands back **owned** data first —
addresses when it already has them, a `String` when a lookup is genuinely
needed. Same shapes callers already pass, same call sites, and only a real
hostname crosses to the pool. tokio solves it the same way and for the same
reason.

The two `bind` calls are not `async` and so cannot reach the pool. They
resolve inline, which is defensible for a once-at-startup call, and no longer
panic on a resolver error.

Note a literal `"127.0.0.1:8080"` never reaches the resolver, since std parses
it first — so this hurt exactly the programs that connect by name, and never
showed up in a benchmark that dials an IP.

## 2. ~~No signals, so no graceful shutdown~~ — fixed 2026-08-25

There is no equivalent of `tokio::signal`. A server that must drain on
`SIGTERM` — every server that runs anywhere — has to roll its own, and the
obvious hand-rolled version (a signal handler that touches an executor) is
wrong on a runtime whose state is per-core and `!Send`.

The runtime already uses signals internally: the stall detector raises
`SIGUSR1`. So a signal story has to coexist with that, which is an argument for
glommio owning it rather than each consumer improvising.

**Fixed** as `glommio::signal`, built on the readiness API below: `signalfd`
is a pollable descriptor, so signals arrive as ordinary readable events on a
chosen core with no handler-context restrictions. What could not be fixed and
is documented instead: signals must be blocked before `signalfd` sees them,
the mask is per thread and inherited across spawn (so `block()` belongs in
`main`), and one signal goes to one descriptor, so a pool fans out through a
shared channel rather than each core watching.

## 3. ~~No way to await readiness on a foreign descriptor~~ — fixed 2026-08-25

tokio has `AsyncFd`; glommio has `poll_read_ready`/`poll_write_ready` on the
reactor and both are crate-private. So a caller cannot integrate anything that
hands out a descriptor — `inotify`, `timerfd`, `signalfd`, a C library's fd, a
second io_uring — without going outside the runtime.

This is the difference between a runtime an ecosystem can be built on and one
that only supports what it ships. It is also nearly free: the machinery exists
and is exercised on every socket read.

**Fixed** as `glommio::io::PollableFd`, which registers through the same
`IORING_OP_POLL_ADD` the sockets use. `glommio::signal` is its first consumer,
which is reasonable evidence the shape is right.

Readiness here is one-shot rather than edge-triggered, so there is no
`clear_ready` dance and no guard type; each wait registers afresh.

## 4. ~~No task-local storage~~ — fixed 2026-08-25

`thread_local!` is *almost* right here — tasks never migrate between cores —
but it is shared by every task on the core, so a request id stored there is
visible to the wrong request. Anything carrying per-request context through a
call stack (tracing spans, request ids, deadlines) wants task-scoped storage
and there is none.

**Fixed** as `glommio::task_local!`, a future combinator rather than a change
to the task structures: the value is swapped in around each poll of the scoped
future and taken out again, so nothing is allocated per task. Reading one
costs a thread-local access.

A task-local is deliberately *not* inherited by a task spawned inside the
scope — the child is a separate task and the value would otherwise outlive
the future it belongs to.

## 5. ~~No zero-copy file-to-socket send~~ — fixed 2026-08-26

tokio has `TcpStream::from_std` plus `AsyncFd` and, on Linux, crates built on
`sendfile`/`splice` for this; glommio had nothing, so serving a file meant a
read into a buffer followed by a write, copying through userspace twice for
every byte.

**Fixed** as `TcpStream::send_file`, built on `IORING_OP_SPLICE` through a
pipe (a direct file-to-socket splice is not a kernel-supported pairing; the
pipe is the intermediate `splice` needs). It takes any `SpliceSource`, sealed
and implemented for `BufferedFile` and `DmaFile` — a `DmaFile`'s `O_DIRECT`
alignment requirement on the splice offset is checked up front rather than
surfacing as a bare kernel `EINVAL`.

Two things remain out of reach through the same door:

- `UnixStream::send_file` — the socket-side plumbing `send_file` needs exists
  only on `TcpStream` today; nothing about `splice` is TCP-specific, so this
  is wiring, not design.
- `SpliceSource for ImmutableFile` — deferred deliberately. `ImmutableFile`
  holds a `DmaStreamReaderBuilder` and a size rather than a `DmaFile`, and
  exposes no `AsRawFd`, so there is no descriptor to hand `splice` without
  first changing what the type holds.

## The long tail, and why it is fine

| tokio | glommio | verdict |
|---|---|---|
| `Notify` | — | buildable on `Semaphore`; everyone builds it, so it may be worth shipping |
| `task_local!` | present since 2026-08-25 | — |
| `AsyncFd` | `io::PollableFd` | — |
| `signal` | `signal::Signals` | — |
| `Barrier` | — | rare in per-core designs |
| `JoinSet` | — | `FuturesUnordered` covers it |
| `mpsc`/`oneshot`/`broadcast`/`watch` | `local_channel`, `shared_channel`, `oneshot`, `broadcast`, `watch` | present, and each now has a cross-core form |
| `Mutex`/`RwLock`/`Semaphore`/`OnceCell` | present | — |
| `CancellationToken` (tokio-util) | present, plus `foreign_child` for cross-core | — |
| `time::{sleep, interval, timeout}` | `Timer`, `Interval`, `future::timeout` | — |
| `fs::read_to_string` and friends | `DmaFile`, `BufferedFile`, `Directory` | no convenience helpers; `sync_read_dir` blocks, but says so |
| `io::{copy, split}` | via `futures-lite` | — |
| codec / `Framed` | — | belongs in a separate crate |
| `process` | — | out of scope |
| TLS | kTLS record reads only | correct: glommio should not do TLS |
| work stealing, `block_in_place`, multi-thread runtime | — | **the point of the project** |
| runtime metrics | `ExecutorStats`, `TaskQueueStats`, `IoStats` | present, arguably better |

## Recommended order

1. ~~The DNS fix.~~ Done.
2. ~~A public readiness API for a foreign fd.~~ Done.
3. ~~Signals via `signalfd`.~~ Done.
4. `Notify` and async `read_dir` — still only if a real consumer asks.

## The sweep of the older surface (2026-08-26)

Done, and the method is worth keeping: **rustdoc only documents reachable
items**, so diffing `target/doc/glommio/all.html` against every `pub` item the
source declares finds anything a caller meets but cannot write down. 470
declared, 177 reachable; after discarding methods (which `all.html` does not
list) and `#[doc(hidden)]`, thirteen candidates remained and six were real:

| type | met by callers as | now |
|---|---|---|
| `NonBuffered` | the default receive buffer of `TcpStream`/`UnixStream` | exported |
| `Tick` | the future `Interval::tick` returns | exported |
| `CpuSetGenerator` | returned by `Placement::generate_cpu_set` | exported |
| `CpuIter` | returned by `CpuSetGenerator::next` | exported |
| `ReadManyArgs`, `ScheduledSource` | the item type of the stream `read_many` returns | exported |
| `Statx` | argument of a public `From<Statx> for Stat` | impl retired |

Each was usable inline and impossible to store, wrap, or implement a trait for.
`Statx` went the other way: exporting the raw kernel structure would commit us
to its twenty fields forever, so the conversion became a crate-private
constructor instead — and `unreachable_pub` immediately flagged `Statx`, which
confirms the public `From` impl was what had been holding it reachable.

That is the same mechanism that hid `RxBuf` for years: a public *impl* or
supertrait keeps rustc from complaining while the type stays unnameable. The
lint cannot see the difference; the docs diff can.

`TaskDebugger` and `NopSubmitter` also showed up and are fine — they are
behind the `debugging` and `bench` features, which the doc build does not
enable.

The remaining unreachable `pub` items are genuinely internal (`Source`,
`Level`, `Node`, the storage `Sealed` trait) and appear in no public
signature.

The first two are worth doing without waiting for anyone to ask. The rest
should follow evidence, which is how the rest of this fork's decisions have
gone: the arena, multishot accept and the depth-1 read ladder were all
plausible and all wrong, and each cost less than it would have because a
measurement came before the work.
