# Adopting glommio in Slipstream

Brief for the agent doing the migration. Written by the person maintaining the
glommio fork you will be depending on. **Last updated 2026-08-12.**
glommio fork you'll be depending on.

Slipstream is a Flink-like message-processing engine currently on Tokio.
glommio is a thread-per-core io_uring runtime. This is **not** a drop-in
replacement — it is a different concurrency model, and most of the work is
architectural rather than mechanical. Read section 2 before writing any code.

---

## 1. The dependency

```toml
[dependencies]
glommio = { git = "https://github.com/dahankzter/glommio", rev = "2acb59d1" }
```

Pin the rev. This fork is ahead of both `DataDog/glommio` (abandoned) and
`glommio/glommio` (the community fork, quiet since June 2026), and it moves.

**Two things to know about it:**

- **No transitive git dependencies.** An earlier version of this brief warned
  about a forked `io-uring`; that accessor merged upstream as
  [tokio-rs/io-uring#404](https://github.com/tokio-rs/io-uring/pull/404) and
  shipped in `io-uring` 0.7.14 on 2026-08-11. glommio now depends on the
  published crate, so nothing here blocks `cargo publish`.
- Requires **Linux with kernel 5.8+**. No macOS, no Windows, no CI runner
  without io_uring. Check `ulimit -l` — io_uring registers locked memory, and
  the default 64 KB is too small for a real ring depth. Raise `memlock` to
  unlimited, or expect obscure `ENOMEM` at executor startup.

MSRV is 1.92, edition 2021.

Recommended, and measured — put this in the binary:

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

glommio allocates one block per spawned task. At 512 live tasks, spawn costs
~45 ns under glibc malloc, ~30 ns under jemalloc, ~24 ns under mimalloc. glommio
deliberately has no allocator of its own — a thread-local free list was built,
measured, and thrown away because it bought nothing under mimalloc.

---

## 2. The model shift — read this before writing code

Tokio is a work-stealing scheduler over a thread pool. glommio is one executor
per core, pinned, sharing nothing. Almost every reflex you have from Tokio is
either unnecessary or actively wrong here.

| | Tokio | glommio |
|---|---|---|
| Futures | must be `Send` | **`!Send` is normal and preferred** |
| Shared state | `Arc<Mutex<T>>` | **`Rc<RefCell<T>>`** — no atomics, no contention |
| Spawning | `tokio::spawn` anywhere | `spawn_local` **only inside an executor** |
| Cross-thread | implicit, via the scheduler | **explicit channels only** |
| Blocking | degrades a pool thread | **stalls an entire core** |
| Task migration | yes | **never** |

**The single most important consequence:** inside one shard you are
single-threaded. Drop the synchronization. `Rc<RefCell<OperatorState>>` is the
correct type for operator state, and every `Arc<Mutex<_>>` you carry over is
pure overhead plus a deadlock you don't need.

**The second most important:** nothing moves between cores implicitly. A shuffle
between operators on different shards is an explicit channel send, and you must
design where the shard boundaries are. This is the same discipline Flink already
imposes with keyed streams, so the model should fit Slipstream well — arguably
better than Tokio's does.

### Starting executors

```rust
use glommio::{LocalExecutorPoolBuilder, PoolPlacement, CpuSet};

LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(nr_shards, None))
    .spin_before_park(Duration::from_micros(50))
    .ring_depth(256)
    .on_all_shards(|| async move {
        // one shard's worth of the pipeline
    })?
    .join_all();
```

`Placement`/`PoolPlacement` control pinning. **Placement is not cosmetic** — we
measured cross-shard message costs varying with cache-domain topology, and
modern CPUs have L3 domains that the NUMA/package model doesn't show you. If
shards talk to each other heavily, co-locating them in an L3 domain is
measurable. `MaxSpread` spreads across domains, `MaxPack` fills one first.

### Cross-shard messaging

Use `channels::channel_mesh::MeshBuilder` — an all-to-all mesh built once at
startup, which is exactly the shuffle topology Slipstream needs:

```rust
let mesh = MeshBuilder::<Record, Full>::full(nr_shards, channel_size);
// per shard:
let (senders, receivers) = mesh.join().await?;
senders.send_to(target_shard, record).await?;
```

Route by `hash(key) % nr_shards` for keyed streams. `channels::sharding` has a
higher-level `Sharded` abstraction over this if you want the routing built in.

`channels::local_channel` for same-shard, `channels::shared_channel` for a
one-off cross-shard pair outside the mesh.

---

## 3. API map

| Tokio | glommio |
|---|---|
| `tokio::spawn` | `glommio::spawn_local` |
| `tokio::net::TcpStream` | `glommio::net::TcpStream` |
| `tokio::fs::File` | `glommio::io::BufferedFile`, or `DmaFile` for O_DIRECT |
| `tokio::time::sleep` | `glommio::timer::sleep` |
| `tokio::time::timeout` | `glommio::timer::timeout` |
| `tokio::sync::mpsc` | `glommio::channels::local_channel` |
| `tokio::task::yield_now` | `glommio::yield_if_needed().await` |
| `tokio::task::spawn_blocking` | `glommio::executor().spawn_blocking` — see below |
| `AsyncRead`/`AsyncWrite` (tokio traits) | **futures-lite / futures traits** |

That last row bites early. glommio uses the `futures` I/O traits, not Tokio's.
Anything generic over `tokio::io::AsyncRead` needs `tokio-util::compat` or a
trait-boundary change. Decide which at the start — retrofitting is miserable.

`DmaFile` requires **alignment**: offsets, lengths and buffers must be aligned
to the device's logical block size. Use `allocate_dma_buffer`. If you don't need
O_DIRECT, `BufferedFile` has none of these constraints.

---

## 4. Performance guidance, from measurement

These are all things measured on this fork, not folklore. See
`docs/investigations/` in the glommio repo for the probes and numbers.

**glommio costs ~2 µs per I/O operation that actually blocks.** That is the
executor round trip: park, kernel enter, completion, wake, reschedule. Context:
6% of a 35 µs NVMe read, but 53% of a 3.8 µs loopback TCP round trip.

**Concurrency amortizes it, and this is the main lever you have.** Going from 1
to 4 concurrent connections dropped per-round-trip cost 2.4x. Practically:

- Do **not** build request-response chains that hold one operation in flight.
  That is the worst case for this runtime and the case where Tokio may beat it.
- Keep many operations outstanding. Batch. Pipeline. A stream engine naturally
  has many records in flight — lean on that.
- Beware `StreamExt::then` — **it is sequential**. It pinned queue depth at 1 in
  one of our own benchmarks and produced a completely fake flat latency curve.
  Use `spawn_local` per item, or an explicit concurrency-limited buffer, when
  you want parallelism.

**Give the scheduler cooperation points.** Long CPU-bound loops (window
aggregation, sorting, serialization of a large batch) starve everything else on
the core, including the I/O reactor. Insert:

```rust
if glommio::executor().need_preempt() {
    glommio::yield_if_needed().await;
}
```

`need_preempt()` is two loads, no syscall — cheap enough for an inner loop.

**Use task queues for priority.** This maps well onto a stream engine's
structure: the data path, checkpointing, and control plane should not compete
as equals.

```rust
let control = glommio::executor().create_task_queue(
    Shares::Static(1000), Latency::Matters(Duration::from_millis(1)), "control");
let data = glommio::executor().create_task_queue(
    Shares::Static(100), Latency::NotImportant, "data");

spawn_local_into(async { ... }, control)?.detach();
```

`Latency::Matters` tasks get their own io_uring, so a bulk file write can't
delay them behind a deep queue.

**`spin_before_park`** trades CPU for latency — the executor busy-waits before
sleeping. Worth ~18% on a loopback ping-pong. Costs a hot core when idle. Right
for a latency-sensitive data plane, wrong if you're packing shards onto shared
hardware.

**Never block.** No `std::fs`, no blocking `Mutex`, no `reqwest::blocking`, no
long `std::thread::sleep`. Each one freezes a whole core including its reactor.
For genuinely blocking work use `executor().spawn_blocking`, which hands it to a
pool thread — but that costs a thread handoff, so it is for rare operations
(config load, DNS), not for anything on the record path.

**The stall detector will fire, and it will be right.** Leave it on during
migration; it prints a backtrace of whatever monopolized the core. It is the
single best tool for finding accidental blocking. Treat every report as a bug in
your code, not noise.

---

## 5. Migration strategy

**Do not big-bang this.** Suggested order:

1. **Prove the model on one operator.** Take a single stateful operator, run it
   in a one-shard glommio executor, feed it from the existing pipeline. Confirm
   the `!Send` state actually simplifies (it should — the `Arc<Mutex<_>>` should
   disappear) and that throughput is sane.
2. **Establish the baseline first, and keep it runnable.** You already have
   Tokio numbers. Keep the Tokio path behind a feature flag or a trait so you
   can A/B on the same hardware, same day. Cross-session performance comparison
   is worthless — we saw an unchanged code path drift 28.7 → 19.5 → 16.5 ns
   between sessions and drew a wrong conclusion from it before catching it.
3. **Then the shard topology.** Executor pool, mesh, keyed routing. This is the
   real work and the part where the design either fits or doesn't.
4. **Then the I/O edges** — sources and sinks. These are where third-party
   crates will fight you, because most async crates assume Tokio. Expect to
   write raw protocol handling for anything glommio doesn't cover, or to keep a
   small Tokio runtime on a separate thread for the control plane and talk to it
   over a channel. That hybrid is legitimate and often the right answer.

**Where glommio will lose to Tokio, honestly:** low queue depth
request-response, workloads with genuinely blocking dependencies, anything
needing the Tokio ecosystem (tonic, hyper's default stack, most database
drivers), and any deployment that isn't Linux 5.8+. Confirm Slipstream's
workload isn't in that list before committing to the migration. A stream engine
with high in-flight record counts and explicit sharding is close to the ideal
case — but "close to ideal" is a hypothesis until it's measured.

---

## 6. Measurement discipline

We wasted real time on each of these. Don't repeat them.

- **Measure the premise before building the fix.** A task-arena allocator was
  built to solve malloc contention on the spawn path. The contention did not
  exist. Months, then reverted.
- **Interleave A/B runs.** Not run-A-then-run-B, and never across sessions.
- **Check both sides of a comparison are configured identically.** We produced a
  fake 10x by leaving `TCP_NODELAY` off on the control and on for glommio. A
  10x result should make you suspicious, not excited.
- **A suspiciously flat curve means your benchmark isn't doing what you think.**
  See the `StreamExt::then` note above.
- **Implement the design by hand to separate design from implementation.** When
  glommio's network path looked slow, hand-writing its exact strategy without
  glommio showed the *design* cost −34 ns — the overhead was the executor round
  trip, not the design. That distinction changes what you'd fix.

---

## 7. Reference

- Fork: https://github.com/dahankzter/glommio (`docs/investigations/` has the
  measurements behind every number above)
- Live upstream: https://github.com/glommio/glommio
- `examples/` in the fork — `sharding.rs` is the channel-mesh one, closest to
  Slipstream's shape
- Report anything that looks like a glommio bug back to the fork maintainer;
  several fixes have come out of exactly this kind of adoption.
