//! How many heap allocations does glommio make per unit of work?
//!
//! The premise behind "chase the allocations" has never been tested on this
//! fork's I/O paths. It is cheap to test: wrap the global allocator in a
//! counter, run each path, divide. A path that allocates zero times per
//! operation cannot be improved by removing allocations, and a path that
//! allocates once per operation is worth exactly one allocation, which is
//! ~24 ns under mimalloc and less under glibc for a hot size class.
//!
//! Counts allocations, not time. Deliberately: time on these paths is
//! dominated by io_uring and the device, which is what makes an allocation
//! count the more honest signal about whether there is anything here at all.
//!
//! Run with the same allocator deployments use:
//!
//! ```sh
//! cargo run --release --example alloc_per_op
//! RUSTFLAGS='--cfg alloc_mimalloc' cargo run --release --example alloc_per_op
//! ```

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    LocalExecutorBuilder, Placement,
    net::{TcpListener, TcpStream},
};

/// Counts every allocation the process makes while `ARMED` is set.
struct Counting;

static ARMED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

/// Exact-size histogram, so a count can be matched against the size of a
/// concrete type rather than guessed at from a total.
const HIST_MAX: usize = 8192;
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicUsize = AtomicUsize::new(0);
static HIST: [AtomicUsize; HIST_MAX] = [ZERO; HIST_MAX];

/// The allocator underneath the counter. Switching it is the premise test:
/// if the allocation count on a path mattered, a faster allocator would move
/// that path's wall time.
#[cfg(alloc_mimalloc)]
static UNDER: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(not(alloc_mimalloc))]
static UNDER: System = System;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            if layout.size() < HIST_MAX {
                HIST[layout.size()].fetch_add(1, Ordering::Relaxed);
            }
        }
        unsafe { UNDER.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { UNDER.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        unsafe { UNDER.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

thread_local! {
    static STARTED: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

/// Starts counting. Paired with [`report`], which stops and divides.
fn arm() {
    STARTED.with(|s| s.set(Some(std::time::Instant::now())));
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    for slot in HIST.iter() {
        slot.store(0, Ordering::Relaxed);
    }
    ARMED.store(true, Ordering::Relaxed);
}

/// Stops counting and reports allocations per operation.
fn report(name: &str, ops: usize) {
    ARMED.store(false, Ordering::Relaxed);
    let elapsed = STARTED.with(|s| s.get().unwrap().elapsed());

    let allocs = ALLOCS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    println!(
        "{name:<34} {:>9.2} allocs/op  {:>9.1} bytes/op  {:>9.0} ns/op",
        allocs as f64 / ops as f64,
        bytes as f64 / ops as f64,
        elapsed.as_nanos() as f64 / ops as f64,
    );

    let mut sizes: Vec<_> = HIST
        .iter()
        .enumerate()
        .filter_map(|(size, count)| match count.load(Ordering::Relaxed) {
            0 => None,
            count => Some((size, count)),
        })
        .collect();
    sizes.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    for (size, count) in sizes.iter().take(8) {
        println!("    {:>5} B x {:>6.2}/op", size, *count as f64 / ops as f64);
    }
}

const ROUNDS: usize = 20_000;
const MSG: usize = 64;

fn main() {
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async {
            // Warm every lazily-initialised path before arming: one-time setup
            // is not a per-operation cost and would otherwise be smeared
            // across the first measurement.
            glommio::executor().yield_now().await;
            glommio::spawn_local(async {}).await;

            arm();
            for _ in 0..ROUNDS {
                glommio::spawn_local(async {}).await;
            }
            report("spawn_local + await", ROUNDS);

            arm();
            for _ in 0..ROUNDS {
                glommio::executor().yield_now().await;
            }
            report("yield_now", ROUNDS);

            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let server = glommio::spawn_local(async move {
                let mut stream = listener.accept().await.unwrap();
                let mut buf = [0u8; MSG];
                while stream.read_exact(&mut buf).await.is_ok() {
                    stream.write_all(&buf).await.unwrap();
                }
            });

            let mut client = TcpStream::connect(addr).await.unwrap();
            let mut buf = [0u8; MSG];

            // One round trip outside the counter: the first touches connection
            // setup, buffer pools and source registration.
            client.write_all(&buf).await.unwrap();
            client.read_exact(&mut buf).await.unwrap();

            arm();
            for _ in 0..ROUNDS {
                client.write_all(&buf).await.unwrap();
                client.read_exact(&mut buf).await.unwrap();
            }
            report("TCP round trip (64B, depth 1)", ROUNDS);

            drop(client);
            let _ = server.await;

            let (sender, receiver) = glommio::channels::local_channel::new_bounded(4);
            let consumer = glommio::spawn_local(async move {
                let mut seen = 0usize;
                while receiver.recv().await.is_some() {
                    seen += 1;
                }
                seen
            });

            arm();
            for i in 0..ROUNDS {
                sender.send(i).await.unwrap();
            }
            report("local channel send", ROUNDS);
            drop(sender);
            consumer.await;
        })
        .unwrap();

    ex.join().unwrap();
}
