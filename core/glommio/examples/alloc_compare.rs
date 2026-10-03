//! Task spawn cost under different global allocators.
//!
//! Glommio allocates one block per spawned task and frees it on completion, on
//! the same thread, which is exactly the pattern a modern per-thread allocator
//! is built for. This measures how much that choice is worth.
//!
//! Build with one of:
//!   RUSTFLAGS='--cfg alloc_system'   (default glibc malloc)
//!   RUSTFLAGS='--cfg alloc_mimalloc'
//!   RUSTFLAGS='--cfg alloc_jemalloc'
//!
//! then: cargo run --release --example alloc_compare

#[cfg(alloc_mimalloc)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(alloc_jemalloc)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use glommio::{spawn_local, LocalExecutor};
use std::time::Instant;

const REPS: usize = 7;

fn allocator_name() -> &'static str {
    if cfg!(alloc_mimalloc) {
        "mimalloc"
    } else if cfg!(alloc_jemalloc) {
        "jemalloc"
    } else {
        "system"
    }
}

/// Spawn `count` tasks so they are live simultaneously, then await them.
async fn batch(count: usize) -> f64 {
    let start = Instant::now();
    let handles: Vec<_> = (0..count)
        .map(|i| spawn_local(async move { std::hint::black_box(i) }))
        .collect();
    for h in handles {
        h.await;
    }
    start.elapsed().as_nanos() as f64 / count as f64
}

fn main() {
    LocalExecutor::default().run(async {
        batch(10_000).await; // warm up

        println!("# allocator={}", allocator_name());
        println!("{:>11},{:>9}", "live_tasks", "ns_per_task");
        for &n in &[1usize, 8, 32, 64, 128, 256, 512, 1024, 2048, 8192] {
            let mut s = Vec::new();
            for _ in 0..REPS {
                s.push(batch(n).await);
            }
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!("{:>11},{:>9.1}", n, s[s.len() / 2]);
        }
    });
}
