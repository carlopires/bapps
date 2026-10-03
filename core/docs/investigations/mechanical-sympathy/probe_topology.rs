//! Cross-core coherence probe: what does one shard touching another shard's
//! cache line actually cost on this part, within an L3 domain vs across one?
//!
//! Relevant because glommio is thread-per-core: every cross-shard wakeup
//! (shared_channel, foreign wake) moves a line between shards.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const ITERS: u64 = 2_000_000;

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// Ping-pong a single cache line between two pinned threads.
/// Reports the round-trip latency, i.e. the cost of moving the line both ways.
fn ping_pong(cpu_a: usize, cpu_b: usize) -> f64 {
    let flag = Arc::new(AtomicU64::new(0));
    let f2 = flag.clone();

    let b = thread::spawn(move || {
        pin(cpu_b);
        let mut expect = 1;
        while expect <= ITERS {
            while f2.load(Ordering::Acquire) != expect {
                std::hint::spin_loop();
            }
            f2.store(expect + 1, Ordering::Release);
            expect += 2;
        }
    });

    pin(cpu_a);
    // warmup
    let start = Instant::now();
    let mut v = 0;
    while v < ITERS {
        flag.store(v + 1, Ordering::Release);
        while flag.load(Ordering::Acquire) != v + 2 {
            std::hint::spin_loop();
        }
        v += 2;
    }
    let el = start.elapsed();
    b.join().unwrap();
    black_box(&flag);
    el.as_nanos() as f64 / (ITERS / 2) as f64
}

/// Contended atomic increment from N threads on one line.
fn contended(cpus: &[usize]) -> f64 {
    let counter = Arc::new(AtomicU64::new(0));
    let per = 1_000_000u64;
    let start = Instant::now();
    let handles: Vec<_> = cpus
        .iter()
        .map(|&c| {
            let ctr = counter.clone();
            thread::spawn(move || {
                pin(c);
                for _ in 0..per {
                    ctr.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let el = start.elapsed();
    el.as_nanos() as f64 / (per * cpus.len() as u64) as f64
}

fn main() {
    println!("AMD Threadripper PRO 9975WX: 32 cores / 4 L3 domains (8 cores each)\n");

    // cores 0-7 share an L3; 0 and 16 are in different L3 domains.
    println!("cache-line ping-pong (round trip):");
    println!("  same core, sibling SMT (0<->1)   {:>7.1} ns", ping_pong(0, 1));
    println!("  same L3 domain     (0<->4)       {:>7.1} ns", ping_pong(0, 4));
    println!("  cross L3 domain    (0<->16)      {:>7.1} ns", ping_pong(0, 16));
    println!("  far cross domain   (0<->24)      {:>7.1} ns", ping_pong(0, 24));

    println!("\ncontended fetch_add on one line:");
    println!("  1 thread                         {:>7.1} ns/op", contended(&[0]));
    println!("  2 threads, same L3               {:>7.1} ns/op", contended(&[0, 4]));
    println!("  8 threads, same L3               {:>7.1} ns/op", contended(&[0, 1, 2, 3, 4, 5, 6, 7]));
    println!(
        "  8 threads, spread across L3      {:>7.1} ns/op",
        contended(&[0, 4, 8, 12, 16, 20, 24, 28])
    );
    println!(
        "  32 threads, all domains          {:>7.1} ns/op",
        contended(&(0..32).collect::<Vec<_>>())
    );
}
