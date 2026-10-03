//! What the storage seam costs.
//!
//! The channels in `glommio::channels` are written once and instantiated over
//! two storages: `Local` (`Rc<RefCell<_>>`) and `Shared` (`Arc<Mutex<_>>`).
//! Two questions follow, and this answers both with numbers rather than
//! confidence:
//!
//! 1. Does reaching the state through a closure cost the local channels
//!    anything? It should not -- a `RefCell` borrow behind a call that inlines
//!    -- but "should not" is how the task arena started.
//! 2. What does the shared channel cost when several cores read through one
//!    mutex? That is the shape the design deliberately does not optimise, so
//!    the number should be known rather than hand-waved.
//!
//! Run with:
//! ```bash
//! cargo run --release --example channel_storage_cost
//! ```
//!
//! Question 1 is answered by comparing the local numbers against the same file
//! run on a revision without the seam: it uses only the public local API,
//! which the seam did not change.

use futures_lite::future::block_on;
use glommio::{
    channels::{broadcast, oneshot, watch},
    LocalExecutor, LocalExecutorBuilder, Placement,
};
use std::time::{Duration, Instant};

const ROUNDS: usize = 200_000;

fn main() {
    LocalExecutor::default().run(async {
        local_broadcast();
        local_watch();
        local_oneshot();
        shared_broadcast_single_reader();
    });

    for cores in [2, 4, 8] {
        shared_broadcast_contended(cores);
    }
}

fn report(what: &str, elapsed: Duration, ops: usize) {
    println!(
        "{what:<46} {:>8.1} ns/op",
        elapsed.as_nanos() as f64 / ops as f64
    );
}

fn local_broadcast() {
    let (sender, mut receiver) = broadcast::broadcast::<u64>(16);

    let start = Instant::now();
    for value in 0..ROUNDS as u64 {
        sender.send(value).unwrap();
        std::hint::black_box(receiver.try_recv().unwrap());
    }
    report("local broadcast send+recv", start.elapsed(), ROUNDS);
}

fn local_watch() {
    let (sender, receiver) = watch::watch(0u64);

    let start = Instant::now();
    for value in 0..ROUNDS as u64 {
        sender.send(value).unwrap();
        std::hint::black_box(*receiver.borrow());
    }
    report("local watch send+borrow", start.elapsed(), ROUNDS);
}

fn local_oneshot() {
    // A fresh pair each round: creating one is most of what a oneshot costs.
    let start = Instant::now();
    for value in 0..ROUNDS as u64 {
        let (sender, receiver) = oneshot::oneshot::<u64>();
        sender.send(value).unwrap();
        std::hint::black_box(block_on(receiver).unwrap());
    }
    report("local oneshot create+send+recv", start.elapsed(), ROUNDS);
}

fn shared_broadcast_single_reader() {
    // The uncontended cost of the same channel over a mutex.
    let (sender, mut receiver) = broadcast::shared::<u64>(16);

    let start = Instant::now();
    for value in 0..ROUNDS as u64 {
        sender.send(value).unwrap();
        std::hint::black_box(receiver.try_recv().unwrap());
    }
    report(
        "shared broadcast send+recv, 1 reader",
        start.elapsed(),
        ROUNDS,
    );
}

fn shared_broadcast_contended(cores: usize) {
    // Every reader is on its own executor, all reading through one mutex.
    // This is the contention the fan-out shape trades for a single
    // implementation, and what the docs tell people to keep to the control
    // plane.
    let per_core = ROUNDS / cores;
    let (sender, first) = broadcast::shared::<u64>(1024);

    let readers: Vec<_> = (0..cores)
        .map(|core| {
            let mut receiver = if core == 0 {
                first.clone()
            } else {
                sender.subscribe()
            };
            LocalExecutorBuilder::new(Placement::Unbound)
                .spawn(move || async move {
                    let mut seen = 0usize;
                    while seen < per_core {
                        match receiver.recv().await {
                            Ok(_) => seen += 1,
                            Err(broadcast::RecvError::Lagged(missed)) => seen += missed as usize,
                            Err(broadcast::RecvError::Closed) => break,
                        }
                    }
                    seen
                })
                .unwrap()
        })
        .collect();
    drop(first);

    let start = Instant::now();
    for value in 0..per_core as u64 {
        sender.send(value).unwrap();
    }
    let elapsed = start.elapsed();
    drop(sender);

    for reader in readers {
        reader.join().unwrap();
    }
    report(
        &format!("shared broadcast send, {cores} readers on {cores} cores"),
        elapsed,
        per_core,
    );
}
