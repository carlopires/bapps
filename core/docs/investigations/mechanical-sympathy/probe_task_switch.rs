//! What does one task switch (wake -> schedule -> poll) cost in glommio today?
use futures_lite::future::yield_now;
use glommio::{LocalExecutor, LocalExecutorBuilder, Placement};
use std::time::Instant;

fn main() {
    let h = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| async {
            const N: usize = 2_000_000;
            // one task yielding in a loop: pure wake/schedule/poll cycle
            for _ in 0..100_000 { yield_now().await; }
            let start = Instant::now();
            for _ in 0..N { yield_now().await; }
            let el = start.elapsed();
            println!("single task, yield loop      : {:.2} ns/switch",
                     el.as_nanos() as f64 / N as f64);

            // two tasks ping-ponging through the queue
            const M: usize = 500_000;
            let t = glommio::spawn_local(async {
                for _ in 0..M { yield_now().await; }
            });
            let start = Instant::now();
            for _ in 0..M { yield_now().await; }
            t.await;
            let el = start.elapsed();
            println!("two tasks alternating        : {:.2} ns/switch",
                     el.as_nanos() as f64 / (2 * M) as f64);
        })
        .unwrap();
    h.join().unwrap();
    let _ = LocalExecutor::default();
}
