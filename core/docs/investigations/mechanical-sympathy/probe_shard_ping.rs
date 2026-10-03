//! Runtime-level probe for the placement candidate: how much does the L3
//! domain of the peer shard cost a glommio shared_channel round trip?
//!
//! Two shards ping-pong a u64 over a pair of shared_channels. Only the CPU
//! assignment changes between runs.

use glommio::{channels::shared_channel, LocalExecutorBuilder, Placement};
use std::time::{Duration, Instant};

const ROUNDS: usize = 20_000;

fn round_trip(cpu_a: usize, cpu_b: usize, spin: Option<Duration>) -> f64 {
    // a -> b
    let (tx1, rx1) = shared_channel::new_bounded::<u64>(1);
    // b -> a
    let (tx2, rx2) = shared_channel::new_bounded::<u64>(1);

    let mut bb = LocalExecutorBuilder::new(Placement::Fixed(cpu_b));
    if let Some(s) = spin {
        bb = bb.spin_before_park(s);
    }
    let b = bb
        .spawn(move || async move {
            let rx = rx1.connect().await;
            let tx = tx2.connect().await;
            while let Some(v) = rx.recv().await {
                if tx.send(v).await.is_err() {
                    break;
                }
            }
        })
        .unwrap();

    let mut ab = LocalExecutorBuilder::new(Placement::Fixed(cpu_a));
    if let Some(s) = spin {
        ab = ab.spin_before_park(s);
    }
    let a = ab
        .spawn(move || async move {
            let tx = tx1.connect().await;
            let rx = rx2.connect().await;

            // warmup
            for i in 0..2_000u64 {
                tx.send(i).await.unwrap();
                rx.recv().await.unwrap();
            }

            let start = Instant::now();
            for i in 0..ROUNDS as u64 {
                tx.send(i).await.unwrap();
                rx.recv().await.unwrap();
            }
            let el = start.elapsed();
            drop(tx);
            el.as_nanos() as f64 / ROUNDS as f64
        })
        .unwrap();

    let ns = a.join().unwrap();
    let _ = b.join();
    ns
}

fn main() {
    println!("glommio shared_channel round trip, by peer placement");
    println!("(this part: L3 domains are cpus 0-7, 8-15, 16-23, 24-31)\n");

    let spins = [
        ("park immediately (default)", None),
        ("spin_before_park(10us)", Some(Duration::from_micros(10))),
        ("spin_before_park(1ms)", Some(Duration::from_millis(1))),
    ];
    let cases = [
        ("same L3   (0<->4)", 0usize, 4usize),
        ("cross L3  (0<->16)", 0, 16),
    ];

    for (sname, spin) in spins {
        println!("{sname}:");
        for (name, a, b) in cases {
            let mut best = f64::MAX;
            for _ in 0..3 {
                let v = round_trip(a, b, spin);
                if v < best {
                    best = v;
                }
            }
            println!("  {name:<22} {best:>9.0} ns/round-trip");
        }
        println!();
    }
}
