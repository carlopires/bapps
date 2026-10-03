// Copyright 2024 Glommio Project Authors. Licensed under Apache-2.0.

//! Timer re-entrancy soak test
//!
//! This test validates that the timing wheel integration handles re-entrancy
//! correctly under heavy async load. It spawns thousands of tasks that
//! continuously re-register timers, stressing the RefCell borrowing in the
//! Reactor.
//!
//! Run with: cargo run --features timing-wheel --example timer_soak_test

use glommio::{spawn_local, timer::sleep, LocalExecutor};
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

const NUM_TASKS: usize = 10_000;
const TEST_DURATION_SECS: u64 = 300; // 5 minutes
const MIN_TIMER_MS: u64 = 1;
const MAX_TIMER_MS: u64 = 100;

thread_local! {
    static RNG_SEED: Cell<u64> = const { Cell::new(0x123456789abcdef0) };
}

/// Simple xorshift RNG (no need for rand crate)
fn fast_rand() -> u64 {
    RNG_SEED.with(|seed| {
        let mut x = seed.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        seed.set(x);
        x
    })
}

fn random_delay() -> Duration {
    let ms = MIN_TIMER_MS + (fast_rand() % (MAX_TIMER_MS - MIN_TIMER_MS));
    Duration::from_millis(ms)
}

async fn timer_churner(task_id: usize, end_time: Instant, stats: Rc<Cell<u64>>) {
    let mut iterations = 0u64;

    while Instant::now() < end_time {
        // Random sleep duration
        let delay = random_delay();
        sleep(delay).await;

        iterations += 1;

        // Update stats every 100 iterations to reduce contention
        if iterations.is_multiple_of(100) {
            stats.set(stats.get() + 100);
        }
    }

    // Final stats update
    stats.set(stats.get() + (iterations % 100));

    println!("Task {} completed {} iterations", task_id, iterations);
}

fn main() {
    println!("{}", "=".repeat(70));
    println!("TIMER RE-ENTRANCY SOAK TEST");
    println!("{}", "=".repeat(70));
    println!("Configuration:");
    println!("  Tasks:             {}", NUM_TASKS);
    println!("  Test Duration:     {}s", TEST_DURATION_SECS);
    println!("  Timer Range:       {}-{}ms", MIN_TIMER_MS, MAX_TIMER_MS);
    println!("{}", "=".repeat(70));

    let start = Instant::now();
    let end_time = start + Duration::from_secs(TEST_DURATION_SECS);

    let ex = LocalExecutor::default();

    ex.run(async move {
        println!("\n🚀 Spawning {} tasks...", NUM_TASKS);

        let stats = Rc::new(Cell::new(0u64));
        let mut tasks = Vec::with_capacity(NUM_TASKS);

        // Spawn all churner tasks
        for i in 0..NUM_TASKS {
            let stats_clone = stats.clone();
            let task = spawn_local(async move {
                timer_churner(i, end_time, stats_clone).await;
            });
            tasks.push(task);

            // Print progress every 1000 tasks
            if (i + 1) % 1000 == 0 {
                println!("  Spawned {}/{} tasks...", i + 1, NUM_TASKS);
            }
        }

        println!("✅ All tasks spawned!\n");

        // Monitor progress
        let monitor_task = spawn_local(async move {
            let mut last_count = 0u64;
            let mut report_interval = 0;

            loop {
                sleep(Duration::from_secs(10)).await;

                if Instant::now() >= end_time {
                    break;
                }

                report_interval += 10;
                let current_count = stats.get();
                let delta = current_count - last_count;
                last_count = current_count;

                println!(
                    "[{:3}s] Total iterations: {} (+{} in last 10s, {}/s)",
                    report_interval,
                    current_count,
                    delta,
                    delta / 10
                );
            }

            stats.get()
        });

        // Wait for all tasks to complete
        let final_count = monitor_task.await;

        for task in tasks {
            task.await;
        }

        let elapsed = start.elapsed();

        println!("\n{}", "=".repeat(70));
        println!("TEST COMPLETE");
        println!("{}", "=".repeat(70));
        println!("Duration:          {:.2}s", elapsed.as_secs_f64());
        println!("Total iterations:  {}", final_count);
        println!(
            "Iterations/sec:    {:.0}",
            final_count as f64 / elapsed.as_secs_f64()
        );
        println!(
            "Iterations/task:   {:.0}",
            final_count as f64 / NUM_TASKS as f64
        );
        println!("{}", "=".repeat(70));
        println!("\n✅ No panics detected - re-entrancy safety confirmed!");
    });
}
