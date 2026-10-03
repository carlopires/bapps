//! What the timer structure costs, in the shape glommio uses it.
//!
//! Instrumenting a server first said the workload is arming and cancelling
//! with almost no expiry: a read timeout is set per operation and withdrawn
//! when the read completes, so at 4096 connections every timer was cancelled
//! and none fired. These cases are weighted accordingly.
//!
//! Timed through `iter_custom` with the executor driven by hand rather than
//! `to_async`, because the measured window has to exclude building the timers
//! and dropping them, which are the other case in the same benchmark.

mod common;

use common::Glommio;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_lite::future::poll_once;
use glommio::timer::{sleep, Timer};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

const POPULATIONS: &[usize] = &[64, 256, 1_024, 4_096];

/// Far enough out that nothing in these cases reaches it.
const PARKED: Duration = Duration::from_secs(3_600);

/// Registering a timer that will not fire.
fn arm(c: &mut Criterion) {
    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/arm");

    for &n in POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut timers: Vec<Timer> = (0..n).map(|_| Timer::new(PARKED)).collect();

                        let started = Instant::now();
                        for timer in timers.iter_mut() {
                            // The first poll is what registers it.
                            black_box(poll_once(timer).await);
                        }
                        total += started.elapsed() / n as u32;

                        drop(timers);
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

/// Withdrawing a timer that has not fired, which the measured workload does to
/// every timer it creates.
fn cancel(c: &mut Criterion) {
    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/cancel");

    for &n in POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let mut timers: Vec<Timer> = (0..n).map(|_| Timer::new(PARKED)).collect();
                        for timer in timers.iter_mut() {
                            poll_once(timer).await;
                        }

                        let started = Instant::now();
                        drop(timers);
                        total += started.elapsed() / n as u32;
                    }
                    total
                })
            })
        });
    }
    group.finish();
}

/// A short sleep with a population already waiting.
///
/// Two things show up here. A structure that answers "what is due next" by
/// scanning grows with the population. And one that reports the tick a
/// deadline was rounded into rather than the deadline itself puts a floor
/// under every sleep shorter than its resolution.
fn sleep_under_population(c: &mut Criterion) {
    let ex = Glommio::default();
    let mut group = c.benchmark_group("timer/sleep_100us");

    for &n in POPULATIONS {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                ex.0.run(async {
                    let mut parked: Vec<Timer> = (0..n).map(|_| Timer::new(PARKED)).collect();
                    for timer in parked.iter_mut() {
                        poll_once(timer).await;
                    }

                    let started = Instant::now();
                    for _ in 0..iters {
                        sleep(Duration::from_micros(100)).await;
                    }
                    let elapsed = started.elapsed();

                    drop(parked);
                    elapsed
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, arm, cancel, sleep_under_population);
criterion_main!(benches);
