//! to_thread::run_sync: blocking work on the executor's blocking thread pool,
//! bounded by a CapacityLimiter, cancelled cooperatively.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use bapps_trio::{
    CancelScope, Nursery,
    sync::CapacityLimiter,
    to_thread::{self, ThreadCancel},
    with_cancel_scope, with_nursery,
};
use glommio::{LocalExecutor, LocalExecutorBuilder, Placement, PoolPlacement};

fn executor_with_pool(threads: usize) -> LocalExecutor {
    LocalExecutorBuilder::new(Placement::Unbound)
        .blocking_thread_pool_placement(PoolPlacement::Unbound(threads))
        .make()
        .expect("executor")
}

#[test]
fn a_job_runs_off_the_executor_thread() {
    let (executor_thread, job_thread) = LocalExecutor::default().run(async {
        let here = thread::current().id();
        let there = to_thread::run_sync(|_| thread::current().id())
            .await
            .unwrap();
        (here, there)
    });
    assert_ne!(executor_thread, job_thread);
}

#[test]
fn the_default_limiter_matches_the_default_pool() {
    let total =
        LocalExecutor::default().run(async { to_thread::default_thread_limiter().total_tokens() });
    assert_eq!(total, 1);
}

/// Six 30 ms jobs on a four-thread pool, through a limiter of `tokens`:
/// the most that ever ran at once.
fn most_at_once(tokens: usize) -> usize {
    executor_with_pool(4).run(async move {
        let limiter = CapacityLimiter::new(tokens);
        let (active, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let result = most.clone();
        with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
            Box::pin(async move {
                for _ in 0..6 {
                    let (limiter, active, most) = (limiter.clone(), active.clone(), most.clone());
                    nursery
                        .spawn(move |_| async move {
                            to_thread::run_sync_with(&limiter, move |_| {
                                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                                most.fetch_max(now, Ordering::SeqCst);
                                thread::sleep(Duration::from_millis(30));
                                active.fetch_sub(1, Ordering::SeqCst);
                            })
                            .await
                            .unwrap();
                            Ok(())
                        })
                        .unwrap();
                }
            })
        })
        .await
        .unwrap();
        result.load(Ordering::SeqCst)
    })
}

#[test]
fn the_limiter_bounds_how_many_jobs_run_at_once() {
    assert_eq!(most_at_once(2), 2);
    // The pool itself allows four: the bound above is the limiter's.
    assert_eq!(most_at_once(4), 4);
}

#[test]
fn a_job_cancelled_while_waiting_never_runs() {
    let (result, ran, holders) = LocalExecutor::default().run(async {
        let limiter = CapacityLimiter::new(1);
        let held = limiter.try_acquire().unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let scope = CancelScope::new();
        let (canceller, job_ran) = (scope.clone(), ran.clone());
        let result = futures_lite::future::zip(
            with_cancel_scope(
                scope,
                to_thread::run_sync_with(&limiter, move |_| job_ran.store(true, Ordering::SeqCst)),
            ),
            async {
                // Cancel once the job is queued in the limiter: an explicit
                // state, not a sleep that hopes it got there.
                // Bounded, as a deadlock guard: a job that never queues fails
                // the test instead of hanging it.
                for _ in 0..100_000 {
                    if limiter.waiting() > 0 {
                        break;
                    }
                    futures_lite::future::yield_now().await;
                }
                assert_eq!(limiter.waiting(), 1, "the job never queued in the limiter");
                canceller.cancel();
            },
        )
        .await
        .0;
        drop(held);
        // Only this test holds `ran` now: the job's closure, which captured a
        // clone, was dropped without being submitted, so it can never run.
        (result, ran.load(Ordering::SeqCst), Arc::strong_count(&ran))
    });
    assert!(result.is_err(), "{result:?}");
    assert!(!ran, "a job cancelled before it started must not run");
    assert_eq!(
        holders, 1,
        "the cancelled job was dropped, not kept for later"
    );
}

#[test]
fn a_running_job_is_told_and_waited_for() {
    let (result, took) = LocalExecutor::default().run(async {
        let scope = CancelScope::new();
        let canceller = scope.clone();
        let start = Instant::now();
        let (running_tx, running) = async_channel::bounded(1);
        let result = futures_lite::future::zip(
            with_cancel_scope(
                scope,
                to_thread::run_sync(move |cancel: &ThreadCancel| {
                    let _ = running_tx.send_blocking(());
                    let start = Instant::now();
                    while !cancel.is_cancelled() && start.elapsed() < Duration::from_secs(2) {
                        thread::sleep(Duration::from_millis(1));
                    }
                    cancel.is_cancelled()
                }),
            ),
            async move {
                // Cancel once the job is running on its thread.
                let _ = running.recv().await;
                canceller.cancel();
            },
        )
        .await
        .0;
        (result, start.elapsed())
    });
    assert_eq!(
        result,
        Ok(true),
        "the job saw the cancellation and its result came back"
    );
    assert!(took < Duration::from_secs(1), "it stopped early: {took:?}");
}
