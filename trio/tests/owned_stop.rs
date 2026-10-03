//! Stop operations on an `OwnedTask` are ordinary futures: their callers may
//! drop them (a lost race, an outer force-stop). Task accounting must not depend
//! on the stop future running to completion.

use std::{
    cell::Cell,
    future::{Future, pending, poll_fn},
    pin::pin,
    rc::Rc,
    task::Poll,
    time::Duration,
};

use bapps_trio::{Nursery, StopOutcome, testing::TestClock, time::with_clock, with_nursery};
use glommio::{LocalExecutor, timer::Timer};

/// Poll `future` once and drop it.
async fn start_then_abandon<F: Future>(future: F) {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let _ = future.as_mut().poll(cx);
        Poll::Ready(())
    })
    .await;
}

/// Run `future` against a real-time watchdog; `None` means it hung.
async fn within<F: Future>(limit: Duration, future: F) -> Option<F::Output> {
    let mut future = pin!(future);
    let mut watchdog = pin!(Timer::new(limit));
    poll_fn(|cx| {
        if let Poll::Ready(value) = future.as_mut().poll(cx) {
            return Poll::Ready(Some(value));
        }
        if watchdog.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

/// Destructor probe: records that the task future was actually destroyed.
struct Dropped(Rc<Cell<bool>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn abandoned_abort_does_not_strand_nursery_accounting() {
    LocalExecutor::default().run(async {
        let destroyed = Rc::new(Cell::new(false));
        let probe = destroyed.clone();
        let exited = within(
            Duration::from_secs(2),
            with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
                Box::pin(async move {
                    let task = nursery
                        .spawn_owned(move |_scope| async move {
                            let _probe = Dropped(probe);
                            // Ignores cooperative cancellation on purpose.
                            pending::<()>().await;
                            Ok(())
                        })
                        .unwrap();
                    futures_lite::future::yield_now().await;
                    start_then_abandon(task.abort()).await;
                })
            }),
        )
        .await;
        assert!(
            matches!(exited, Some(Ok(()))),
            "nursery must exit once the aborted task is destroyed"
        );
        assert!(destroyed.get(), "aborted task future must be destroyed");
    });
}

#[test]
fn abandoned_cancel_and_wait_does_not_strand_nursery_accounting() {
    LocalExecutor::default().run(async {
        let clock = TestClock::new();
        let exited = within(
            Duration::from_secs(2),
            with_clock(
                clock.shared(),
                with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
                    let clock = clock.clone();
                    Box::pin(async move {
                        let task = nursery
                            .spawn_owned(|_scope| async move {
                                pending::<()>().await;
                                Ok(())
                            })
                            .unwrap();
                        let stop = task.cancel_and_wait(Duration::from_secs(5));
                        let mut stop = pin!(stop);
                        // Enter the grace wait, expire it, enter the forced path,
                        // then abandon the stop future mid-abort.
                        poll_fn(|cx| {
                            let _ = stop.as_mut().poll(cx);
                            Poll::Ready(())
                        })
                        .await;
                        clock.advance(Duration::from_secs(5));
                        futures_lite::future::yield_now().await;
                        poll_fn(|cx| {
                            let _ = stop.as_mut().poll(cx);
                            Poll::Ready(())
                        })
                        .await;
                    })
                }),
            ),
        )
        .await;
        assert!(matches!(exited, Some(Ok(()))));
    });
}

#[test]
fn abort_reports_destruction_and_repeated_stops_converge() {
    LocalExecutor::default().run(async {
        let destroyed = Rc::new(Cell::new(false));
        let probe = destroyed.clone();
        with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
            Box::pin(async move {
                let task = nursery
                    .spawn_owned(move |_scope| async move {
                        let _probe = Dropped(probe);
                        pending::<()>().await;
                        Ok(())
                    })
                    .unwrap();
                futures_lite::future::yield_now().await;
                assert!(task.abort().await, "first abort removes the live task");
                assert!(destroyed.get(), "abort returns after destruction");
                assert!(task.is_finished());
                assert!(!task.abort().await, "second abort is a no-op");
                assert_eq!(
                    task.cancel_and_wait(Duration::from_secs(1)).await,
                    StopOutcome::Graceful,
                    "a finished task has nothing left to force"
                );
                assert_eq!(nursery.active_tasks(), 0);
            })
        })
        .await
        .expect("nursery");
    });
}

#[test]
fn dropped_control_handle_leaves_task_owned_by_nursery() {
    LocalExecutor::default().run(async {
        let finished = Rc::new(Cell::new(false));
        let flag = finished.clone();
        with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
            Box::pin(async move {
                let task = nursery
                    .spawn_owned(move |_scope| async move {
                        futures_lite::future::yield_now().await;
                        flag.set(true);
                        Ok(())
                    })
                    .unwrap();
                drop(task);
            })
        })
        .await
        .expect("nursery");
        assert!(finished.get(), "nursery joined the task after handle drop");
    });
}

#[test]
fn dropping_a_running_nursery_destroys_its_children() {
    LocalExecutor::default().run(async {
        let destroyed = Rc::new(Cell::new(false));
        let started = Rc::new(Cell::new(false));
        let (probe, running) = (destroyed.clone(), started.clone());
        let nursery = with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
            Box::pin(async move {
                nursery
                    .spawn(move |_scope| async move {
                        let _probe = Dropped(probe);
                        running.set(true);
                        pending::<()>().await;
                        Ok(())
                    })
                    .unwrap();
                pending::<()>().await;
            })
        });
        // Abrupt owner destruction once the child runs: the forced escape
        // hatch, not graceful exit.
        let mut nursery = Box::pin(nursery);
        poll_fn(|cx| {
            let _ = nursery.as_mut().poll(cx);
            if started.get() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
        drop(nursery);
        for _ in 0..10 {
            if destroyed.get() {
                break;
            }
            futures_lite::future::yield_now().await;
        }
        assert!(destroyed.get(), "child future destroyed with its nursery");
    });
}
