use std::{cell::Cell, rc::Rc, time::Duration};

use bapps_trio::{
    CancelScope, FailAfterError, NurseryError, TaskQueues, fail_after, sleep,
    testing::{Sequencer, TestClock},
    with_clock, with_nursery, with_nursery_with_queues,
};
use glommio::LocalExecutor;

#[derive(Debug)]
struct TestError(&'static str);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[test]
fn child_error_cancels_sibling_and_nursery_waits() {
    LocalExecutor::default().run(async {
        let cleaned = Rc::new(Cell::new(false));
        let cleaned_child = cleaned.clone();

        let result = with_nursery::<TestError, _, _>(|nursery| {
            Box::pin(async move {
                nursery
                    .spawn(|_cancel| async move { Err(TestError("boom")) })
                    .unwrap();
                nursery
                    .spawn(move |_cancel| async move {
                        let _ = sleep(Duration::from_secs(60)).await;
                        cleaned_child.set(true);
                        Ok(())
                    })
                    .unwrap();
            })
        })
        .await;

        assert!(matches!(result, Err(NurseryError::Child(_))));
        assert!(cleaned.get());
    });
}

#[test]
fn shield_does_not_inherit_parent_cancellation() {
    let parent = CancelScope::new();
    let child = parent.child();
    let shield = parent.shielded();

    parent.cancel();
    assert!(child.is_cancelled());
    assert!(!shield.is_cancelled());
}

#[test]
fn fail_after_uses_cooperative_cancellation() {
    LocalExecutor::default().run(async {
        let result = fail_after(Duration::from_millis(1), |_scope| async {
            let _ = sleep(Duration::from_secs(10)).await;
            7
        })
        .await;
        assert!(matches!(result, Err(FailAfterError::TooSlow)));
    });
}

#[test]
fn test_clock_drives_time_without_wall_clock_sleep() {
    LocalExecutor::default().run(async {
        let clock = TestClock::new();
        let driver = clock.clone();
        let seq = Sequencer::new();
        let seq_worker = seq.clone();
        let queues = TaskQueues::current();

        with_clock(clock.shared(), async move {
            let result = with_nursery_with_queues::<TestError, _, _>(queues, |nursery| {
                Box::pin(async move {
                    nursery
                        .spawn(move |_cancel| async move {
                            seq_worker.advance();
                            sleep(Duration::from_secs(5))
                                .await
                                .map_err(|_| TestError("cancelled"))?;
                            seq_worker.advance();
                            Ok(())
                        })
                        .unwrap();

                    seq.wait_for(1).await.unwrap();
                    driver.advance(Duration::from_secs(5));
                    seq.wait_for(2).await.unwrap();
                })
            })
            .await;

            assert!(result.is_ok());
        })
        .await;
    });
}

#[test]
fn multi_owner_scope_cancels_when_either_parent_cancels() {
    let caller = CancelScope::new();
    let service = CancelScope::new();
    let operation = CancelScope::any([caller.clone(), service.clone()]);

    assert!(!operation.is_cancelled());
    service.cancel();
    assert!(operation.is_cancelled());
}

#[test]
fn owned_task_forces_after_grace_period() {
    LocalExecutor::default().run(async {
        let result = with_nursery::<TestError, _, _>(|nursery| {
            Box::pin(async move {
                let task = nursery
                    .spawn_owned(|_scope| async move {
                        // Deliberately uses a raw Glommio timer, which does not
                        // observe the Trio scope. The structured stop path must
                        // therefore force-abort this owned task.
                        glommio::timer::Timer::new(Duration::from_secs(60)).await;
                        Ok(())
                    })
                    .unwrap();

                let outcome = task.cancel_and_wait(Duration::ZERO).await;
                assert_eq!(outcome, bapps_trio::StopOutcome::Forced);
            })
        })
        .await;

        assert!(result.is_ok());
    });
}

#[test]
fn foreign_future_honors_pre_cancelled_scope() {
    LocalExecutor::default().run(async {
        let scope = CancelScope::new();
        scope.cancel();
        let polled = Rc::new(Cell::new(false));
        let polled_future = polled.clone();

        let result = bapps_trio::cancel_on(&scope, async move {
            polled_future.set(true);
            42
        })
        .await;

        assert!(result.is_err());
        assert!(!polled.get());
    });
}
