// Integration test for issue #689 - Task panic in non-default queue causes process abort
// This test verifies that when a task panics in a custom task queue, the panic is
// handled gracefully without aborting the process

// The tests below all exercise spawn_local_into, so everything they import is
// only in scope under the same feature gate.
use futures::join;
use glommio::{Latency, LocalExecutorBuilder, Placement, Shares};
use std::panic;
use std::time::Duration;

// These tests require spawn_local_into (unsafe detached spawn)
#[test]
fn test_panic_in_default_queue() {
    // This should work - panic in default queue is handled correctly
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .make()
        .unwrap();

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        ex.run(async move {
            let tq = glommio::executor().current_task_queue();
            let task1 = glommio::spawn_local_into(
                async move {
                    glommio::timer::sleep(Duration::from_millis(1)).await;
                    panic!("intentional panic in default queue");
                },
                tq,
            )
            .unwrap();

            let task2 = glommio::spawn_local_into(
                async move {
                    glommio::timer::sleep(Duration::from_secs(10)).await;
                },
                tq,
            )
            .unwrap();

            join!(task1, task2);
        });
    }));

    assert!(result.is_err(), "Expected panic to be caught");
}

#[test]
fn test_panic_in_custom_queue() {
    // This is the critical test - panic in custom queue should NOT abort process
    // Before fix: This would abort the entire process
    // After fix: Panic should be caught and handled gracefully
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .make()
        .unwrap();

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        ex.run(async move {
            // Create custom task queue - this is the key difference from test above
            let tq = glommio::executor().create_task_queue(
                Shares::Static(10),
                Latency::NotImportant,
                "custom queue",
            );

            let task1 = glommio::spawn_local_into(
                async move {
                    glommio::timer::sleep(Duration::from_millis(1)).await;
                    panic!("intentional panic in custom queue");
                },
                tq,
            )
            .unwrap();

            let task2 = glommio::spawn_local_into(
                async move {
                    glommio::timer::sleep(Duration::from_secs(10)).await;
                },
                tq,
            )
            .unwrap();

            join!(task1, task2);
        });
    }));

    // The key assertion: panic should be caught, NOT abort the process
    assert!(
        result.is_err(),
        "Expected panic to be caught in custom queue without aborting process"
    );
}

#[test]
fn test_multiple_panics_in_custom_queues() {
    // Test multiple custom queues with panics
    let ex = LocalExecutorBuilder::new(Placement::Fixed(0))
        .make()
        .unwrap();

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        ex.run(async move {
            let tq1 = glommio::executor().create_task_queue(
                Shares::Static(10),
                Latency::NotImportant,
                "queue 1",
            );

            let tq2 = glommio::executor().create_task_queue(
                Shares::Static(10),
                Latency::NotImportant,
                "queue 2",
            );

            let task1 = glommio::spawn_local_into(
                async move {
                    glommio::timer::sleep(Duration::from_millis(1)).await;
                    panic!("panic in queue 1");
                },
                tq1,
            )
            .unwrap();

            let task2 = glommio::spawn_local_into(
                async move {
                    glommio::timer::sleep(Duration::from_millis(2)).await;
                    panic!("panic in queue 2");
                },
                tq2,
            )
            .unwrap();

            join!(task1, task2);
        });
    }));

    assert!(
        result.is_err(),
        "Expected panics in multiple custom queues to be caught"
    );
}
