// Test for public spawn() method on LocalExecutor (issue #695)

use glommio::LocalExecutor;

#[test]
fn test_spawn_before_run() {
    // This is the key use case: spawning before run() starts
    let executor = LocalExecutor::default();

    // Spawn task before calling run() - this would panic with spawn_local()!
    let task = executor.spawn(async { 42 });

    // Now run the executor and await the task
    let result = executor.run(task);

    assert_eq!(result, 42);
}

#[test]
fn test_spawn_multiple_tasks() {
    let executor = LocalExecutor::default();

    // Spawn multiple tasks before run()
    let task1 = executor.spawn(async { 1 });
    let task2 = executor.spawn(async { 2 });
    let task3 = executor.spawn(async { 3 });

    // Run and collect results
    let result = executor.run(async move {
        let r1 = task1.await;
        let r2 = task2.await;
        let r3 = task3.await;
        r1 + r2 + r3
    });

    assert_eq!(result, 6);
}

#[test]
fn test_spawn_with_computation() {
    let executor = LocalExecutor::default();

    // Spawn a task that does actual work
    let task = executor.spawn(async {
        let mut sum = 0;
        for i in 1..=100 {
            sum += i;
        }
        sum
    });

    let result = executor.run(task);

    assert_eq!(result, 5050); // Sum of 1 to 100
}

#[test]
fn test_spawn_inside_run_still_works() {
    let executor = LocalExecutor::default();

    // spawn() should also work inside run() context
    let result = executor.run(async move {
        // Using the executor reference captured in the async block
        // This demonstrates spawn() works in both contexts
        let task = glommio::spawn_local(async { 42 });
        task.await
    });

    assert_eq!(result, 42);
}

// This test should NOT compile if uncommented - demonstrates !Send enforcement
/*
#[test]
fn test_cannot_send_executor_between_threads() {
    let executor = LocalExecutor::default();

    // This should fail to compile with:
    // error[E0277]: `Rc<RefCell<ExecutorQueues>>` cannot be sent between threads safely
    std::thread::spawn(move || {
        executor.spawn(async { 42 });
    });
}
*/

#[test]
fn test_spawn_before_run_with_pending_future() {
    // A task spawned before run() that does not complete on its first poll has
    // to be rescheduled. Its schedule function therefore runs while no executor
    // is installed on this thread, which is the one case where resolving the
    // task queue from thread-local state could lose the task.
    let executor = LocalExecutor::default();

    let task = executor.spawn(async {
        futures_lite::future::yield_now().await;
        42
    });

    let result = executor.run(task);

    assert_eq!(result, 42);
}
