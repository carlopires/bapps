// Integration test for issue #448 - Eventfd leak on executor drop
// This test verifies that eventfds are properly closed when executors drop

use glommio::channels::shared_channel;
use glommio::prelude::*;

#[cfg(target_os = "linux")]
fn count_open_fds() -> usize {
    let pid = std::process::id();
    let fd_dir = format!("/proc/{}/fd", pid);

    std::fs::read_dir(&fd_dir)
        .map(|entries| entries.count())
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn count_open_fds() -> usize {
    let pid = std::process::id();
    let output = std::process::Command::new("lsof")
        .args(&["-p", &pid.to_string()])
        .output()
        .expect("Failed to run lsof");

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.contains("eventfd") || line.contains("KQUEUE"))
        .count()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn count_open_fds() -> usize {
    0 // Unsupported platform
}

/// Serialises the tests in this file.
///
/// Every test here measures `/proc/self/fd`, which counts descriptors for the
/// whole PROCESS. `cargo test` runs the tests of one binary on parallel
/// threads, so without this they observe each other's executors and each
/// other's descriptors, and fail on counts that have nothing to do with the
/// leak under test.
///
/// Under `cargo nextest`, which gives each test its own process, the lock is
/// uncontended and the isolation is real rather than borrowed -- so this is
/// correct under both runners.
static FD_COUNTING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Takes the lock, ignoring poisoning: a panicking test has already failed and
/// its report is what matters, not a second failure in every test after it.
fn serialised() -> std::sync::MutexGuard<'static, ()> {
    FD_COUNTING.lock().unwrap_or_else(|e| e.into_inner())
}

fn test_spsc_cycle(capacity: usize, runs: u32) {
    let (sender, receiver) = shared_channel::new_bounded(capacity);

    let sender_handle = LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let sender = sender.connect().await;
            for _ in 0..runs {
                sender.send(1).await.unwrap();
            }
            drop(sender);
        })
        .unwrap();

    let receiver_handle = LocalExecutorBuilder::new(Placement::Fixed(1))
        .spawn(move || async move {
            let receiver = receiver.connect().await;
            for _ in 0..runs {
                receiver.recv().await.unwrap();
            }
        })
        .unwrap();

    sender_handle.join().unwrap();
    receiver_handle.join().unwrap();
}

#[test]
fn test_no_eventfd_leak_on_executor_drop() {
    let _guard = serialised();
    let initial_fds = count_open_fds();
    println!("Initial FD count: {}", initial_fds);

    // Run multiple cycles of executor creation/destruction
    for round in 0..5 {
        println!("\n=== Round {} ===", round);

        // Create and destroy executors with shared channels
        test_spsc_cycle(100, 100);
        test_spsc_cycle(1000, 100);
        test_spsc_cycle(10000, 100);

        let current_fds = count_open_fds();
        let leaked = current_fds.saturating_sub(initial_fds);

        println!("Current FD count: {} (leaked: {})", current_fds, leaked);

        // Allow some tolerance for timing variations, but leak should be minimal
        // Before fix: ~36 fds leaked per cycle (12 per test * 3 tests)
        // After fix: should be 0-5 (just timing noise)
        assert!(
            leaked < 10,
            "Too many FDs leaked: {} after {} rounds. Expected < 10",
            leaked,
            round + 1
        );
    }

    let final_fds = count_open_fds();
    println!(
        "\n✅ Test passed! FD count stable: {} -> {}",
        initial_fds, final_fds
    );

    // Final check: total leak should be very small
    let total_leaked = final_fds.saturating_sub(initial_fds);
    assert!(
        total_leaked < 15,
        "Total FDs leaked: {}. Fix may not be working correctly",
        total_leaked
    );
}

#[test]
fn test_rapid_executor_creation() {
    let _guard = serialised();
    let initial_fds = count_open_fds();
    println!("Initial FD count: {}", initial_fds);

    // Rapidly create and destroy many executors
    for _ in 0..20 {
        let executor = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                // Minimal work
                glommio::timer::sleep(std::time::Duration::from_millis(1)).await;
            })
            .unwrap();

        executor.join().unwrap();
    }

    let final_fds = count_open_fds();
    let leaked = final_fds.saturating_sub(initial_fds);

    println!("Final FD count: {} (leaked: {})", final_fds, leaked);

    // Should have minimal leak (< 5 for noise)
    assert!(
        leaked < 5,
        "FDs leaked after rapid executor creation: {}",
        leaked
    );
}

// This test requires spawn_local (unsafe detached spawn)
#[test]
fn test_executor_with_tasks() {
    let _guard = serialised();
    let initial_fds = count_open_fds();

    // Create executors that spawn tasks
    for _ in 0..10 {
        let executor = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spawn(|| async move {
                // Spawn some tasks
                let tasks: Vec<_> = (0..10)
                    .map(|i| glommio::spawn_local(async move { i * 2 }))
                    .collect();

                // Await some (but not all) tasks
                for task in tasks.into_iter().take(5) {
                    task.await;
                }

                // Leave some tasks non-runnable when executor drops
            })
            .unwrap();

        executor.join().unwrap();
    }

    let final_fds = count_open_fds();
    let leaked = final_fds.saturating_sub(initial_fds);

    println!(
        "FD count after executor with tasks: {} -> {} (leaked: {})",
        initial_fds, final_fds, leaked
    );

    // Even with non-runnable tasks, eventfds should be closed
    assert!(leaked < 5, "FDs leaked with non-runnable tasks: {}", leaked);
}

/// The strict form of the same question, and the one that would catch a
/// regression immediately.
///
/// The tests above allow up to ten stray descriptors, which is the right
/// tolerance for a cycle that also builds shared channels and pins threads.
/// A bare executor that runs nothing has no such noise: it must not grow the
/// count at all, and one leaked eventfd per executor would show immediately.
///
/// This matters more than it used to. `close_eventfd` used to force the
/// notifier's eventfd shut from `Reactor::drop` as a second line of defence.
/// It was removed once it was redundant -- storing `executor_id` in the task
/// header rather than an `Arc<SleepNotifier>` means nothing outlives the
/// executor holding its notifier alive -- and removing it also removed a
/// use-after-close, since a `shared_channel` peer on another executor can
/// legitimately still hold that `Arc`. With the belt gone, this test is the
/// braces.
#[test]
fn a_bare_executor_lifecycle_does_not_grow_the_descriptor_count() {
    let _guard = serialised();

    // The first executor in a process initialises state that is never torn
    // down, so it is warm-up rather than part of the measurement.
    LocalExecutor::default().run(async {});

    let before = count_open_fds();
    for _ in 0..20 {
        LocalExecutor::default().run(async {});
    }
    // A descriptor closes when its last owner drops it, and that is not always
    // done the instant `run` returns (community glommio 09373d7 hit the same
    // thing). Wait for the count to settle: a leak -- one descriptor per
    // lifecycle -- never comes back down, so this costs a real one nothing.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut after = count_open_fds();
    while after > before && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
        after = count_open_fds();
    }

    // Not equality: the count may legitimately fall, since descriptors held
    // lazily elsewhere in the process can be released during the run. What
    // must never happen is growth, and one leaked eventfd per executor is
    // what a regression looks like -- 20 lifecycles, 20 descriptors.
    assert!(
        after <= before,
        "20 executor lifecycles grew the process descriptor count from {before} to {after}"
    );
}
