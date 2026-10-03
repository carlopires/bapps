// Reproduction test for issue #448 - Eventfd leak
// Based on the original issue report

use glommio::channels::shared_channel;
use glommio::prelude::*;

fn test_spsc(capacity: usize) {
    let runs: u32 = 1_000;  // Reduced for faster testing
    let (sender, receiver) = shared_channel::new_bounded(capacity);

    let sender = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spawn(move || async move {
            let sender = sender.connect().await;
            for _ in 0..runs {
                sender.send(1).await.unwrap();
            }
            drop(sender);
        })
        .unwrap();

    let receiver = LocalExecutorBuilder::new()
        .pin_to_cpu(1)
        .spawn(move || async move {
            let receiver = receiver.connect().await;
            for _ in 0..runs {
                receiver.recv().await.unwrap();
            }
        })
        .unwrap();

    sender.join().unwrap();
    receiver.join().unwrap();
}

fn count_open_fds() -> usize {
    let pid = std::process::id();
    let fd_dir = format!("/proc/{}/fd", pid);

    if let Ok(entries) = std::fs::read_dir(&fd_dir) {
        entries.count()
    } else {
        // On macOS, use lsof (slower but works)
        let output = std::process::Command::new("lsof")
            .args(&["-p", &pid.to_string()])
            .output()
            .expect("Failed to run lsof");

        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| line.contains("eventfd") || line.contains("KQUEUE"))
            .count()
    }
}

fn main() {
    println!("Testing eventfd leak (issue #448)...\n");

    let initial_fds = count_open_fds();
    println!("Initial FD count: {}", initial_fds);

    for i in 0..10 {
        println!("\n=== Round {} ===", i);
        test_spsc(100);
        test_spsc(1000);
        test_spsc(10000);

        let current_fds = count_open_fds();
        let leaked = current_fds.saturating_sub(initial_fds);
        println!("Current FD count: {} (leaked: {})", current_fds, leaked);

        if leaked > 50 {
            println!("\n❌ LEAK DETECTED: {} FDs leaked after {} rounds", leaked, i + 1);
            println!("   Expected: ~{} FDs", initial_fds);
            println!("   Actual:   {} FDs", current_fds);
            std::process::exit(1);
        }
    }

    let final_fds = count_open_fds();
    println!("\n✅ Test passed! FD count stable: {} -> {}", initial_fds, final_fds);
}
