use std::time::Duration;

use bapps_trio::{FailAfterError, fail_after, sleep};
use glommio::LocalExecutor;

fn main() {
    LocalExecutor::default().run(async {
        let result = fail_after(Duration::from_millis(10), |_scope| async {
            let _ = sleep(Duration::from_secs(60)).await;
            42
        })
        .await;

        assert!(matches!(result, Err(FailAfterError::TooSlow)));
        println!("deadline cancelled work and waited for cooperative cleanup");
    });
}
