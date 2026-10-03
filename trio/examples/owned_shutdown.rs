use std::time::Duration;

use bapps_trio::{StopOutcome, with_nursery};
use glommio::LocalExecutor;

fn main() {
    LocalExecutor::default().run(async {
        let result = with_nursery::<(), _, _>(|nursery| {
            Box::pin(async move {
                let task = nursery
                    .spawn_owned(|scope| async move {
                        let _ = scope.cancelled().await;
                        Ok(())
                    })
                    .expect("spawn");

                let outcome = task.cancel_and_wait(Duration::from_secs(1)).await;
                assert_eq!(outcome, StopOutcome::Graceful);
            })
        })
        .await;

        assert!(result.is_ok());
    });
}
