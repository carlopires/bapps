use std::time::Duration;

use bapps_trio::{TaskClass, TaskQueues, sleep, with_nursery_with_queues};
use glommio::LocalExecutor;

#[derive(Debug)]
struct Error(&'static str);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn main() {
    LocalExecutor::default().run(async {
        let queues = TaskQueues::storage_defaults();

        let result = with_nursery_with_queues::<Error, _, _>(queues, |nursery| {
            Box::pin(async move {
                nursery
                    .spawn_into(TaskClass::ForegroundRead, |_cancel| async move {
                        sleep(Duration::from_millis(10))
                            .await
                            .map_err(|_| Error("cancelled"))?;
                        println!("foreground read finished");
                        Ok(())
                    })
                    .unwrap();

                nursery
                    .spawn_into(TaskClass::Repair, |_cancel| async move {
                        sleep(Duration::from_millis(20))
                            .await
                            .map_err(|_| Error("cancelled"))?;
                        println!("repair finished");
                        Ok(())
                    })
                    .unwrap();
            })
        })
        .await;

        match result {
            Ok(()) => println!("nursery complete"),
            Err(error) => println!("nursery failed: {error:?}"),
        }
    });
}
