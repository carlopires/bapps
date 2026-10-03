use std::time::Duration;

use bapps_trio::{sleep, with_nursery};
use glommio::LocalExecutor;

#[derive(Debug)]
struct Error;

fn main() {
    LocalExecutor::default().run(async {
        let result = with_nursery::<Error, _, _>(|nursery| {
            Box::pin(async move {
                let handle = nursery
                    .start(|_cancel, started| async move {
                        // initialization
                        sleep(Duration::from_millis(5)).await.map_err(|_| Error)?;
                        started.started("ready").map_err(|_| Error)?;

                        // finite steady state for the example. A real service would
                        // normally be owned by an OTP-like supervisor above this crate.
                        sleep(Duration::from_millis(20)).await.map_err(|_| Error)?;
                        Ok(())
                    })
                    .await
                    .expect("service failed before readiness");

                println!("service reported: {handle}");
            })
        })
        .await;

        println!("result: {result:?}");
    });
}
