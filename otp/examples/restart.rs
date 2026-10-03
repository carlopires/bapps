use std::{cell::Cell, io, rc::Rc, time::Duration};

use bapps_otp::{Application, CancelScope, ChildSpec, Strategy, SupervisorSpec, TaskQueues};
use glommio::{LocalExecutorBuilder, Placement};

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Unbound)
        .name("otp-restart-example")
        .spawn(|| async move {
            let shutdown = CancelScope::new();
            let attempts = Rc::new(Cell::new(0_u32));

            let child_shutdown = shutdown.clone();
            let child_attempts = attempts.clone();
            let flaky = ChildSpec::worker("flaky", move |ctx, started| {
                let shutdown = child_shutdown.clone();
                let attempts = child_attempts.clone();
                async move {
                    let attempt = attempts.get() + 1;
                    attempts.set(attempt);
                    started.started(()).map_err(|error| {
                        io::Error::other(format!("readiness receiver gone: {error:?}"))
                    })?;

                    if attempt < 3 {
                        return Err(io::Error::other(format!(
                            "deliberate failure on attempt {attempt}"
                        )));
                    }

                    // Prove the third generation is alive, then ask the whole
                    // application to shut down cooperatively.
                    shutdown.cancel();
                    let _ = ctx.scope().cancelled().await;
                    Ok(())
                }
            });

            let root = SupervisorSpec::new("root", Strategy::OneForOne)
                .restart_intensity(5, Duration::from_secs(10))
                .child(flaky);
            let app = Application::new("demo", root);
            let tree = app.tree();

            app.run(shutdown, TaskQueues::current())
                .await
                .expect("application should finish cleanly");

            assert_eq!(attempts.get(), 3);
            for node in tree.snapshot() {
                println!("{node:#?}");
            }
        })
        .expect("spawn executor");

    handle.join().expect("executor thread");
}
