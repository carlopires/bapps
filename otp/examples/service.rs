use std::{io, time::Duration};

use bapps_otp::{
    Application, CancelScope, ChildSpec, ServiceGeneration, Strategy, SupervisorSpec, TaskQueues,
};
use glommio::{LocalExecutorBuilder, Placement};

#[derive(Clone)]
struct Handle {
    generation: ServiceGeneration,
}

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Unbound)
        .name("otp-service-example")
        .spawn(|| async move {
            let shutdown = CancelScope::new();
            let shutdown_child = shutdown.clone();

            let worker = ChildSpec::worker("service", move |ctx, started| {
                let shutdown = shutdown_child.clone();
                async move {
                    let (_tx, _rx) = ctx.mailbox::<u64>("commands", 64);
                    let handle = Handle {
                        generation: ctx.generation(),
                    };
                    handle.generation.ensure_alive().map_err(io::Error::other)?;

                    started.started(()).map_err(|error| {
                        io::Error::other(format!("readiness receiver gone: {error:?}"))
                    })?;

                    ctx.tasks()
                        .spawn(|scope| async move {
                            let _ = scope.cancelled().await;
                            Ok::<(), io::Error>(())
                        })
                        .map_err(|error| io::Error::other(format!("spawn failed: {error:?}")))?;

                    shutdown.cancel();
                    let _ = ctx.scope().cancelled().await;
                    Ok::<(), io::Error>(())
                }
            })
            .shutdown_after(Duration::from_secs(1));

            let root = SupervisorSpec::new("root", Strategy::OneForOne).child(worker);
            Application::new("example", root)
                .run(shutdown, TaskQueues::current())
                .await
                .expect("application");
        })
        .expect("spawn executor");

    handle.join().expect("executor thread");
}
