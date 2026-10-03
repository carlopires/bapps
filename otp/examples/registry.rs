use std::{io, rc::Rc};

use bapps_otp::{
    Application, CancelScope, ChildSpec, ServiceKey, Strategy, SupervisorSpec, TaskQueues,
};
use glommio::{LocalExecutorBuilder, Placement};

#[derive(Clone)]
struct StorageHandle {
    label: Rc<str>,
}

static STORAGE: ServiceKey<StorageHandle> = ServiceKey::new("storage");

fn main() {
    let handle = LocalExecutorBuilder::new(Placement::Unbound)
        .name("otp-registry-example")
        .spawn(|| async move {
            let shutdown = CancelScope::new();

            let storage = ChildSpec::worker("storage", |ctx, started| async move {
                ctx.register(
                    STORAGE,
                    StorageHandle {
                        label: Rc::from("primary"),
                    },
                )
                .map_err(|error| io::Error::other(error.to_string()))?;
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            });

            let router_shutdown = shutdown.clone();
            let router = ChildSpec::worker("router", move |ctx, started| {
                let shutdown = router_shutdown.clone();
                async move {
                    let storage = ctx
                        .service(STORAGE)
                        .ok_or_else(|| io::Error::other("storage handle missing"))?;
                    println!("router resolved storage: {}", storage.label);
                    started.started(()).map_err(|error| {
                        io::Error::other(format!("readiness receiver gone: {error:?}"))
                    })?;
                    shutdown.cancel();
                    let _ = ctx.scope().cancelled().await;
                    Ok::<(), io::Error>(())
                }
            });

            let root = SupervisorSpec::new("root", Strategy::RestForOne)
                .child(storage)
                .child(router);
            Application::new("demo", root)
                .run(shutdown, TaskQueues::current())
                .await
                .expect("application");
        })
        .expect("spawn executor");

    handle.join().expect("executor thread");
}
