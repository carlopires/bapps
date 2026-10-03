use std::{cell::Cell, io, rc::Rc, time::Duration};

use bapps_otp::{
    Application, CancelScope, ChildSpec, ExitReason, NodeStatus, OtpError, Restart, Strategy,
    SupervisorSpec, TaskQueues, TreeNodeSnapshot,
};
use glommio::{LocalExecutorBuilder, Placement};

fn run_on_glommio<F, Fut>(name: &str, factory: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    let handle = LocalExecutorBuilder::new(Placement::Unbound)
        .name(name)
        .spawn(factory)
        .expect("spawn Glommio executor");
    handle.join().expect("executor thread");
}

#[test]
fn one_for_one_restarts_failed_child() {
    run_on_glommio("otp-test-one-for-one", || async move {
        let shutdown = CancelScope::new();
        let attempts = Rc::new(Cell::new(0_u32));
        let attempts_for_child = attempts.clone();
        let shutdown_for_child = shutdown.clone();

        let child = ChildSpec::worker("worker", move |ctx, started| {
            let attempts = attempts_for_child.clone();
            let shutdown = shutdown_for_child.clone();
            async move {
                let attempt = attempts.get() + 1;
                attempts.set(attempt);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                if attempt < 3 {
                    return Err(io::Error::other("deliberate failure"));
                }
                shutdown.cancel();
                let _ = ctx.scope().cancelled().await;
                Ok(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne)
            .restart_intensity(5, Duration::from_secs(10))
            .child(child);
        let app = Application::new("test", root);
        let tree = app.tree();

        app.run(shutdown, TaskQueues::current())
            .await
            .expect("application");

        assert_eq!(attempts.get(), 3);
        let worker = tree
            .snapshot()
            .into_iter()
            .find_map(|node| match node {
                TreeNodeSnapshot::Child(child) if child.path == "test/root/worker" => Some(child),
                _ => None,
            })
            .expect("worker in tree");
        assert_eq!(worker.restart_count, 2);
        assert_eq!(worker.status, NodeStatus::Stopped);
        assert_eq!(worker.last_exit, Some(ExitReason::Shutdown));
    });
}

#[test]
fn one_for_all_restarts_sibling() {
    run_on_glommio("otp-test-one-for-all", || async move {
        let shutdown = CancelScope::new();
        let a_starts = Rc::new(Cell::new(0_u32));
        let b_starts = Rc::new(Cell::new(0_u32));

        let a_counts = a_starts.clone();
        let a = ChildSpec::worker("a", move |ctx, started| {
            let starts = a_counts.clone();
            async move {
                let count = starts.get() + 1;
                starts.set(count);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                if count == 1 {
                    return Err(io::Error::other("restart the group"));
                }
                let _ = ctx.scope().cancelled().await;
                Ok(())
            }
        });

        let b_counts = b_starts.clone();
        let b_shutdown = shutdown.clone();
        let b = ChildSpec::worker("b", move |ctx, started| {
            let starts = b_counts.clone();
            let shutdown = b_shutdown.clone();
            async move {
                let count = starts.get() + 1;
                starts.set(count);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                if count == 2 {
                    shutdown.cancel();
                }
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForAll)
            .child(a)
            .child(b);
        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("application");

        assert_eq!(a_starts.get(), 2);
        assert_eq!(b_starts.get(), 2);
    });
}

#[test]
fn transient_normal_exit_is_not_restarted() {
    run_on_glommio("otp-test-transient", || async move {
        let shutdown = CancelScope::new();
        let starts = Rc::new(Cell::new(0_u32));
        let starts_for_child = starts.clone();
        let shutdown_for_child = shutdown.clone();

        let child = ChildSpec::worker("once", move |_ctx, started| {
            let starts = starts_for_child.clone();
            let shutdown = shutdown_for_child.clone();
            async move {
                starts.set(starts.get() + 1);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                shutdown.cancel();
                Ok::<(), io::Error>(())
            }
        })
        .restart(Restart::Transient);

        let root = SupervisorSpec::new("root", Strategy::OneForOne).child(child);
        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("application");
        assert_eq!(starts.get(), 1);
    });
}

#[test]
fn restart_intensity_escalates_supervisor() {
    run_on_glommio("otp-test-intensity", || async move {
        let shutdown = CancelScope::new();
        let child = ChildSpec::worker("bad", |_ctx, started| async move {
            started
                .started(())
                .map_err(|error| io::Error::other(format!("readiness receiver gone: {error:?}")))?;
            Err::<(), io::Error>(io::Error::other("always bad"))
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne)
            .restart_intensity(2, Duration::from_secs(60))
            .child(child);
        let result = Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await;

        assert!(matches!(
            result,
            Err(OtpError::RestartIntensityExceeded {
                max_restarts: 2,
                ..
            })
        ));
    });
}

#[test]
fn panic_is_converted_to_exit_reason_and_restarted() {
    run_on_glommio("otp-test-panic", || async move {
        let shutdown = CancelScope::new();
        let starts = Rc::new(Cell::new(0_u32));
        let starts_for_child = starts.clone();
        let shutdown_for_child = shutdown.clone();

        let child = ChildSpec::worker("panicky", move |ctx, started| {
            let starts = starts_for_child.clone();
            let shutdown = shutdown_for_child.clone();
            async move {
                let count = starts.get() + 1;
                starts.set(count);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                if count == 1 {
                    panic!("deliberate service panic");
                }
                shutdown.cancel();
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne).child(child);
        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("panic should be supervised");
        assert_eq!(starts.get(), 2);
    });
}

#[test]
fn nested_supervisor_reports_ready_after_its_children() {
    run_on_glommio("otp-test-nested", || async move {
        let shutdown = CancelScope::new();
        let storage_started = Rc::new(Cell::new(false));
        let router_observed_storage = Rc::new(Cell::new(false));

        let storage_flag = storage_started.clone();
        let storage = ChildSpec::worker("storage", move |ctx, started| {
            let storage_flag = storage_flag.clone();
            async move {
                storage_flag.set(true);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let serving = SupervisorSpec::new("serving", Strategy::RestForOne).child(storage);

        let observed = router_observed_storage.clone();
        let storage_flag_for_api = storage_started.clone();
        let api_shutdown = shutdown.clone();
        let api = ChildSpec::worker("api", move |ctx, started| {
            let observed = observed.clone();
            let storage_flag = storage_flag_for_api.clone();
            let shutdown = api_shutdown.clone();
            async move {
                observed.set(storage_flag.get());
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                shutdown.cancel();
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne)
            .child(ChildSpec::supervisor("serving", serving))
            .child(api);

        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("application");

        assert!(storage_started.get());
        assert!(router_observed_storage.get());
    });
}
