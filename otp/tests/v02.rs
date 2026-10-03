use std::{
    cell::{Cell, RefCell},
    io,
    rc::Rc,
    time::Duration,
};

use bapps_otp::{
    Application, CancelScope, ChildSpec, ExitReason, LocalMailbox, NodeStatus, ServiceGeneration,
    Strategy, SupervisorSpec, TaskQueues,
};
use glommio::{LocalExecutorBuilder, Placement};

struct DropFlag(Rc<Cell<bool>>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

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
fn local_mailbox_is_bounded_and_observable() {
    let (tx, rx) = LocalMailbox::<u64>::bounded(2);
    tx.try_send(1).expect("first");
    tx.try_send(2).expect("second");
    assert_eq!(tx.snapshot().depth, 2);
    assert!(tx.try_send(3).is_err());
    assert_eq!(rx.try_recv().expect("open"), Some(1));
    assert_eq!(tx.snapshot().depth, 1);
}

#[test]
fn framework_marks_generation_stopped() {
    run_on_glommio("otp-v02-generation", || async move {
        let shutdown = CancelScope::new();
        let captured: Rc<RefCell<Option<ServiceGeneration>>> = Rc::new(RefCell::new(None));
        let captured_child = captured.clone();
        let shutdown_child = shutdown.clone();

        let child = ChildSpec::worker("worker", move |ctx, started| {
            let captured = captured_child.clone();
            let shutdown = shutdown_child.clone();
            async move {
                *captured.borrow_mut() = Some(ctx.generation());
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                shutdown.cancel();
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne).child(child);
        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("application");

        let generation = captured.borrow().clone().expect("generation captured");
        assert!(!generation.is_alive());
        assert!(generation.ensure_alive().is_err());
    });
}

#[test]
fn shutdown_grace_can_force_abort_owned_child() {
    run_on_glommio("otp-v02-force", || async move {
        let shutdown = CancelScope::new();
        let shutdown_child = shutdown.clone();

        let child = ChildSpec::worker("stubborn", move |_ctx, started| {
            let shutdown = shutdown_child.clone();
            async move {
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                shutdown.cancel();

                // Intentionally foreign/non-cooperative work.
                glommio::timer::Timer::new(Duration::from_secs(60)).await;
                Ok::<(), io::Error>(())
            }
        })
        .shutdown_after(Duration::ZERO);

        let root = SupervisorSpec::new("root", Strategy::OneForOne).child(child);
        let app = Application::new("test", root);
        let tree = app.tree();
        app.run(shutdown, TaskQueues::current())
            .await
            .expect("application");

        let child = tree.child("test/root/stubborn").expect("child snapshot");
        assert_eq!(child.status, NodeStatus::Failed);
        assert_eq!(child.last_exit, Some(ExitReason::Killed));
        assert!(!child.recent_exits.is_empty());
    });
}

#[test]
fn context_mailbox_is_visible_in_runtime_tree() {
    run_on_glommio("otp-v02-mailbox-tree", || async move {
        let shutdown = CancelScope::new();
        let shutdown_child = shutdown.clone();

        let child = ChildSpec::worker("mailbox-owner", move |ctx, started| {
            let shutdown = shutdown_child.clone();
            async move {
                let (_tx, _rx) = ctx.mailbox::<u64>("commands", 8);
                let snapshot = ctx.tree().child(ctx.path()).expect("child snapshot");
                assert_eq!(snapshot.mailboxes.len(), 1);
                assert_eq!(snapshot.mailboxes[0].name, "commands");
                assert_eq!(snapshot.mailboxes[0].capacity, 8);

                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                shutdown.cancel();
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne).child(child);
        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("application");
    });
}

#[test]
fn service_tasks_end_with_the_service_generation() {
    run_on_glommio("otp-v02-service-tasks", || async move {
        let shutdown = CancelScope::new();
        let shutdown_child = shutdown.clone();
        let cleaned = Rc::new(Cell::new(false));
        let cleaned_child = cleaned.clone();

        let child = ChildSpec::worker("owner", move |ctx, started| {
            let shutdown = shutdown_child.clone();
            let cleaned = cleaned_child.clone();
            async move {
                ctx.tasks()
                    .spawn(move |scope| async move {
                        let _guard = DropFlag(cleaned);
                        let _ = scope.cancelled().await;
                        Ok::<(), io::Error>(())
                    })
                    .expect("spawn service task");

                futures_lite::future::yield_now().await;
                assert_eq!(ctx.tasks().active_tasks(), 1);
                started.started(()).map_err(|error| {
                    io::Error::other(format!("readiness receiver gone: {error:?}"))
                })?;
                shutdown.cancel();
                let _ = ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });

        let root = SupervisorSpec::new("root", Strategy::OneForOne).child(child);
        Application::new("test", root)
            .run(shutdown, TaskQueues::current())
            .await
            .expect("application");
        assert!(cleaned.get());
    });
}
