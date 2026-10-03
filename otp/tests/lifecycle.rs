//! Generation phases, advertisement and the supervision cancellation matrix:
//! stop requested before readiness, failure before readiness, cooperative
//! drain, a failure during drain, and stale handles after a restart.

use std::{
    cell::{Cell, RefCell},
    io,
    rc::Rc,
};

use bapps_otp::{
    Application, CancelScope, ChildSpec, ExitReason, GenerationPhase, NodeStatus, OtpError,
    ServiceGeneration, ServiceKey, Strategy, SupervisorSpec, TaskQueues,
};
use bapps_trio::{sync::Event, with_nursery};
use glommio::LocalExecutor;

static HANDLE: ServiceKey<ServiceGeneration> = ServiceKey::new("handle");

fn err(error: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("{error:?}"))
}

/// Run `application` until `body` finishes, then stop it.
async fn run_while<F, Fut>(application: Application, shutdown: CancelScope, body: F)
where
    F: FnOnce() -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    with_nursery::<String, _, _>(|nursery| {
        Box::pin(async move {
            let app_shutdown = shutdown.clone();
            nursery
                .spawn(move |_| async move {
                    application
                        .run(app_shutdown, TaskQueues::current())
                        .await
                        .map_err(|e| e.to_string())
                })
                .expect("spawn application");
            body().await;
            shutdown.cancel();
        })
    })
    .await
    .expect("application");
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        futures_lite::future::yield_now().await;
    }
}

/// Starting -> Ready -> Draining -> Stopped, with admission and registry
/// advertisement ending at the drain linearization point, not at exit.
#[test]
fn draining_generation_rejects_work_and_is_not_advertised() {
    LocalExecutor::default().run(async {
        let seen_before_ready = Rc::new(Cell::new(None));
        let during_drain = Rc::new(RefCell::new(Vec::new()));
        let (before, during) = (seen_before_ready.clone(), during_drain.clone());
        let worker = ChildSpec::worker("worker", move |ctx, started| {
            let (before, during) = (before.clone(), during.clone());
            async move {
                let generation = ctx.generation();
                ctx.register(HANDLE, generation.clone()).map_err(err)?;
                before.set(Some(generation.phase()));
                started.started(()).map_err(err)?;
                ctx.scope().cancelled().await;
                // Cooperative drain: observe what the rest of the node sees.
                during.borrow_mut().push((
                    generation.phase(),
                    ctx.service(HANDLE).is_some(),
                    generation.operation_scope(&CancelScope::new()).is_ok(),
                ));
                Ok::<(), io::Error>(())
            }
        });
        let application = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForOne).child(worker),
        );
        let registry = application.registry();
        let tree = application.tree();
        let generation = Rc::new(RefCell::new(None));
        let captured = generation.clone();
        run_while(application, CancelScope::new(), move || async move {
            wait_until(|| registry.get(HANDLE).is_some()).await;
            let handle = registry.get(HANDLE).unwrap();
            wait_until(|| handle.phase() == GenerationPhase::Ready).await;
            assert!(handle.operation_scope(&CancelScope::new()).is_ok());
            *captured.borrow_mut() = Some(handle);
        })
        .await;

        assert_eq!(seen_before_ready.get(), Some(GenerationPhase::Starting));
        assert_eq!(
            *during_drain.borrow(),
            [(GenerationPhase::Draining, false, false)],
            "draining: rejects new operations and is not advertised"
        );
        let generation = generation.borrow().clone().unwrap();
        assert_eq!(generation.phase(), GenerationPhase::Stopped);
        let child = tree.child("test/root/worker").expect("snapshot");
        assert_eq!(child.last_exit, Some(ExitReason::Shutdown));
    });
}

/// A handle cached before a restart stays stale forever; the replacement's
/// handle is a different generation.
#[test]
fn cached_handle_stays_stale_after_restart() {
    LocalExecutor::default().run(async {
        let crash = Event::new();
        let crash_child = crash.clone();
        let worker = ChildSpec::worker("worker", move |ctx, started| {
            let crash = crash_child.clone();
            async move {
                let generation = ctx.generation();
                ctx.register(HANDLE, generation.clone()).map_err(err)?;
                started.started(()).map_err(err)?;
                if generation.id() == 1 {
                    let _ = crash.wait().await;
                    return Err(io::Error::other("injected failure"));
                }
                ctx.scope().cancelled().await;
                Ok(())
            }
        });
        let application = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForOne).child(worker),
        );
        let registry = application.registry();
        run_while(application, CancelScope::new(), move || async move {
            wait_until(|| registry.get(HANDLE).is_some()).await;
            let old = registry.get(HANDLE).unwrap();
            crash.set();
            wait_until(|| registry.get(HANDLE).is_some_and(|h| h.id() == 2)).await;
            let new = registry.get(HANDLE).unwrap();
            wait_until(|| new.phase() == GenerationPhase::Ready).await;

            assert_eq!(old.phase(), GenerationPhase::Stopped);
            assert!(old.operation_scope(&CancelScope::new()).is_err());
            assert!(new.operation_scope(&CancelScope::new()).is_ok());
            assert!(
                old.operation_scope(&CancelScope::new()).is_err(),
                "the old handle does not revive with its replacement"
            );
        })
        .await;
    });
}

/// Stop requested while a child is still initializing: the child observes
/// cancellation, the supervisor returns cleanly, and nothing restarts.
#[test]
fn stop_before_readiness_ends_cleanly_without_restart() {
    LocalExecutor::default().run(async {
        let initializing = Event::new();
        let saw_cancel = Rc::new(Cell::new(false));
        let starts = Rc::new(Cell::new(0));
        let (entered, cancelled, count) =
            (initializing.clone(), saw_cancel.clone(), starts.clone());
        let worker = ChildSpec::worker("slow-init", move |ctx, started| {
            let (entered, cancelled, count) = (entered.clone(), cancelled.clone(), count.clone());
            async move {
                count.set(count.get() + 1);
                entered.set();
                ctx.scope().cancelled().await; // never becomes ready
                cancelled.set(true);
                drop(started);
                Ok::<(), io::Error>(())
            }
        });
        let shutdown = CancelScope::new();
        let stop = shutdown.clone();
        let result = futures_lite::future::zip(
            Application::new(
                "test",
                SupervisorSpec::new("root", Strategy::OneForOne).child(worker),
            )
            .run(shutdown, TaskQueues::current()),
            async move {
                let _ = initializing.wait().await;
                stop.cancel();
            },
        )
        .await
        .0;

        assert!(result.is_ok(), "{result:?}");
        assert!(saw_cancel.get());
        assert_eq!(starts.get(), 1);
    });
}

/// A child that fails before readiness is a start failure of its supervisor,
/// not a restart loop.
#[test]
fn failure_before_readiness_fails_startup() {
    LocalExecutor::default().run(async {
        let worker = ChildSpec::worker("broken", |_ctx, _started| async move {
            Err::<(), _>(io::Error::other("cannot initialize"))
        });
        let result = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForOne).child(worker),
        )
        .run(CancelScope::new(), TaskQueues::current())
        .await;
        assert!(
            matches!(result, Err(OtpError::ChildStartFailed { .. })),
            "{result:?}"
        );
    });
}

/// An error returned while draining for a requested stop is part of the
/// shutdown, not a failure: it is recorded as `Shutdown` and not restarted.
#[test]
fn error_during_requested_drain_is_shutdown_not_restart() {
    LocalExecutor::default().run(async {
        let starts = Rc::new(Cell::new(0));
        let count = starts.clone();
        let worker = ChildSpec::worker("messy", move |ctx, started| {
            let count = count.clone();
            async move {
                count.set(count.get() + 1);
                started.started(()).map_err(err)?;
                ctx.scope().cancelled().await;
                Err::<(), _>(io::Error::other("cleanup failed"))
            }
        });
        let application = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForOne).child(worker),
        );
        let tree = application.tree();
        run_while(application, CancelScope::new(), move || async move {
            for _ in 0..8 {
                futures_lite::future::yield_now().await;
            }
        })
        .await;
        assert_eq!(starts.get(), 1);
        let child = tree.child("test/root/messy").expect("snapshot");
        assert_eq!(child.last_exit, Some(ExitReason::Shutdown));
    });
}

/// Exit history says why a generation was cancelled: which supervisor, and
/// whether it was a shutdown or a restart caused by another child.
#[test]
fn exit_history_records_why_cancellation_started() {
    LocalExecutor::default().run(async {
        let fail = Event::new();
        let fail_child = fail.clone();
        let flaky = ChildSpec::worker("flaky", move |ctx, started| {
            let fail = fail_child.clone();
            async move {
                started.started(()).map_err(err)?;
                if ctx.generation().id() == 1 {
                    let _ = fail.wait().await;
                    return Err(io::Error::other("boom"));
                }
                ctx.scope().cancelled().await;
                Ok(())
            }
        });
        let steady = ChildSpec::worker("steady", |ctx, started| async move {
            started.started(()).map_err(err)?;
            ctx.scope().cancelled().await;
            Ok::<(), io::Error>(())
        });
        let application = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForAll)
                .child(flaky)
                .child(steady),
        );
        let tree = application.tree();
        let observed = tree.clone();
        run_while(application, CancelScope::new(), move || async move {
            fail.set();
            wait_until(|| {
                observed
                    .child("test/root/steady")
                    .is_some_and(|c| c.generation == 2 && c.status == NodeStatus::Running)
            })
            .await;
        })
        .await;

        let steady = tree.child("test/root/steady").expect("snapshot");
        let origins: Vec<_> = steady
            .recent_exits
            .iter()
            .map(|exit| exit.cause.as_ref().and_then(|c| c.origin.clone()))
            .collect();
        assert_eq!(
            origins,
            [
                Some("supervisor test/root: restart after flaky exited (failure: boom)".into()),
                Some("supervisor test/root: supervisor shutdown".into()),
            ]
        );
    });
}
