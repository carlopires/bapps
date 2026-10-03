use bapps_otp::{
    Application, CancelScope, ChildSpec, LocalMailbox, Strategy, SupervisorSpec, TaskQueues,
};
use bapps_trio::{sync::Event, with_nursery_with_queues};
use glommio::{LocalExecutorBuilder, Placement};
use std::{cell::Cell, io, rc::Rc};

#[test]
fn cancelled_mailbox_send_does_not_enqueue() {
    let executor = LocalExecutorBuilder::new(Placement::Unbound)
        .make()
        .expect("executor");
    executor.run(async {
        let (tx, rx) = LocalMailbox::<u64>::bounded(2);
        let scope = CancelScope::new();
        scope.cancel();
        assert!(tx.send_in(&scope, 7).await.is_err());
        assert_eq!(rx.try_recv().expect("open"), None);
    });
}

#[test]
fn root_readiness_and_task_admission_are_explicit() {
    let executor = LocalExecutorBuilder::new(Placement::Unbound)
        .make()
        .expect("executor");
    executor.run(async {
        let shutdown = CancelScope::new();
        let stop = shutdown.clone();
        let initialized = Rc::new(Cell::new(false));
        let seen = initialized.clone();
        let child = ChildSpec::worker("worker", move |ctx, started| {
            let initialized = initialized.clone();
            async move {
                let release = Event::new();
                let done = release.clone();
                ctx.tasks()
                    .spawn(move |_| async move {
                        done.wait()
                            .await
                            .map_err(|e| io::Error::other(format!("{e:?}")))?;
                        Ok::<(), io::Error>(())
                    })
                    .expect("task");
                // No yield: reservation must already be visible before first poll.
                assert_eq!(ctx.tasks().active_tasks(), 1);
                release.set();
                ctx.tasks()
                    .wait_below(1)
                    .await
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                initialized.set(true);
                started
                    .started(())
                    .map_err(|e| io::Error::other(format!("{e:?}")))?;
                ctx.scope().cancelled().await;
                Ok::<(), io::Error>(())
            }
        });
        let app = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForOne).child(child),
        );
        with_nursery_with_queues::<String, _, _>(TaskQueues::current(), |nursery| {
            Box::pin(async move {
                nursery
                    .start(move |_, started| async move {
                        app.run_started(stop, TaskQueues::current(), started)
                            .await
                            .map_err(|e| e.to_string())
                    })
                    .await
                    .expect("root readiness");
                assert!(seen.get());
                shutdown.cancel();
            })
        })
        .await
        .expect("root nursery");
    });
}
