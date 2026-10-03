//! Explicit Linux integration gate. No silently passing "skip" on one CPU.
use bapps_app::{AppBuilder, CallError, CallOptions, RpcLimits, ShardId};
use bapps_otp::{Application, ChildSpec, Strategy, SupervisorSpec};
use bapps_trio::{TaskQueues, with_nursery_with_queues};
use std::{
    cell::Cell,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

enum Request {
    Add(u64),
    Wait(async_channel::Sender<()>),
}

#[test]
#[ignore = "requires Linux io_uring and at least two allowed CPUs; make integration"]
fn isolated_state_roundtrip_cancel_ack_and_node_shutdown() {
    let cleaned = Arc::new(AtomicUsize::new(0));
    let captured = cleaned.clone();
    AppBuilder::new()
        .shards(2)
        .limits(RpcLimits {
            max_in_flight: 2,
            queue_capacity: 4,
            ..RpcLimits::default()
        })
        .run::<Request, u64, _, _>(move |shard| {
            let id = shard.shard_id();
            let inbox = shard.inbox();
            let client = shard.client();
            let gate = shard.ready_gate();
            let node = shard.node_control();
            let cleaned = captured.clone();
            async move {
                let endpoint = ChildSpec::worker("endpoint", move |ctx, started| {
                    let state = Rc::new(Cell::new(0));
                    let cleaned = cleaned.clone();
                    bapps_app::serve(ctx, started, inbox.clone(), move |request, scope| {
                        let state = state.clone();
                        let cleaned = cleaned.clone();
                        async move {
                            match request {
                                Request::Add(n) => {
                                    state.set(state.get() + n);
                                    Ok(state.get())
                                }
                                Request::Wait(ready) => {
                                    ready.send(()).await.map_err(|e| e.to_string())?;
                                    scope.cancelled().await;
                                    cleaned.fetch_add(1, Ordering::SeqCst);
                                    Err("cooperative cleanup complete".into())
                                }
                            }
                        }
                    })
                });
                let mut root = SupervisorSpec::new("root", Strategy::OneForOne).child(endpoint);
                if id == ShardId(0) {
                    root = root.child(ChildSpec::worker("driver", move |ctx, started| {
                        let client = client.clone();
                        let gate = gate.clone();
                        let node = node.clone();
                        async move {
                            started.started(()).map_err(|e| format!("{e:?}"))?;
                            let scope = ctx.scope();
                            gate.wait(&scope).await.map_err(|e| e.to_string())?;
                            assert_eq!(
                                client
                                    .call(
                                        &scope,
                                        ShardId(0),
                                        Request::Add(3),
                                        CallOptions::default()
                                    )
                                    .await
                                    .unwrap(),
                                3
                            );
                            assert_eq!(
                                client
                                    .call(
                                        &scope,
                                        ShardId(1),
                                        Request::Add(9),
                                        CallOptions::default()
                                    )
                                    .await
                                    .unwrap(),
                                9
                            );
                            assert_eq!(
                                client
                                    .call(
                                        &scope,
                                        ShardId(0),
                                        Request::Add(2),
                                        CallOptions::default()
                                    )
                                    .await
                                    .unwrap(),
                                5
                            );
                            let (ready, entered) = async_channel::bounded(1);
                            let operation = scope.child();
                            let task_scope = operation.clone();
                            with_nursery_with_queues::<String, _, _>(
                                TaskQueues::current(),
                                |nursery| {
                                    Box::pin(async move {
                                        nursery
                                            .spawn(move |_| async move {
                                                let result = client
                                                    .call(
                                                        &task_scope,
                                                        ShardId(1),
                                                        Request::Wait(ready),
                                                        CallOptions::default(),
                                                    )
                                                    .await;
                                                assert!(
                                                    matches!(
                                                        result,
                                                        Err(CallError::Cancelled {
                                                            acknowledged: true
                                                        })
                                                    ),
                                                    "{result:?}"
                                                );
                                                Ok(())
                                            })
                                            .unwrap();
                                        entered.recv().await.unwrap(); // event, not a timing guess
                                        operation.cancel();
                                    })
                                },
                            )
                            .await
                            .unwrap();
                            node.shutdown();
                            scope.cancelled().await;
                            Ok::<(), String>(())
                        }
                    }));
                }
                shard.run_application(Application::new("test", root)).await
            }
        })
        .expect("all executor threads joined successfully");
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[test]
#[ignore = "requires Linux io_uring and two allowed CPUs; make integration"]
fn one_failed_shard_startup_stops_its_sibling() {
    let result = AppBuilder::new()
        .shards(2)
        .startup_timeout(Duration::from_secs(5))
        .run::<(), (), _, _>(|shard| async move {
            if shard.shard_id() == ShardId(1) {
                return Err("injected shard construction failure".into());
            }
            let child = ChildSpec::worker("idle", |ctx, started| async move {
                started.started(()).map_err(|e| format!("{e:?}"))?;
                ctx.scope().cancelled().await;
                Ok::<(), String>(())
            });
            shard
                .run_application(Application::new(
                    "test",
                    SupervisorSpec::new("root", Strategy::OneForOne).child(child),
                ))
                .await
        });
    assert!(result.is_err());
}

enum LossRequest {
    Hold(async_channel::Sender<()>),
}

/// Executor loss: shard 1's root fails while a call from shard 0 is running
/// on it. The node fail-stops, every thread is joined, and the caller never
/// sees success or "not executed" for a request that had started.
#[test]
#[ignore = "requires Linux io_uring and two allowed CPUs; make integration"]
fn losing_an_executor_with_a_call_in_flight_fail_stops_the_node() {
    let (outcome_tx, outcome_rx) = std::sync::mpsc::channel();
    let (bomb_tx, bomb_rx) = async_channel::bounded::<()>(1);
    let result = AppBuilder::new()
        .shards(2)
        .run::<LossRequest, u64, _, _>(move |shard| {
            let id = shard.shard_id();
            let inbox = shard.inbox();
            let client = shard.client();
            let gate = shard.ready_gate();
            let outcome_tx = outcome_tx.clone();
            let (bomb_tx, bomb_rx) = (bomb_tx.clone(), bomb_rx.clone());
            async move {
                let endpoint = ChildSpec::worker("endpoint", move |ctx, started| {
                    bapps_app::serve(ctx, started, inbox.clone(), |request, _scope| async move {
                        match request {
                            LossRequest::Hold(entered) => {
                                let _ = entered.send(()).await;
                                futures_lite::future::pending::<()>().await;
                                Ok(0)
                            }
                        }
                    })
                });
                let mut root = SupervisorSpec::new("root", Strategy::OneForOne)
                    .restart_intensity(0, Duration::from_secs(1))
                    .child(endpoint);
                if id == ShardId(1) {
                    // Fails the whole shard root on demand: restart budget 0.
                    root = root.child(ChildSpec::worker("bomb", move |ctx, started| {
                        let bomb = bomb_rx.clone();
                        async move {
                            started.started(()).map_err(|e| format!("{e:?}"))?;
                            let _ = bapps_trio::cancel_on(&ctx.scope(), bomb.recv()).await;
                            Err::<(), _>("injected shard failure".to_string())
                        }
                    }));
                } else {
                    root = root.child(ChildSpec::worker("driver", move |ctx, started| {
                        let (client, gate) = (client.clone(), gate.clone());
                        let (outcome_tx, bomb_tx) = (outcome_tx.clone(), bomb_tx.clone());
                        async move {
                            started.started(()).map_err(|e| format!("{e:?}"))?;
                            let scope = ctx.scope();
                            gate.wait(&scope).await.map_err(|e| e.to_string())?;
                            let (entered_tx, entered) = async_channel::bounded(1);
                            let call = client.call(
                                &scope,
                                ShardId(1),
                                LossRequest::Hold(entered_tx),
                                CallOptions::default(),
                            );
                            let (result, ()) = futures_lite::future::zip(call, async {
                                entered.recv().await.expect("handler entered");
                                bomb_tx.send(()).await.expect("trigger shard failure");
                            })
                            .await;
                            let _ = outcome_tx.send(result);
                            scope.cancelled().await;
                            Ok::<(), String>(())
                        }
                    }));
                }
                shard.run_application(Application::new("test", root)).await
            }
        });
    assert!(
        result.is_err(),
        "a lost shard fail-stops the node: {result:?}"
    );
    let outcome = outcome_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the caller finished");
    assert!(
        matches!(
            outcome,
            Err(CallError::OutcomeUnknown
                | CallError::Cancelled { .. }
                | CallError::Deadline { .. })
        ),
        "a started request is never reported as success or as not executed: {outcome:?}"
    );
}
