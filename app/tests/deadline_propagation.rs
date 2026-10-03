//! A caller's deadline travels with its cross-shard calls: a call never waits
//! longer than the caller has left, and the handler sees the same deadline,
//! so calls it makes are bounded too.

use std::{cell::RefCell, rc::Rc, time::Duration};

use bapps_app::{CallError, CallOptions, RpcLimits, ShardId, lab::run_node};
use bapps_otp::{Application, ChildSpec, Strategy, SupervisorSpec};
use bapps_trio::{
    CancelScope, current_clock, remaining, sleep,
    testing::{Lab, LabConfig},
    with_cancel_scope,
};

enum Request {
    /// Reply with the handler's remaining time, in milliseconds.
    Remaining,
    /// Sleep this long, then reply.
    Sleep(Duration),
}

#[derive(Debug, Default, Clone)]
struct Seen {
    handler_remaining_ms: Option<u64>,
    capped: Option<(Result<u64, CallError>, Duration)>,
    uncapped: Option<(Result<u64, CallError>, Duration)>,
}

/// A scope that carries a deadline nobody enforces: only `call`'s cap can
/// end a call early under it.
fn deadline_in(after: Duration) -> CancelScope {
    let scope = CancelScope::new();
    scope.set_deadline(current_clock().now() + after);
    scope
}

async fn scenario() -> Seen {
    let seen = Rc::new(RefCell::new(Seen::default()));
    let out = seen.clone();
    run_node::<Request, u64, _, _>(2, RpcLimits::default(), move |shard| {
        let seen = seen.clone();
        async move {
            let id = shard.shard_id();
            let inbox = shard.inbox();
            let (client, gate, node) = (shard.client(), shard.ready_gate(), shard.node_control());
            let endpoint = ChildSpec::worker("endpoint", move |ctx, started| {
                bapps_app::serve(ctx, started, inbox.clone(), |request, _scope| async move {
                    match request {
                        Request::Remaining => {
                            Ok(remaining().map_or(u64::MAX, |left| left.as_millis() as u64))
                        }
                        Request::Sleep(time) => {
                            let _ = sleep(time).await;
                            Ok(0)
                        }
                    }
                })
            });
            let mut root = SupervisorSpec::new("root", Strategy::OneForOne).child(endpoint);
            if id == ShardId(0) {
                root = root.child(ChildSpec::worker("driver", move |ctx, started| {
                    let (client, gate, node, seen) =
                        (client.clone(), gate.clone(), node.clone(), seen.clone());
                    async move {
                        started.started(()).map_err(|e| format!("{e:?}"))?;
                        let scope = ctx.scope();
                        gate.wait(&scope).await.map_err(|e| e.to_string())?;
                        let call = |caller: CancelScope, request| {
                            let client = client.clone();
                            async move {
                                let start = current_clock().now();
                                let result = with_cancel_scope(caller.clone(), async {
                                    client
                                        .call(&caller, ShardId(1), request, CallOptions::default())
                                        .await
                                })
                                .await;
                                (result, current_clock().now() - start)
                            }
                        };
                        let (left, _) =
                            call(deadline_in(Duration::from_millis(80)), Request::Remaining).await;
                        seen.borrow_mut().handler_remaining_ms = left.ok();
                        let sleep_1s = || Request::Sleep(Duration::from_secs(1));
                        seen.borrow_mut().capped =
                            Some(call(deadline_in(Duration::from_millis(50)), sleep_1s()).await);
                        seen.borrow_mut().uncapped =
                            Some(call(CancelScope::new(), sleep_1s()).await);
                        node.shutdown();
                        scope.cancelled().await;
                        Ok::<(), String>(())
                    }
                }));
            }
            shard.run_application(Application::new("lab", root)).await
        }
    })
    .await
    .expect("node");
    out.borrow().clone()
}

#[test]
fn a_call_carries_the_callers_deadline_under_every_seed() {
    for seed in 0..10 {
        let report = Lab::run(LabConfig::new(seed), scenario());
        assert!(report.is_clean(), "seed {seed}: not clean");
        let seen = report.output.expect("finished");
        assert_eq!(
            seen.handler_remaining_ms,
            Some(80),
            "seed {seed}: the handler sees the caller's remaining time"
        );
        let (capped, took) = seen.capped.clone().unwrap();
        assert!(
            matches!(capped, Err(CallError::Deadline { .. })),
            "seed {seed}: {capped:?}"
        );
        assert_eq!(
            took,
            Duration::from_millis(50),
            "seed {seed}: capped at the caller's deadline"
        );
        let (uncapped, took) = seen.uncapped.clone().unwrap();
        assert_eq!(
            uncapped.ok(),
            Some(0),
            "seed {seed}: no deadline, the call completes"
        );
        assert_eq!(took, Duration::from_secs(1), "seed {seed}");
    }
}
