//! A whole multi-shard node inside the deterministic lab.

use std::{cell::Cell, rc::Rc};

use bapps_app::{CallError, CallOptions, RpcLimits, ShardId, lab::run_node};
use bapps_otp::{Application, ChildSpec, Strategy, SupervisorSpec};
use bapps_trio::{
    CancelScope,
    testing::{Lab, LabConfig},
};

enum Request {
    Add(u64),
    /// Hold until the handler's scope is cancelled; report cleanup.
    Hold(async_channel::Sender<()>),
}

/// Two shards. Shard 0's driver adds on both shards, then starts a call it
/// cancels mid-handler, then stops the node. Returns what the driver saw.
async fn two_shard_node() -> (Vec<u64>, Option<CallError>) {
    let seen = Rc::new(std::cell::RefCell::new((Vec::new(), None)));
    let out = seen.clone();
    let result = run_node::<Request, u64, _, _>(2, RpcLimits::default(), move |shard| {
        let seen = seen.clone();
        async move {
            let id = shard.shard_id();
            let inbox = shard.inbox();
            let client = shard.client();
            let gate = shard.ready_gate();
            let node = shard.node_control();
            let total = Rc::new(Cell::new(0_u64));
            let endpoint = ChildSpec::worker("endpoint", move |ctx, started| {
                let total = total.clone();
                bapps_app::serve(ctx, started, inbox.clone(), move |request, scope| {
                    let total = total.clone();
                    async move {
                        match request {
                            Request::Add(n) => {
                                total.set(total.get() + n);
                                Ok(total.get())
                            }
                            Request::Hold(entered) => {
                                let _ = entered.send(()).await;
                                scope.cancelled().await;
                                Err("cancelled".into())
                            }
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
                        for (target, n) in [(0, 3), (1, 9), (0, 2), (1, 1)] {
                            let total = client
                                .call(
                                    &scope,
                                    ShardId(target),
                                    Request::Add(n),
                                    CallOptions::default(),
                                )
                                .await
                                .map_err(|e| format!("{e:?}"))?;
                            seen.borrow_mut().0.push(total);
                        }
                        let (entered_tx, entered) = async_channel::bounded(1);
                        let caller = CancelScope::new();
                        let (result, ()) = futures_lite::future::zip(
                            client.call(
                                &caller,
                                ShardId(1),
                                Request::Hold(entered_tx),
                                CallOptions::default(),
                            ),
                            async {
                                let _ = entered.recv().await;
                                caller.cancel();
                            },
                        )
                        .await;
                        seen.borrow_mut().1 = result.err();
                        node.shutdown();
                        scope.cancelled().await;
                        Ok::<(), String>(())
                    }
                }));
            }
            shard.run_application(Application::new("lab", root)).await
        }
    })
    .await;
    result.expect("node");
    let (totals, cancelled) = out.borrow().clone();
    (totals, cancelled)
}

#[test]
fn a_two_shard_node_runs_clean_under_every_seed() {
    let failures = Lab::explore(0..100, two_shard_node, |report| {
        report.is_clean()
            && report.output
                == Some((
                    vec![3, 9, 5, 10],
                    Some(CallError::Cancelled { acknowledged: true }),
                ))
    });
    assert!(
        failures.is_empty(),
        "{} failed; first: {:?}",
        failures.len(),
        failures.first()
    );
}

#[test]
fn seeds_interleave_the_shards() {
    let traces: std::collections::HashSet<_> = (0..100)
        .map(|seed| Lab::run(LabConfig::new(seed), two_shard_node()).trace)
        .collect();
    assert!(traces.len() > 10, "{} distinct interleavings", traces.len());
}
