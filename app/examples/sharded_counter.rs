//! cargo run --example sharded_counter -- 2
use bapps_app::{AppBuilder, CallOptions, ShardId};
use bapps_otp::{Application, ChildSpec, Strategy, SupervisorSpec};
use std::{cell::Cell, rc::Rc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "2".into())
        .parse::<usize>()?;
    AppBuilder::new()
        .shards(count)
        .run::<u64, u64, _, _>(|shard| {
            // Construct local state here, not on the host and not inside an Arc<Mutex>.
            let id = shard.shard_id();
            let cpu = shard.cpu_id();
            let inbox = shard.inbox();
            let client = shard.client();
            let gate = shard.ready_gate();
            let node = shard.node_control();
            async move {
                let endpoint = ChildSpec::worker("counter", move |ctx, started| {
                    let counter = Rc::new(Cell::new(0_u64));
                    bapps_app::serve(ctx, started, inbox.clone(), move |amount, _scope| {
                        let counter = counter.clone();
                        async move {
                            counter.set(counter.get() + amount);
                            Ok(counter.get())
                        }
                    })
                });
                let mut root = SupervisorSpec::new("root", Strategy::OneForOne).child(endpoint);
                if id == ShardId(0) {
                    // A node-wide service is explicitly placed, not secretly shared.
                    root = root.child(ChildSpec::worker("demo-driver", move |ctx, started| {
                        let client = client.clone();
                        let gate = gate.clone();
                        let node = node.clone();
                        async move {
                            started.started(()).map_err(|e| format!("{e:?}"))?;
                            let scope = ctx.scope();
                            gate.wait(&scope).await.map_err(|e| e.to_string())?;
                            for target in 0..client.shard_count() {
                                let value = client
                                    .call(&scope, ShardId(target), 7, CallOptions::default())
                                    .await
                                    .map_err(|e| e.to_string())?;
                                println!("shard {target}: {value}");
                            }
                            node.shutdown();
                            scope.cancelled().await;
                            Ok::<(), String>(())
                        }
                    }));
                }
                println!("shard {id} pinned to Linux CPU {cpu}");
                shard
                    .run_application(Application::new("counter", root))
                    .await
            }
        })?;
    Ok(())
}
