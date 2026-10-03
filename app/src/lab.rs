//! Run a whole shard-per-core node inside `bapps_trio::testing::Lab`.
//!
//! [`run_node`] is [`AppBuilder::run`](crate::AppBuilder::run) without threads:
//! every shard's factory runs as a task of the current lab executor, the host
//! barrier and fail-stop logic run as another task, and cross-shard calls use
//! the same bounded channels with deadlines on the lab's virtual clock. Seeds
//! then explore interleavings *across shards*, which real threads never repeat.
//!
//! Shard CPU ids are fake (`0..shards`); nothing is pinned. Call it only while
//! a lab is running.

use std::{future::Future, rc::Rc, sync::Arc, time::Instant};

use bapps_trio::{Nursery, with_nursery};

use crate::{
    AppError, RpcLimits, ShardClient, ShardContext, ShardId,
    rpc::{TrioClock, fabric},
    runtime::{HostEvent, HostSink, control_plane},
};

/// Run a node of `shards` shards on the current lab executor until every
/// shard's factory returns. Same contract as `AppBuilder::run`: all roots must
/// report ready before the gate opens; any shard exit stops the node.
pub async fn run_node<M, R, F, Fut>(
    shards: usize,
    limits: RpcLimits,
    factory: F,
) -> Result<(), AppError>
where
    M: Send + 'static,
    R: Send + 'static,
    F: Fn(ShardContext<M, R>) -> Fut + 'static,
    Fut: Future<Output = Result<(), String>> + 'static,
{
    assert!(
        bapps_trio::testing::Lab::is_running(),
        "bapps_app::lab::run_node needs a running bapps_trio lab"
    );
    limits.validate().map_err(AppError)?;
    let base = Instant::now();
    let cpus = Arc::new((0..shards).collect::<Vec<_>>());
    let (senders, receivers) = fabric::<M, R>(shards, limits.queue_capacity);
    let (stops, stop_receivers): (Vec<_>, Vec<_>) =
        (0..shards).map(|_| async_channel::bounded(1)).unzip();
    let (control, gate) = control_plane(stops);
    let (host_tx, host_rx) = async_channel::unbounded();

    let result = with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
        Box::pin(async move {
            for (index, (receiver, stop)) in receivers.into_iter().zip(stop_receivers).enumerate() {
                let id = ShardId(index);
                let context = ShardContext {
                    id,
                    cpu: index,
                    cpus: cpus.clone(),
                    client: ShardClient::bind(
                        id,
                        senders.clone(),
                        limits,
                        gate.clone(),
                        Rc::new(TrioClock::current(base)),
                    ),
                    inbox: crate::ShardInbox::bind(
                        id,
                        receiver,
                        limits,
                        Rc::new(TrioClock::current(base)),
                    ),
                    control: control.clone(),
                    gate: gate.clone(),
                    stop,
                    host: HostSink::Lab(host_tx.clone()),
                };
                let shard = factory(context);
                let host = host_tx.clone();
                nursery
                    .spawn(move |_| async move {
                        let outcome = shard.await;
                        let _ = host.try_send(HostEvent::Exited(id, outcome));
                        Ok(())
                    })
                    .map_err(|e| AppError(format!("spawn shard {index}: {e}")))?;
            }
            drop(host_tx);
            drop(senders);

            let mut ready = vec![false; shards];
            let mut exited = 0;
            let mut failure = None;
            while exited < shards {
                match host_rx.recv().await {
                    Ok(HostEvent::Ready(id)) => {
                        ready[id.0] = true;
                        if ready.iter().all(|value| *value) {
                            control.release();
                        }
                    }
                    Ok(HostEvent::Exited(id, result)) => {
                        exited += 1;
                        if let Err(error) = result {
                            failure.get_or_insert(error);
                        } else if !control.is_stopping() {
                            failure
                                .get_or_insert_with(|| format!("shard {id} exited unexpectedly"));
                        }
                        control.shutdown();
                    }
                    Err(_) => break,
                }
            }
            failure.map_or(Ok(()), |error| Err(AppError(error)))
        })
    })
    .await;
    match result {
        Ok(outcome) => outcome,
        Err(error) => Err(AppError(format!("lab node: {error:?}"))),
    }
}
