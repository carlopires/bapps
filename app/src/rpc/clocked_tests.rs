//! Deadline, multi-hop and re-entry races on a virtual clock.
//!
//! Two shards share one executor and a virtual clock built on Trio's
//! `TestClock`, so deadlines expire exactly when a test advances time, in the
//! same step as other events when that is the race being tested.

use std::{cell::Cell, rc::Rc, time::Duration};

use bapps_otp::{Application, ChildSpec, Shutdown, Strategy, SupervisorSpec};
use bapps_trio::{CancelScope, Clock, TaskQueues, cancel_on, testing::TestClock, with_nursery};
use futures_lite::future;
use glommio::LocalExecutor;

use super::*;
use crate::runtime::open_gate_for_tests;

/// RPC time backed by a Trio `TestClock`: `now` only moves when advanced.
struct VirtualClock {
    base: Instant,
    clock: TestClock,
}

impl RpcClock for VirtualClock {
    fn now(&self) -> Instant {
        self.base + self.clock.now()
    }
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Future<Output = ()>>> {
        self.clock
            .sleep_until(deadline.saturating_duration_since(self.base))
    }
}

enum Msg {
    Add(u64),
    /// Add, report `entered`, then hold until `release` (or until the handler
    /// scope is cancelled, when `cooperative`).
    Hold {
        n: u64,
        entered: async_channel::Sender<()>,
        release: async_channel::Receiver<()>,
        cooperative: bool,
    },
    /// Call `to` with `inner` under the handler's own scope, so cancellation
    /// of this request propagates downstream.
    Forward {
        to: ShardId,
        inner: Box<Msg>,
        options: CallOptions,
    },
}

#[derive(Clone, Default)]
struct Effects {
    total: Rc<Cell<u64>>,
    cleaned_up: Rc<Cell<u64>>,
}

struct Shard {
    client: ShardClient<Msg, u64>,
    inbox: ShardInbox<Msg, u64>,
    effects: Effects,
}

struct Node {
    shards: Vec<Shard>,
    clock: TestClock,
}

impl Node {
    /// Advance virtual time in small steps until `done` holds, letting every
    /// woken task run between steps.
    async fn pump_until(&self, step: Duration, mut done: impl FnMut() -> bool) {
        while !done() {
            self.clock.advance(step);
            for _ in 0..16 {
                future::yield_now().await;
            }
        }
    }
}

fn endpoint(shard: &Shard) -> ChildSpec {
    let inbox = shard.inbox.clone();
    let client = shard.client.clone();
    let effects = shard.effects.clone();
    ChildSpec::worker("endpoint", move |ctx, started| {
        let (client, effects) = (client.clone(), effects.clone());
        serve(ctx, started, inbox.clone(), move |message, scope| {
            let (client, effects) = (client.clone(), effects.clone());
            async move {
                match message {
                    Msg::Add(n) => {
                        effects.total.set(effects.total.get() + n);
                        Ok(effects.total.get())
                    }
                    Msg::Hold {
                        n,
                        entered,
                        release,
                        cooperative,
                    } => {
                        effects.total.set(effects.total.get() + n);
                        let _ = entered.send(()).await;
                        if cooperative {
                            if cancel_on(&scope, release.recv()).await.is_err() {
                                effects.cleaned_up.set(effects.cleaned_up.get() + 1);
                                return Err("cancelled; cleaned up".into());
                            }
                        } else {
                            let _ = release.recv().await;
                        }
                        Ok(effects.total.get())
                    }
                    Msg::Forward { to, inner, options } => client
                        .call(&scope, to, *inner, options)
                        .await
                        .map_err(|error| format!("downstream: {error:?}")),
                }
            }
        })
    })
    .shutdown(Shutdown::BrutalKill)
}

fn with_node<F, Fut>(shards: usize, limits: RpcLimits, body: F)
where
    F: FnOnce(Rc<Node>) -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    LocalExecutor::default().run(async move {
        let clock = TestClock::new();
        let base = Instant::now();
        let rpc_clock = || -> ClockRef {
            Rc::new(VirtualClock {
                base,
                clock: clock.clone(),
            })
        };
        let (senders, receivers) = fabric(shards, limits.queue_capacity);
        let shards: Vec<Shard> = receivers
            .into_iter()
            .enumerate()
            .map(|(id, receiver)| Shard {
                client: ShardClient::bind(
                    ShardId(id),
                    senders.clone(),
                    limits,
                    open_gate_for_tests(),
                    rpc_clock(),
                ),
                inbox: ShardInbox::bind(ShardId(id), receiver, limits, rpc_clock()),
                effects: Effects::default(),
            })
            .collect();
        let root = shards.iter().fold(
            SupervisorSpec::new("root", Strategy::OneForOne),
            |root, shard| root.child(endpoint(shard)),
        );
        let node = Rc::new(Node {
            shards,
            clock: clock.clone(),
        });
        let stop = CancelScope::new();
        // The supervision tree keeps real time for its own shutdown policy.
        let result = with_nursery::<String, _, _>(|nursery| {
            Box::pin(async move {
                let app_stop = stop.clone();
                nursery
                    .spawn(move |_| async move {
                        Application::new("test", root)
                            .run(app_stop, TaskQueues::current())
                            .await
                            .map_err(|e| e.to_string())
                    })
                    .expect("spawn application");
                body(node.clone()).await;
                stop.cancel();
                for shard in &node.shards {
                    assert_eq!(shard.client.metrics().active, 0, "outbound reservations");
                }
            })
        })
        .await;
        result.expect("test application");
    });
}

fn options(timeout_ms: u64) -> CallOptions {
    CallOptions {
        timeout: Duration::from_millis(timeout_ms),
        cancellation_grace: Duration::from_millis(1_000),
        task_class: bapps_trio::TaskClass::Default,
    }
}

fn hold(
    n: u64,
    cooperative: bool,
) -> (Msg, async_channel::Receiver<()>, async_channel::Sender<()>) {
    let (entered_tx, entered) = async_channel::bounded(1);
    let (release, release_rx) = async_channel::bounded(1);
    let message = Msg::Hold {
        n,
        entered: entered_tx,
        release: release_rx,
        cooperative,
    };
    (message, entered, release)
}

/// The deadline expires in the same step the handler is released. The
/// deadline wins; the reply that arrives during cleanup grace counts as
/// acknowledgement, and the effect stays applied.
#[test]
fn deadline_beats_a_reply_released_in_the_same_step() {
    with_node(1, RpcLimits::default(), |node| async move {
        let shard = &node.shards[0];
        let (message, entered, release) = hold(4, false);
        let (result, ()) = future::zip(
            shard
                .client
                .call(&CancelScope::new(), ShardId(0), message, options(5_000)),
            async {
                entered.recv().await.expect("entered");
                release.try_send(()).expect("release");
                node.clock.advance(Duration::from_millis(5_000));
            },
        )
        .await;
        assert_eq!(result, Err(CallError::Deadline { acknowledged: true }));
        assert_eq!(shard.effects.total.get(), 4);
    });
}

/// One tick before the deadline, the same release produces the result.
#[test]
fn reply_one_tick_before_the_deadline_wins() {
    with_node(1, RpcLimits::default(), |node| async move {
        let shard = &node.shards[0];
        let (message, entered, release) = hold(4, false);
        let (result, ()) = future::zip(
            shard
                .client
                .call(&CancelScope::new(), ShardId(0), message, options(5_000)),
            async {
                entered.recv().await.expect("entered");
                node.clock.advance(Duration::from_millis(4_999));
                release.try_send(()).expect("release");
            },
        )
        .await;
        assert_eq!(result, Ok(4));
    });
}

/// Cancellation crosses two hops when each handler passes its scope on:
/// caller -> shard 0 -> shard 1, and shard 1 cleans up cooperatively.
#[test]
fn cancellation_propagates_across_two_hops() {
    with_node(2, RpcLimits::default(), |node| async move {
        let caller = CancelScope::new();
        let (inner, entered, _release) = hold(1, true);
        let forward = Msg::Forward {
            to: ShardId(1),
            inner: Box::new(inner),
            options: options(20_000),
        };
        let (result, ()) = future::zip(
            node.shards[0]
                .client
                .call(&caller, ShardId(0), forward, options(20_000)),
            async {
                entered.recv().await.expect("second hop entered");
                caller.cancel();
            },
        )
        .await;
        assert_eq!(result, Err(CallError::Cancelled { acknowledged: true }));
        assert_eq!(
            node.shards[1].effects.cleaned_up.get(),
            1,
            "second hop cleaned up"
        );
        assert_eq!(node.shards[0].inbox.metrics().active, 0);
        assert_eq!(node.shards[1].inbox.metrics().active, 0);
    });
}

/// The capacity-deadlock boundary: a handler that re-enters its own saturated
/// endpoint cannot be served. The nested call ends by deadline plus cleanup
/// grace, unacknowledged, and the queued nested request never runs.
#[test]
fn saturated_reentry_ends_by_deadline_not_deadlock() {
    let limits = RpcLimits {
        max_in_flight: 1,
        ..RpcLimits::default()
    };
    with_node(1, limits, |node| async move {
        let shard = &node.shards[0];
        let reentrant = Msg::Forward {
            to: ShardId(0),
            inner: Box::new(Msg::Add(1)),
            options: options(1_000),
        };
        let done = Rc::new(Cell::new(false));
        let finished = done.clone();
        let (result, ()) = future::zip(
            async {
                let result = shard
                    .client
                    .call(&CancelScope::new(), ShardId(0), reentrant, options(20_000))
                    .await;
                finished.set(true);
                result
            },
            node.pump_until(Duration::from_millis(100), || done.get()),
        )
        .await;

        let Err(CallError::Remote(message)) = result else {
            panic!("expected the outer handler to report the nested failure: {result:?}");
        };
        assert!(
            message.contains("Deadline { acknowledged: false }"),
            "{message}"
        );
        // Let the leftover nested request reach the endpoint: it is refused.
        node.pump_until(Duration::from_millis(100), || {
            shard.inbox.metrics().queued == 0
        })
        .await;
        for _ in 0..16 {
            future::yield_now().await;
        }
        assert_eq!(shard.effects.total.get(), 0, "the nested Add never ran");
    });
}
