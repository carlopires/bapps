//! Request-lifecycle races, driven deterministically on one executor.
//!
//! One shard calls itself: the client, the inbox and the OTP endpoint all live
//! on the same Glommio executor, so each race is staged with explicit events
//! (`entered`, `release`) rather than timing guesses. Each test checks the
//! caller-visible outcome class, the destination-side effect, and that every
//! admission reservation is released.

use std::{cell::Cell, rc::Rc, time::Duration};

use bapps_otp::{Application, ChildSpec, Shutdown, Strategy, SupervisorSpec};
use bapps_trio::{CancelScope, TaskQueues, cancel_on, with_nursery};
use futures_lite::future::{self, FutureExt};
use glommio::LocalExecutor;

use super::*;
use crate::runtime::open_gate_for_tests;

/// Messages understood by the test endpoint. Channels make each step of a
/// race observable and controllable from the test body.
enum Msg {
    /// Add to the shard's total and reply with the new total.
    Add(u64),
    /// Add, report `entered`, then hold until `release` (or, if
    /// `cooperative`, until the handler scope is cancelled).
    AddThenHold {
        n: u64,
        entered: async_channel::Sender<()>,
        release: async_channel::Receiver<()>,
        cooperative: bool,
    },
    /// Report `entered`, hold until `release`, then panic.
    PanicAfter {
        entered: async_channel::Sender<()>,
        release: async_channel::Receiver<()>,
    },
}

/// What the destination actually did, independent of what callers observed.
#[derive(Clone, Default)]
struct Effects {
    total: Rc<Cell<u64>>,
    started: Rc<Cell<u64>>,
    cleaned_up: Rc<Cell<u64>>,
}

struct Fixture {
    client: ShardClient<Msg, u64>,
    inbox: ShardInbox<Msg, u64>,
    effects: Effects,
}

fn endpoint(fixture: &Fixture, shutdown: Shutdown) -> ChildSpec {
    let inbox = fixture.inbox.clone();
    let effects = fixture.effects.clone();
    ChildSpec::worker("endpoint", move |ctx, started| {
        let effects = effects.clone();
        serve(ctx, started, inbox.clone(), move |message, scope| {
            let effects = effects.clone();
            async move {
                effects.started.set(effects.started.get() + 1);
                match message {
                    Msg::Add(n) => {
                        effects.total.set(effects.total.get() + n);
                        Ok(effects.total.get())
                    }
                    Msg::AddThenHold {
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
                    Msg::PanicAfter { entered, release } => {
                        let _ = entered.send(()).await;
                        let _ = release.recv().await;
                        panic!("injected handler panic");
                    }
                }
            }
        })
    })
    .shutdown(shutdown)
}

/// Run `body` against one shard serving its own inbox under a supervisor.
fn with_endpoint<F, Fut>(limits: RpcLimits, shutdown: Shutdown, body: F)
where
    F: FnOnce(Rc<Fixture>, CancelScope) -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    LocalExecutor::default().run(async move {
        let (senders, receivers) = fabric(1, limits.queue_capacity);
        let receiver = receivers.into_iter().next().expect("one shard");
        let fixture = Rc::new(Fixture {
            client: ShardClient::bind(
                ShardId(0),
                senders,
                limits,
                open_gate_for_tests(),
                Rc::new(SystemClock),
            ),
            inbox: ShardInbox::bind(ShardId(0), receiver, limits, Rc::new(SystemClock)),
            effects: Effects::default(),
        });
        let application = Application::new(
            "test",
            SupervisorSpec::new("root", Strategy::OneForOne)
                .restart_intensity(10, Duration::from_secs(10))
                .child(endpoint(&fixture, shutdown)),
        );
        let stop = CancelScope::new();
        let ledger = bapps_trio::obligation_stats();
        let result = with_nursery::<String, _, _>(|nursery| {
            let stop = stop.clone();
            Box::pin(async move {
                let app_stop = stop.clone();
                nursery
                    .spawn(move |_| async move {
                        application
                            .run(app_stop, TaskQueues::current())
                            .await
                            .map_err(|e| e.to_string())
                    })
                    .expect("spawn application");
                body(fixture.clone(), stop.clone()).await;
                stop.cancel();
                assert_eq!(fixture.client.metrics().active, 0, "outbound reservations");
            })
        })
        .await;
        result.expect("test application");
        // Every started request ended in exactly one reply decision.
        let after = bapps_trio::obligation_stats();
        assert_eq!(after.pending, ledger.pending, "no reply left unresolved");
        assert_eq!(after.leaked, ledger.leaked, "no reply forgotten");
    });
}

fn options() -> CallOptions {
    CallOptions {
        timeout: Duration::from_secs(5),
        cancellation_grace: Duration::from_secs(2),
        task_class: bapps_trio::TaskClass::Default,
    }
}

fn hold(
    n: u64,
    cooperative: bool,
) -> (Msg, async_channel::Receiver<()>, async_channel::Sender<()>) {
    let (entered_tx, entered) = async_channel::bounded(1);
    let (release, release_rx) = async_channel::bounded(1);
    let message = Msg::AddThenHold {
        n,
        entered: entered_tx,
        release: release_rx,
        cooperative,
    };
    (message, entered, release)
}

#[test]
fn completed_call_returns_the_handler_result() {
    with_endpoint(
        RpcLimits::default(),
        Shutdown::BrutalKill,
        |f, _| async move {
            let total = f
                .client
                .call(&CancelScope::new(), ShardId(0), Msg::Add(3), options())
                .await;
            assert_eq!(total, Ok(3));
        },
    );
}

/// Cancellation is not rollback: an effect applied before the caller gave up
/// stays applied, and the transport never retries the call.
#[test]
fn caller_cancellation_keeps_the_applied_effect() {
    with_endpoint(
        RpcLimits::default(),
        Shutdown::BrutalKill,
        |f, _| async move {
            let caller = CancelScope::new();
            let (message, entered, _release) = hold(5, true);
            let (result, ()) = future::zip(
                f.client.call(&caller, ShardId(0), message, options()),
                async {
                    entered.recv().await.expect("handler entered");
                    caller.cancel();
                },
            )
            .await;

            assert_eq!(result, Err(CallError::Cancelled { acknowledged: true }));
            assert_eq!(
                f.effects.cleaned_up.get(),
                1,
                "handler observed cancellation"
            );
            let now = f
                .client
                .call(&CancelScope::new(), ShardId(0), Msg::Add(0), options())
                .await;
            assert_eq!(now, Ok(5), "effect is visible after the cancelled call");
            assert_eq!(f.effects.started.get(), 2, "no retry of the cancelled call");
        },
    );
}

/// A handler that ignores cancellation is destroyed after the destination's
/// grace. That is `OutcomeUnknown` on the wire, and the cancelled caller must
/// not report it as acknowledged.
#[test]
fn forced_handler_is_not_acknowledged() {
    let limits = RpcLimits {
        handler_cancel_grace: Duration::from_millis(20),
        ..RpcLimits::default()
    };
    with_endpoint(limits, Shutdown::BrutalKill, |f, _| async move {
        let caller = CancelScope::new();
        let (message, entered, _release) = hold(7, false);
        let (result, ()) = future::zip(
            f.client.call(&caller, ShardId(0), message, options()),
            async {
                entered.recv().await.expect("handler entered");
                caller.cancel();
            },
        )
        .await;

        assert_eq!(
            result,
            Err(CallError::Cancelled {
                acknowledged: false
            })
        );
        assert_eq!(f.effects.total.get(), 7, "the effect happened anyway");
        assert_eq!(f.inbox.metrics().interrupted, 1);
    });
}

/// A caller that is still waiting when its handler is destroyed without a
/// reply (here: a panic) learns `OutcomeUnknown`, not `TargetStopped`.
/// The request queued behind it was never started, so the restarted
/// generation serves it exactly once.
#[test]
fn panicking_handler_is_outcome_unknown_and_queued_work_moves_to_next_generation() {
    let limits = RpcLimits {
        max_in_flight: 1,
        ..RpcLimits::default()
    };
    with_endpoint(limits, Shutdown::BrutalKill, |f, _| async move {
        let (entered_tx, entered) = async_channel::bounded(1);
        let (release, release_rx) = async_channel::bounded(1);
        let panicking = Msg::PanicAfter {
            entered: entered_tx,
            release: release_rx,
        };
        let scope = CancelScope::new();
        let (first, second) = future::zip(
            f.client.call(&scope, ShardId(0), panicking, options()),
            async {
                entered.recv().await.expect("handler entered");
                // `Add` is admitted to the queue behind the held handler.
                let queued = f.client.call(&scope, ShardId(0), Msg::Add(1), options());
                let (result, ()) = future::zip(queued, async {
                    while f.inbox.metrics().queued == 0 {
                        future::yield_now().await;
                    }
                    release.send(()).await.expect("release");
                })
                .await;
                result
            },
        )
        .await;

        assert_eq!(first, Err(CallError::OutcomeUnknown));
        assert_eq!(
            second,
            Ok(1),
            "queued request served by the next generation"
        );
        assert_eq!(f.effects.started.get(), 2, "each request started once");
    });
}

/// A caller that gives up while still waiting for queue capacity never
/// delivered its request: `NotAdmitted`, and the handler never sees it.
#[test]
fn interruption_before_admission_is_not_admitted() {
    let limits = RpcLimits {
        max_in_flight: 1,
        queue_capacity: 1,
        ..RpcLimits::default()
    };
    with_endpoint(limits, Shutdown::BrutalKill, |f, _| async move {
        let scope = CancelScope::new();
        let late = CancelScope::new();
        let (held, entered, release) = hold(1, false);
        let short = CallOptions {
            timeout: Duration::from_millis(50),
            ..options()
        };
        let (first, (queued, (by_deadline, by_cancel))) =
            future::zip(f.client.call(&scope, ShardId(0), held, options()), async {
                entered.recv().await.expect("handler entered");
                // In flight: 1 of 1. Queue: 1 of 1 after `Add(10)`.
                future::zip(
                    f.client.call(&scope, ShardId(0), Msg::Add(10), options()),
                    async {
                        while f.inbox.metrics().queued == 0 {
                            future::yield_now().await;
                        }
                        let outcomes = future::zip(
                            f.client.call(&scope, ShardId(0), Msg::Add(100), short),
                            async {
                                let waiting =
                                    f.client.call(&late, ShardId(0), Msg::Add(1000), options());
                                let cancel = async {
                                    // Both late callers hold an outbound slot while blocked.
                                    while f.client.metrics().active < 4 {
                                        future::yield_now().await;
                                    }
                                    late.cancel();
                                    future::pending::<Result<u64, CallError>>().await
                                };
                                waiting.or(cancel).await
                            },
                        )
                        .await;
                        release.send(()).await.expect("release");
                        outcomes
                    },
                )
                .await
            })
            .await;

        assert_eq!(
            by_deadline,
            Err(CallError::NotAdmitted(Interruption::Deadline))
        );
        assert_eq!(
            by_cancel,
            Err(CallError::NotAdmitted(Interruption::Cancelled))
        );
        assert_eq!(first, Ok(1));
        assert_eq!(queued, Ok(11));
        assert_eq!(
            f.effects.started.get(),
            2,
            "rejected requests never started"
        );
    });
}

/// Outbound admission is shared by client clones and released on every exit,
/// including when an admitted call's future is dropped mid-flight. A dropped
/// call still sends a best-effort cancellation to its handler.
#[test]
fn outbound_budget_is_shared_and_released_when_a_call_is_dropped() {
    let limits = RpcLimits {
        max_outbound: 1,
        ..RpcLimits::default()
    };
    with_endpoint(limits, Shutdown::BrutalKill, |f, _| async move {
        let (message, entered, _release) = hold(1, true);
        let scope = CancelScope::new();
        let call = f.client.call(&scope, ShardId(0), message, options());
        let dropped = async { Some(call.await) }
            .or(async {
                entered.recv().await.expect("handler entered");
                let clone = f.client.clone();
                let refused = clone.call(&scope, ShardId(0), Msg::Add(1), options()).await;
                assert_eq!(refused, Err(CallError::Overloaded));
                None
            })
            .await;
        assert!(
            dropped.is_none(),
            "the held call was dropped, not completed"
        );
        assert_eq!(
            f.client.metrics().active,
            0,
            "dropped call released its slot"
        );

        while f.effects.cleaned_up.get() == 0 {
            future::yield_now().await;
        }
        let after = f
            .client
            .call(&scope, ShardId(0), Msg::Add(0), options())
            .await;
        assert_eq!(after, Ok(1));
    });
}

/// A generation that is force-stopped while a handler runs cannot vouch for
/// that handler: a caller that did not cancel gets `OutcomeUnknown`.
#[test]
fn generation_force_stop_mid_handler_is_outcome_unknown() {
    with_endpoint(
        RpcLimits::default(),
        Shutdown::BrutalKill,
        |f, stop| async move {
            let (message, entered, _release) = hold(2, false);
            let (result, ()) = future::zip(
                f.client
                    .call(&CancelScope::new(), ShardId(0), message, options()),
                async {
                    entered.recv().await.expect("handler entered");
                    stop.cancel();
                },
            )
            .await;
            assert_eq!(result, Err(CallError::OutcomeUnknown));
            assert_eq!(f.effects.total.get(), 2);
            assert!(
                bapps_trio::obligation_stats().aborted >= 1,
                "the forced handler's reply was an explicit OutcomeUnknown decision"
            );
        },
    );
}
