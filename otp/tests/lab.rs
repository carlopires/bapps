//! Supervision trees under the deterministic lab: every scenario runs under
//! many seeds, and every run must end clean (root finished, no deadlock, no
//! leftover task, no pending or leaked obligation).

use std::{cell::Cell, io, rc::Rc, time::Duration};

use bapps_otp::{
    Application, CancelScope, ChildSpec, ExitReason, LocalMailbox, OtpError, Strategy,
    SupervisorSpec, TaskQueues,
};
use bapps_trio::{
    Obligation, sleep,
    testing::{Lab, LabConfig, LabReport},
    with_nursery,
};

const SEEDS: std::ops::Range<u64> = 0..200;

fn err(error: impl std::fmt::Debug) -> io::Error {
    io::Error::other(format!("{error:?}"))
}

fn assert_all_clean<T: std::fmt::Debug>(failures: Vec<LabReport<T>>) {
    if let Some(first) = failures.first() {
        panic!(
            "{} of {} seeds failed; first: seed {} {first:?}",
            failures.len(),
            SEEDS.end - SEEDS.start,
            first.seed
        );
    }
}

/// OneForAll: a worker fails twice; each failure restarts both children; the
/// supervisor then runs until a virtual-time shutdown.
async fn one_for_all_restarts() -> (u64, u64) {
    let starts = Rc::new((Cell::new(0_u64), Cell::new(0_u64)));
    let (flaky_starts, steady_starts) = (starts.clone(), starts.clone());
    let flaky = ChildSpec::worker("flaky", move |ctx, started| {
        let starts = flaky_starts.clone();
        async move {
            starts.0.set(starts.0.get() + 1);
            started.started(()).map_err(err)?;
            if ctx.generation().id() <= 2 {
                let _ = sleep(Duration::from_millis(10)).await;
                return Err(io::Error::other("boom"));
            }
            ctx.scope().cancelled().await;
            Ok(())
        }
    });
    let steady = ChildSpec::worker("steady", move |ctx, started| {
        let starts = steady_starts.clone();
        async move {
            starts.1.set(starts.1.get() + 1);
            // Holds a promise for its whole life and settles it on the way out.
            let lease = Obligation::new("steady lease");
            started.started(()).map_err(err)?;
            ctx.scope().cancelled().await;
            lease.abort();
            Ok::<(), io::Error>(())
        }
    });
    let shutdown = CancelScope::new();
    let stop = shutdown.clone();
    let application = Application::new(
        "lab",
        SupervisorSpec::new("root", Strategy::OneForAll)
            .restart_intensity(5, Duration::from_secs(1))
            .child(flaky)
            .child(steady),
    );
    let (result, ()) = futures_lite::future::zip(
        application.run(shutdown, TaskQueues::current()),
        async move {
            let _ = sleep(Duration::from_secs(1)).await;
            stop.cancel();
        },
    )
    .await;
    result.expect("application");
    (starts.0.get(), starts.1.get())
}

#[test]
fn one_for_all_restarts_are_clean_under_every_seed() {
    assert_all_clean(Lab::explore(SEEDS, one_for_all_restarts, |report| {
        report.is_clean() && report.output == Some((3, 3))
    }));
}

/// Restart intensity exhausted in virtual time escalates as an error, and
/// still leaves nothing behind.
#[test]
fn intensity_escalation_is_clean_under_every_seed() {
    let make = || async {
        let crasher = ChildSpec::worker("crasher", |_ctx, started| async move {
            started.started(()).map_err(err)?;
            let _ = sleep(Duration::from_millis(1)).await;
            Err::<(), _>(io::Error::other("always"))
        });
        Application::new(
            "lab",
            SupervisorSpec::new("root", Strategy::OneForOne)
                .restart_intensity(3, Duration::from_secs(10))
                .child(crasher),
        )
        .run(CancelScope::new(), TaskQueues::current())
        .await
    };
    assert_all_clean(Lab::explore(SEEDS, make, |report| {
        report.is_clean()
            && matches!(
                report.output,
                Some(Err(OtpError::RestartIntensityExceeded { .. }))
            )
    }));
}

/// Producers race to fill a small mailbox while their caller is cancelled
/// mid-way. With reserve-then-send, no produced item is ever lost: every item
/// is either delivered or was never built.
async fn producers_scenario() -> (u32, u32) {
    let (tx, rx) = LocalMailbox::<u32>::bounded(2);
    let caller = CancelScope::new();
    let built = Rc::new(Cell::new(0_u32));
    let received = Rc::new(Cell::new(0_u32));
    let (counted, seen) = (built.clone(), received.clone());
    with_nursery::<(), _, _>(|nursery| {
        Box::pin(async move {
            for p in 0..3_u32 {
                let (tx, caller, built) = (tx.clone(), caller.clone(), counted.clone());
                nursery
                    .spawn(move |_| async move {
                        for i in 0..4 {
                            let Ok(permit) = tx.reserve_in(&caller).await else {
                                return Ok(());
                            };
                            built.set(built.get() + 1); // the item exists only now
                            permit.send(p * 10 + i).expect("receiver alive");
                        }
                        Ok(())
                    })
                    .unwrap();
            }
            drop(tx);
            nursery
                .spawn(move |_| async move {
                    for _ in 0..5 {
                        rx.recv_in(&CancelScope::new()).await.expect("item");
                        seen.set(seen.get() + 1);
                    }
                    caller.cancel();
                    // Producers already holding a permit still deliver.
                    while let Ok(item) = rx.recv_in(&CancelScope::new()).await {
                        let _ = item;
                        seen.set(seen.get() + 1);
                    }
                    Ok(())
                })
                .unwrap();
        })
    })
    .await
    .unwrap();
    (built.get(), received.get())
}

#[test]
fn cancelled_producers_never_lose_a_built_item() {
    assert_all_clean(Lab::explore(SEEDS, producers_scenario, |report| {
        report.is_clean() && matches!(report.output, Some((built, received)) if built == received)
    }));
}

#[test]
fn a_shutdown_during_restarts_records_its_cause() {
    let report = Lab::run(LabConfig::new(11), async {
        let worker = ChildSpec::worker("w", |ctx, started| async move {
            started.started(()).map_err(err)?;
            ctx.scope().cancelled().await;
            Ok::<(), io::Error>(())
        });
        let application = Application::new(
            "lab",
            SupervisorSpec::new("root", Strategy::OneForOne).child(worker),
        );
        let tree = application.tree();
        let shutdown = CancelScope::new();
        let stop = shutdown.clone();
        let (result, ()) = futures_lite::future::zip(
            application.run(shutdown, TaskQueues::current()),
            async move {
                let _ = sleep(Duration::from_secs(2)).await;
                stop.cancel();
            },
        )
        .await;
        result.expect("application");
        tree.child("lab/root/w").expect("snapshot").last_exit
    });
    assert!(report.is_clean(), "{report:?}");
    assert_eq!(report.output, Some(Some(ExitReason::Shutdown)));
    assert_eq!(report.virtual_time, Duration::from_secs(2));
}

/// The sweep only means something if seeds actually change the interleaving.
/// Supervision itself is sequential (children start one at a time behind
/// readiness), so the tree scenario has few interleavings; concurrent producers
/// have many.
#[test]
fn seeds_explore_distinct_interleavings() {
    let distinct = |traces: Vec<Vec<u64>>| {
        traces
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
    };
    let producers = distinct(
        SEEDS
            .map(|s| Lab::run(LabConfig::new(s), producers_scenario()).trace)
            .collect(),
    );
    let tree = distinct(
        SEEDS
            .map(|s| Lab::run(LabConfig::new(s), one_for_all_restarts()).trace)
            .collect(),
    );
    eprintln!(
        "distinct interleavings over {} seeds: producers={producers} one_for_all={tree}",
        SEEDS.end
    );
    assert!(producers > 50, "producers: {producers}");
}
