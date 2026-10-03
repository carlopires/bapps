//! The deterministic lab executor: seeded interleavings, virtual time,
//! deadlock and obligation oracles, replay.

use std::{cell::Cell, rc::Rc, time::Duration};

use bapps_trio::{
    FailAfterError, Nursery, Obligation, StopOutcome, fail_after,
    sync::Event,
    testing::{Lab, LabConfig},
    with_nursery,
};
use futures_lite::future::yield_now;

/// Two workers each do a read-modify-write of a shared counter with an await
/// between the read and the write: a classic lost update, which only some
/// interleavings expose.
async fn racy_counter() -> u64 {
    let counter = Rc::new(Cell::new(0_u64));
    let shared = counter.clone();
    with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
        Box::pin(async move {
            for _ in 0..2 {
                let counter = shared.clone();
                nursery
                    .spawn(move |_| async move {
                        for _ in 0..3 {
                            let read = counter.get();
                            yield_now().await;
                            counter.set(read + 1);
                        }
                        Ok(())
                    })
                    .unwrap();
            }
        })
    })
    .await
    .unwrap();
    counter.get()
}

#[test]
fn the_same_seed_replays_the_same_interleaving() {
    let a = Lab::run(LabConfig::new(7), racy_counter());
    let b = Lab::run(LabConfig::new(7), racy_counter());
    assert!(a.is_clean(), "{a:?}");
    assert_eq!(a.trace, b.trace);
    assert_eq!(a.output, b.output);
}

#[test]
fn exploring_seeds_finds_the_lost_update_and_its_seed_replays() {
    let failures = Lab::explore(0..64, racy_counter, |report| report.output == Some(6));
    assert!(!failures.is_empty(), "some interleaving loses an update");
    assert!(failures.len() < 64, "and some do not");

    let failing = &failures[0];
    let replay = Lab::run(LabConfig::new(failing.seed), racy_counter());
    assert_eq!(replay.output, failing.output, "a failing seed fails again");
    assert_eq!(replay.trace, failing.trace, "with the same interleaving");
}

#[test]
fn deadlines_run_on_virtual_time() {
    let report = Lab::run(LabConfig::new(1), async {
        fail_after(Duration::from_secs(30), |scope| async move {
            scope.cancelled().await;
        })
        .await
    });
    assert_eq!(report.output, Some(Err(FailAfterError::TooSlow)));
    assert_eq!(report.virtual_time, Duration::from_secs(30));
    assert!(report.is_clean(), "{report:?}");
}

/// `fail_after` is cooperative: a body that ignores cancellation keeps the
/// deadline waiting. The lab reports that as a deadlock instead of hanging.
#[test]
fn a_body_that_ignores_its_deadline_is_a_reported_deadlock() {
    let report = Lab::run(LabConfig::new(1), async {
        fail_after(Duration::from_secs(30), |_| std::future::pending::<()>()).await
    });
    assert!(report.deadlocked);
    assert_eq!(report.virtual_time, Duration::from_secs(30));
}

#[test]
fn forced_stop_after_grace_on_virtual_time() {
    let report = Lab::run(LabConfig::new(3), async {
        with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
            Box::pin(async move {
                let stubborn = nursery
                    .spawn_owned(|_| async {
                        std::future::pending::<()>().await;
                        Ok(())
                    })
                    .unwrap();
                yield_now().await;
                stubborn.cancel_and_wait(Duration::from_secs(5)).await
            })
        })
        .await
        .unwrap()
    });
    assert_eq!(report.output, Some(StopOutcome::Forced));
    assert_eq!(report.virtual_time, Duration::from_secs(5));
    assert!(report.is_clean(), "{report:?}");
}

/// A task waits for an event nobody sets while holding an obligation: the
/// run deadlocks, and the report names the pending obligation.
#[test]
fn a_deadlock_holding_an_obligation_is_reported() {
    let report = Lab::run(LabConfig::new(0), async {
        let never = Event::new();
        let _promise = Obligation::new("reply owed by a stuck task");
        let _ = never.wait().await;
    });
    assert!(report.deadlocked);
    assert!(report.output.is_none());
    assert_eq!(report.obligations_pending, 1);
    assert!(!report.is_clean());
}

#[test]
fn a_forgotten_obligation_fails_the_run() {
    let report = Lab::run(LabConfig::new(0), async {
        drop(Obligation::new("forgotten"));
    });
    assert!(report.output.is_some());
    assert_eq!(report.obligations_leaked, 1);
    assert!(!report.is_clean());
}
