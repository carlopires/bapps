//! Wait registrations must not outlive the wait that created them.
//!
//! A long-lived owner (a service generation scope, a mailbox condition, a
//! nursery event) sees many short-lived waits. Each wait registers a waker; if
//! a dropped wait leaves that waker behind, the owner grows by one waker per
//! request forever, and in Glommio a retained waker keeps its task allocation
//! alive. These tests are black-box: a probe waker counts its own clones.

use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Wake, Waker},
    time::Duration,
};

use bapps_trio::{
    CancelScope,
    sync::{Condition, Event},
    testing::{Sequencer, TestClock},
    time::Clock,
    with_cancel_scope,
};

const WAITS: usize = 1_000;

struct Probe;
impl Wake for Probe {
    fn wake(self: Arc<Self>) {}
}

/// Poll `future` exactly once with a fresh probe waker, then drop the future.
/// Returns the probe so the caller can check whether anything kept a clone.
fn poll_once_and_drop<F: Future>(future: F) -> Arc<Probe> {
    let probe = Arc::new(Probe);
    let waker = Waker::from(probe.clone());
    let mut future = pin!(future);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "the wait under test must be pending"
    );
    probe
}

fn retained(probes: &[Arc<Probe>]) -> usize {
    probes.iter().filter(|p| Arc::strong_count(p) > 1).count()
}

#[test]
fn dropped_scope_waits_release_every_ancestor_registration() {
    let service = CancelScope::new();
    let other_owner = CancelScope::new();
    let probes: Vec<_> = (0..WAITS)
        .map(|_| {
            // A request scope owned by both a caller and the service, as in
            // `CancelScope::any([caller, generation])`.
            let request = CancelScope::any([service.child(), other_owner.clone()]);
            poll_once_and_drop(request.cancelled())
        })
        .collect();
    assert_eq!(retained(&probes), 0);
    assert!(!service.is_cancelled());
}

#[test]
fn repolling_with_a_new_waker_replaces_the_registration() {
    let scope = CancelScope::new();
    let first = Arc::new(Probe);
    let second = Arc::new(Probe);
    let mut wait = pin!(scope.cancelled());
    let _ = wait
        .as_mut()
        .poll(&mut Context::from_waker(&Waker::from(first.clone())));
    let _ = wait
        .as_mut()
        .poll(&mut Context::from_waker(&Waker::from(second.clone())));
    assert_eq!(Arc::strong_count(&first), 1, "stale waker must be replaced");
    assert_eq!(Arc::strong_count(&second), 2, "current waker is registered");
}

#[test]
fn cancellation_still_wakes_a_live_registration() {
    struct Flag(std::sync::atomic::AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let parent = CancelScope::new();
    let child = CancelScope::any([parent.child()]);
    let flag = Arc::new(Flag(false.into()));
    let waker = Waker::from(flag.clone());
    let mut wait = pin!(child.cancelled());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    parent.cancel();
    assert!(flag.0.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
}

#[test]
fn dropped_event_and_condition_waits_are_released() {
    let event = Event::new();
    let condition = Condition::new();
    let owner = CancelScope::new();
    let mut probes = Vec::new();
    for _ in 0..WAITS {
        // Cancellation-aware waits capture the task-local scope; the scope is
        // part of what must be released.
        probes.push(poll_once_and_drop(with_cancel_scope(
            owner.child(),
            event.wait(),
        )));
        let observed = condition.generation();
        probes.push(poll_once_and_drop(with_cancel_scope(
            owner.child(),
            condition.wait_for_change(observed),
        )));
    }
    assert_eq!(retained(&probes), 0);
}

#[test]
fn dropped_sequencer_and_test_clock_waits_are_released() {
    let sequencer = Sequencer::new();
    let clock = TestClock::new();
    let mut probes = Vec::new();
    for _ in 0..WAITS {
        probes.push(poll_once_and_drop(sequencer.wait_for(1)));
        probes.push(poll_once_and_drop(
            clock.sleep_until(Duration::from_secs(1)),
        ));
    }
    assert_eq!(retained(&probes), 0);
}
