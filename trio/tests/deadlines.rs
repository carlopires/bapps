//! Deadlines are part of a cancel scope: code can ask how much time it has
//! left, and the answer accounts for every enclosing deadline.

use std::time::Duration;

use bapps_trio::{
    CancelScope, FailAfterError, current_clock, current_effective_deadline, fail_after,
    fail_after_shielded, fail_at, move_on_after, move_on_at, remaining, sleep,
    testing::{Lab, LabConfig},
};

const MS: fn(u64) -> Duration = Duration::from_millis;

fn lab<T: 'static>(body: impl Future<Output = T> + 'static) -> T {
    Lab::run(LabConfig::new(0), body)
        .output
        .expect("lab root finished")
}

#[test]
fn there_is_no_deadline_outside_a_deadline_scope() {
    let (deadline, left) = lab(async { (current_effective_deadline(), remaining()) });
    assert_eq!((deadline, left), (None, None));
}

#[test]
fn remaining_time_counts_down_on_the_clock() {
    let (start, deadline, at_start, after_30) = lab(async {
        let start = current_clock().now();
        fail_after(MS(100), |_| async move {
            let deadline = current_effective_deadline();
            let at_start = remaining();
            sleep(MS(30)).await.unwrap();
            (start, deadline, at_start, remaining())
        })
        .await
        .unwrap()
    });
    assert_eq!(deadline, Some(start + MS(100)));
    assert_eq!(at_start, Some(MS(100)));
    assert_eq!(after_30, Some(MS(70)));
}

#[test]
fn the_tighter_of_nested_deadlines_wins_either_way() {
    let (start, outer_tighter, inner_tighter) = lab(async {
        let start = current_clock().now();
        let outer_tighter = fail_after(MS(100), |_| async {
            fail_after(MS(500), |_| async { current_effective_deadline() })
                .await
                .unwrap()
        })
        .await
        .unwrap();
        let start2 = current_clock().now();
        let inner_tighter = fail_after(MS(500), |_| async {
            fail_after(MS(100), |_| async { current_effective_deadline() })
                .await
                .unwrap()
        })
        .await
        .unwrap()
        .map(|deadline| deadline - start2);
        (start, outer_tighter, inner_tighter)
    });
    assert_eq!(outer_tighter, Some(start + MS(100)));
    assert_eq!(inner_tighter, Some(MS(100)));
}

#[test]
fn a_shield_hides_outer_deadlines() {
    let (start, shielded) = lab(async {
        let start = current_clock().now();
        let shielded = fail_after(MS(100), |_| async {
            fail_after_shielded(MS(500), |_| async { current_effective_deadline() })
                .await
                .unwrap()
        })
        .await
        .unwrap();
        (start, shielded)
    });
    assert_eq!(shielded, Some(start + MS(500)));
}

#[test]
fn a_scope_with_several_owners_takes_the_earliest_deadline() {
    let (start, combined, own) = lab(async {
        let start = current_clock().now();
        fail_after(MS(100), |near| async move {
            fail_after_shielded(MS(300), |far| async move {
                let combined = CancelScope::any([far.clone(), near.clone()]);
                (start, combined.effective_deadline(), far.deadline())
            })
            .await
            .unwrap()
        })
        .await
        .unwrap()
    });
    assert_eq!(combined, Some(start + MS(100)));
    assert_eq!(
        own,
        Some(start + MS(300)),
        "a scope's own deadline is its own"
    );
}

#[test]
fn absolute_deadlines_are_recorded_and_enforced() {
    let (start, seen, timed_out, moved_on) = lab(async {
        let start = current_clock().now();
        let at = start + MS(50);
        let mut seen = None;
        let result = fail_at(at, |_| async {
            seen = current_effective_deadline();
            sleep(MS(80)).await
        })
        .await;
        let moved_on = move_on_at(current_clock().now() + MS(10), |_| async {
            sleep(MS(80)).await
        })
        .await;
        (start, seen, result, moved_on.timed_out)
    });
    assert_eq!(seen, Some(start + MS(50)));
    assert!(
        matches!(timed_out, Err(FailAfterError::TooSlow)),
        "{timed_out:?}"
    );
    assert!(moved_on);
}

#[test]
fn a_relative_deadline_still_times_out_as_before() {
    let outcome = lab(async { move_on_after(MS(10), |_| async { sleep(MS(80)).await }).await });
    assert!(outcome.timed_out && outcome.value.is_none());
}
