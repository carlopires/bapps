//! Cancellation attribution: the initiating cause survives propagation and
//! later cancellations, and multi-owner scopes report the earliest owner.

use bapps_trio::{CancelReason, CancelScope};

#[test]
fn origin_is_recorded_and_not_overwritten() {
    let scope = CancelScope::new();
    scope.cancel_by(CancelReason::Deadline, "request 42 deadline");
    scope.cancel_by(CancelReason::NurseryClosing, "supervisor shutdown");

    let cause = scope.cause().expect("cancelled");
    assert_eq!(cause.reason, CancelReason::Deadline);
    assert_eq!(cause.origin.as_deref(), Some("request 42 deadline"));
    assert!(!cause.inherited);
    assert_eq!(cause.to_string(), "deadline expired by request 42 deadline");
}

#[test]
fn children_report_the_inherited_initiating_cause() {
    let parent = CancelScope::new();
    let child = parent.child();
    parent.cancel_by(CancelReason::NurseryFailure, "task storage/compact failed");
    // A later, direct cancellation of the child records its own cause.
    let cause = child.cause().expect("inherited");
    assert!(cause.inherited);
    assert_eq!(cause.origin.as_deref(), Some("task storage/compact failed"));

    child.cancel_by(CancelReason::Explicit, "late");
    assert_eq!(
        child.cause().unwrap().origin.as_deref(),
        Some("late"),
        "a scope's own cancellation is its cause"
    );
}

#[test]
fn multi_owner_scope_reports_the_earliest_owner_not_list_order() {
    let caller = CancelScope::new();
    let service = CancelScope::new();
    let operation = CancelScope::any([caller.clone(), service.clone()]);

    service.cancel_by(CancelReason::NurseryClosing, "service generation stop");
    caller.cancel_by(CancelReason::Explicit, "client disconnected");

    let cause = operation.cause().expect("cancelled");
    assert_eq!(cause.origin.as_deref(), Some("service generation stop"));
    assert_eq!(operation.reason(), Some(CancelReason::NurseryClosing));
}

#[test]
fn uncancelled_scope_has_no_cause() {
    assert_eq!(CancelScope::new().child().cause(), None);
}
