//! Obligations: values that must be explicitly committed or aborted.

use bapps_trio::{Obligation, obligation_stats};

#[test]
fn resolved_obligations_are_not_leaks() {
    let before = obligation_stats();
    let committed = Obligation::new("reply");
    let aborted = Obligation::new("reply");
    assert_eq!(obligation_stats().pending, before.pending + 2);
    committed.commit();
    aborted.abort();
    let after = obligation_stats();
    assert_eq!(after.pending, before.pending);
    assert_eq!(after.committed, before.committed + 1);
    assert_eq!(after.aborted, before.aborted + 1);
    assert_eq!(after.leaked, before.leaked);
}

#[test]
fn a_dropped_unresolved_obligation_is_recorded_with_its_label() {
    let before = obligation_stats();
    drop(Obligation::new("storage put reply"));
    let after = obligation_stats();
    assert_eq!(after.pending, before.pending);
    assert_eq!(after.leaked, before.leaked + 1);
    assert_eq!(
        after.recent_leaks.back().copied(),
        Some("storage put reply")
    );
}

#[test]
fn leak_history_is_bounded() {
    for _ in 0..1_000 {
        drop(Obligation::new("noise"));
    }
    assert!(obligation_stats().recent_leaks.len() <= 16);
}
