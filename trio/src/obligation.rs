//! Obligations: work that must end in an explicit decision.
//!
//! An [`Obligation`] stands for something a component promised to finish: a
//! reply it owes, a reservation it holds, a claim it must publish or release.
//! It is resolved by [`Obligation::commit`] (the effect happened) or
//! [`Obligation::abort`] (it deliberately did not). Dropping it unresolved is a
//! *leak*: a code path forgot its promise. Leaks are counted, with the label of
//! the forgotten obligation, in a per-executor ledger that tests and admin
//! surfaces can read through [`obligation_stats`].
//!
//! A type that has a defined fallback (for example "reply `OutcomeUnknown` if
//! the handler is destroyed") resolves its obligation in its own `Drop`, so the
//! fallback is an explicit decision and not a leak.
//!
//! Leaks never panic: dropping during a forced stop or a panic must stay safe.
//! The ledger is executor-local, like everything else in this crate.

use std::{cell::RefCell, collections::VecDeque};

const RECENT_LEAKS: usize = 16;

/// Executor-local obligation counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ObligationStats {
    /// Created and not yet resolved or dropped.
    pub pending: u64,
    pub committed: u64,
    pub aborted: u64,
    /// Dropped without a decision.
    pub leaked: u64,
    /// Labels of the most recent leaks, oldest first (bounded).
    pub recent_leaks: VecDeque<&'static str>,
}

thread_local! {
    static LEDGER: RefCell<ObligationStats> = RefCell::new(ObligationStats::default());
}

fn record(update: impl FnOnce(&mut ObligationStats)) {
    // `try_with`: obligations may be dropped during thread teardown.
    let _ = LEDGER.try_with(|ledger| update(&mut ledger.borrow_mut()));
}

/// A snapshot of this executor's obligation ledger.
pub fn obligation_stats() -> ObligationStats {
    LEDGER.with(|ledger| ledger.borrow().clone())
}

/// Something that must be explicitly committed or aborted.
#[must_use = "an obligation must be committed or aborted"]
#[derive(Debug)]
pub struct Obligation {
    label: &'static str,
    resolved: bool,
}

impl Obligation {
    pub fn new(label: &'static str) -> Self {
        record(|ledger| ledger.pending += 1);
        Self {
            label,
            resolved: false,
        }
    }

    pub fn label(&self) -> &'static str {
        self.label
    }

    /// The promised effect happened.
    pub fn commit(mut self) {
        self.resolved = true;
        record(|ledger| {
            ledger.pending -= 1;
            ledger.committed += 1;
        });
    }

    /// The promised effect deliberately did not happen.
    pub fn abort(mut self) {
        self.resolved = true;
        record(|ledger| {
            ledger.pending -= 1;
            ledger.aborted += 1;
        });
    }
}

impl Drop for Obligation {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let label = self.label;
        record(|ledger| {
            ledger.pending -= 1;
            ledger.leaked += 1;
            if ledger.recent_leaks.len() == RECENT_LEAKS {
                ledger.recent_leaks.pop_front();
            }
            ledger.recent_leaks.push_back(label);
        });
    }
}
