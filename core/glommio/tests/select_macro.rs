//! `glommio::select!` against the real runtime.
#![cfg(feature = "macros")]

use glommio::{select, timer::sleep};
use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

#[glommio::test]
async fn the_ready_branch_wins_and_binds_its_output() {
    let value = select! {
        v = async { 7u32 } => v,
        _ = sleep(Duration::from_secs(30)) => 0,
    };
    assert_eq!(value, 7);
}

#[glommio::test]
async fn a_branch_body_can_await() {
    // The reason the expansion splits into a poll phase and a handler phase:
    // handlers run in the caller's async context.
    let value = select! {
        v = async { 3u32 } => {
            sleep(Duration::from_millis(1)).await;
            v * 2
        },
        _ = sleep(Duration::from_secs(30)) => 0,
    };
    assert_eq!(value, 6);
}

#[glommio::test]
async fn the_losing_future_is_dropped() {
    struct NotesItsDrop(Rc<RefCell<bool>>);
    impl Drop for NotesItsDrop {
        fn drop(&mut self) {
            *self.0.borrow_mut() = true;
        }
    }

    let dropped = Rc::new(RefCell::new(false));
    let flag = dropped.clone();

    // Constructed here, not inside the async block: a branch that never gets
    // polled never runs its body, so a guard created in there would prove
    // nothing about dropping the future.
    let notes = NotesItsDrop(flag);
    select! {
        v = async { 1u32 } => v,
        _ = async move {
            let _notes = notes;
            sleep(Duration::from_secs(30)).await;
        } => 0,
    };

    assert!(
        *dropped.borrow(),
        "the losing future should be dropped, cancelling its work"
    );
}

#[glommio::test]
async fn biased_polls_top_to_bottom() {
    // Both branches are ready, so only the order decides.
    for _ in 0..8 {
        let winner = select! {
            biased;
            _ = async {} => "first",
            _ = async {} => "second",
        };
        assert_eq!(winner, "first");
    }
}

#[glommio::test]
async fn the_default_order_rotates_so_neither_branch_starves() {
    let mut seen_first = false;
    let mut seen_second = false;

    for _ in 0..8 {
        match select! {
            _ = async {} => "first",
            _ = async {} => "second",
        } {
            "first" => seen_first = true,
            _ => seen_second = true,
        }
    }

    assert!(
        seen_first && seen_second,
        "with two always-ready branches the default order should reach both; \
         a fixed order starves the later one"
    );
}

#[glommio::test]
async fn five_branches_work() {
    // The largest arity in the downstream that asked for this.
    let value = select! {
        _ = sleep(Duration::from_secs(30)) => 1u32,
        _ = sleep(Duration::from_secs(30)) => 2,
        v = async { 3u32 } => v,
        _ = sleep(Duration::from_secs(30)) => 4,
        _ = sleep(Duration::from_secs(30)) => 5,
    };
    assert_eq!(value, 3);
}

#[glommio::test]
async fn patterns_bind_as_written() {
    let unit = select! {
        () = async {} => "unit",
        _ = sleep(Duration::from_secs(30)) => "timer",
    };
    assert_eq!(unit, "unit");

    let named = select! {
        maybe = async { Some(4u8) } => maybe,
        _ = sleep(Duration::from_secs(30)) => None,
    };
    assert_eq!(named, Some(4));
}

#[glommio::test]
async fn a_pending_branch_does_not_win() {
    let started = Instant::now();
    let value = select! {
        _ = sleep(Duration::from_secs(30)) => "slow",
        _ = sleep(Duration::from_millis(5)) => "quick",
    };
    assert_eq!(value, "quick");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[glommio::test]
async fn a_handler_can_use_what_its_own_branch_borrowed() {
    // tokio drops the branch futures before running the handler, so the
    // handler may touch whatever they borrowed. Holding them across the
    // handler makes this a borrow error -- correct code that will not
    // compile.
    // A branch future that borrows mutably *and* has a destructor: without a
    // `Drop` impl the compiler ends the borrow at its last use, so the hazard
    // only appears for futures that own cleanup -- which real ones do.
    struct BorrowsUntilDropped<'a>(&'a mut u32);

    impl Drop for BorrowsUntilDropped<'_> {
        fn drop(&mut self) {
            *self.0 += 1;
        }
    }

    impl std::future::Future for BorrowsUntilDropped<'_> {
        type Output = u32;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<u32> {
            std::task::Poll::Ready(*self.0)
        }
    }

    let mut counter = 1u32;

    let total = select! {
        first = BorrowsUntilDropped(&mut counter) => {
            // `counter` was borrowed by the branch future. tokio drops branch
            // futures before running the handler, so this is legal there.
            counter += first;
            counter
        },
        _ = sleep(Duration::from_secs(30)) => 0,
    };

    // 1 polled, +1 from the destructor, then + the polled value.
    assert_eq!(total, 3);
}

#[glommio::test]
async fn handlers_keep_the_enclosing_control_flow() {
    // `break`, `continue` and `return` must mean what they mean at the call
    // site: the handler runs there, not inside a closure.
    let mut ticks = 0;

    let total = loop {
        ticks += 1;

        select! {
            _ = async {} => {
                if ticks < 3 {
                    continue;
                }
                break ticks;
            },
            _ = sleep(Duration::from_secs(30)) => break 0,
        }
    };

    assert_eq!(total, 3);
}

#[glommio::test]
async fn a_handler_can_take_self_mutably_after_a_branch_borrowed_it() {
    // The second downstream shape: the branch borrows `self`, the handler
    // calls a `&mut self` method. The borrow is of `self` rather than of a
    // local, which is where a scope-based fix might still trip.
    struct Source {
        pending: Vec<u32>,
        drained: u32,
    }

    impl Source {
        async fn next(&mut self) -> Option<u32> {
            self.pending.pop()
        }

        fn drain(&mut self) -> u32 {
            self.drained += 1;
            self.drained
        }

        async fn run(&mut self) -> u32 {
            select! {
                item = self.next() => {
                    let seen = item.unwrap_or(0);
                    seen + self.drain()
                },
                _ = sleep(Duration::from_secs(30)) => 0,
            }
        }
    }

    let mut source = Source {
        pending: vec![41],
        drained: 0,
    };

    assert_eq!(source.run().await, 42);
}

/// A syntax corpus rather than a semantic one.
///
/// The comma-after-block defect was invisible to every behavioural test:
/// each of those happened to write the comma. Only a spread of *forms* --
/// block body with and without the comma, bare expression, unit pattern,
/// mixed, trailing comma present and absent -- reaches it, and real call
/// sites produce that spread naturally, which is why 51 of them found it in
/// one build.
mod syntax_corpus {
    use super::*;

    #[glommio::test]
    async fn block_body_without_trailing_comma() {
        let value = select! {
            v = async { 1u32 } => {
                v
            }
            _ = sleep(Duration::from_secs(30)) => 0,
        };
        assert_eq!(value, 1);
    }

    #[glommio::test]
    async fn block_body_with_trailing_comma() {
        let value = select! {
            v = async { 1u32 } => {
                v
            },
            _ = sleep(Duration::from_secs(30)) => 0,
        };
        assert_eq!(value, 1);
    }

    #[glommio::test]
    async fn bare_expression_bodies() {
        let value = select! {
            v = async { 2u32 } => v,
            _ = sleep(Duration::from_secs(30)) => 0
        };
        assert_eq!(value, 2);
    }

    #[glommio::test]
    async fn mixed_block_and_expression_bodies() {
        let value = select! {
            _ = sleep(Duration::from_secs(30)) => 0,
            v = async { 3u32 } => {
                v + 1
            }
            _ = sleep(Duration::from_secs(30)) => 0,
        };
        assert_eq!(value, 4);
    }

    #[glommio::test]
    async fn if_and_match_bodies_also_stand_alone() {
        // Every body that ends in a block, not just a literal `{ .. }`.
        let value = select! {
            v = async { 5u32 } => if v > 4 {
                v
            } else {
                0
            }
            _ = sleep(Duration::from_secs(30)) => 0,
        };
        assert_eq!(value, 5);

        let value = select! {
            v = async { 6u32 } => match v {
                6 => v,
                other => other,
            }
            _ = sleep(Duration::from_secs(30)) => 0,
        };
        assert_eq!(value, 6);
    }

    #[glommio::test]
    async fn unit_pattern_alone_and_alongside() {
        let value = select! {
            () = async {} => 7u32,
        };
        assert_eq!(value, 7);

        // `biased;` because both branches are ready and the default order
        // rotates -- the caveat this macro documents, tripped over here while
        // writing a test about syntax.
        let value = select! {
            biased;
            () = async {} => {
                8u32
            }
            () = async {} => 0,
        };
        assert_eq!(value, 8);
    }

    #[glommio::test]
    async fn biased_with_a_block_body() {
        let value = select! {
            biased;
            v = async { 9u32 } => {
                v
            }
            _ = async {} => 0,
        };
        assert_eq!(value, 9);
    }
}
