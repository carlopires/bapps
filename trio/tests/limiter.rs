//! CapacityLimiter: a bounded number of tokens, granted first come first
//! served; giving up while waiting never blocks the waiters behind.

use std::{cell::RefCell, rc::Rc, time::Duration};

use bapps_trio::{
    CancelScope, Nursery,
    sync::CapacityLimiter,
    testing::{Lab, LabConfig},
    with_cancel_scope, with_nursery,
};

const MS: fn(u64) -> Duration = Duration::from_millis;

/// One token. `holder` keeps it 10 ms; waiters `a`, `b`, `c` queue in that
/// order, `b` gives up (cancelled) while queued. Returns the grant order.
async fn queue_with_a_dropout() -> Vec<&'static str> {
    let limiter = CapacityLimiter::new(1);
    let order = Rc::new(RefCell::new(Vec::new()));
    let log = order.clone();
    with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
        Box::pin(async move {
            let held = limiter.acquire().await.expect("free");
            for (queued, name) in ["a", "b", "c"].into_iter().enumerate() {
                let (waiter, log) = (limiter.clone(), log.clone());
                let scope = CancelScope::new();
                if name == "b" {
                    let scope = scope.clone();
                    nursery
                        .spawn(move |_| async move {
                            let _ = bapps_trio::sleep(MS(5)).await;
                            scope.cancel();
                            Ok(())
                        })
                        .unwrap();
                }
                nursery
                    .spawn(move |_| async move {
                        let token = with_cancel_scope(scope, waiter.acquire()).await;
                        if let Ok(token) = token {
                            log.borrow_mut().push(name);
                            let _ = bapps_trio::sleep(MS(1)).await;
                            drop(token);
                        }
                        Ok(())
                    })
                    .unwrap();
                // Queue in order: each waiter is queued before the next spawns.
                while limiter.waiting() < queued + 1 {
                    futures_lite::future::yield_now().await;
                }
            }
            let _ = bapps_trio::sleep(MS(10)).await;
            drop(held);
        })
    })
    .await
    .unwrap();
    order.take()
}

#[test]
fn waiters_are_served_in_order_and_a_dropout_does_not_block_the_queue() {
    for seed in 0..20 {
        let report = Lab::run(LabConfig::new(seed), queue_with_a_dropout());
        assert!(
            report.is_clean(),
            "seed {seed}: not clean (deadlock or leftovers)"
        );
        assert_eq!(report.output, Some(vec!["a", "c"]), "seed {seed}");
    }
}

#[test]
fn tokens_are_counted_and_capacity_can_change() {
    let out = Lab::run(LabConfig::new(0), async {
        let limiter = CapacityLimiter::new(2);
        let one = limiter.try_acquire().expect("first");
        let two = limiter.try_acquire().expect("second");
        let full = limiter.try_acquire().is_none();
        let counts = (limiter.borrowed_tokens(), limiter.available_tokens());
        limiter.set_total_tokens(3);
        let three = limiter.try_acquire().expect("after growing");
        limiter.set_total_tokens(1);
        // Shrinking does not take tokens back; it stops new grants.
        let over = (limiter.borrowed_tokens(), limiter.available_tokens());
        drop((one, two, three));
        let after = (limiter.borrowed_tokens(), limiter.available_tokens());
        (full, counts, over, after)
    })
    .output
    .unwrap();
    assert_eq!(out, (true, (2, 0), (3, 0), (0, 1)));
}

#[test]
fn try_acquire_does_not_jump_the_queue() {
    let jumped = Lab::run(LabConfig::new(0), async {
        let limiter = CapacityLimiter::new(1);
        let held = limiter.try_acquire().unwrap();
        let waiter = limiter.clone();
        with_nursery::<(), _, _>(|nursery: &mut Nursery<()>| {
            Box::pin(async move {
                nursery
                    .spawn(move |_| async move {
                        let _token = waiter.acquire().await.unwrap();
                        Ok(())
                    })
                    .unwrap();
                futures_lite::future::yield_now().await;
                drop(held);
                // The queued waiter is owed the token, so this must fail.
                limiter.try_acquire().is_some()
            })
        })
        .await
        .unwrap()
    })
    .output
    .unwrap();
    assert!(!jumped);
}
