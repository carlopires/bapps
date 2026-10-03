//! Reserve-then-send mailbox permits.

use bapps_otp::{CancelScope, LocalMailbox, MailboxError, TrySendError};
use glommio::LocalExecutor;

#[test]
fn a_permit_holds_capacity_until_sent_or_dropped() {
    LocalExecutor::default().run(async {
        let (tx, rx) = LocalMailbox::<u32>::bounded(2);
        let scope = CancelScope::new();
        let a = tx.reserve_in(&scope).await.unwrap();
        let b = tx.reserve_in(&scope).await.unwrap();
        assert_eq!(tx.snapshot().reserved, 2);
        assert!(
            matches!(tx.try_send(9), Err(TrySendError::Full(9))),
            "reserved slots are taken"
        );

        a.send(1).unwrap();
        drop(b); // releases its slot without sending
        assert_eq!(tx.snapshot().reserved, 0);
        tx.try_send(2).unwrap();
        assert_eq!(rx.try_recv().unwrap(), Some(1));
        assert_eq!(rx.try_recv().unwrap(), Some(2));
    });
}

/// The point of permits: a caller cancelled while waiting for capacity has
/// not built its message yet, so nothing (a reply channel, a claim) is lost.
#[test]
fn cancellation_while_waiting_for_capacity_loses_nothing() {
    LocalExecutor::default().run(async {
        let (tx, rx) = LocalMailbox::<u32>::bounded(1);
        tx.try_send(0).unwrap();
        let scope = CancelScope::new();
        let waiting = tx.reserve_in(&scope);
        let (result, ()) = futures_lite::future::zip(waiting, async { scope.cancel() }).await;
        assert!(matches!(result, Err(MailboxError::Cancelled(_))));
        assert_eq!(tx.snapshot().reserved, 0);
        assert_eq!(rx.try_recv().unwrap(), Some(0));
    });
}

#[test]
fn a_permit_returns_the_value_when_the_receiver_is_gone() {
    LocalExecutor::default().run(async {
        let (tx, rx) = LocalMailbox::<String>::bounded(1);
        let permit = tx.reserve_in(&CancelScope::new()).await.unwrap();
        drop(rx);
        assert_eq!(permit.send("reply".into()), Err("reply".to_string()));
    });
}

#[test]
fn a_waiting_reservation_wakes_when_a_permit_is_dropped() {
    LocalExecutor::default().run(async {
        let (tx, _rx) = LocalMailbox::<u32>::bounded(1);
        let scope = CancelScope::new();
        let held = tx.reserve_in(&scope).await.unwrap();
        let (second, ()) =
            futures_lite::future::zip(tx.reserve_in(&scope), async { drop(held) }).await;
        assert!(second.is_ok());
    });
}
