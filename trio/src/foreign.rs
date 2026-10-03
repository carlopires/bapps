//! Cancellation adapters for futures that do not know about `bapps_trio`.
//!
//! Rust cancellation is still drop-based at this boundary. The helpers here
//! make that boundary explicit and, importantly, give cancellation priority if
//! the operation and cancellation become ready in the same poll.

use std::{future::Future, task::Poll};

use futures_lite::future::poll_fn;

use crate::{CancelScope, Cancelled, current_cancel_scope};

/// Race a foreign future against an explicit scope.
///
/// If `scope` is already cancelled, the future is not polled. If cancellation
/// and the future become ready together, cancellation wins and the foreign
/// future is dropped.
pub async fn cancel_on<F>(scope: &CancelScope, future: F) -> Result<F::Output, Cancelled>
where
    F: Future,
{
    scope.check_cancelled()?;

    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(scope.cancelled());

    poll_fn(|cx| {
        if let Poll::Ready(cancelled) = cancelled.as_mut().poll(cx) {
            return Poll::Ready(Err(cancelled));
        }
        if let Poll::Ready(value) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(value));
        }
        Poll::Pending
    })
    .await
}

/// Race a foreign future against the active task-local scope when one exists.
/// With no active scope, this simply awaits the future.
pub async fn cancel_on_current<F>(future: F) -> Result<F::Output, Cancelled>
where
    F: Future,
{
    match current_cancel_scope() {
        Some(scope) => cancel_on(&scope, future).await,
        None => Ok(future.await),
    }
}

/// Race a foreign future against several independent owners.
pub async fn cancel_on_any<I, F>(owners: I, future: F) -> Result<F::Output, Cancelled>
where
    I: IntoIterator<Item = CancelScope>,
    F: Future,
{
    let scope = CancelScope::any(owners);
    cancel_on(&scope, future).await
}
