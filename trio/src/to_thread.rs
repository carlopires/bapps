//! Blocking work on the executor's blocking thread pool.
//!
//! [`run_sync`] runs a blocking closure (a SQLite call, hashing, compression)
//! on a helper thread of the current executor and awaits its result without
//! blocking the executor. How many such jobs run at once is bounded by a
//! [`CapacityLimiter`]; jobs beyond it wait in that limiter's queue, where
//! waiting is cancellable.
//!
//! Cancellation is cooperative, like everywhere in this crate:
//!
//! - cancelled while waiting for a token: the job never starts, and the call
//!   returns `Err(Cancelled)`;
//! - cancelled while running: the job is told through its [`ThreadCancel`]
//!   and should return early; the caller still waits for it and gets its
//!   result, so no side effect is silently lost. The cancellation is observed
//!   at the caller's next checkpoint.
//!
//! There is deliberately no way to abandon a running job: an abandoned thread
//! keeps running with no owner, which this crate's ownership rules exclude.
//!
//! The executor's blocking pool has one thread unless it was built with
//! `LocalExecutorBuilder::blocking_thread_pool_placement`, and Glommio uses
//! the same pool for some file-system calls (rename, remove, directory
//! creation). The default limiter therefore has one token: size it to the
//! pool with [`CapacityLimiter::set_total_tokens`], or pass your own limiter
//! to [`run_sync_with`]. Keeping the limiter no larger than the pool keeps
//! waiting jobs in the limiter, where they can still be cancelled, instead of
//! in the pool's queue, where they cannot.

use std::{
    future::poll_fn,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Poll,
};

use crate::{
    cancel::{Cancelled, current_cancel_scope},
    sync::CapacityLimiter,
};

thread_local! {
    static DEFAULT_LIMITER: CapacityLimiter = CapacityLimiter::new(1);
}

/// This executor's limiter for [`run_sync`]: one token unless resized.
pub fn default_thread_limiter() -> CapacityLimiter {
    DEFAULT_LIMITER.with(Clone::clone)
}

/// Tells a running job that its caller was cancelled. `Send`: it is read on
/// the helper thread.
#[derive(Clone, Debug, Default)]
pub struct ThreadCancel {
    flag: Arc<AtomicBool>,
}

impl ThreadCancel {
    /// Whether the caller was cancelled; poll it and return early.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// `Err` once the caller was cancelled, for `?` in the job.
    pub fn check(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            Err(Cancelled { reason: None })
        } else {
            Ok(())
        }
    }

    fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
    }
}

/// Run `job` on a helper thread, bounded by [`default_thread_limiter`].
pub async fn run_sync<F, R>(job: F) -> Result<R, Cancelled>
where
    F: FnOnce(&ThreadCancel) -> R + Send + 'static,
    R: Send + 'static,
{
    run_sync_with(&default_thread_limiter(), job).await
}

/// Run `job` on a helper thread, bounded by `limiter`. See the module docs
/// for what cancellation does at each stage.
pub async fn run_sync_with<F, R>(limiter: &CapacityLimiter, job: F) -> Result<R, Cancelled>
where
    F: FnOnce(&ThreadCancel) -> R + Send + 'static,
    R: Send + 'static,
{
    let token = limiter.acquire().await?;
    let scope = current_cancel_scope();
    // Cancelled just as the token was granted: still nothing has started.
    if let Some(scope) = &scope {
        scope.check_cancelled()?;
    }
    let cancel = ThreadCancel::default();
    let told = cancel.clone();
    let mut running = Box::pin(glommio::executor().spawn_blocking(move || job(&told)));
    let result = match scope {
        None => running.await,
        Some(scope) => {
            let mut cancelled = Some(Box::pin(scope.cancelled()));
            poll_fn(|cx| {
                if let Poll::Ready(result) = running.as_mut().poll(cx) {
                    return Poll::Ready(result);
                }
                if let Some(waiting) = &mut cancelled
                    && waiting.as_mut().poll(cx).is_ready()
                {
                    // Tell the job once, then keep waiting for it.
                    cancel.cancel();
                    cancelled = None;
                }
                Poll::Pending
            })
            .await
        }
    };
    drop(token);
    Ok(result)
}
