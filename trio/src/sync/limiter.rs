//! A bounded pool of tokens, granted first come first served.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    rc::Rc,
};

use super::Condition;
use crate::cancel::Cancelled;

/// Limits how many holders proceed at once: blocking jobs on helper threads
/// ([`crate::to_thread`]), uploads, open files. Executor-local.
///
/// [`acquire`](Self::acquire) waits for a token in arrival order and is
/// cancellable through the current cancel scope; a waiter that is cancelled
/// or dropped leaves the queue, so it never blocks the waiters behind it.
/// A token is returned when its [`CapacityToken`] is dropped.
#[derive(Clone)]
pub struct CapacityLimiter {
    inner: Rc<Inner>,
}

struct Inner {
    total: Cell<usize>,
    borrowed: Cell<usize>,
    /// Tickets of waiters, oldest first.
    queue: RefCell<VecDeque<u64>>,
    next_ticket: Cell<u64>,
    changed: Condition,
}

impl CapacityLimiter {
    /// A limiter with `total` tokens.
    ///
    /// # Panics
    /// If `total` is zero: nothing could ever proceed.
    pub fn new(total: usize) -> Self {
        assert!(total > 0, "a capacity limiter needs at least one token");
        Self {
            inner: Rc::new(Inner {
                total: Cell::new(total),
                borrowed: Cell::new(0),
                queue: RefCell::default(),
                next_ticket: Cell::new(0),
                changed: Condition::new(),
            }),
        }
    }

    /// Tokens in the pool.
    pub fn total_tokens(&self) -> usize {
        self.inner.total.get()
    }

    /// Tokens currently held.
    pub fn borrowed_tokens(&self) -> usize {
        self.inner.borrowed.get()
    }

    /// Tokens free now (zero while more are held than the total, after a
    /// shrink).
    pub fn available_tokens(&self) -> usize {
        self.total_tokens().saturating_sub(self.borrowed_tokens())
    }

    /// Waiters queued for a token.
    pub fn waiting(&self) -> usize {
        self.inner.queue.borrow().len()
    }

    /// Change the number of tokens. Growing lets waiters proceed; shrinking
    /// takes no token back, it only stops new grants until enough return.
    ///
    /// # Panics
    /// If `total` is zero.
    pub fn set_total_tokens(&self, total: usize) {
        assert!(total > 0, "a capacity limiter needs at least one token");
        self.inner.total.set(total);
        self.inner.changed.notify_all();
    }

    /// A token now, if one is free and nobody is queued ahead.
    pub fn try_acquire(&self) -> Option<CapacityToken> {
        (self.inner.queue.borrow().is_empty() && self.available_tokens() > 0).then(|| self.grant())
    }

    /// Wait for a token, in arrival order. Fails only when the current cancel
    /// scope is cancelled, and then holds no token and no place in the queue.
    pub async fn acquire(&self) -> Result<CapacityToken, Cancelled> {
        let ticket = self.inner.next_ticket.get();
        self.inner.next_ticket.set(ticket.wrapping_add(1));
        self.inner.queue.borrow_mut().push_back(ticket);
        // Leaves the queue however this future ends: granted, cancelled, dropped.
        let place = QueuePlace {
            limiter: self,
            ticket,
        };
        loop {
            let observed = self.inner.changed.generation();
            let first = self.inner.queue.borrow().front() == Some(&ticket);
            if first && self.available_tokens() > 0 {
                drop(place);
                return Ok(self.grant());
            }
            self.inner.changed.wait_for_change(observed).await?;
        }
    }

    fn grant(&self) -> CapacityToken {
        self.inner.borrowed.set(self.inner.borrowed.get() + 1);
        CapacityToken {
            limiter: self.clone(),
        }
    }
}

impl std::fmt::Debug for CapacityLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapacityLimiter")
            .field("total", &self.total_tokens())
            .field("borrowed", &self.borrowed_tokens())
            .field("waiting", &self.waiting())
            .finish()
    }
}

struct QueuePlace<'a> {
    limiter: &'a CapacityLimiter,
    ticket: u64,
}

impl Drop for QueuePlace<'_> {
    fn drop(&mut self) {
        let inner = &self.limiter.inner;
        inner
            .queue
            .borrow_mut()
            .retain(|ticket| *ticket != self.ticket);
        // The next waiter may now be first.
        inner.changed.notify_all();
    }
}

/// One token of a [`CapacityLimiter`], returned on drop.
#[must_use = "dropping the token returns it immediately"]
pub struct CapacityToken {
    limiter: CapacityLimiter,
}

impl Drop for CapacityToken {
    fn drop(&mut self) {
        let inner = &self.limiter.inner;
        inner.borrowed.set(inner.borrowed.get() - 1);
        inner.changed.notify_all();
    }
}
