//! Values scoped to a task rather than to a thread.
//!
//! `thread_local!` is *almost* right on a runtime whose tasks never migrate:
//! the value stays put, and no synchronisation is needed to reach it. What it
//! gets wrong is sharing. Every task on a core sees the same slot, so a
//! request id written there is visible to the next request, and to every
//! other task running between the two.
//!
//! A task-local is set for the duration of one future and restored
//! afterwards, so two tasks interleaving on the same core each read their
//! own.
//!
//! ```
//! use glommio::LocalExecutor;
//!
//! glommio::task_local! {
//!     static REQUEST: u32;
//! }
//!
//! LocalExecutor::default().run(async {
//!     REQUEST
//!         .scope(7, async {
//!             assert_eq!(REQUEST.with(|request| *request), 7);
//!         })
//!         .await;
//! });
//! ```
//!
//! # What it costs, and what it does not
//!
//! The value lives in a thread-local and is swapped in around every poll of
//! the scoped future, so reading one is a thread-local access and no more.
//! Nothing is allocated per task and the task structures are untouched.
//!
//! Also unlike a thread-local: a task-local is *not* visible to a task
//! [`spawn_local`](crate::spawn_local)'d from inside the scope. The child is
//! a separate task with its own scopes, and the value would otherwise outlive
//! the future it belongs to. Pass what a child needs to the child.

use std::{
    cell::RefCell,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Declares task-local values.
///
/// Each one becomes a [`LocalKey`], which is set with
/// [`scope`](LocalKey::scope) and read with [`with`](LocalKey::with).
///
/// ```
/// glommio::task_local! {
///     static REQUEST_ID: u64;
///     pub static TENANT: String;
/// }
/// ```
#[macro_export]
macro_rules! task_local {
    () => {};

    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty; $($rest:tt)*) => {
        $crate::task_local!($(#[$attr])* $vis static $name: $t);
        $crate::task_local!($($rest)*);
    };

    ($(#[$attr:meta])* $vis:vis static $name:ident: $t:ty) => {
        $(#[$attr])*
        $vis static $name: $crate::task::LocalKey<$t> = {
            ::std::thread_local! {
                static __KEY: ::std::cell::RefCell<::std::option::Option<$t>> =
                    const { ::std::cell::RefCell::new(::std::option::Option::None) };
            }
            $crate::task::LocalKey { __key: &__KEY }
        };
    };
}

/// A task-local value, declared with [`task_local!`](crate::task_local).
pub struct LocalKey<T: 'static> {
    /// Constructed by the macro, which is the only way to make one.
    #[doc(hidden)]
    pub __key: &'static std::thread::LocalKey<RefCell<Option<T>>>,
}

impl<T: 'static> LocalKey<T> {
    /// Runs `future` with this key set to `value`.
    ///
    /// The value is installed around each poll and taken out again when the
    /// poll returns, so a future suspended inside the scope does not leave it
    /// visible to whatever runs next.
    pub fn scope<F: Future>(&'static self, value: T, future: F) -> TaskLocalFuture<T, F> {
        TaskLocalFuture {
            key: self,
            value: Some(value),
            future,
        }
    }

    /// Reads the value set for the current task.
    ///
    /// # Panics
    ///
    /// If the key is not set here. Use [`try_with`](Self::try_with) where
    /// that is a question rather than a bug.
    pub fn with<R>(&'static self, f: impl FnOnce(&T) -> R) -> R {
        self.try_with(f)
            .expect("a task-local read outside the scope that sets it")
    }

    /// Reads the value, or reports that there is none.
    pub fn try_with<R>(&'static self, f: impl FnOnce(&T) -> R) -> Result<R, AccessError> {
        self.__key.with(|slot| match &*slot.borrow() {
            Some(value) => Ok(f(value)),
            None => Err(AccessError),
        })
    }
}

impl<T: 'static> fmt::Debug for LocalKey<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalKey").finish_non_exhaustive()
    }
}

/// The key was read where it is not set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessError;

impl fmt::Display for AccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the task-local is not set on this task")
    }
}

impl std::error::Error for AccessError {}

pin_project_lite::pin_project! {
    /// The future returned by [`LocalKey::scope`].
    pub struct TaskLocalFuture<T: 'static, F> {
        key: &'static LocalKey<T>,
        value: Option<T>,
        #[pin]
        future: F,
    }
}

impl<T: 'static, F: Future> Future for TaskLocalFuture<T, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let key = this.key;
        let value = this.value;

        // Swap ours in, remembering whatever was there so nesting restores
        // the outer value rather than clearing it.
        let previous = key.__key.with(|slot| slot.replace(value.take()));

        // Put it back even if the future panics, or the scope leaks into
        // whatever runs next on this core.
        let restore = scopeguard::guard((key, value), |(key, value)| {
            *value = key.__key.with(|slot| slot.replace(previous));
        });

        let polled = this.future.poll(cx);
        drop(restore);
        polled
    }
}
