//! A cell initialised at most once, possibly by an async initialiser.
//!
//! [`std::cell::OnceCell`] cannot help when the value must be produced by
//! something that awaits -- opening a file, resolving a name, asking a peer.
//! This one can, and it guarantees the initialiser runs once even when several
//! tasks reach the cell while initialisation is still in flight: the later
//! arrivals wait for the first rather than starting their own.
//!
//! # Examples
//!
//! ```
//! use glommio::{sync::OnceCell, LocalExecutor};
//!
//! let ex = LocalExecutor::default();
//! ex.run(async {
//!     let cell = OnceCell::new();
//!     let value = cell.get_or_init(|| async { 42 }).await;
//!     assert_eq!(*value, 42);
//! });
//! ```

use super::Semaphore;
use std::{cell::UnsafeCell, future::Future};

/// A cell holding a value that is initialised at most once.
#[derive(Debug)]
pub struct OnceCell<T> {
    value: UnsafeCell<Option<T>>,
    /// Held for the duration of an initialisation, so a second caller waits
    /// for the first instead of running its own initialiser.
    initialising: Semaphore,
}

impl<T> OnceCell<T> {
    /// Creates an empty cell.
    pub fn new() -> Self {
        OnceCell {
            value: UnsafeCell::new(None),
            initialising: Semaphore::new(1),
        }
    }

    /// Returns the value, or `None` if the cell is still empty.
    pub fn get(&self) -> Option<&T> {
        // Safety: the value is written at most once and never removed or
        // replaced, so a shared reference handed out here cannot be
        // invalidated while it lives. Every writer checks the cell is empty
        // and writes without awaiting in between, which on a single-threaded
        // executor is what makes "at most once" true. Note the semaphore alone
        // does not: `set` never takes it.
        unsafe { (*self.value.get()).as_ref() }
    }

    /// Returns whether the cell holds a value.
    pub fn is_initialized(&self) -> bool {
        self.get().is_some()
    }

    /// Sets the value if the cell is empty.
    ///
    /// # Errors
    ///
    /// Hands `value` back if the cell already holds one.
    pub fn set(&self, value: T) -> Result<(), T> {
        if self.is_initialized() {
            return Err(value);
        }

        // Safety: as in `get`, and nothing can be mid-initialisation here:
        // `set` does not await, so no other task can be running.
        unsafe { *self.value.get() = Some(value) };
        Ok(())
    }

    /// Returns the value, running a fallible `init` to produce it if the cell
    /// is empty.
    ///
    /// **A failed initialiser does not poison the cell.** Lazily-initialised
    /// resources are usually fallible -- connecting to a catalog, opening a
    /// file -- and a transient failure should leave the next caller free to
    /// try again. A caller queued behind one that failed runs its own
    /// initialiser rather than inheriting the error.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{sync::OnceCell, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let cell = OnceCell::new();
    ///     let value = cell
    ///         .get_or_try_init(|| async { Ok::<_, std::io::Error>(42) })
    ///         .await
    ///         .unwrap();
    ///     assert_eq!(*value, 42);
    /// });
    /// ```
    pub async fn get_or_try_init<F, Fut, E>(&self, init: F) -> Result<&T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.get() {
            return Ok(value);
        }

        let _permit = self
            .initialising
            .acquire_permit(1)
            .await
            .expect("the initialisation semaphore is private and never closed");

        // Re-checked under the permit: whoever held it before may have
        // succeeded, in which case there is nothing to do -- or failed, in
        // which case the cell is still empty and this caller tries.
        if self.get().is_none() {
            let value = init().await?;
            // Re-checked after the await, not before it. `set` publishes
            // without taking a permit, so it can land while `init` is
            // suspended, and overwriting then would drop a value that `get`
            // has already handed out a reference to. First publication wins;
            // a later initialiser's value is dropped instead.
            //
            // Safety: nothing awaits between the check and the write, so on a
            // single-threaded executor no other task can publish in between.
            if self.get().is_none() {
                unsafe { *self.value.get() = Some(value) };
            }
        }

        Ok(self.get().expect("the cell was just initialised"))
    }

    /// Returns the value, running `init` to produce it if the cell is empty.
    ///
    /// If another task is already initialising the cell, this waits for that
    /// one to finish rather than running `init` -- so the initialiser runs
    /// exactly once however many callers arrive.
    pub async fn get_or_init<F, Fut>(&self, init: F) -> &T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        if let Some(value) = self.get() {
            return value;
        }

        // Whoever takes the permit does the work; everyone else queues here
        // and finds the value already present when they get it.
        let _permit = self
            .initialising
            .acquire_permit(1)
            .await
            .expect("the initialisation semaphore is private and never closed");

        if self.get().is_none() {
            let value = init().await;
            // See `get_or_try_init`: the check has to be on this side of the
            // await, because `set` does not take the permit.
            //
            // Safety: nothing awaits between the check and the write, so no
            // other task can publish in between.
            if self.get().is_none() {
                unsafe { *self.value.get() = Some(value) };
            }
        }

        self.get().expect("the cell was just initialised")
    }
}

impl<T> Default for OnceCell<T> {
    fn default() -> Self {
        OnceCell::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::{cell::RefCell, rc::Rc, time::Duration};

    #[test]
    fn an_empty_cell_has_nothing_in_it() {
        let cell: OnceCell<u32> = OnceCell::new();
        assert!(cell.get().is_none());
        assert!(!cell.is_initialized());
    }

    #[test]
    fn set_stores_a_value_once() {
        let cell = OnceCell::new();
        assert!(cell.set(1).is_ok());
        assert_eq!(cell.get(), Some(&1));
        assert!(cell.is_initialized());
        assert_eq!(
            cell.set(2),
            Err(2),
            "a second set should hand the value back"
        );
        assert_eq!(cell.get(), Some(&1), "the first value must survive");
    }

    #[test]
    fn get_or_init_runs_the_initialiser_once() {
        LocalExecutor::default().run(async {
            let cell = OnceCell::new();
            let runs = Rc::new(RefCell::new(0));

            for _ in 0..3 {
                let value = cell
                    .get_or_init(|| {
                        let runs = runs.clone();
                        async move {
                            *runs.borrow_mut() += 1;
                            41
                        }
                    })
                    .await;
                assert_eq!(*value, 41);
            }

            assert_eq!(*runs.borrow(), 1, "the initialiser ran more than once");
        });
    }

    #[test]
    fn get_or_try_init_stores_a_successful_value() {
        LocalExecutor::default().run(async {
            let cell = OnceCell::new();
            let value = cell
                .get_or_try_init(|| async { Ok::<_, String>(3) })
                .await
                .unwrap();
            assert_eq!(*value, 3);
            assert!(cell.is_initialized());
        });
    }

    #[test]
    fn a_failed_get_or_try_init_leaves_the_cell_empty() {
        LocalExecutor::default().run(async {
            let cell: OnceCell<u32> = OnceCell::new();
            let err = cell
                .get_or_try_init(|| async { Err::<u32, _>("catalog unreachable") })
                .await
                .unwrap_err();

            assert_eq!(err, "catalog unreachable");
            assert!(
                !cell.is_initialized(),
                "a failed initialiser must not poison the cell"
            );
            assert!(cell.get().is_none());
        });
    }

    #[test]
    fn a_cell_can_be_retried_after_a_failure() {
        LocalExecutor::default().run(async {
            let cell = OnceCell::new();
            let attempts = Rc::new(RefCell::new(0));

            for _ in 0..2 {
                let _ = cell
                    .get_or_try_init(|| {
                        let attempts = attempts.clone();
                        async move {
                            *attempts.borrow_mut() += 1;
                            Err::<u32, _>("still down")
                        }
                    })
                    .await;
            }

            let value = cell
                .get_or_try_init(|| async { Ok::<_, &str>(7) })
                .await
                .unwrap();

            assert_eq!(*value, 7);
            assert_eq!(*attempts.borrow(), 2, "each failure should have retried");
        });
    }

    #[test]
    fn a_caller_waiting_behind_a_failed_init_runs_its_own() {
        LocalExecutor::default().run(async {
            let cell = Rc::new(OnceCell::new());
            let order = Rc::new(RefCell::new(Vec::new()));

            // The first initialiser suspends, then fails, so the second is
            // queued behind it and must not inherit the failure.
            let failing = crate::spawn_local({
                let cell = cell.clone();
                let order = order.clone();
                async move {
                    cell.get_or_try_init(|| {
                        let order = order.clone();
                        async move {
                            Timer::new(Duration::from_millis(20)).await;
                            order.borrow_mut().push("first failed");
                            Err::<u32, _>("no")
                        }
                    })
                    .await
                    .is_err()
                }
            })
            .detach();

            Timer::new(Duration::from_millis(5)).await;

            let second = cell
                .get_or_try_init(|| {
                    let order = order.clone();
                    async move {
                        order.borrow_mut().push("second ran");
                        Ok::<_, &str>(11)
                    }
                })
                .await;

            assert!(failing.await.unwrap());
            assert_eq!(*second.unwrap(), 11);
            assert_eq!(*order.borrow(), vec!["first failed", "second ran"]);
        });
    }

    #[test]
    fn a_concurrent_get_or_init_waits_rather_than_initialising_again() {
        LocalExecutor::default().run(async {
            let cell = Rc::new(OnceCell::new());
            let runs = Rc::new(RefCell::new(0));

            // The first initialiser suspends, so the second arrives while
            // initialisation is still in flight.
            let slow = crate::spawn_local({
                let cell = cell.clone();
                let runs = runs.clone();
                async move {
                    *cell
                        .get_or_init(|| {
                            let runs = runs.clone();
                            async move {
                                Timer::new(Duration::from_millis(20)).await;
                                *runs.borrow_mut() += 1;
                                7
                            }
                        })
                        .await
                }
            })
            .detach();

            Timer::new(Duration::from_millis(5)).await;

            let second = *cell
                .get_or_init(|| {
                    let runs = runs.clone();
                    async move {
                        *runs.borrow_mut() += 1;
                        99
                    }
                })
                .await;

            assert_eq!(slow.await.unwrap(), 7);
            assert_eq!(second, 7, "the second caller should see the first value");
            assert_eq!(*runs.borrow(), 1, "the initialiser ran more than once");
        });
    }
}

#[cfg(test)]
mod write_once {
    use super::*;
    use crate::{timer::sleep, LocalExecutor};
    use std::{rc::Rc, time::Duration};

    /// `set` takes no permit, so it can publish while an initialiser holding
    /// the permit is suspended in `init().await`. If `get_or_init` then wrote
    /// on the strength of a check it made before that await, it would replace
    /// a value `get` may already have handed out a reference to: for any `T`
    /// with a destructor, that drops the value out from under a live `&T`.
    ///
    /// First publication wins, and the later value is dropped instead.
    #[test]
    fn a_value_published_while_an_initialiser_runs_is_not_replaced() {
        LocalExecutor::default().run(async {
            let cell: Rc<OnceCell<u32>> = Rc::new(OnceCell::new());

            let initialiser = crate::spawn_local({
                let cell = cell.clone();
                async move {
                    cell.get_or_init(|| async {
                        sleep(Duration::from_millis(20)).await;
                        2
                    })
                    .await;
                }
            });

            // Let the initialiser take the permit and park inside `init`.
            sleep(Duration::from_millis(5)).await;

            assert_eq!(cell.set(1), Ok(()), "the cell is still empty");
            let published = cell.get().expect("just set");
            assert_eq!(*published, 1);

            initialiser.await;

            assert_eq!(
                cell.get(),
                Some(&1),
                "the initialiser replaced a value that was already published"
            );
        });
    }

    #[test]
    fn an_initialiser_still_wins_when_nothing_races_it() {
        LocalExecutor::default().run(async {
            let cell: OnceCell<u32> = OnceCell::new();
            assert_eq!(*cell.get_or_init(|| async { 7 }).await, 7);
            assert_eq!(cell.set(9), Err(9), "already initialised");
            assert_eq!(cell.get(), Some(&7));
        });
    }
}
