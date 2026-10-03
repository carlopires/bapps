//! An async mutex for single-threaded executors.

use super::{LockResult, Semaphore, TryLockResult};
use crate::{error::ResourceType, GlommioError};
use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
};

/// A mutual exclusion primitive for tasks on the same executor.
///
/// Unlike [`std::sync::Mutex`] this one is `!Send`: it is meant for state that
/// stays on one core, and holding the lock across an `await` is the point
/// rather than a hazard. Where no borrow crosses an `await`, a
/// [`RefCell`](std::cell::RefCell) is cheaper and should be preferred.
///
/// Like [`RwLock`](super::RwLock) it cannot be poisoned, but it can be closed:
/// a closed mutex fails every attempt to lock it, and wakes anyone already
/// waiting with an error.
///
/// # Examples
///
/// ```
/// use glommio::{sync::Mutex, LocalExecutor};
///
/// let ex = LocalExecutor::default();
/// ex.run(async {
///     let mutex = Mutex::new(0);
///     *mutex.lock().await.unwrap() += 1;
///     assert_eq!(*mutex.lock().await.unwrap(), 1);
/// });
/// ```
#[derive(Debug)]
pub struct Mutex<T> {
    /// One permit, held for as long as a guard is alive. The semaphore already
    /// carries the waiter queue, its fairness, and the close semantics.
    semaphore: Semaphore,
    value: UnsafeCell<T>,
}

/// Grants access to the value while it is alive, and releases the lock on drop.
#[derive(Debug)]
pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // Safety: the guard exists only while its holder owns the semaphore's
        // single permit, so no other reference to the value can be alive.
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // Safety: as above, and `&mut self` proves this is the only guard.
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.semaphore.signal(1);
    }
}

impl<T> Mutex<T> {
    /// Creates a new mutex holding `value`.
    pub fn new(value: T) -> Self {
        Mutex {
            semaphore: Semaphore::new(1),
            value: UnsafeCell::new(value),
        }
    }

    /// Locks the mutex, suspending until it is available.
    ///
    /// Returns an error if the mutex is closed, including when it is closed
    /// while this call is already waiting.
    pub async fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        self.semaphore.acquire(1).await?;
        Ok(MutexGuard { mutex: self })
    }

    /// Locks the mutex if it is free, and fails rather than suspending.
    pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        if self.semaphore.try_acquire(1)? {
            Ok(MutexGuard { mutex: self })
        } else {
            Err(GlommioError::WouldBlock(ResourceType::Semaphore {
                requested: 1,
                available: 0,
            }))
        }
    }

    /// Closes the mutex. Every subsequent lock fails, and anyone waiting is
    /// woken with an error.
    pub fn close(&self) {
        self.semaphore.close();
    }

    /// Returns whether the mutex has been closed.
    pub fn is_closed(&self) -> bool {
        self.semaphore.is_closed()
    }

    /// Borrows the value directly. No locking is needed: the borrow checker
    /// proves this is the only reference to the mutex.
    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        Ok(self.value.get_mut())
    }

    /// Consumes the mutex and returns the value it guards.
    pub fn into_inner(self) -> LockResult<T> {
        Ok(self.value.into_inner())
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Mutex::new(T::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::{cell::RefCell, rc::Rc, time::Duration};

    #[test]
    fn lock_hands_out_the_value() {
        LocalExecutor::default().run(async {
            let mutex = Mutex::new(5);
            let mut guard = mutex.lock().await.unwrap();
            assert_eq!(*guard, 5);
            *guard = 7;
            drop(guard);
            assert_eq!(*mutex.lock().await.unwrap(), 7);
        });
    }

    #[test]
    fn a_second_lock_waits_for_the_first_to_be_dropped() {
        LocalExecutor::default().run(async {
            let mutex = Rc::new(Mutex::new(0));
            let order = Rc::new(RefCell::new(Vec::new()));

            let guard = mutex.lock().await.unwrap();

            let waiter = crate::spawn_local({
                let mutex = mutex.clone();
                let order = order.clone();
                async move {
                    let mut guard = mutex.lock().await.unwrap();
                    order.borrow_mut().push("second acquired");
                    *guard += 1;
                }
            })
            .detach();

            // Give the spawned task every chance to acquire it early.
            Timer::new(Duration::from_millis(10)).await;
            order.borrow_mut().push("first still held");
            drop(guard);

            waiter.await;
            assert_eq!(
                *order.borrow(),
                vec!["first still held", "second acquired"],
                "the second lock ran before the first guard was dropped"
            );
            assert_eq!(*mutex.lock().await.unwrap(), 1);
        });
    }

    #[test]
    fn try_lock_fails_while_the_lock_is_held() {
        LocalExecutor::default().run(async {
            let mutex = Mutex::new(0);
            let guard = mutex.lock().await.unwrap();
            assert!(mutex.try_lock().is_err());
            drop(guard);
            assert!(mutex.try_lock().is_ok());
        });
    }

    #[test]
    fn locking_a_closed_mutex_fails() {
        LocalExecutor::default().run(async {
            let mutex = Mutex::new(0);
            assert!(!mutex.is_closed());
            mutex.close();
            assert!(mutex.is_closed());
            assert!(mutex.lock().await.is_err());
            assert!(mutex.try_lock().is_err());
        });
    }

    #[test]
    fn closing_wakes_a_waiter_with_an_error() {
        LocalExecutor::default().run(async {
            let mutex = Rc::new(Mutex::new(0));
            let guard = mutex.lock().await.unwrap();

            let waiter = crate::spawn_local({
                let mutex = mutex.clone();
                async move { mutex.lock().await.map(|_| ()) }
            })
            .detach();

            Timer::new(Duration::from_millis(10)).await;
            mutex.close();
            drop(guard);

            assert!(
                waiter.await.unwrap().is_err(),
                "a task waiting on a closed mutex should be woken with an error"
            );
        });
    }

    #[test]
    fn into_inner_and_get_mut_reach_the_value_without_locking() {
        let mut mutex = Mutex::new(1);
        *mutex.get_mut().unwrap() = 2;
        assert_eq!(mutex.into_inner().unwrap(), 2);
    }
}
