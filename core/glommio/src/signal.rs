//! Signals, delivered as ordinary I/O.
//!
//! A server has to drain on `SIGTERM`, and the usual answer -- a signal
//! handler -- is a bad fit here twice over: a handler runs on whichever
//! thread the kernel picks, and almost nothing in a per-core runtime is safe
//! to touch from there. `signalfd` avoids both. Signals become a readable
//! descriptor, the descriptor goes on the reactor like any other, and the
//! code that reacts is ordinary async code on a chosen core.
//!
//! # Signals must be blocked first, on every thread
//!
//! `signalfd` only sees a signal if it is blocked; otherwise the default
//! disposition runs first, and for `SIGTERM` that means the process is gone
//! before anything reads the descriptor. The signal mask is **per thread**
//! and is inherited across `spawn`, so the reliable order is:
//!
//! ```no_run
//! # use glommio::{signal, LocalExecutorBuilder, Placement};
//! // In main, before any executor exists: every thread spawned after this
//! // inherits the mask.
//! signal::block(&[libc::SIGTERM, libc::SIGINT]).unwrap();
//!
//! let handle = LocalExecutorBuilder::new(Placement::Unbound)
//!     .spawn(|| async move {
//!         let signals = signal::Signals::new(&[libc::SIGTERM, libc::SIGINT]).unwrap();
//!         let received = signals.recv().await.unwrap();
//!         println!("draining on signal {received}");
//!     })
//!     .unwrap();
//! # let _ = handle;
//! ```
//!
//! [`Signals::new`] blocks them on its own thread too, which is enough for a
//! single-executor program and not enough for a pool. Nothing can check the
//! other threads for you, which is why this is written down rather than
//! detected.
//!
//! # One reader
//!
//! A signal is delivered to one `signalfd`. Two executors each watching
//! `SIGTERM` race for it and only one wins, so put the descriptor on one core
//! and tell the others through a
//! [`shared_channel`](crate::channels::shared_channel) or a
//! [`ForeignCancellation`](crate::sync::ForeignCancellation).

use crate::{io::PollableFd, GlommioError};
use std::{
    io,
    mem::MaybeUninit,
    os::unix::io::{FromRawFd, OwnedFd},
};

type Result<T> = crate::Result<T, ()>;

fn sigset(signals: &[libc::c_int]) -> io::Result<libc::sigset_t> {
    let mut set = MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: `sigemptyset` initialises what it is given, and every
    // `sigaddset` after it operates on an initialised set.
    unsafe {
        if libc::sigemptyset(set.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }
        for signal in signals {
            if libc::sigaddset(set.as_mut_ptr(), *signal) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(set.assume_init())
    }
}

/// Blocks `signals` on the calling thread, so a [`Signals`] can see them.
///
/// Call this in `main` before spawning executors: the mask is inherited, so
/// blocking once up front covers every thread that follows. Calling it later
/// covers only the thread that calls it, which is how a pool ends up with one
/// thread that still dies on `SIGTERM`.
pub fn block(signals: &[libc::c_int]) -> Result<()> {
    let set = sigset(signals)?;
    // SAFETY: `set` is initialised above; a null second argument means "do
    // not report the previous mask".
    let err = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) };
    if err != 0 {
        return Err(GlommioError::IoError(io::Error::from_raw_os_error(err)));
    }
    Ok(())
}

/// Signals arriving as readable events on the reactor.
#[derive(Debug)]
pub struct Signals {
    fd: PollableFd<OwnedFd>,
}

impl Signals {
    /// Watches `signals` on this executor, blocking them on this thread.
    ///
    /// Blocking here is not enough for a program with more than one executor:
    /// see the module documentation.
    pub fn new(signals: &[libc::c_int]) -> Result<Self> {
        let set = sigset(signals)?;
        block(signals)?;

        // SAFETY: `set` is initialised; -1 means "make a new descriptor".
        let fd = unsafe { libc::signalfd(-1, &set, libc::SFD_NONBLOCK | libc::SFD_CLOEXEC) };
        if fd < 0 {
            return Err(GlommioError::IoError(io::Error::last_os_error()));
        }

        // SAFETY: `signalfd` returned it and nothing else owns it.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Signals {
            fd: PollableFd::new(fd)?,
        })
    }

    /// Waits for the next signal, returning its number.
    ///
    /// Signals coalesce: several of the same kind arriving while nothing is
    /// reading produce one wakeup, which is the same rule handlers follow and
    /// is fine for the things signals are used for -- shutdown, reload.
    pub async fn recv(&self) -> Result<libc::c_int> {
        loop {
            match self.try_recv()? {
                Some(signal) => return Ok(signal),
                None => self.fd.readable().await?,
            }
        }
    }

    /// Takes a signal if one is waiting, without suspending.
    pub fn try_recv(&self) -> Result<Option<libc::c_int>> {
        let mut info = MaybeUninit::<libc::signalfd_siginfo>::uninit();
        let size = std::mem::size_of::<libc::signalfd_siginfo>();

        // SAFETY: reading exactly one `signalfd_siginfo` into space for one.
        let read = unsafe {
            libc::read(
                std::os::unix::io::AsRawFd::as_raw_fd(&self.fd),
                info.as_mut_ptr() as *mut libc::c_void,
                size,
            )
        };

        if read < 0 {
            let err = io::Error::last_os_error();
            return match err.kind() {
                io::ErrorKind::WouldBlock => Ok(None),
                _ => Err(GlommioError::IoError(err)),
            };
        }

        debug_assert_eq!(read as usize, size, "a short read from a signalfd");
        // SAFETY: the kernel filled it.
        let info = unsafe { info.assume_init() };
        Ok(Some(info.ssi_signo as libc::c_int))
    }
}

/// Waits for `SIGINT`, the interrupt a terminal sends on Ctrl-C.
///
/// Blocks `SIGINT` on this thread, with the same caveat as [`Signals::new`]
/// about other threads.
pub async fn ctrl_c() -> Result<()> {
    Signals::new(&[libc::SIGINT])?.recv().await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutor};
    use std::time::Duration;

    // SIGUSR1 belongs to the stall detector; SIGUSR2 is free.
    const TEST_SIGNAL: libc::c_int = libc::SIGUSR2;

    #[test]
    fn a_raised_signal_arrives_as_a_readable_event() {
        LocalExecutor::default().run(async {
            let signals = Signals::new(&[TEST_SIGNAL]).unwrap();

            // Raised after the reader is waiting, on this thread, which is
            // the one that blocked it.
            crate::spawn_local(async {
                Timer::new(Duration::from_millis(20)).await;
                unsafe { libc::raise(TEST_SIGNAL) };
            })
            .detach();

            assert_eq!(signals.recv().await.unwrap(), TEST_SIGNAL);
        });
    }

    #[test]
    fn nothing_arrives_when_nothing_was_raised() {
        LocalExecutor::default().run(async {
            let signals = Signals::new(&[TEST_SIGNAL]).unwrap();
            assert_eq!(signals.try_recv().unwrap(), None);

            let waited = crate::future::timeout(Duration::from_millis(50), async {
                signals.recv().await.map(|_| ())
            })
            .await;
            assert!(waited.is_err(), "a signal arrived that nobody raised");
        });
    }

    #[test]
    fn a_signal_raised_before_the_wait_is_still_delivered() {
        // Signals are queued by the mask, not dropped, so one that arrives
        // between blocking and reading is not lost -- which is the whole
        // reason to block them rather than install a handler.
        LocalExecutor::default().run(async {
            let signals = Signals::new(&[TEST_SIGNAL]).unwrap();
            unsafe { libc::raise(TEST_SIGNAL) };
            assert_eq!(signals.recv().await.unwrap(), TEST_SIGNAL);
        });
    }
}
