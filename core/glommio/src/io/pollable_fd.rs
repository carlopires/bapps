//! Waiting on a descriptor glommio did not create.
//!
//! Everything glommio owns -- sockets, files, timers -- already goes through
//! the reactor. Anything else does not: an `inotify` watch, a `timerfd`, a
//! `signalfd`, a device node, a descriptor from a C library. Without a way to
//! wait on those, a program has to leave the runtime to use them, which is
//! the difference between a runtime an ecosystem can build on and one that
//! supports only what it ships.
//!
//! [`PollableFd`] is that way in. It registers interest through the same
//! `IORING_OP_POLL_ADD` the socket paths use, so a foreign descriptor parks
//! the executor in the kernel alongside everything else rather than being
//! polled on the side.

use crate::{reactor::Reactor, GlommioError};
use std::{
    os::unix::io::{AsRawFd, RawFd},
    rc::{Rc, Weak},
    task::{Context, Poll},
};

type Result<T> = crate::Result<T, ()>;

/// A descriptor the reactor will tell you about.
///
/// Owns whatever it is given, so the descriptor cannot be closed while a
/// registration is outstanding. [`into_inner`](Self::into_inner) takes it
/// back.
///
/// # The descriptor should be non-blocking
///
/// This reports readiness; the read or write itself stays the caller's, and
/// on a blocking descriptor that call can still block the executor after a
/// spurious wakeup. `O_NONBLOCK` is the caller's to set, because only the
/// caller knows what the descriptor is.
///
/// # Readiness here is not edge-triggered
///
/// Each call registers a fresh one-shot poll and resolves when the kernel
/// says the descriptor is ready, so there is no readiness state to clear and
/// no guard to return -- the `AsyncFd`/`clear_ready` dance epoll requires has
/// no counterpart. If data is already waiting, the call returns immediately.
///
/// # Examples
///
/// ```
/// use glommio::{io::PollableFd, LocalExecutor};
/// use std::os::unix::io::{AsRawFd, FromRawFd};
///
/// let mut fds = [0 as libc::c_int; 2];
/// // A pipe stands in for anything glommio does not own.
/// unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) };
/// let (reader, writer) = (fds[0], fds[1]);
///
/// LocalExecutor::default().run(async move {
///     let reader = PollableFd::new(unsafe { std::fs::File::from_raw_fd(reader) }).unwrap();
///
///     unsafe { libc::write(writer, b"x".as_ptr() as *const libc::c_void, 1) };
///     reader.readable().await.unwrap();
///
///     let mut byte = [0u8; 1];
///     unsafe { libc::read(reader.get_ref().as_raw_fd(), byte.as_mut_ptr() as *mut _, 1) };
///     assert_eq!(&byte, b"x");
///     unsafe { libc::close(writer) };
/// });
/// ```
#[derive(Debug)]
pub struct PollableFd<T: AsRawFd> {
    inner: T,
    reactor: Weak<Reactor>,
}

impl<T: AsRawFd> PollableFd<T> {
    /// Registers `inner` with the reactor of the executor this runs on.
    ///
    /// The descriptor stays with that executor: a `PollableFd` is `!Send`
    /// like everything else here, so it cannot be woken from a reactor that
    /// does not own it.
    pub fn new(inner: T) -> Result<Self> {
        let reactor = crate::executor().reactor();
        Ok(PollableFd {
            inner,
            reactor: Rc::downgrade(&reactor),
        })
    }

    /// Resolves when the descriptor is readable.
    ///
    /// Also resolves on hangup and error, since a caller that wants to read
    /// wants to hear about those too -- the following read reports which it
    /// was.
    pub async fn readable(&self) -> Result<()> {
        let reactor = self.upgrade()?;
        let source = reactor.poll_read_ready(self.inner.as_raw_fd());
        source.collect_rw().await?;
        Ok(())
    }

    /// Resolves when the descriptor is writable.
    pub async fn writable(&self) -> Result<()> {
        let reactor = self.upgrade()?;
        let source = reactor.poll_write_ready(self.inner.as_raw_fd());
        source.collect_rw().await?;
        Ok(())
    }

    /// Poll form of [`readable`](Self::readable), for hand-written futures.
    ///
    /// Unlike the `async` form this needs somewhere to keep the registration
    /// between polls, which is what `state` is: the caller holds it, and it
    /// is dropped when the wait is over.
    pub fn poll_readable(&self, cx: &mut Context<'_>, state: &mut PollState) -> Poll<Result<()>> {
        self.poll_ready(cx, state, true)
    }

    /// Poll form of [`writable`](Self::writable).
    pub fn poll_writable(&self, cx: &mut Context<'_>, state: &mut PollState) -> Poll<Result<()>> {
        self.poll_ready(cx, state, false)
    }

    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        state: &mut PollState,
        read: bool,
    ) -> Poll<Result<()>> {
        let reactor = match self.upgrade() {
            Ok(reactor) => reactor,
            Err(err) => return Poll::Ready(Err(err)),
        };

        let source = state.source.get_or_insert_with(|| {
            if read {
                reactor.poll_read_ready(self.inner.as_raw_fd())
            } else {
                reactor.poll_write_ready(self.inner.as_raw_fd())
            }
        });

        match source.poll_collect_rw(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                // Registration spent: the next call starts a fresh one rather
                // than resolving instantly off a stale completion.
                state.source = None;
                Poll::Ready(result.map(|_| ()).map_err(Into::into))
            }
        }
    }

    /// The descriptor, borrowed.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// The descriptor, mutably.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Takes the descriptor back and stops watching it.
    pub fn into_inner(self) -> T {
        self.inner
    }

    fn upgrade(&self) -> Result<Rc<Reactor>> {
        self.reactor.upgrade().ok_or_else(|| {
            GlommioError::IoError(std::io::Error::other(
                "the executor this descriptor was registered with is gone",
            ))
        })
    }
}

impl<T: AsRawFd> AsRawFd for PollableFd<T> {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Where [`PollableFd::poll_readable`] keeps its registration between polls.
///
/// One per wait, not one per descriptor: a `PollState` carrying a live
/// registration must not be reused for a different direction or a different
/// descriptor, which is why it is passed in rather than kept inside
/// `PollableFd`.
#[derive(Debug, Default)]
pub struct PollState {
    source: Option<crate::sys::Source>,
}

impl PollState {
    /// A state with nothing registered.
    pub fn new() -> Self {
        Self::default()
    }
}
