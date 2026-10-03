//! Waiting on a descriptor glommio did not create, from outside the crate.
//!
//! A pipe stands in for the real cases -- `inotify`, `timerfd`, `signalfd`, a
//! C library's descriptor -- because it is the one every machine has and it
//! needs no privileges.

use glommio::{io::PollableFd, timer::Timer, LocalExecutor};
use std::{
    io::{Read, Write},
    os::unix::io::{AsRawFd, FromRawFd},
    time::Duration,
};

/// A non-blocking pipe, as owned halves.
fn pipe() -> (std::fs::File, std::fs::File) {
    let mut fds = [0 as libc::c_int; 2];
    assert_eq!(
        unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) },
        0,
        "pipe2 failed"
    );
    unsafe {
        (
            std::fs::File::from_raw_fd(fds[0]),
            std::fs::File::from_raw_fd(fds[1]),
        )
    }
}

#[test]
fn readable_resolves_when_a_foreign_fd_has_data() {
    LocalExecutor::default().run(async {
        let (reader, mut writer) = pipe();
        let reader = PollableFd::new(reader).unwrap();

        let written = glommio::spawn_local(async move {
            // Long enough that the reader is certainly parked in the kernel:
            // being woken from there is the whole mechanism.
            Timer::new(Duration::from_millis(50)).await;
            writer.write_all(b"foreign").unwrap();
            writer
        })
        .detach();

        reader.readable().await.unwrap();

        let mut buf = [0u8; 16];
        let read = reader.get_ref().read(&mut buf).unwrap();
        assert_eq!(&buf[..read], b"foreign");

        drop(written.await);
    });
}

#[test]
fn readable_does_not_resolve_while_the_pipe_is_empty() {
    // Otherwise `readable` would be a very fast way of returning nothing.
    LocalExecutor::default().run(async {
        let (reader, _writer) = pipe();
        let reader = PollableFd::new(reader).unwrap();

        let outcome = glommio::future::timeout(Duration::from_millis(50), async {
            reader.readable().await.unwrap();
            Ok(()) as glommio::Result<(), ()>
        })
        .await;

        assert!(outcome.is_err(), "readable resolved with nothing to read");
    });
}

#[test]
fn writable_resolves_for_a_pipe_with_room() {
    LocalExecutor::default().run(async {
        let (_reader, writer) = pipe();
        let writer = PollableFd::new(writer).unwrap();
        writer.writable().await.unwrap();
    });
}

#[test]
fn the_descriptor_can_be_taken_back() {
    LocalExecutor::default().run(async {
        let (reader, _writer) = pipe();
        let fd = reader.as_raw_fd();
        let pollable = PollableFd::new(reader).unwrap();
        assert_eq!(pollable.as_raw_fd(), fd);

        let reader = pollable.into_inner();
        assert_eq!(reader.as_raw_fd(), fd, "the same descriptor comes back");
    });
}

#[test]
fn a_hand_written_future_can_poll_readiness() {
    use glommio::io::PollState;
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    struct WaitReadable<'a> {
        fd: &'a PollableFd<std::fs::File>,
        state: PollState,
    }

    impl Future for WaitReadable<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            match this.fd.poll_readable(cx, &mut this.state) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    result.unwrap();
                    Poll::Ready(())
                }
            }
        }
    }

    LocalExecutor::default().run(async {
        let (reader, mut writer) = pipe();
        let reader = PollableFd::new(reader).unwrap();

        let writing = glommio::spawn_local(async move {
            Timer::new(Duration::from_millis(30)).await;
            writer.write_all(b"!").unwrap();
            writer
        })
        .detach();

        WaitReadable {
            fd: &reader,
            state: PollState::new(),
        }
        .await;

        let mut buf = [0u8; 1];
        assert_eq!(reader.get_ref().read(&mut buf).unwrap(), 1);
        drop(writing.await);
    });
}
