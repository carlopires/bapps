//! `TcpStream::send_file` driven the way a consumer drives it.

use futures_lite::io::AsyncReadExt;
use glommio::{
    io::{BufferedFile, DmaFile},
    net::{TcpListener, TcpStream},
    timer::Timer,
    GlommioError, LocalExecutor,
};
use std::{cell::Cell, os::unix::io::AsRawFd, rc::Rc, time::Duration};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("glommio-send-file-{}-{}", std::process::id(), name));
    p
}

/// A path under the workspace `target/` directory rather than the system
/// temp directory.
///
/// Used only by the misaligned-offset test below -- do not "simplify" that
/// test back to `tmp_path()`/`std::env::temp_dir()`. `std::env::temp_dir()`
/// is `/tmp`, which on this machine (and commonly in CI containers) is
/// `tmpfs`. This does not disable glommio's own alignment check: `DmaFile`
/// clamps its reported alignment to `.max(512)` regardless of filesystem, so
/// the check still fires from `/tmp` too. What `tmpfs` breaks is the test's
/// ability to *prove the check is needed*: `tmpfs` accepts `O_DIRECT` but
/// does not enforce the offset-alignment requirement at the kernel level, so
/// an unguarded misaligned splice against a `tmpfs`-backed file simply
/// succeeds there -- there is no kernel `EINVAL` to distinguish glommio's
/// error from. `target/` sits on whatever real filesystem backs the
/// checkout, where an unguarded misaligned splice does fail `EINVAL`, which
/// is what this test needs in order to tell "refused before submission"
/// apart from "the kernel rejected it".
fn dma_tmp_path(name: &str) -> std::path::PathBuf {
    // Cargo's per-target scratch directory for integration tests: it exists
    // wherever the target directory is, unlike `<manifest>/../target`, which
    // assumed the crate sits one level below its workspace root.
    let mut p: std::path::PathBuf = env!("CARGO_TARGET_TMPDIR").into();
    p.push(format!("glommio-send-file-{}-{}", std::process::id(), name));
    p
}

/// Writes `contents` to a fresh temp file and returns its path.
fn seed(name: &str, contents: &[u8]) -> std::path::PathBuf {
    let path = tmp_path(name);
    std::fs::write(&path, contents).unwrap();
    path
}

#[test]
fn a_small_file_arrives_intact() {
    LocalExecutor::default().run(async {
        let payload = b"the quick brown fox jumps over the lazy dog".repeat(10);
        let path = seed("small", &payload);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let expected_len = payload.len();

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();
        let file = BufferedFile::open(&path).await.unwrap();
        let sent = writer.send_file(&file, 0, expected_len).await.unwrap();
        file.close().await.unwrap();
        drop(writer);

        assert_eq!(
            sent, expected_len,
            "send_file should report every byte sent"
        );
        let got = reader.await.unwrap();
        assert_eq!(got, payload, "the bytes on the wire must match the file");

        std::fs::remove_file(&path).ok();
    });
}

#[test]
fn a_file_larger_than_the_pipe_arrives_intact() {
    // 65536 is the pipe capacity, so 200 KiB forces the loop to go round
    // several times. A single-chunk implementation truncates here.
    LocalExecutor::default().run(async {
        let payload: Vec<u8> = (0..200 * 1024).map(|i| (i % 251) as u8).collect();
        let path = seed("large", &payload);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let expected_len = payload.len();

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();
        let file = BufferedFile::open(&path).await.unwrap();
        let sent = writer.send_file(&file, 0, expected_len).await.unwrap();
        file.close().await.unwrap();
        drop(writer);

        assert_eq!(sent, expected_len);
        let got = reader.await.unwrap();
        assert_eq!(got.len(), payload.len(), "every byte must arrive");
        assert_eq!(got, payload, "and in the right order");

        std::fs::remove_file(&path).ok();
    });
}

#[test]
fn asking_past_the_end_returns_short_rather_than_erroring() {
    LocalExecutor::default().run(async {
        let payload = b"only sixteen by".to_vec();
        let path = seed("short", &payload);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();
        let file = BufferedFile::open(&path).await.unwrap();
        // Ask for far more than the file holds.
        let sent = writer.send_file(&file, 0, 1024 * 1024).await.unwrap();
        file.close().await.unwrap();
        drop(writer);

        assert_eq!(sent, payload.len(), "a short file is short, not an error");
        assert_eq!(reader.await.unwrap(), payload);

        std::fs::remove_file(&path).ok();
    });
}

// This test is discriminated by mutating the initial `let mut pos = offset;`
// in `send_file` (change it to start at 0): that sends payload[0..2048]
// instead of payload[1024..3072], same length, wrong window. It is NOT
// discriminated by mutating the `pos += filled as u64;` advance inside the
// loop -- at 2048 bytes this transfer fits in a single splice, so the loop
// only runs once and that line's effect on later iterations never fires.
// The large-file test is what proves `pos` accumulates correctly.
#[test]
fn sending_from_an_offset_skips_the_prefix() {
    LocalExecutor::default().run(async {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let path = seed("offset", &payload);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();
        let file = BufferedFile::open(&path).await.unwrap();
        let sent = writer.send_file(&file, 1024, 2048).await.unwrap();
        file.close().await.unwrap();
        drop(writer);

        assert_eq!(sent, 2048);
        assert_eq!(
            reader.await.unwrap(),
            payload[1024..3072].to_vec(),
            "the offset must select the right window, not just the right length"
        );

        std::fs::remove_file(&path).ok();
    });
}

#[test]
fn a_misaligned_offset_on_a_dma_file_is_an_error_not_an_einval() {
    LocalExecutor::default().run(async {
        let path = dma_tmp_path("dma-misaligned");
        {
            // Seed through std so the file has contents before O_DIRECT opens it.
            let payload: Vec<u8> = (0..64 * 1024).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &payload).unwrap();
        }

        let file = DmaFile::open(&path).await.unwrap();
        let alignment = file.align_up(1);
        if alignment <= 1 {
            eprintln!(
                "skipping: this filesystem reports no O_DIRECT alignment, so there is \
                 no misaligned offset to reject here."
            );
            file.close().await.unwrap();
            std::fs::remove_file(&path).ok();
            return;
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted =
            glommio::spawn_local(async move { listener.accept().await.unwrap() }).detach();
        let mut writer = TcpStream::connect(addr).await.unwrap();
        let _reader = accepted.await.unwrap();

        // One byte past an aligned boundary: never valid under O_DIRECT.
        let misaligned = alignment + 1;
        let result = writer.send_file(&file, misaligned, 4096).await;
        let err = result.expect_err("a misaligned offset must be refused, not sent to the kernel");
        let message = err.to_string();
        // The message is about *what* the error says: it must name the
        // offset and the alignment it had to satisfy, which a raw kernel
        // EINVAL never would.
        assert!(
            message.contains(&misaligned.to_string()) && message.contains(&alignment.to_string()),
            "expected glommio's own alignment error naming offset {misaligned} and \
             alignment {alignment}, got: {message}"
        );
        // `raw_os_error()` is about *where* the error came from: it is
        // `Some` only when the `io::Error` underneath wraps a raw OS error
        // code, which is exactly what a kernel-returned EINVAL would be. Our
        // check builds its error with `io::Error::new(InvalidInput, ..)`,
        // which carries no OS code, so this is `None` here and would be
        // `Some(22)` if the offset had instead reached the kernel and come
        // back EINVAL. This is a structural check -- it survives any future
        // rewording of the message, unlike a substring match would.
        assert!(
            err.raw_os_error().is_none(),
            "this must be glommio's own pre-submission check, not a raw kernel EINVAL \
             (raw_os_error = {:?}): {message}",
            err.raw_os_error()
        );
        // And the `ErrorKind` should be the one the check actually
        // constructs, not whatever `io::Error` maps EINVAL to.
        match &err {
            GlommioError::IoError(io_err) => {
                assert_eq!(
                    io_err.kind(),
                    std::io::ErrorKind::InvalidInput,
                    "expected the check's own InvalidInput, got: {io_err:?}"
                );
            }
            other => panic!("expected GlommioError::IoError, got: {other:?}"),
        }

        // An aligned offset on the same file still works, so the check is not
        // simply refusing everything.
        let ok = writer.send_file(&file, alignment, 4096).await;
        assert!(
            ok.is_ok(),
            "an aligned offset must still be accepted, got: {ok:?}"
        );

        file.close().await.unwrap();
        std::fs::remove_file(&path).ok();
    });
}

#[test]
fn a_slow_reader_does_not_stall_send_file() {
    // A small send buffer plus a reader that only drains after the send has
    // begun genuinely backpressures the transfer (see
    // `a_backpressured_send_does_not_stall_the_executor` below for proof the
    // executor itself stays responsive during that stretch). This test
    // covers a different property: that the backpressured transfer still
    // arrives complete and byte-for-byte correct, with neither truncation
    // nor corruption. It does not exercise, and makes no claim about,
    // splice(pipe -> socket) returning EAGAIN or any writability wait --
    // see the EAGAIN arm's own comment in tcp_socket.rs for what is and is
    // not known about that path.
    LocalExecutor::default().run(async {
        let payload: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
        let path = seed("backpressure", &payload);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let expected_len = payload.len();

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();

        // Squeeze the send buffer so the socket fills almost immediately.
        let small: libc::c_int = 4096;
        let ok = unsafe {
            libc::setsockopt(
                writer.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &small as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(ok, 0, "SO_SNDBUF should be settable");

        let file = BufferedFile::open(&path).await.unwrap();
        let sent = writer.send_file(&file, 0, expected_len).await.unwrap();
        file.close().await.unwrap();
        drop(writer);

        assert_eq!(
            sent, expected_len,
            "backpressure must not truncate the send"
        );
        assert_eq!(reader.await.unwrap(), payload, "nor corrupt it");

        std::fs::remove_file(&path).ok();
    });
}

#[test]
fn a_backpressured_send_does_not_stall_the_executor() {
    // Blocking slowly is not the same as blocking correctly. A send_file
    // that suspends its own task while the send buffer is full is fine; one
    // that stalls the whole single-threaded reactor while it waits would
    // starve every other task on the executor, and no test up to this point
    // would notice the difference -- they only check that send_file itself
    // eventually finishes with the right bytes, which is true either way.
    //
    // This proves a neighbour task specifically progresses *during* the
    // backpressured stretch, not merely before or after it. A task is also
    // polled once when it is first spawned, so a counter that is merely
    // nonzero by the end would prove nothing -- this is the same trap
    // documented in the hostname-resolution fix: asserting a property
    // "around" an operation that suspends passes whether or not the
    // operation itself blocks. The discriminator here is sampling strictly
    // inside a window the reader is guaranteed not to have started draining
    // yet, so any neighbour progress recorded there could only have
    // happened while send_file was genuinely parked.
    LocalExecutor::default().run(async {
        let payload: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let path = seed("neighbour", &payload);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let expected_len = payload.len();

        // The reader does not touch the socket until this elapses, so any
        // window that ends before it does is entirely inside send_file's
        // backpressured stretch: the squeezed send buffer fills in low
        // single-digit milliseconds against a 2 MiB payload, far inside
        // this margin.
        let stall = Duration::from_millis(600);

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            Timer::new(stall).await;
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();

        // Squeeze the send buffer so the socket fills almost immediately.
        let small: libc::c_int = 4096;
        let ok = unsafe {
            libc::setsockopt(
                writer.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &small as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(ok, 0, "SO_SNDBUF should be settable");

        let file = BufferedFile::open(&path).await.unwrap();

        let ticks = Rc::new(Cell::new(0u64));
        let sender_done = Rc::new(Cell::new(false));

        let neighbour = {
            let ticks = ticks.clone();
            let sender_done = sender_done.clone();
            glommio::spawn_local(async move {
                // Ticks a short timer repeatedly rather than merely
                // cooperatively yielding: reaching completion requires the
                // reactor to actually service another task's timer while
                // send_file's own splice is outstanding, which is exactly
                // the property in question.
                while !sender_done.get() {
                    Timer::new(Duration::from_millis(5)).await;
                    ticks.set(ticks.get() + 1);
                }
            })
            .detach()
        };

        let sender = {
            let sender_done = sender_done.clone();
            glommio::spawn_local(async move {
                let sent = writer.send_file(&file, 0, expected_len).await.unwrap();
                file.close().await.unwrap();
                drop(writer);
                sender_done.set(true);
                sent
            })
            .detach()
        };

        // Sample a window fully before the reader's stall elapses: the
        // first checkpoint is at t=50ms, the second at t=50+450=500ms, so the
        // measured window between them is 450ms, not the 400ms the two
        // Timer durations might suggest at a glance.
        Timer::new(Duration::from_millis(50)).await;
        assert!(
            !sender_done.get(),
            "send_file finished before the reader started reading at all; \
             the buffer squeeze did not produce genuine backpressure here, \
             so this run cannot measure what it is meant to"
        );
        let before = ticks.get();

        Timer::new(Duration::from_millis(450)).await;
        assert!(
            !sender_done.get(),
            "send_file finished before the reader started reading; same \
             concern as the first checkpoint"
        );
        let after = ticks.get();

        let sent = sender.await.unwrap();
        assert_eq!(sent, expected_len);
        let got = reader.await.unwrap();
        assert_eq!(got, payload);
        neighbour.await;

        let progressed = after - before;
        // Ticks are spaced 5ms apart over the 450ms window between the two
        // checkpoints (t=50ms to t=500ms), entirely inside the stall, so an
        // unstarved neighbour should log roughly 90. The threshold is well
        // below that: it only needs to rule out "the executor was stalled
        // and the neighbour barely ran at all".
        assert!(
            progressed >= 40,
            "neighbour should have made substantial progress while \
             send_file was backpressured (expected roughly 90 ticks over \
             this 450ms window, got {progressed}) -- a low count here would \
             mean the executor stalls while send_file waits on a full send \
             buffer, not merely that send_file itself is slow"
        );

        std::fs::remove_file(&path).ok();
    });
}
