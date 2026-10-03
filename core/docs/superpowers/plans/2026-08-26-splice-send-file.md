# TcpStream::send_file Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Send a file over a `TcpStream` without the bytes entering userspace, using `IORING_OP_SPLICE` through a per-call pipe.

**Architecture:** A new no-buffer io_uring op (`SourceType::Splice`) follows the existing `connect`/`fallocate` pattern exactly. On top of it, `TcpStream::send_file` opens a nonblocking pipe per call and loops: splice file→pipe, drain pipe→socket, advance. A sealed `SpliceSource` trait supplies the descriptor and the offset alignment `O_DIRECT` requires.

**Tech Stack:** Rust, io-uring 0.7.14 (`opcode::Splice`, `opcode.rs:1215`), libc, Linux 5.7+.

**Spec:** `docs/superpowers/specs/2026-08-26-splice-send-file-design.md`

## Global Constraints

- **No new dependencies.** `opcode::Splice` is already in the pinned io-uring 0.7.14. Tests use `std::fs` and `std::env::temp_dir()`; there is no `tempfile` dev-dependency and you must not add one.
- **Pipe capacity is 65536.** Any transfer larger than that loops.
- **The pipe MUST be `O_NONBLOCK`.** Without it, `splice(file → pipe)` blocks forever once the pipe fills. This was reproduced: the probe hung with no output.
- **`O_DIRECT` requires an offset aligned to the device logical block size.** Length does not need alignment; an over-long length is capped at pipe capacity.
- **Run `make fmt` before every commit** and `make ci` before pushing. `make ci` flakes on timer tests roughly 1 run in 3 — re-run before believing a failure, and investigate only if a *non-timer* test fails.
- **Every new test must be mutation-checked.** Break the implementation deliberately, confirm the test fails, restore. A test that passes for the wrong reason is this repo's most expensive recurring bug.
- Commits are signed off (`git commit -s`) and never mention AI assistance.

---

### Task 1: Ring plumbing for splice

**Files:**
- Modify: `glommio/src/sys/source.rs` (add `SourceType::Splice` to the enum near line 32)
- Modify: `glommio/src/sys/uring.rs` (add `UringOpDescriptor::Splice` near line 52, a match arm in `fill_sqe` near line 490, and a `splice` method near the `fallocate` method at line 1736)
- Modify: `glommio/src/reactor.rs` (add `Reactor::splice` near `fallocate` at line 592)
- Test: `glommio/src/sys/uring.rs` (new `#[cfg(test)] mod splice_tests` at end of file)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `SourceType::Splice` — a unit variant carrying no data.
  - `Reactor::splice(&self, fd_in: RawFd, off_in: i64, fd_out: RawFd, len: u32) -> Source` — `pub(crate)`. `off_out` is always `-1` internally, because both our destinations (a pipe, a socket) are unseekable.
  - Awaiting the returned `Source` via `source.collect_rw().await` yields `io::Result<usize>`, the number of bytes moved.

**Why this is safe:** `SourceType::Splice` carries no buffer. Nothing of ours is lent to the kernel — the op is two descriptors and a length — so there is no analogue of the buffer-lifetime hazard that made `MSG_ZEROCOPY` a use-after-free.

**Note:** every existing `match` on `SourceType` has a `_ =>` fallback arm (checked: `sys/source.rs:299`, `sys/uring.rs:381,459,514,533,545,1104`), so adding a variant will not break compilation elsewhere.

- [ ] **Step 1: Write the failing test**

Splices bytes from one pipe to another through the reactor. Pipes only — no filesystem, so the test is independent of the mount it runs on.

Add at the end of `glommio/src/sys/uring.rs`:

```rust
#[cfg(test)]
mod splice_tests {
    use crate::LocalExecutor;
    use std::os::unix::io::RawFd;

    fn nonblocking_pipe() -> (RawFd, RawFd) {
        let mut fds = [0 as RawFd; 2];
        let ok = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        assert_eq!(ok, 0, "pipe2 failed");
        (fds[0], fds[1])
    }

    #[test]
    fn splice_moves_bytes_between_pipes() {
        LocalExecutor::default().run(async {
            let (src_r, src_w) = nonblocking_pipe();
            let (dst_r, dst_w) = nonblocking_pipe();

            let payload = b"spliced through the ring";
            let written =
                unsafe { libc::write(src_w, payload.as_ptr() as *const libc::c_void, payload.len()) };
            assert_eq!(written, payload.len() as isize);

            let reactor = crate::executor().reactor();
            // off_in is -1: the source is a pipe, which is not seekable.
            let source = reactor.splice(src_r, -1, dst_w, payload.len() as u32);
            let moved = source.collect_rw().await.unwrap();
            assert_eq!(moved, payload.len(), "splice should move the whole payload");

            let mut got = vec![0u8; payload.len()];
            let read = unsafe { libc::read(dst_r, got.as_mut_ptr() as *mut libc::c_void, got.len()) };
            assert_eq!(read, payload.len() as isize);
            assert_eq!(&got[..], payload, "bytes must survive the splice intact");

            for fd in [src_r, src_w, dst_r, dst_w] {
                unsafe { libc::close(fd) };
            }
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p glommio --lib splice_tests -- --nocapture`
Expected: FAIL to compile, `no method named 'splice' found for struct 'Reactor'`.

- [ ] **Step 3: Add the `SourceType` variant**

In `glommio/src/sys/source.rs`, inside `pub(crate) enum SourceType`, after the `Fallocate` variant:

```rust
    /// A splice between two descriptors. Carries nothing: no buffer of ours
    /// is lent to the kernel, only two descriptors and a length.
    Splice,
```

- [ ] **Step 4: Add the uring op descriptor and its SQE**

In `glommio/src/sys/uring.rs`, add to `enum UringOpDescriptor` (near line 52):

```rust
    Splice {
        fd_in: RawFd,
        off_in: i64,
        off_out: i64,
        len: u32,
        flags: u32,
    },
```

Then add a match arm in `fill_sqe`, next to the `WriteFixed` arm (near line 490). Note `fd` is already bound as `types::Fd(op.fd)` at line 357 and is the *output* descriptor, because `opcode::Splice` assigns `sqe.fd = fd_out`:

```rust
            UringOpDescriptor::Splice {
                fd_in,
                off_in,
                off_out,
                len,
                flags,
            } => opcode::Splice::new(types::Fd(fd_in), off_in, fd, off_out, len)
                .flags(flags)
                .build(),
```

- [ ] **Step 5: Add the uring submit method**

In `glommio/src/sys/uring.rs`, next to `fallocate` (line 1736):

```rust
    pub(crate) fn splice(&self, source: &Source, fd_in: RawFd, off_in: i64, len: u32) {
        let op = UringOpDescriptor::Splice {
            fd_in,
            off_in,
            // Both destinations we splice into -- a pipe and a socket -- are
            // unseekable, so the output offset is always -1.
            off_out: -1,
            len,
            flags: 0,
        };
        queue_request_into_ring(
            &mut *self.ring_for_source(source),
            source,
            op,
            &mut self.source_map.borrow_mut(),
        );
    }
```

- [ ] **Step 6: Add the reactor entry point**

In `glommio/src/reactor.rs`, next to `fallocate` (line 592):

```rust
    pub(crate) fn splice(&self, fd_in: RawFd, off_in: i64, fd_out: RawFd, len: u32) -> Source {
        let source = self.new_source(fd_out, SourceType::Splice, None);
        self.sys.splice(&source, fd_in, off_in, len);
        source
    }
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cargo test -p glommio --lib splice_tests -- --nocapture`
Expected: PASS.

- [ ] **Step 8: Mutation-check the test**

Change the `len` passed to `opcode::Splice::new` to `0` in the `fill_sqe` arm. Re-run: the test must fail with `moved == 0`. Restore the correct code and confirm it passes again.

- [ ] **Step 9: Commit**

```bash
make fmt
git add glommio/src/sys/source.rs glommio/src/sys/uring.rs glommio/src/reactor.rs
git commit -s -m "$(cat <<'EOF'
feat: put splice on the ring

IORING_OP_SPLICE moves page references between descriptors without the
bytes passing through userspace. The op carries no buffer of ours, only
two descriptors and a length, so SourceType::Splice holds nothing and
there is no completion-lifetime obligation of the kind a zero-copy send
would impose.

Not yet reachable from public API; the file-to-socket path is next.
EOF
)"
```

---

### Task 2: The `SpliceSource` trait

**Files:**
- Create: `glommio/src/io/splice_source.rs`
- Modify: `glommio/src/io/mod.rs` (declare the module, re-export the trait from the `pub use self::{...}` block at line 159)
- Test: inside `glommio/src/io/splice_source.rs`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces:
  - `pub trait SpliceSource: sealed::Sealed` with `fn splice_fd(&self) -> RawFd` and `fn splice_offset_alignment(&self) -> u64`.
  - Implementations for `BufferedFile` (alignment `1`) and `DmaFile` (alignment = its `o_direct_alignment`).
  - Exported as `glommio::io::SpliceSource`.

**Why the alignment method exists:** splicing from an `O_DIRECT` descriptor at a misaligned offset fails `EINVAL` (verified: offset 4095 fails; 0, 512, 1024, 1536, 4096, 8192 succeed). A bare `EINVAL` here is close to undiagnosable, so `send_file` checks up front.

`DmaFile` already stores `o_direct_alignment` (`io/dma_file.rs:141`). The impl lives in this new file, so add a `pub(crate) fn o_direct_alignment(&self) -> u64` accessor to `DmaFile` in `io/dma_file.rs` next to `align_up` (line 149) — the field is private to its own module.

- [ ] **Step 1: Write the failing test**

Create `glommio/src/io/splice_source.rs` containing only the test for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{BufferedFile, DmaFile};
    use crate::LocalExecutor;
    use std::os::unix::io::AsRawFd;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("glommio-splice-{}-{}", std::process::id(), name));
        p
    }

    #[test]
    fn buffered_file_needs_no_alignment() {
        LocalExecutor::default().run(async {
            let path = tmp_path("buffered-align");
            let file = BufferedFile::create(&path).await.unwrap();
            assert_eq!(
                file.splice_offset_alignment(),
                1,
                "a page-cache file can be spliced from any offset"
            );
            assert_eq!(file.splice_fd(), file.as_raw_fd());
            file.close().await.unwrap();
            std::fs::remove_file(&path).ok();
        });
    }

    #[test]
    fn dma_file_reports_its_o_direct_alignment() {
        LocalExecutor::default().run(async {
            let path = tmp_path("dma-align");
            let file = DmaFile::create(&path).await.unwrap();
            let alignment = file.splice_offset_alignment();
            assert!(
                alignment.is_power_of_two(),
                "alignment must be a power of two, got {alignment}"
            );
            assert_eq!(
                alignment,
                file.align_up(1),
                "the trait must report the same alignment the file already enforces"
            );
            assert_eq!(file.splice_fd(), file.as_raw_fd());
            file.close().await.unwrap();
            std::fs::remove_file(&path).ok();
        });
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p glommio --lib splice_source -- --nocapture`
Expected: FAIL to compile — the module is not declared and the trait does not exist.

- [ ] **Step 3: Write the trait and its implementations**

Put this above the test module in `glommio/src/io/splice_source.rs`:

```rust
// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::{BufferedFile, DmaFile};
use std::os::unix::io::{AsRawFd, RawFd};

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::io::BufferedFile {}
    impl Sealed for crate::io::DmaFile {}
}

/// A file whose contents can be sent straight to a socket, without the bytes
/// passing through this process.
///
/// Implemented for [`BufferedFile`] and [`DmaFile`]. It is sealed: the two
/// methods below are an internal contract with
/// [`send_file`](crate::net::TcpStream::send_file), not an extension point.
pub trait SpliceSource: sealed::Sealed {
    /// The descriptor to splice from.
    fn splice_fd(&self) -> RawFd;

    /// The alignment a splice offset must satisfy.
    ///
    /// `1` for a page-cache file, which accepts any offset. A file opened
    /// `O_DIRECT` requires its offset to be a multiple of the device's
    /// logical block size, and splicing from a misaligned offset fails with
    /// `EINVAL`.
    fn splice_offset_alignment(&self) -> u64;
}

impl SpliceSource for BufferedFile {
    fn splice_fd(&self) -> RawFd {
        self.as_raw_fd()
    }

    fn splice_offset_alignment(&self) -> u64 {
        1
    }
}

impl SpliceSource for DmaFile {
    fn splice_fd(&self) -> RawFd {
        self.as_raw_fd()
    }

    fn splice_offset_alignment(&self) -> u64 {
        self.o_direct_alignment()
    }
}
```

- [ ] **Step 4: Add the `DmaFile` accessor and declare the module**

In `glommio/src/io/dma_file.rs`, next to `align_up` (line 149):

```rust
    /// The alignment `O_DIRECT` imposes on offsets and lengths for this file.
    pub(crate) fn o_direct_alignment(&self) -> u64 {
        self.o_direct_alignment
    }
```

In `glommio/src/io/mod.rs`, add `mod splice_source;` alongside the other module declarations, and add `splice_source::SpliceSource,` to the `pub use self::{...}` block at line 159.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p glommio --lib splice_source -- --nocapture`
Expected: PASS, both tests.

- [ ] **Step 6: Mutation-check the test**

Change `BufferedFile`'s `splice_offset_alignment` to return `512`. Re-run: `buffered_file_needs_no_alignment` must fail. Restore.

- [ ] **Step 7: Commit**

```bash
make fmt
git add glommio/src/io/splice_source.rs glommio/src/io/mod.rs glommio/src/io/dma_file.rs
git commit -s -m "$(cat <<'EOF'
feat: describe which files can be spliced, and at what alignment

Both file kinds can be spliced -- measured, including that an O_DIRECT
splice honours O_DIRECT and leaves the page cache untouched -- so the
trait exists for the one way they differ. A page-cache file accepts any
offset; an O_DIRECT file requires a multiple of the device logical block
size and fails EINVAL otherwise, which is not an error anyone can act on
by the time it surfaces.

Sealed, because the two methods are an internal contract with send_file
rather than an extension point.
EOF
)"
```

---

### Task 3: `send_file`, single chunk

**Files:**
- Modify: `glommio/src/net/tcp_socket.rs` (add `send_file` to the `impl<B: RxBuf + Unpin> TcpStream<B>` block that already holds `recv_tls_record`, near line 801)
- Test: `glommio/tests/send_file.rs` (create)

**Interfaces:**
- Consumes: `Reactor::splice` (Task 1), `SpliceSource` (Task 2).
- Produces: `TcpStream::send_file<F: SpliceSource>(&mut self, file: &F, offset: u64, len: usize) -> Result<usize>`.

This task covers only a payload smaller than one pipe-load (65536) with the socket never applying backpressure. The loop and the `EAGAIN` path come in Tasks 4 and 6.

- [ ] **Step 1: Write the failing test**

Create `glommio/tests/send_file.rs`. It is an integration test — a separate crate — because this repo has twice shipped public API that no unit test could see was unusable.

```rust
//! `TcpStream::send_file` driven the way a consumer drives it.

use futures_lite::io::AsyncReadExt;
use glommio::{
    io::BufferedFile,
    net::{TcpListener, TcpStream},
    LocalExecutor,
};

fn tmp_path(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
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

        assert_eq!(sent, expected_len, "send_file should report every byte sent");
        let got = reader.await.unwrap();
        assert_eq!(got, payload, "the bytes on the wire must match the file");

        std::fs::remove_file(&path).ok();
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p glommio --test send_file -- --nocapture`
Expected: FAIL to compile, `no method named 'send_file'`.

- [ ] **Step 3: Write the implementation**

Add to `glommio/src/net/tcp_socket.rs`. Put it in the same `impl<B: RxBuf + Unpin> TcpStream<B>` block as `recv_tls_record`, and add `use crate::io::SpliceSource;` to the imports.

```rust
    /// Sends `len` bytes of `file`, starting at `offset`, without the bytes
    /// entering this process.
    ///
    /// The kernel moves page references through a pipe rather than copying
    /// through a user buffer, so the cost does not scale with the size of the
    /// payload the way a read-then-write does.
    ///
    /// Returns the number of bytes actually sent, which is short if the file
    /// ends before `len` bytes are available.
    ///
    /// # Alignment
    ///
    /// A [`DmaFile`](crate::io::DmaFile) is open `O_DIRECT`, which requires
    /// `offset` to be a multiple of the device's logical block size. Passing a
    /// misaligned offset returns an error rather than reaching the kernel,
    /// where it would surface as an unexplained `EINVAL`. A
    /// [`BufferedFile`](crate::io::BufferedFile) accepts any offset.
    pub async fn send_file<F: SpliceSource>(
        &mut self,
        file: &F,
        offset: u64,
        len: usize,
    ) -> Result<usize> {
        let pipe = Pipe::new()?;
        let reactor = crate::executor().reactor();
        let fd_in = file.splice_fd();
        // `GlommioStream` implements `AsRawFd` only for `NonBuffered`, so reach
        // the socket the way this file's own `AsRawFd` impl does.
        let fd_out = self.stream.stream().as_raw_fd();

        let mut sent = 0usize;
        let mut pos = offset;

        while sent < len {
            let want = std::cmp::min(len - sent, PIPE_CAPACITY) as u32;
            let filled = reactor
                .splice(fd_in, pos as i64, pipe.writer(), want)
                .collect_rw()
                .await?;
            if filled == 0 {
                // End of file: nothing more to send.
                break;
            }

            let mut drained = 0usize;
            while drained < filled {
                let moved = reactor
                    .splice(pipe.reader(), -1, fd_out, (filled - drained) as u32)
                    .collect_rw()
                    .await?;
                drained += moved;
            }

            pos += filled as u64;
            sent += filled;
        }

        Ok(sent)
    }
```

Add these above the `impl` block in the same file:

```rust
/// A pipe's capacity, and so the most one splice can move.
const PIPE_CAPACITY: usize = 65536;

/// A pipe owned for the duration of one `send_file`.
///
/// Per call rather than pooled, and `O_NONBLOCK` rather than blocking. Both
/// are load-bearing: a blocking pipe makes `splice` into a full pipe block
/// forever, and a pipe reused across calls could carry one transfer's
/// leftovers into the next. A pipe that dies with its call cannot do either.
struct Pipe([RawFd; 2]);

impl Pipe {
    fn new() -> Result<Self> {
        let mut fds = [0 as RawFd; 2];
        let ok = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if ok < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Pipe(fds))
    }

    fn reader(&self) -> RawFd {
        self.0[0]
    }

    fn writer(&self) -> RawFd {
        self.0[1]
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        // Closed on every exit path, including a panic or a cancelled future,
        // so a half-drained pipe is never reachable by anything else.
        for fd in self.0 {
            unsafe { libc::close(fd) };
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p glommio --test send_file -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Mutation-check the test**

Change `sent += filled;` to `sent += filled; break;` so only one chunk is ever sent, then separately make `send_file` return `Ok(0)` without sending. Each must fail the test. Restore.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio/src/net/tcp_socket.rs glommio/tests/send_file.rs
git commit -s -m "$(cat <<'EOF'
feat: send a file to a socket without copying it through userspace

send_file splices the file into a pipe and the pipe into the socket, so
the bytes never enter this process. Sending a response body cost two
copies before this: device to page cache, page cache to a user buffer,
user buffer to the socket.

The pipe is created per call and is O_NONBLOCK, both deliberately. A
blocking pipe blocks forever once it fills, and a pipe shared between
calls could carry one transfer's leftovers into the next; a pipe that
dies with its call puts both out of reach rather than handling them.

The test is an integration test because it has to see what a consumer
sees. From inside the crate every module is in scope, which is how two
unusable public APIs shipped from here before.
EOF
)"
```

---

### Task 4: The loop, and a short file

**Files:**
- Modify: `glommio/tests/send_file.rs`

**Interfaces:**
- Consumes: `send_file` (Task 3). No signature changes; Task 3's loop should already handle both cases, and these tests prove it.

- [ ] **Step 1: Write the failing tests**

Append to `glommio/tests/send_file.rs`:

```rust
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
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p glommio --test send_file -- --nocapture`
Expected: PASS. Task 3's implementation already loops; these prove it.

If `a_file_larger_than_the_pipe_arrives_intact` hangs, the pipe is not `O_NONBLOCK` — check `Pipe::new`.

- [ ] **Step 3: Mutation-check each test**

Run each mutation, confirm the named test fails, then restore:

- Replace `pos += filled as u64;` with `pos += 0;` → `sending_from_an_offset_skips_the_prefix` and the large-file test must fail (the same chunk is sent repeatedly).
- Replace `let want = std::cmp::min(len - sent, PIPE_CAPACITY) as u32;` with `let want = (len - sent) as u32;` → the large-file test must still pass (splice caps at capacity) but note the result; this confirms the `min` is defensive rather than load-bearing, which is worth knowing.
- Replace `if filled == 0 { break; }` with `if filled == 0 { continue; }` → `asking_past_the_end_returns_short_rather_than_erroring` must hang or spin. Restore immediately.

- [ ] **Step 4: Commit**

```bash
make fmt
git add glommio/tests/send_file.rs
git commit -s -m "$(cat <<'EOF'
test: cover the send_file loop, a short file, and an offset

A pipe holds 65536 bytes, so anything larger goes round the loop more
than once and a single-chunk implementation would truncate silently.
The offset test asserts the window rather than only the length, because
a send_file that ignored the offset would satisfy a length-only
assertion.

Asking past the end of a file is short rather than an error: a caller
that trusts a stale stat should get the bytes that exist, not a failure.
EOF
)"
```

---

### Task 5: Reject a misaligned offset before the kernel sees it

**Files:**
- Modify: `glommio/src/net/tcp_socket.rs` (`send_file`)
- Modify: `glommio/tests/send_file.rs`

**Interfaces:**
- Consumes: `SpliceSource::splice_offset_alignment` (Task 2), `send_file` (Task 3).
- Produces: no signature change. `send_file` returns `Err` for a misaligned offset on an `O_DIRECT` file.

- [ ] **Step 1: Write the failing test**

Append to `glommio/tests/send_file.rs`, and add `use glommio::io::DmaFile;` to the imports:

```rust
#[test]
fn a_misaligned_offset_on_a_dma_file_is_an_error_not_an_einval() {
    LocalExecutor::default().run(async {
        let path = tmp_path("dma-misaligned");
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
        let accepted = glommio::spawn_local(async move { listener.accept().await.unwrap() }).detach();
        let mut writer = TcpStream::connect(addr).await.unwrap();
        let _reader = accepted.await.unwrap();

        // One byte past an aligned boundary: never valid under O_DIRECT.
        let result = writer.send_file(&file, alignment + 1, 4096).await;
        assert!(
            result.is_err(),
            "a misaligned offset must be refused, not sent to the kernel"
        );

        // An aligned offset on the same file still works, so the check is not
        // simply refusing everything.
        let ok = writer.send_file(&file, alignment, 4096).await;
        assert!(ok.is_ok(), "an aligned offset must still be accepted");

        file.close().await.unwrap();
        std::fs::remove_file(&path).ok();
    });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p glommio --test send_file a_misaligned -- --nocapture`
Expected: FAIL — without the check, the kernel returns `EINVAL` and the assertion on `is_err()` may pass for the wrong reason, or the call may succeed. Read the output: if it already errors, confirm via the message that it is a raw `EINVAL` and not glommio's own error.

- [ ] **Step 3: Add the check**

At the top of `send_file`, before creating the pipe:

```rust
        let alignment = file.splice_offset_alignment();
        if alignment > 1 && offset % alignment != 0 {
            return Err(crate::GlommioError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "send_file offset {offset} is not a multiple of {alignment}; a file opened \
                     O_DIRECT can only be spliced from an aligned offset"
                ),
            )));
        }
```

`GlommioError::IoError(io::Error)` is the right variant (`glommio/src/error.rs:214`), and `send_file` returns the file-local `type Result<T> = crate::Result<T, ()>` (`net/tcp_socket.rs:40`), so the `()` parameter is inferred at the return position.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p glommio --test send_file a_misaligned -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Mutation-check**

Change the condition to `if false` → the test must fail (or the error message must no longer name the alignment). Change it to `if true` → the aligned half of the test must fail. Restore.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio/src/net/tcp_socket.rs glommio/tests/send_file.rs
git commit -s -m "$(cat <<'EOF'
fix: refuse a misaligned send_file offset instead of passing it down

Splicing from an O_DIRECT descriptor at an offset that is not a multiple
of the device logical block size fails EINVAL. Arriving from inside a
send path, with no mention of alignment or of which offset was wrong,
that is close to undiagnosable.

Checked before anything is submitted, and the error names both the
offset and the alignment it had to satisfy. The test asserts an aligned
offset still works, so the check cannot pass by refusing everything.
EOF
)"
```

---

### Task 6: Backpressure — the `EAGAIN` path

**Files:**
- Modify: `glommio/src/net/tcp_socket.rs` (`send_file`)
- Modify: `glommio/tests/send_file.rs`

**Interfaces:**
- Consumes: `Reactor::poll_read_ready`'s sibling `Reactor::poll_write_ready(fd) -> Source` (`glommio/src/reactor.rs:372`).
- Produces: no signature change.

**Why this matters:** `splice(pipe → socket)` returns `EAGAIN` whenever the send buffer is full, which for any file worth splicing is most iterations, not an edge case. Verified: with `SO_SNDBUF` at 4096, `pipe → socket` returned `EAGAIN` after 65482 bytes, and after the peer read 4096 bytes it *still* returned `EAGAIN` — so writability must be waited on, not assumed after a partial drain. Getting this wrong looks like a hang on large files under load.

- [ ] **Step 1: Write the failing test**

Append to `glommio/tests/send_file.rs`, adding `use std::os::unix::io::AsRawFd;` to the imports:

```rust
#[test]
fn a_slow_reader_does_not_stall_send_file() {
    // A small send buffer plus a reader that only drains after the send has
    // begun forces splice(pipe -> socket) to return EAGAIN repeatedly. An
    // implementation that does not wait for writability hangs here.
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

        assert_eq!(sent, expected_len, "backpressure must not truncate the send");
        assert_eq!(reader.await.unwrap(), payload, "nor corrupt it");

        std::fs::remove_file(&path).ok();
    });
}
```

`libc` needs no Cargo change: it is a normal dependency (`glommio/Cargo.toml:54`), and Cargo makes normal dependencies available to integration tests alongside dev-dependencies. `glommio/tests/ktls_records.rs` already relies on this.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p glommio --test send_file a_slow_reader -- --nocapture`
Expected: the test **hangs**, or fails with an `EAGAIN` error surfacing from `collect_rw`. Kill it after ~30 seconds if it hangs; that hang is the failure this task fixes.

- [ ] **Step 3: Handle `EAGAIN` by waiting for writability**

Replace the inner drain loop in `send_file` with:

```rust
            let mut drained = 0usize;
            while drained < filled {
                let moved = match reactor
                    .splice(pipe.reader(), -1, fd_out, (filled - drained) as u32)
                    .collect_rw()
                    .await
                {
                    Ok(moved) => moved,
                    Err(err) if err.raw_os_error() == Some(libc::EAGAIN) => {
                        // The send buffer is full. Park until the socket is
                        // writable again rather than spinning: after a partial
                        // drain by the peer, an immediate retry still returns
                        // EAGAIN.
                        reactor.poll_write_ready(fd_out).collect_rw().await?;
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                drained += moved;
            }
```

Note the outer `splice(file → pipe)` needs no such handling: the pipe is drained fully before it is refilled, so it is never full when written to. A regular file is always ready, so there is nothing to wait on there.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p glommio --test send_file -- --nocapture`
Expected: PASS, all tests.

- [ ] **Step 5: Mutation-check**

Replace the `EAGAIN` arm with `Err(err) => return Err(err.into()),` (i.e. drop the special case) → `a_slow_reader_does_not_stall_send_file` must fail or hang. Then replace `reactor.poll_write_ready(fd_out).collect_rw().await?; continue;` with a bare `continue;` → the test must still pass but will spin the CPU; confirm by watching it take noticeably longer, then restore. That second mutation demonstrates the wait is about not spinning, not about correctness.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio/src/net/tcp_socket.rs glommio/tests/send_file.rs
git commit -s -m "$(cat <<'EOF'
fix: wait for writability when a slow peer fills the send buffer

Splicing into a socket returns EAGAIN as soon as the send buffer is
full, which for any file large enough to be worth splicing is most
iterations rather than an edge case. Without a wait, send_file either
surfaces EAGAIN to a caller who cannot act on it or spins.

Retrying immediately is not enough: with the send buffer squeezed to
4096 bytes, a retry straight after the peer read 4096 bytes still
returned EAGAIN. The socket has to be waited on.

Only the pipe-to-socket direction needs this. The pipe is drained fully
before it is refilled, so it is never full when written to, and a
regular file is always ready.
EOF
)"
```

---

### Task 7: Prove it from outside, and document it

**Files:**
- Modify: `glommio/tests/public_api_is_usable.rs`
- Modify: `glommio/src/net/tcp_socket.rs` (rustdoc example on `send_file`)
- Modify: `docs/FEATURE_GAP.md`

**Interfaces:**
- Consumes: everything above. No new API.

This task exists because of a rule this repo learned twice the hard way: a type can be reachable in the type graph and unnameable in practice, and `#![deny(unreachable_pub)]` does not catch it.

- [ ] **Step 1: Add the reachability check**

Append to `glommio/tests/public_api_is_usable.rs`, following the shape of what is already there:

```rust
/// `send_file` must be callable, and `SpliceSource` nameable, from outside.
/// A trait that cannot be named is a trait whose method cannot be discussed
/// in a downstream signature.
#[test]
fn send_file_and_splice_source_are_reachable() {
    use glommio::io::{BufferedFile, SpliceSource};

    // Nameable in a signature is the property under test.
    fn alignment_of<F: SpliceSource>(f: &F) -> u64 {
        f.splice_offset_alignment()
    }

    glommio::LocalExecutor::default().run(async {
        let mut path = std::env::temp_dir();
        path.push(format!("glommio-public-send-file-{}", std::process::id()));
        std::fs::write(&path, b"reachable").unwrap();

        let file = BufferedFile::open(&path).await.unwrap();
        assert_eq!(alignment_of(&file), 1);

        let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted =
            glommio::spawn_local(async move { listener.accept().await.unwrap() }).detach();
        let mut writer = glommio::net::TcpStream::connect(addr).await.unwrap();
        let _reader = accepted.await.unwrap();

        let sent = writer.send_file(&file, 0, b"reachable".len()).await.unwrap();
        assert_eq!(sent, b"reachable".len());

        file.close().await.unwrap();
        std::fs::remove_file(&path).ok();
    });
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p glommio --test public_api_is_usable -- --nocapture`
Expected: PASS. If `SpliceSource` cannot be imported, fix the re-export in `glommio/src/io/mod.rs` — that is exactly the bug this test exists to catch.

- [ ] **Step 3: Add a rustdoc example**

Add to the `send_file` doc comment, before the `# Alignment` section:

```rust
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{io::BufferedFile, net::TcpStream, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async {
    /// let file = BufferedFile::open("index.html").await.unwrap();
    /// let size = file.file_size().await.unwrap() as usize;
    /// let mut stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
    /// let sent = stream.send_file(&file, 0, size).await.unwrap();
    /// assert_eq!(sent, size);
    /// # });
    /// ```
```

`BufferedFile::file_size(&self) -> Result<u64>` exists (`io/buffered_file.rs:248`), so the example compiles as written.

- [ ] **Step 4: Record it in the feature gap doc**

In `docs/FEATURE_GAP.md`, move or add an entry noting that zero-copy file-to-socket is now covered by `TcpStream::send_file`, and that `UnixStream::send_file` and `ImmutableFile` support remain unimplemented.

- [ ] **Step 5: Run the full gate**

Run: `make ci`
Expected: green. If a *timer* test fails, re-run — that flake is pre-existing at roughly 1 run in 3. Investigate only a non-timer failure.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio/tests/public_api_is_usable.rs glommio/src/net/tcp_socket.rs docs/FEATURE_GAP.md
git commit -s -m "$(cat <<'EOF'
test: prove send_file and SpliceSource are reachable from outside

A public trait can be reachable in the type graph and unnameable in
practice, and deny(unreachable_pub) does not catch it -- that is what
hid RxBuf for years. The only vantage point that sees the difference is
a separate crate, so the check names SpliceSource in a generic bound
rather than only calling send_file.
EOF
)"
```

---

### Task 8: Measure the crossover

**Files:**
- Create: `glommio/examples/send_file_ladder.rs`
- Modify: `docs/investigations/io-path/network.md`

**Interfaces:**
- Consumes: `send_file`. No API changes.

The spec deliberately commits to building the feature without claiming a speedup. This task establishes the size below which `send_file` loses to a plain read-and-write, which is what a caller needs in order to gate on it. **It does not gate the feature** — a negative result here means "document the threshold", not "revert".

Follow the conventions the existing ladders use (`glommio/examples/recv_ladder.rs`): server and client pinned to separate cores, warm-up discarded, and **`CLOCK_THREAD_CPUTIME_ID` on the server thread** — wall clock cannot rank these because the client is the bottleneck.

- [ ] **Step 1: Read an existing ladder for the house pattern**

Run: `sed -n 1,80p glommio/examples/recv_ladder.rs`

Copy its measurement harness rather than inventing one. The traps it already avoids are recorded in `docs/investigations/io-path/network.md`.

- [ ] **Step 2: Write the ladder**

Create `glommio/examples/send_file_ladder.rs` comparing, at each of several file sizes (1 KiB, 4 KiB, 16 KiB, 64 KiB, 256 KiB, 1 MiB, 8 MiB):

1. `read` into a buffer then `write_all` — what callers do today.
2. `TcpStream::send_file` — the new path.

Report CPU nanoseconds per response for each rung, and the ratio. Discard a warm-up round. Read the file fresh each iteration so the page-cache state is representative rather than accidentally ideal.

- [ ] **Step 3: Run it**

Run: `cargo run --release --example send_file_ladder`
Expected: a table. Record the size at which `send_file` overtakes read-and-write.

- [ ] **Step 4: Write the findings down**

Add a section to `docs/investigations/io-path/network.md` with the numbers and the crossover size, stated plainly including if the result is unflattering. If `send_file` never wins in the measured range, say so and say what that implies for the doc comment.

- [ ] **Step 5: Reflect the threshold in the docs**

Update the `send_file` rustdoc with the measured crossover so a caller can gate on size rather than guess.

- [ ] **Step 6: Commit**

```bash
make fmt
git add glommio/examples/send_file_ladder.rs docs/investigations/io-path/network.md glommio/src/net/tcp_socket.rs
git commit -s -m "$(cat <<'EOF'
perf: measure send_file against read plus write

The feature was built without a claimed speedup, so this establishes the
size below which splicing loses to copying and writes it into the doc
comment, where a caller deciding whether to reach for it can see it.

Measured on the server thread with CLOCK_THREAD_CPUTIME_ID: wall clock
cannot rank these because the client is the bottleneck, which is the
error that made an earlier ladder report a regression that was not
there.
EOF
)"
```

---

## Self-Review

**Spec coverage.** Every section of the spec maps to a task: public API → 3, `SpliceSource` and alignment → 2 and 5, mechanism and the per-call `O_NONBLOCK` pipe → 3, plumbing → 1, the error table → 3/4/5/6, testing → 3/4/5/6/7, deferred items → untouched by design. The spec's "open question for the consumer" needs no task; both file types work.

**Deferred, and deliberately absent from every task:** `IOSQE_IO_LINK`, a reactor pipe pool, `F_SETPIPE_SZ`, `UnixStream::send_file`, and `SpliceSource for ImmutableFile`.

**Type consistency.** `Reactor::splice(fd_in, off_in, fd_out, len)` is defined in Task 1 and used with that argument order in Tasks 3 and 6. `SpliceSource::splice_fd`/`splice_offset_alignment` are defined in Task 2 and used in Tasks 3, 5 and 7. `PIPE_CAPACITY` and `Pipe` are introduced in Task 3 and referenced in Tasks 4 and 6.

**One known soft spot.** Task 5's step 2 may show the test passing before the fix, because the kernel's own `EINVAL` also produces `Err`. The step says to read the error message and confirm it is glommio's, not the kernel's — an implementer who skips that will record a false "it failed first". This is the weakest gate in the plan and worth extra care.
