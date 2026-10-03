# Zero-copy file to socket: `TcpStream::send_file`

**Status:** design, not built
**Date:** 2026-08-26

## The gap

Sending a file over a socket costs two copies today: device to page cache,
page cache to a user buffer, user buffer to the socket. glommio has no path
that avoids them, and no `splice` anywhere in `glommio/src` — the only mention
is a doc comment at `io/dma_file.rs:566` comparing `copy_file_range` to it.

`splice(2)` moves page references through a pipe instead of bytes through
userspace. It cannot go file to socket directly; a pipe is mandatory, so the
transfer is two calls per chunk.

The consumer is collimator, an HTTP framework built on this fork. Its `File`
response body is its entire zero-copy story, and it already counts every
chunk that degrades for want of this
(`crates/collimator-http/src/metrics.rs:22`, `file_read_fallbacks`).

`opcode::Splice` is in the pinned io-uring 0.7.14 at `opcode.rs:1215`
(kernel 5.7+). No dependency moves.

## What was measured before designing

Every claim below was probed on this machine (xfs on NVMe, logical block size
512, kernel 7.2.0), because the first version of this design was built on an
assumption that turned out to be false.

**Splice works on `O_DIRECT`, and honors it.** The design originally scoped
this feature to `BufferedFile` on the reasoning that splice needs page-cache
pages to refcount and `O_DIRECT` deliberately has none. That is wrong.
Splicing 256 KiB through an `O_DIRECT` descriptor returned correct bytes and
left the page cache at **0 of 256 pages resident**; the same splice through a
buffered descriptor left 160 pages resident. So `DmaFile` can be spliced
without silently reintroducing the caching its user opened `O_DIRECT` to
avoid.

**`O_DIRECT` splice requires an aligned source offset; length is free.**
Offset 4095 fails `EINVAL`; 0, 512, 1024, 1536, 4096 and 8192 all succeed.
The granularity is the device logical block size, which `DmaFile` already
tracks as `o_direct_alignment` (`io/dma_file.rs:141`). An over-long length is
harmless — it is capped at pipe capacity.

**Pipe capacity is 65536**, so any transfer larger than that is a loop
regardless of design.

**Both splices can return `EAGAIN`, and a blocking pipe hangs — true of the
raw syscall, corrected below for the path this design actually uses.** With a
blocking pipe, `splice(file → pipe)` on a full pipe blocks forever — the
first probe hung with no output, which is the failure mode this design must
structurally prevent; that result stands. The `EAGAIN` claim that followed it
does not, for the path this design ships: it came from a raw `splice(2)`
syscall probed directly in Python, outside io_uring. With `O_NONBLOCK` on
that raw path, `file → pipe` returns `EAGAIN` when the pipe is full, and
`pipe → socket` returns `EAGAIN` when the send buffer is full; after the peer
read 4096 bytes, `pipe → socket` still returned `EAGAIN` there, so the raw
syscall's writability genuinely needs to be waited on rather than assumed
after a partial drain. This is real and true of `splice(2)` itself — it is
simply the wrong path to have measured, since `send_file` never calls
`splice(2)` directly.

Through this reactor's io_uring, submitted with `SPLICE_F_NONBLOCK` on the
`pipe → socket` leg, that `EAGAIN` was never observed: every splice
completed with the full requested length in one shot, flag or not, never a
short count or an `EAGAIN` error, even against a genuinely stalled peer that
measurably backpressured the transfer. That much is measured directly, by
instrumenting the drain loop. *Why* is not verified against kernel source
and is stated here as a hypothesis, with two candidates: either io_uring's
poll-based retry for pollable descriptors resolves the wait before posting a
completion, in which case a different kernel or ring mode could still
surface `EAGAIN`; or `IORING_OP_SPLICE` punts to an io-wq worker where
`-EAGAIN` is purely an internal retry signal that never becomes a CQE, in
which case this path is unreachable by construction for this op rather than
incidentally dormant. `send_file` still carries an `EAGAIN` arm for this leg
regardless of which candidate is true — `splice(2)` documents `EAGAIN` as a
permitted return and both candidates are implementation details, not a
contract — but it is dormant on the kernel and io-uring version this was
built against (7.2.0, io-uring 0.7.14). What *was* verified on the io_uring
path is the property that actually matters: that a backpressured send
suspends its own task instead of stalling the executor. That is proven by
`a_backpressured_send_does_not_stall_the_executor`
(`glommio/tests/send_file.rs`), which spawns a neighbour task on the same
executor and shows it keeps making progress throughout a window guaranteed
to fall inside the send's backpressured stretch, rather than by watching for
`EAGAIN` on the completion, which does not arrive.

The `file → pipe` distinction from the raw probe still holds structurally
even though the `EAGAIN` details above changed: **`file → pipe` `EAGAIN`
would mean the pipe is full and is fixed by draining, not by polling.**
Regular files are always ready; there is nothing to wait for. Only
`pipe → socket` needs a readiness wait — provisioned for even though it was
not observed to be exercised on this configuration.

## Public API

Socket-side. glommio owns the pipe and it never appears in the API, so there
is no way for a caller to misuse it.

```rust
impl<B: RxBuf + Unpin> TcpStream<B> {
    /// Sends `len` bytes of `file` starting at `offset`, without the bytes
    /// entering userspace. Returns the number actually sent, which is short
    /// at end of file.
    pub async fn send_file<F: SpliceSource>(
        &mut self, file: &F, offset: u64, len: usize,
    ) -> Result<usize>;
}

/// A file that can be spliced. Sealed: implemented for `BufferedFile` and
/// `DmaFile`.
pub trait SpliceSource: sealed::Sealed {
    fn splice_fd(&self) -> RawFd;
    /// 1 for page-cache files; the device logical block size under
    /// `O_DIRECT`.
    fn splice_offset_alignment(&self) -> u64;
}
```

The alignment method is the trait's whole reason to exist. `send_file`
validates `offset % alignment == 0` before submitting anything and returns a
glommio error naming the requirement. A bare `EINVAL` surfacing from the
kernel here is close to undiagnosable.

## Mechanism

A pipe per call, `libc::pipe2(O_CLOEXEC | O_NONBLOCK)`, closed on every exit
path including panic and cancellation. That makes a contaminated pipe
unreachable by construction: a cancelled or failed transfer takes its dirty
pipe to the grave. `O_NONBLOCK` is not optional — without it the hang above
is reachable.

Then, per chunk:

1. `splice(file @ offset → pipe_w, min(remaining, 65536))` → `n`.
   `n == 0` is end of file: stop and return what was sent.
2. Drain: `splice(pipe_r → socket)` until those `n` bytes are gone. On
   `EAGAIN`, park on `Reactor::poll_write_ready` (`reactor.rs:372`) and retry.
3. Advance offset, repeat until `len` is satisfied or EOF.

Draining fully before refilling keeps the pipe's contents unambiguous, so
step 1 never meets a full pipe and its `EAGAIN` case is structurally
unreachable rather than handled.

Both splices are ordinary `Source`s on the ring, sequential and unlinked.

### Why sequential is not a regression

Two ops per 65536-byte chunk is the same op count as the read-plus-write it
replaces, which is also two per chunk. The round trips are not extra; the
copies are simply gone.

`IOSQE_IO_LINK` could halve them, and is deliberately not attempted here: the
second SQE's length must be chosen before the first completes, so it is a
guess, and a wrong guess asks the pipe for more than it holds. **Measure with
a `send_file` ladder before attempting it.**

## Plumbing

Follows the existing op pattern exactly.

- `UringOpDescriptor::Splice { fd_in, off_in, off_out, len, flags }` in
  `sys/uring.rs`. `UringDescriptor.fd` carries `fd_out`, because
  `opcode::Splice` assigns `sqe.fd = fd_out` and puts `fd_in` in
  `splice_fd_in`.
- `SourceType::Splice`, carrying nothing. It is shaped like `Fallocate`, not
  like `SockSend`.
- `Reactor::splice(...) -> Source`, roughly eight lines, matching `connect`
  and `accept`.

**`SourceType::Splice` carrying no buffer is the safety argument.** Nothing of
ours is lent to the kernel — the op is two descriptors and a length. There is
no analogue of the buffer-lifetime hazard that made `MSG_ZEROCOPY` a
user-reachable use-after-free (deleted in `57ca3c6`), and nothing here needs
a notification CQE.

## Error handling

| Case | Behaviour |
|---|---|
| Misaligned offset, `O_DIRECT` | Error before submitting, naming the alignment |
| `splice(file → pipe)` returns 0 | End of file; return bytes sent so far |
| `len` runs past end of file | Short return, not an error |
| `pipe → socket` returns `EAGAIN` | Park on `poll_write_ready`, retry |
| Short splice either direction | Loop continues; no special case |
| Peer resets mid-transfer | Error, pipe closed with whatever it holds |

## Testing

Integration tests in `glommio/tests/`, because this is new public API and
this repo has shipped two unusable APIs that unit tests could not see.

- Round-trip a file over a real `TcpStream`; assert the bytes match. Once for
  `BufferedFile`, once for `DmaFile`.
- A file larger than 65536, to drive the loop rather than one chunk.
- A deliberately small `SO_SNDBUF` to force the `EAGAIN` path. Getting that
  path wrong looks like a hang on large files under backpressure, which is
  exactly the bug worth a dedicated test.
- Misaligned offset on `DmaFile`: clean error, not `EINVAL`, not a panic.
- `len` past end of file: short return.
- **Every test mutation-checked.** A `send_file` that silently sent nothing
  would pass a naive "no error" assertion.

## Deferred

Not in v1, none of them blocking:

- `IOSQE_IO_LINK` batching — behind a measurement.
- A per-reactor pipe pool — the per-call pipe costs syscalls that are noise
  above the size threshold where splice is used at all. Add only if measured.
- `F_SETPIPE_SZ` to raise the 65536 chunk size.
- `UnixStream::send_file`.
- `SpliceSource for ImmutableFile`. It holds a `DmaStreamReaderBuilder` and
  `size`, not a `DmaFile`, and has no `AsRawFd`, so there is no fd to hand
  over without first exposing one through the builder. That is a separate
  change to a type whose whole point is a sealed abstraction, and it should
  not ride along with this one.

## Open question for the consumer

Not blocking, since both file types work: which does collimator's `File` body
read through? It decides which test is the representative one, not whether
the feature is usable.
