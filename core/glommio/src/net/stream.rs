// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    reactor::Reactor,
    sys::{self, Source, SourceType},
};
use futures_lite::ready;
use nix::sys::socket::MsgFlags;
use std::{
    cell::Cell,
    io::{self, IoSlice},
    net::Shutdown,
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    rc::{Rc, Weak},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

type Result<T> = crate::Result<T, ()>;

/// A receive buffer on loan to the kernel.
///
/// A completion-based read needs its buffer to stay alive until the
/// completion arrives, which may be after the future that started the read is
/// dropped. So the buffer is handed over rather than borrowed: it lives in the
/// `Source`, which the reactor keeps until the kernel is done with it.
///
/// There is nothing to do with one of these except give it back.
#[derive(Debug)]
pub struct OwnedRxBuf {
    memory: Vec<u8>,
}

impl OwnedRxBuf {
    /// Wraps memory to lend to the kernel.
    ///
    /// The kernel writes into `memory[..memory.len()]`, so **the vector's
    /// length is what it may fill, not its capacity**. `vec![0; 8192]` lends
    /// 8 KiB; `Vec::with_capacity(8192)` lends nothing at all.
    ///
    /// # Panics
    ///
    /// If `memory` is empty. A zero-length read completes immediately with 0
    /// bytes, which is exactly what end-of-file looks like -- so lending an
    /// empty buffer would silently close a healthy connection. Refusing it
    /// here reports the mistake where it is made rather than one layer down.
    pub fn new(memory: Vec<u8>) -> Self {
        assert!(
            !memory.is_empty(),
            "an OwnedRxBuf must have a non-zero length: the kernel fills \
             memory[..len], so a vector with capacity but no length lends it \
             nowhere to write, and the resulting zero-byte read is \
             indistinguishable from the peer hanging up. Use vec![0; size] \
             rather than Vec::with_capacity(size)"
        );
        OwnedRxBuf { memory }
    }

    /// Takes the memory back, with whatever the kernel wrote at its start.
    ///
    /// How much it wrote arrives separately, through
    /// [`RxBuf::handle_result`].
    pub fn into_vec(self) -> Vec<u8> {
        self.memory
    }

    /// How many bytes the kernel may write into this buffer.
    pub fn len(&self) -> usize {
        self.memory.len()
    }

    /// Always false: an empty buffer cannot be constructed.
    pub fn is_empty(&self) -> bool {
        false
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.memory.as_mut_ptr()
    }
}

/// Root trait for socket stream receive buffer
pub trait RxBuf {
    /// Moves up to `buf.len()` buffered bytes out, and forgets them.
    fn read(&mut self, buf: &mut [u8]) -> usize;

    /// Copies up to `buf.len()` buffered bytes out, leaving them buffered.
    fn peek(&self, buf: &mut [u8]) -> usize;

    /// Whether anything is buffered and unread.
    ///
    /// Must not claim otherwise while a read is outstanding: it is what
    /// decides whether the stream reads again or hands over what it has.
    fn is_empty(&self) -> bool;

    /// Everything buffered and unread, without consuming it.
    fn as_bytes(&self) -> &[u8];

    /// Forgets the first `amt` buffered bytes.
    fn consume(&mut self, amt: usize);

    /// How much this buffer can hold. A read larger than this bypasses the
    /// buffer and goes straight to the socket.
    fn buffer_size(&self) -> usize;

    /// Records that the socket delivered `result` bytes into
    /// [`unfilled`](Self::unfilled).
    fn handle_result(&mut self, result: usize);

    /// Where the next read should land: the free space after whatever is
    /// still buffered.
    ///
    /// Called *before* the read completes, so an implementation must not
    /// treat this space as holding data -- see [`is_empty`](Self::is_empty).
    fn unfilled(&mut self) -> &mut [u8];

    /// Lends the whole buffer to the kernel for a completion-based read.
    ///
    /// Returning `None` -- the default -- keeps this implementation on the
    /// readiness path, where the kernel is told when to read rather than
    /// asked to. Implementations that return `Some` must also implement
    /// [`restore_kernel_buffer`](Self::restore_kernel_buffer).
    ///
    /// Only ever called when the buffer is empty, so an implementation may
    /// hand over its whole capacity and reset its cursors.
    fn take_kernel_buffer(&mut self) -> Option<OwnedRxBuf> {
        None
    }

    /// Takes the buffer back, empty.
    ///
    /// How many bytes the kernel wrote arrives separately, through
    /// [`handle_result`](Self::handle_result) -- the same call the readiness
    /// path uses -- so there is one place that advances the cursor rather than
    /// two that must agree.
    ///
    /// Unreachable unless [`take_kernel_buffer`](Self::take_kernel_buffer) was
    /// overridden to hand one out, which is why the default says so rather
    /// than silently dropping the buffer.
    fn restore_kernel_buffer(&mut self, buffer: OwnedRxBuf) {
        let _ = buffer;
        unreachable!(
            "restore_kernel_buffer called on an RxBuf that never lends its buffer out: \
             take_kernel_buffer and restore_kernel_buffer must be implemented together"
        )
    }
}

/// The receive buffer of a stream that has none.
///
/// The default for [`TcpStream`](crate::net::TcpStream) and
/// [`UnixStream`](crate::net::UnixStream): reads go straight to the socket.
/// It is nameable so that `TcpStream<NonBuffered>` can be written out, which
/// generic code and trait implementations need.
#[derive(Debug, Default)]
pub struct NonBuffered;

impl RxBuf for NonBuffered {
    fn read(&mut self, _buf: &mut [u8]) -> usize {
        0
    }

    fn peek(&self, _buf: &mut [u8]) -> usize {
        0
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn as_bytes(&self) -> &[u8] {
        &[]
    }

    fn consume(&mut self, _amt: usize) {}

    fn buffer_size(&self) -> usize {
        0
    }

    fn handle_result(&mut self, _result: usize) {}

    fn unfilled(&mut self) -> &mut [u8] {
        &mut []
    }
}

/// Trait for receive buffer implementations
pub trait Buffered: RxBuf {}

/// Non-shared fixed sized receive buffer allocated
/// when buffered stream is created
#[derive(Debug)]
pub struct Preallocated {
    buf: Vec<u8>,
    head: usize,
    tail: usize,
    cap: usize,
}

impl Preallocated {
    const DEFAULT_BUFFER_SIZE: usize = 8192;

    /// Creates a fixed sized receive buffer
    pub fn new(size: usize) -> Self {
        Self {
            buf: vec![0; size],
            tail: 0,
            head: 0,
            cap: size,
        }
    }
}

impl Default for Preallocated {
    fn default() -> Self {
        Self::new(Self::DEFAULT_BUFFER_SIZE)
    }
}

impl Preallocated {
    fn len(&self) -> usize {
        self.tail - self.head
    }
}

impl Buffered for Preallocated {}

impl RxBuf for Preallocated {
    fn read(&mut self, buf: &mut [u8]) -> usize {
        let sz = std::cmp::min(self.len(), buf.len());
        if sz > 0 {
            buf[..sz].copy_from_slice(&self.buf[self.head..self.head + sz]);
            self.head += sz;
        }
        sz
    }

    fn peek(&self, buf: &mut [u8]) -> usize {
        let sz = std::cmp::min(self.len(), buf.len());
        if sz > 0 {
            buf[..sz].copy_from_slice(&self.buf[self.head..self.head + sz]);
        }
        sz
    }

    fn is_empty(&self) -> bool {
        self.head >= self.tail
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buf[self.head..self.tail]
    }

    fn consume(&mut self, amt: usize) {
        self.head += std::cmp::min(self.len(), amt);
    }

    fn buffer_size(&self) -> usize {
        self.cap
    }

    fn handle_result(&mut self, result: usize) {
        self.tail += result;
    }

    fn unfilled(&mut self) -> &mut [u8] {
        if self.len() == 0 {
            self.head = 0;
            self.tail = 0;
        }
        &mut self.buf[self.tail..]
    }

    fn take_kernel_buffer(&mut self) -> Option<OwnedRxBuf> {
        debug_assert!(
            self.is_empty(),
            "lending a buffer that still holds unread bytes would lose them"
        );
        // Lent whole and from the start: the caller only asks when empty, so
        // there is nothing to preserve and the cursors reset.
        self.head = 0;
        self.tail = 0;
        Some(OwnedRxBuf::new(std::mem::take(&mut self.buf)))
    }

    fn restore_kernel_buffer(&mut self, buffer: OwnedRxBuf) {
        self.buf = buffer.into_vec();
        self.head = 0;
        self.tail = 0;
    }
}

#[derive(Debug)]
struct Timeout {
    handle: Cell<Option<crate::timer::slab::TimerId>>,
    timeout: Cell<Option<Duration>>,
    timer: Cell<Option<Instant>>,
}

impl Timeout {
    fn new() -> Self {
        Self {
            handle: Cell::new(None),
            timeout: Cell::new(None),
            timer: Cell::new(None),
        }
    }

    fn get(&self) -> Option<Duration> {
        self.timeout.get()
    }

    fn set(&self, dur: Option<Duration>) -> Result<()> {
        if let Some(dur) = dur.as_ref() {
            if dur.as_nanos() == 0 {
                return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
            }
        }
        self.timeout.set(dur);
        Ok(())
    }

    fn maybe_set_timer(&self, reactor: &Reactor, waker: &Waker) {
        if let Some(timeout) = self.timeout.get() {
            if self.timer.get().is_none() {
                let deadline = Instant::now() + timeout;
                let id = reactor.insert_timer(deadline, waker.clone());
                self.handle.set(Some(id));
                self.timer.set(Some(deadline));
            }
        }
    }

    fn cancel_timer(&self, reactor: &Reactor) {
        if self.timer.take().is_some() {
            if let Some(id) = self.handle.take() {
                reactor.remove_timer(id);
            }
        }
    }

    fn check(&self, reactor: &Reactor) -> io::Result<()> {
        // Ask the deadline, not the registry. A handle can be absent because
        // the timer fired or because it was cancelled, and only one of those
        // is a timeout; the deadline distinguishes them and does not depend on
        // how timers happen to be stored.
        let Some(deadline) = self.timer.get() else {
            return Ok(());
        };
        if Instant::now() < deadline {
            return Ok(());
        }

        if let Some(id) = self.handle.take() {
            reactor.remove_timer(id);
        }
        self.timer.take();
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Operation timed out",
        ))
    }
}

#[derive(Debug)]
pub(crate) struct NonBufferedStream<S> {
    reactor: Weak<Reactor>,
    stream: S,
    source_tx: Option<Source>,
    source_rx: Option<Source>,
    write_timeout: Timeout,
    read_timeout: Timeout,
}

impl<S: AsRawFd> NonBufferedStream<S> {
    fn init(&mut self) {
        let reactor = self.reactor.upgrade().unwrap();
        let stream_fd = self.stream.as_raw_fd();
        self.source_rx = Some(reactor.poll_read_ready(stream_fd));
    }

    pub(crate) fn try_peek(&self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        super::yolo_peek(self.stream.as_raw_fd(), buf)
    }

    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let source = self.reactor.upgrade().unwrap().recv(
            self.stream.as_raw_fd(),
            buf.len(),
            MsgFlags::MSG_PEEK,
        );

        let sz = source.collect_rw().await?;
        match source.extract_source_type() {
            SourceType::SockRecv(mut src) => {
                buf[0..sz].copy_from_slice(&src.take().unwrap().as_bytes()[0..sz]);
            }
            _ => unreachable!(),
        }
        Ok(sz)
    }

    /// One non-blocking `recv`, and no fallback if it comes up empty.
    ///
    /// `None` means the socket had nothing, and the caller decides what to do
    /// about it -- register readiness, or hand the read to the kernel.
    pub(crate) fn poll_read_speculative(&mut self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        super::yolo_recv(self.stream.as_raw_fd(), buf)
    }

    /// Reads one TLS record, reporting the type the kernel attached to it.
    ///
    /// Same speculate-then-register shape as [`poll_read`](Self::poll_read),
    /// with `recvmsg` in place of `recv` so a control record arrives as a
    /// record rather than as `EIO`.
    pub(crate) fn poll_recv_record(
        &mut self,
        cx: &Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, Option<u8>)>> {
        let no_pending_poll = self
            .source_rx
            .as_ref()
            .map(|src| src.result().is_some())
            .unwrap_or(true);

        if no_pending_poll {
            if let Some(result) = super::yolo_recv_record(self.stream.as_raw_fd(), buf) {
                let reactor = self.reactor.upgrade().unwrap();
                self.source_rx.take();
                self.read_timeout.cancel_timer(reactor.as_ref());
                return Poll::Ready(result);
            }
        }

        match self.poll_read_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            // Readable now: take the record without going round again.
            Poll::Ready(Ok(())) => match super::yolo_recv_record(self.stream.as_raw_fd(), buf) {
                Some(result) => Poll::Ready(result),
                None => Poll::Pending,
            },
        }
    }

    /// Registers interest in the socket becoming readable.
    fn poll_read_ready(&mut self, cx: &Context<'_>) -> Poll<io::Result<()>> {
        let reactor = self.reactor.upgrade().unwrap();
        let reactor = reactor.as_ref();
        poll_err!(self.read_timeout.check(reactor));

        let no_pending_poll = self
            .source_rx
            .as_ref()
            .map(|src| src.result().is_some())
            .unwrap_or(true);

        if no_pending_poll {
            self.source_rx = Some(reactor.poll_read_ready(self.stream.as_raw_fd()));
        }

        let source = self.source_rx.as_ref().unwrap();
        source.add_waiter_single(cx.waker());
        self.read_timeout.maybe_set_timer(reactor, cx.waker());
        Poll::Pending
    }

    pub(crate) fn poll_read(
        &mut self,
        cx: &Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let reactor = self.reactor.upgrade().unwrap();
        let reactor = reactor.as_ref();

        let no_pending_poll = self
            .source_rx
            .as_ref()
            .map(|src| src.result().is_some())
            .unwrap_or(true);

        if no_pending_poll {
            if let Some(result) = super::yolo_recv(self.stream.as_raw_fd(), buf) {
                self.source_rx.take();
                self.read_timeout.cancel_timer(reactor);
                let result = poll_err!(result);
                // Start an early poll if the buffer is not fully filled. So when
                // the next time `poll_read` is called, it will be known immediately
                // whether the underlying stream is ready for reading.
                if result > 0 && result < buf.len() {
                    self.source_rx = Some(reactor.poll_read_ready(self.stream.as_raw_fd()));
                    // The `rush_dispatch`s here and after could be removed to
                    // improve performance if #458 is handled appropriately.
                    // reactor.rush_dispatch(self.source_rx.as_ref().unwrap());
                }
                return Poll::Ready(Ok(result));
            }
        }

        poll_err!(self.read_timeout.check(reactor));

        if no_pending_poll {
            self.source_rx = Some(reactor.poll_read_ready(self.stream.as_raw_fd()));
            // reactor.rush_dispatch(self.source_rx.as_ref().unwrap());
        }

        let source = self.source_rx.as_ref().unwrap();
        source.add_waiter_single(cx.waker());
        self.read_timeout.maybe_set_timer(reactor, cx.waker());
        Poll::Pending
    }

    /// Writes several buffers in one syscall.
    ///
    /// Same shape as [`poll_write`](Self::poll_write) -- speculate first, fall
    /// back to a readiness registration -- with `sendmsg` in place of `send`.
    /// The point is the caller: an HTTP response is a status line, headers and
    /// a body, and without this it costs three syscalls and, with
    /// `TCP_NODELAY` set, up to three segments on the wire.
    pub(crate) fn poll_write_vectored(
        &mut self,
        cx: &Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if let Some(result) = super::yolo_sendv(self.stream.as_raw_fd(), bufs) {
            let reactor = self.reactor.upgrade().unwrap();
            self.write_timeout.cancel_timer(reactor.as_ref());
            self.source_tx.take();
            return Poll::Ready(result);
        }

        self.poll_write_ready(cx)
    }

    /// Registers interest in the socket becoming writable, shared by the
    /// scalar and vectored paths.
    fn poll_write_ready(&mut self, cx: &Context<'_>) -> Poll<io::Result<usize>> {
        let reactor = self.reactor.upgrade().unwrap();
        let reactor = reactor.as_ref();
        poll_err!(self.write_timeout.check(reactor));

        let no_pending_poll = self
            .source_tx
            .as_ref()
            .map(|src| src.result().is_some())
            .unwrap_or(true);

        if no_pending_poll {
            self.source_tx = Some(reactor.poll_write_ready(self.stream.as_raw_fd()));
        }

        let source = self.source_tx.as_ref().unwrap();
        source.add_waiter_single(cx.waker());
        self.write_timeout.maybe_set_timer(reactor, cx.waker());
        Poll::Pending
    }

    pub(crate) fn poll_write(&mut self, cx: &Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // On the write path, we always start with calling `yolo_send`, because
        // it is very likely to success. It could be a waste if it already timed
        // out since the last `poll_write`, but it would not cost much more to
        // give it one last chance in this case.
        if let Some(result) = super::yolo_send(self.stream.as_raw_fd(), buf) {
            let reactor = self.reactor.upgrade().unwrap();
            self.write_timeout.cancel_timer(reactor.as_ref());
            self.source_tx.take();
            return Poll::Ready(result);
        }

        self.poll_write_ready(cx)
    }

    pub(crate) fn poll_close(&mut self, _: &Context<'_>) -> Poll<io::Result<()>> {
        self.source_tx.take();
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), Shutdown::Write))
    }

    /// io_uring has support for shutdown now, but it is not in any released
    /// kernel. Even with my "let's use latest" policy it would be crazy to
    /// mandate a kernel that doesn't even exist. So in preparation for that
    /// we'll sync-emulate this but already on an async wrapper
    pub(crate) fn poll_shutdown(
        &self,
        _cx: &mut Context<'_>,
        how: Shutdown,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), how))
    }
}

#[derive(Debug)]
pub(crate) struct GlommioStream<S, B> {
    stream: NonBufferedStream<S>,
    rx_buf: B,
    rx_done: Cell<bool>,
    /// A completion-based read with the receive buffer lent to the kernel.
    rx_source: Option<Source>,
    /// Whether to try a plain `recv` before asking the kernel to do the read.
    ///
    /// A streaming reader almost always has data waiting, and for it one
    /// syscall beats an SQE and a CQE. A connection that goes quiet between
    /// messages has none, and for it the speculation is a wasted `EAGAIN` on
    /// every single message. Rather than pick, this follows: set when a
    /// speculative read pays off, cleared when it does not.
    speculate: Cell<bool>,
}

impl<S> From<socket2::Socket> for GlommioStream<S, NonBuffered>
where
    S: AsRawFd + From<socket2::Socket> + Unpin,
{
    fn from(socket: socket2::Socket) -> Self {
        let reactor = crate::executor().reactor();
        let mut stream = NonBufferedStream {
            reactor: Rc::downgrade(&reactor),
            stream: socket.into(),
            source_tx: None,
            source_rx: None,
            write_timeout: Timeout::new(),
            read_timeout: Timeout::new(),
        };
        stream.init();
        GlommioStream {
            stream,
            rx_buf: NonBuffered,
            rx_done: Cell::new(false),
            rx_source: None,
            speculate: Cell::new(true),
        }
    }
}

impl<S: AsRawFd> AsRawFd for GlommioStream<S, NonBuffered> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.stream.as_raw_fd()
    }
}

impl<S> FromRawFd for GlommioStream<S, NonBuffered>
where
    S: AsRawFd + FromRawFd + From<socket2::Socket> + Unpin,
{
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        GlommioStream::from(socket)
    }
}

impl<S> GlommioStream<S, NonBuffered> {
    pub(crate) fn buffered_with<B: Buffered>(self, rx_buf: B) -> GlommioStream<S, B> {
        GlommioStream {
            stream: self.stream,
            rx_buf,
            rx_done: self.rx_done,
            rx_source: self.rx_source,
            speculate: self.speculate,
        }
    }
}

impl<S: AsRawFd, B: RxBuf> GlommioStream<S, B> {
    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// `MSG_PEEK` as a flag to the underlying `recv` system call.
    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut pos = self.rx_buf.peek(buf);
        if pos < buf.len() && !self.rx_done.get() {
            if let Some(result) = self.stream.try_peek(&mut buf[pos..]) {
                match result {
                    Err(e) => return Err(e),
                    Ok(len) => {
                        pos += len;
                        if len == 0 {
                            self.rx_done.set(true);
                        }
                    }
                }
            }
        }
        if pos > 0 || self.rx_done.get() {
            return Ok(pos);
        }
        self.stream.peek(buf).await
    }

    pub(crate) fn poll_read(
        &mut self,
        cx: &Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.rx_buf.is_empty() {
            if buf.len() >= self.rx_buf.buffer_size() {
                return self.stream.poll_read(cx, buf);
            }
            if !self.rx_done.get() {
                poll_err!(ready!(self.poll_replenish_buffer(cx)));
            }
        }
        Poll::Ready(Ok(self.rx_buf.read(buf)))
    }

    fn poll_replenish_buffer(&mut self, cx: &Context<'_>) -> Poll<io::Result<usize>> {
        let result = poll_err!(ready!(self.poll_fill(cx)));
        self.rx_buf.handle_result(result);
        if result == 0 {
            self.rx_done.set(true);
        }
        Poll::Ready(Ok(result))
    }

    /// Fills the receive buffer, by whichever of the two mechanisms suits.
    ///
    /// The completion read is only reachable here, on the buffered path,
    /// because it hands the buffer to the kernel for longer than a single
    /// poll -- which is sound exactly when glommio owns that buffer.
    fn poll_fill(&mut self, cx: &Context<'_>) -> Poll<io::Result<usize>> {
        if let Some(source) = self.rx_source.as_ref() {
            let Some(result) = source.result() else {
                source.add_waiter_many(cx.waker().clone());
                return Poll::Pending;
            };

            // Reclaim the buffer before anything else can go wrong with the
            // result, or it is gone for the life of the stream.
            let source = self.rx_source.take().unwrap();
            match source.extract_source_type() {
                SourceType::SockRecvInto(Some(buffer)) => {
                    self.rx_buf.restore_kernel_buffer(buffer);
                }
                _ => unreachable!("a completion read came back without its buffer"),
            }

            // A read that filled the buffer suggests the peer has more to
            // say, which makes speculating next time likely to pay.
            self.speculate
                .set(matches!(&result, Ok(read) if *read == self.rx_buf.buffer_size()));

            return Poll::Ready(result);
        }

        if self.speculate.get() {
            match self.stream.poll_read_speculative(self.rx_buf.unfilled()) {
                Some(result) => return Poll::Ready(result),
                None => self.speculate.set(false),
            }
        }

        match self.rx_buf.take_kernel_buffer() {
            Some(buffer) => {
                let reactor = self.stream.reactor.upgrade().unwrap();
                self.rx_source = Some(reactor.recv_into(self.stream.stream.as_raw_fd(), buffer));
                self.poll_fill(cx)
            }
            // An `RxBuf` that will not lend its buffer stays on the readiness
            // path, which is the whole point of the method being defaulted.
            None => self.stream.poll_read(cx, self.rx_buf.unfilled()),
        }
    }

    pub(crate) fn poll_write(&mut self, cx: &Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.stream.poll_write(cx, buf)
    }

    pub(crate) fn poll_write_vectored(
        &mut self,
        cx: &Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.stream.poll_write_vectored(cx, bufs)
    }

    pub(crate) fn poll_recv_record(
        &mut self,
        cx: &Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<(usize, Option<u8>)>> {
        self.stream.poll_recv_record(cx, buf)
    }

    pub(crate) fn poll_flush(&self, _cx: &Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_close(&mut self, cx: &Context<'_>) -> Poll<io::Result<()>> {
        self.stream.poll_close(cx)
    }

    pub(crate) fn poll_shutdown(
        &self,
        cx: &mut Context<'_>,
        how: Shutdown,
    ) -> Poll<io::Result<()>> {
        self.stream.poll_shutdown(cx, how)
    }

    pub(crate) fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.write_timeout.set(dur)
    }

    pub(crate) fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.read_timeout.set(dur)
    }

    pub(crate) fn write_timeout(&self) -> Option<Duration> {
        self.stream.write_timeout.get()
    }

    pub(crate) fn read_timeout(&self) -> Option<Duration> {
        self.stream.read_timeout.get()
    }

    pub(crate) fn stream(&self) -> &S {
        &self.stream.stream
    }
}

impl<S: AsRawFd, B: Buffered> GlommioStream<S, B> {
    pub(crate) fn poll_fill_buf(&mut self, cx: &Context<'_>) -> Poll<io::Result<&[u8]>> {
        // `rx_done` is checked here for the same reason `poll_read` checks it:
        // once the peer is gone there is nothing to wait for, and asking again
        // must not suspend. On the readiness path that happened to hold
        // anyway, because `recv` at EOF returns 0 immediately on every call --
        // a completion read returns `Pending` first, and `poll_fill_buf` is
        // required to stay ready once it has been ready.
        if self.rx_buf.is_empty() && !self.rx_done.get() {
            poll_err!(ready!(self.poll_replenish_buffer(cx)));
        }
        Poll::Ready(Ok(self.rx_buf.as_bytes()))
    }

    pub(crate) fn consume(&mut self, amt: usize) {
        self.rx_buf.consume(amt);
    }
}

impl<S: AsRawFd + IntoRawFd> NonBufferedStream<S> {
    /// Extracts the raw file descriptor, cleaning up glommio state
    /// but keeping the fd open.
    fn into_raw_fd(mut self) -> RawFd {
        // Clean up reactor sources
        self.source_tx.take();
        self.source_rx.take();

        // Cancel any pending timers
        if let Some(reactor) = self.reactor.upgrade() {
            self.write_timeout.cancel_timer(&reactor);
            self.read_timeout.cancel_timer(&reactor);
        }

        // Extract fd from inner stream (this prevents it from closing)
        self.stream.into_raw_fd()
    }
}

impl<S: AsRawFd + IntoRawFd> GlommioStream<S, NonBuffered> {
    /// Extracts the raw file descriptor, cleaning up glommio state
    /// but keeping the fd open.
    pub(crate) fn into_raw_fd(self) -> RawFd {
        self.stream.into_raw_fd()
    }
}
