// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use super::stream::GlommioStream;
use crate::{
    io::SpliceSource,
    net::ToSocketAddrs,
    net::{
        stream::{Buffered, NonBuffered, Preallocated, RxBuf},
        yolo_accept,
    },
    reactor::Reactor,
    sys::Source,
    GlommioError,
};
use futures_lite::{
    future::poll_fn,
    io::{AsyncBufRead, AsyncRead, AsyncWrite},
    ready,
    stream::{self, Stream},
};
use nix::sys::socket::SockaddrStorage;
use pin_project_lite::pin_project;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    cell::RefCell,
    io::{self, IoSlice},
    net::{self, Shutdown, SocketAddr},
    os::{
        fd::AsFd,
        unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    },
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::Duration,
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// A TCP socket server, listening for connections.
///
/// After creating a TcpListener by binding it to a socket address, it listens
/// for incoming TCP connections. These can be accepted by calling [`accept`] or
/// [`shared_accept`], or by iterating over the Incoming iterator returned by
/// [`incoming`].
///
/// A good networking architecture within a thread-per-core model needs to take
/// into account parallelism and spawn work into multiple executors. If
/// everything happens inside the same Executor, then at most one thread is
/// used. Sometimes this is what you want: you may want to dedicate a CPU
/// entirely for networking, or even use specialized ports for each CPU of the
/// application, but most likely it isn't.
///
/// There are two approaches to load balancing possible with the `TcpListener`:
///
/// * By default, the ReusePort flag is set in the socket automatically. The OS
///   already provides some load balancing capabilities with that so you can
///   simply [`bind`] to the same address from many executors.
///
/// * If that is insufficient or otherwise not desirable, it is possible to use
///   [`shared_accept`] instead of [`accept`]: that returns an object that
///   implements [`Send`]. You can then use a [`shared_channel`] to send the
///   accepted connection into multiple executors. The object returned by
///   [`shared_accept`] can then be bound to its executor with
///   [`bind_to_executor`], at which point it becomes a standard [`TcpStream`].
///
/// Relying on the OS is definitely simpler, but which approach is better
/// depends on the specific needs of your application.
///
/// The socket will be closed when the value is dropped.
///
/// [`accept`]: TcpListener::accept
/// [`shared_accept`]: TcpListener::shared_accept
/// [`bind`]: TcpListener::bind
/// [`incoming`]: TcpListener::incoming
/// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`shared_channel`]: ../channels/shared_channel/index.html
pub struct TcpListener {
    reactor: Weak<Reactor>,
    listener: net::TcpListener,
    current_source: RefCell<Option<Source>>,
}

impl FromRawFd for TcpListener {
    /// Convert an already bound and listening RawFd into a TcpListener
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let sk = Socket::from_raw_fd(fd);
        // Same invariant as `bind`: an accept on a blocking listener parks the
        // executor. A caller handing us their own fd does not know that.
        sk.set_nonblocking(true)
            .expect("failed to put the listener in non-blocking mode");
        let listener = sk.into();

        TcpListener {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            listener,
            current_source: Default::default(),
        }
    }
}

impl TcpListener {
    /// Creates a TCP listener bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// This method sets the ReusePort option in the bound socket, so it is
    /// designed to be called from multiple executors to achieve
    /// parallelism.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<TcpListener> {
        // Not `async`, so the lookup cannot go to the blocking pool. Binding
        // happens once at startup rather than per connection, so a stall here
        // holds up nothing that is already running -- but it must not panic,
        // which it used to on both a resolver error and an empty result.
        let addr = super::resolve::resolve_blocking(addr)?[0];

        let domain = if addr.is_ipv6() {
            Domain::IPV6
        } else {
            Domain::IPV4
        };
        let sk = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        let addr = socket2::SockAddr::from(addr);
        sk.set_reuse_port(true)?;
        sk.bind(&addr)?;
        sk.listen(1024)?;
        // `yolo_accept` depends on this: a blocking listener would park the
        // executor inside `accept` instead of returning `EAGAIN`.
        sk.set_nonblocking(true)?;
        let listener = sk.into();

        Ok(TcpListener {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            listener,
            current_source: Default::default(),
        })
    }

    /// Accepts a new incoming TCP connection and allows the result to be sent
    /// to a foreign executor
    ///
    /// This is similar to [`accept`], except it returns an
    /// [`AcceptedTcpStream`] instead of a [`TcpStream`].
    /// [`AcceptedTcpStream`] implements [`Send`], so it can be safely sent
    /// for processing over a shared channel to a different executor.
    ///
    /// This is useful when the user wants to do her own load balancing across
    /// multiple executors instead of relying on the load balancing the OS
    /// would do with the ReusePort property of the bound socket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let stream = listener.shared_accept().await.unwrap();
    /// });
    /// ```
    ///
    /// [`accept`]: TcpListener::accept
    /// [`AcceptedTcpStream`]: struct.AcceptedTcpStream.html
    /// [`TcpStream`]: struct.TcpStream.html
    /// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
    pub async fn shared_accept(&self) -> Result<AcceptedTcpStream> {
        poll_fn(|cx| self.poll_shared_accept(cx)).await
    }

    /// Poll version of [`shared_accept`].
    ///
    /// [`shared_accept`]: TcpListener::shared_accept
    pub fn poll_shared_accept(&self, cx: &mut Context<'_>) -> Poll<Result<AcceptedTcpStream>> {
        let mut poll_source = |source: Source| match source.poll_collect_rw(cx) {
            Poll::Ready(Ok(fd)) => Poll::Ready(Ok(AcceptedTcpStream { fd: fd as RawFd })),
            Poll::Ready(Err(err)) => Poll::Ready(Err(GlommioError::IoError(err))),
            Poll::Pending => {
                *self.current_source.borrow_mut() = Some(source);
                Poll::Pending
            }
        };
        match self.current_source.take() {
            Some(source) => poll_source(source),
            None => {
                let reactor = self.reactor.upgrade().unwrap();
                let fd = self.listener.as_fd();
                match yolo_accept(fd) {
                    Some(r) => match r {
                        Ok(fd) => Poll::Ready(Ok(AcceptedTcpStream { fd })),
                        Err(err) => Poll::Ready(Err(GlommioError::IoError(err))),
                    },
                    None => {
                        let source = reactor.accept(self.listener.as_raw_fd());
                        poll_source(source)
                    }
                }
            }
        }
    }

    /// Accepts a new incoming TCP connection in this executor
    ///
    /// This is similar to calling [`shared_accept`] and [`bind_to_executor`] in
    /// a single operation.
    ///
    /// If this connection once accepted is to be handled by the same executor
    /// in which it was accepted, this version is preferred.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_lite::stream::StreamExt;
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let stream = listener.accept().await.unwrap();
    ///     println!("Accepted client: {:?}", stream.local_addr());
    /// });
    /// ```
    ///
    /// [`shared_accept`]: TcpListener::accept
    /// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
    pub async fn accept(&self) -> Result<TcpStream> {
        let a = self.shared_accept().await?;
        Ok(a.bind_to_executor())
    }

    /// Poll version of [`accept`].
    ///
    /// [`accept`]: TcpListener::accept
    pub fn poll_accept(&self, cx: &mut Context<'_>) -> Poll<Result<TcpStream>> {
        match ready!(self.poll_shared_accept(cx)) {
            Ok(a) => {
                let a = a.bind_to_executor();
                Poll::Ready(Ok(a))
            }
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    /// Creates a stream of incoming connections
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_lite::stream::StreamExt;
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let mut incoming = listener.incoming();
    ///     while let Some(conn) = incoming.next().await {
    ///         println!("Accepted client: {conn:?}");
    ///     }
    /// });
    /// ```
    pub fn incoming(&self) -> impl Stream<Item = Result<TcpStream>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            let res = listener.accept().await;
            Some((res, listener))
        }))
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Gets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///
    ///     listener.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(listener.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn ttl(&self) -> Result<u32> {
        Ok(self.listener.ttl()?)
    }

    /// Sets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///
    ///     listener.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(listener.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        Ok(self.listener.set_ttl(ttl)?)
    }
}

#[derive(Copy, Clone, Debug)]
/// An Accepted Tcp connection that can be moved to a different executor
///
/// This is useful in situations where the load balancing provided by the
/// Operating System through ReusePort is not desirable. The user can accept the
/// connection in one executor through [`shared_accept`] which returns an
/// AcceptedTcpStream.
///
/// Once the `AcceptedTcpStream` arrives at its destination it can then be made
/// active with [`bind_to_executor`]
///
/// [`shared_accept`]: TcpListener::shared_accept
/// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
pub struct AcceptedTcpStream {
    fd: RawFd,
}

impl AcceptedTcpStream {
    /// Returns the socket address of the remote peer
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let socket = unsafe { Socket::from_raw_fd(self.fd) };
        let sock_addr = socket.peer_addr()?;
        // The above from_raw_fd call isn't intended to close the socket. Hence the
        // intentional leak here.
        let _ = socket.into_raw_fd();
        Ok(sock_addr.as_socket().unwrap())
    }

    /// Binds this `AcceptedTcpStream` to the current executor
    ///
    /// This returns a [`TcpStream`] that can then be used normally
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     channels::shared_channel,
    ///     net::TcpListener,
    ///     LocalExecutor,
    ///     LocalExecutorBuilder,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = shared_channel::new_bounded(1);
    ///     let sender = sender.connect().await;
    ///
    ///     let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    ///
    ///     let accepted = listener.shared_accept().await.unwrap();
    ///     sender.try_send(accepted).unwrap();
    ///
    ///     let ex1 = LocalExecutorBuilder::default()
    ///         .spawn(move || async move {
    ///             let receiver = receiver.connect().await;
    ///             let accepted = receiver.recv().await.unwrap();
    ///             let _ = accepted.bind_to_executor();
    ///         })
    ///         .unwrap();
    ///
    ///     ex1.join().unwrap();
    /// });
    /// ```
    pub fn bind_to_executor(self) -> TcpStream {
        TcpStream {
            stream: unsafe { GlommioStream::from_raw_fd(self.fd) },
        }
    }
}

pin_project! {
    #[derive(Debug)]
    /// A Tcp Stream of bytes. This can be used with [`AsyncRead`], [`AsyncBufRead`] and
    /// [`AsyncWrite`]
    ///
    /// [`AsyncRead`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncRead.html
    /// [`AsyncBufRead`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncBufRead.html
    /// [`AsyncWrite`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncWrite.html

    pub struct TcpStream<B: RxBuf = NonBuffered> {
        stream: GlommioStream<net::TcpStream, B>
    }
}

impl From<socket2::Socket> for TcpStream {
    fn from(socket: socket2::Socket) -> TcpStream {
        Self {
            stream: GlommioStream::from(socket),
        }
    }
}

impl<B: RxBuf> AsRawFd for TcpStream<B> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.stream().as_raw_fd()
    }
}

impl FromRawFd for TcpStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        TcpStream::from(socket)
    }
}

impl IntoRawFd for TcpStream<NonBuffered> {
    fn into_raw_fd(self) -> RawFd {
        self.stream.into_raw_fd()
    }
}

impl TcpStream<NonBuffered> {
    /// Converts this TcpStream back into an AcceptedTcpStream that can be
    /// transferred to another executor.
    ///
    /// This properly cleans up glommio internal state (reactor sources,
    /// timers) while keeping the underlying file descriptor open.
    pub fn into_accepted(self) -> AcceptedTcpStream {
        AcceptedTcpStream {
            fd: self.into_raw_fd(),
        }
    }
}

fn make_tcp_socket(addr: &SocketAddr) -> io::Result<Socket> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    Ok(socket)
}

impl TcpStream {
    /// Creates a TCP connection to the specified address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// })
    /// ```
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<TcpStream> {
        let addr = super::resolve::resolve(addr).await?[0];
        let socket = make_tcp_socket(&addr)?;
        let reactor = crate::executor().reactor();
        let source = reactor.connect(socket.as_raw_fd(), SockaddrStorage::from(addr));
        source.collect_rw().await?;

        Ok(TcpStream {
            stream: GlommioStream::from(socket),
        })
    }

    /// Creates a TCP connection to the specified address with a timeout.
    ///
    /// It is an error to pass a zero `Duration` to this function.
    ///
    /// Timeouts are implemented using `io_uring`'s `IORING_OP_LINK_TIMEOUT`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// use std::time::Duration;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     TcpStream::connect_timeout("127.0.0.1:10000", Duration::from_secs(10))
    ///         .await
    ///         .unwrap();
    /// })
    /// ```
    pub async fn connect_timeout<A: ToSocketAddrs>(
        addr: A,
        duration: Duration,
    ) -> Result<TcpStream> {
        if duration.as_secs() == 0 && duration.subsec_nanos() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot set a 0 duration timeout",
            )
            .into());
        }

        let addr = super::resolve::resolve(addr).await?[0];
        let socket = make_tcp_socket(&addr)?;
        let reactor = crate::executor().reactor();
        let source =
            reactor.connect_timeout(socket.as_raw_fd(), SockaddrStorage::from(addr), duration);

        // connect_timeout submits two sqes to io_uring: a connect sqe soft-linked
        // with a LINK_TIMEOUT sqe. If the timeout fires, the connect sqe fails with
        // ECANCELED. We map that error to TimedOut to match the standard library's API.
        source
            .collect_rw()
            .await
            .map_err(|err| match err.raw_os_error() {
                Some(libc::ECANCELED) => {
                    io::Error::new(io::ErrorKind::TimedOut, "connection timed out")
                }
                _ => err,
            })?;

        Ok(TcpStream {
            stream: GlommioStream::from(socket),
        })
    }

    /// Creates a buffered TCP connection with default receive buffer.
    pub fn buffered(self) -> TcpStream<Preallocated> {
        self.buffered_with(Preallocated::default())
    }

    /// Creates a buffered TCP connection with custom receive buffer.
    pub fn buffered_with<B: Buffered>(self, buf: B) -> TcpStream<B> {
        TcpStream {
            stream: self.stream.buffered_with(buf),
        }
    }
}

impl<B: RxBuf> TcpStream<B> {
    /// Sets the read timeout to the timeout specified.
    ///
    /// If the value specified is [`None`], then read calls will block
    /// indefinitely. An [`Err`] is returned if the zero [`Duration`] is
    /// passed to this method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::TcpStream, LocalExecutor};
    /// # use std::time::Duration;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// stream
    ///     .set_read_timeout(Some(Duration::from_secs(1)))
    ///     .unwrap();
    /// # })
    /// ```
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.set_read_timeout(dur)
    }

    /// Sets the write timeout to the timeout specified.
    ///
    /// If the value specified is [`None`], then write calls will block
    /// indefinitely. An [`Err`] is returned if the zero [`Duration`] is
    /// passed to this method.
    ///
    /// ```no_run
    /// # use glommio::{net::TcpStream, LocalExecutor};
    /// # use std::time::Duration;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// stream
    ///     .set_write_timeout(Some(Duration::from_secs(1)))
    ///     .unwrap();
    /// # })
    /// ```
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.set_write_timeout(dur)
    }

    /// Returns the read timeout of this socket.
    pub fn read_timeout(&self) -> Option<Duration> {
        self.stream.read_timeout()
    }

    /// Returns the write timeout of this socket.
    pub fn write_timeout(&self) -> Option<Duration> {
        self.stream.write_timeout()
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub async fn shutdown(&self, how: Shutdown) -> Result<()> {
        poll_fn(|cx| self.poll_shutdown(cx, how)).await
    }

    /// Polling version of [`shutdown`].
    ///
    /// [`shutdown`]: TcpStream::shutdown
    pub fn poll_shutdown(&self, cx: &mut Context<'_>, how: Shutdown) -> Poll<Result<()>> {
        match ready!(self.stream.poll_shutdown(cx, how)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(err) => Poll::Ready(Err(err.into())),
        }
    }

    /// Sets the value of the `TCP_NODELAY` option on this socket.
    ///
    /// If set, this option disables the Nagle algorithm. This means that
    /// segments are always sent as soon as possible, even if there is only a
    /// small amount of data. When not set, data is buffered until there is a
    /// sufficient amount to send out, thereby avoiding the frequent sending of
    /// small packets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_nodelay(true).expect("set_nodelay call failed");
    /// });
    /// ```
    pub fn set_nodelay(&self, value: bool) -> Result<()> {
        self.stream.stream().set_nodelay(value).map_err(Into::into)
    }

    /// Gets the `TCP_NODELAY` option on this socket.
    ///
    /// For more information about this option, see [`TcpStream::set_nodelay`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_nodelay(true).expect("set_nodelay call failed");
    ///     assert_eq!(stream.nodelay().unwrap(), true);
    /// });
    /// ```
    pub fn nodelay(&self) -> Result<bool> {
        self.stream.stream().nodelay().map_err(Into::into)
    }

    /// Gets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(stream.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn ttl(&self) -> Result<u32> {
        Ok(self.stream.stream().ttl()?)
    }

    /// Sets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(stream.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        Ok(self.stream.stream().set_ttl(ttl)?)
    }

    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// MSG_PEEK as a flag to the underlying `recv` system call.
    pub async fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        self.stream.peek(buf).await.map_err(Into::into)
    }

    /// Returns the socket address of the remote peer of this TCP connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     println!("My peer: {:?}", stream.peer_addr());
    /// })
    /// ```
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.stream.stream().peer_addr().map_err(Into::into)
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     println!("My peer: {:?}", stream.local_addr());
    /// })
    /// ```
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.stream.stream().local_addr().map_err(Into::into)
    }
}

impl<B: Buffered + Unpin> AsyncBufRead for TcpStream<B> {
    fn poll_fill_buf<'a>(
        self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        let this = self.project();
        this.stream.poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.stream.consume(amt);
    }
}

/// TLS record types, as the kernel reports them on a socket with kernel TLS
/// enabled.
///
/// Only [`APPLICATION_DATA`](tls_record::APPLICATION_DATA) carries payload a
/// reader wants; the rest have to be handed back to whatever ran the
/// handshake. A TLS 1.3 key update arrives as
/// [`HANDSHAKE`](tls_record::HANDSHAKE), and ignoring it breaks the
/// connection a few records later.
pub mod tls_record {
    /// `change_cipher_spec`.
    pub const CHANGE_CIPHER_SPEC: u8 = 20;
    /// `alert` -- including `close_notify`.
    pub const ALERT: u8 = 21;
    /// `handshake`, which post-handshake means a TLS 1.3 key update.
    pub const HANDSHAKE: u8 = 22;
    /// `application_data`: the bytes an ordinary read would have returned.
    pub const APPLICATION_DATA: u8 = 23;
}

/// A pipe's capacity, and so the most one splice can move.
const PIPE_CAPACITY: usize = 65536;

/// Requests that `splice(2)` fail with `EAGAIN` instead of blocking when
/// the *pipe* end of the call cannot move data immediately.
///
/// Named here, rather than reaching for `libc::SPLICE_F_NONBLOCK` inline at
/// the call site, purely for documentation: what follows is the reasoning
/// for setting it on exactly one of the two splices in `send_file` and not
/// the other, plus a note on what it actually did.
///
/// Per `splice(2)`, this flag governs the *pipe* side of the call; the
/// other descriptor still blocks unless it independently carries
/// `O_NONBLOCK`. Set only on the pipe -> socket splice, never the
/// file -> pipe one -- not because of the socket (which is `O_NONBLOCK`
/// regardless via `set_nonblocking(true)`, and which this flag does not
/// govern), but because a regular file has no meaningful non-blocking
/// semantics to request, and the file -> pipe pipe is always empty when
/// written to (fully drained before each refill), so there is nothing to
/// wait on there either way.
///
/// The pipe here (`Pipe::new`, below) is already opened `O_NONBLOCK` via
/// `pipe2`, so setting this flag is largely redundant with a property the
/// pipe fd already carries -- it is the explicit, in-SQE request for
/// something the fd flag already implies. That redundancy is the simplest
/// available explanation for why setting it was measured to change nothing
/// observable (see the `EAGAIN` arm's comment, below, for what was
/// measured). It is kept because it is the semantically correct request per
/// `splice(2)`, not because the `EAGAIN` arm depends on it: a kernel that
/// surfaces `EAGAIN` on this call does so whether or not this flag is set,
/// since `O_NONBLOCK` on the pipe already asks for exactly that on the side
/// the flag actually controls.
const SPLICE_F_NONBLOCK: u32 = libc::SPLICE_F_NONBLOCK;

/// A pipe owned for the duration of one `send_file`.
///
/// Per call rather than pooled, and `O_NONBLOCK` rather than blocking. Both
/// are load-bearing: a blocking pipe makes `splice` into a full pipe block
/// forever, and a pipe reused across calls could carry one transfer's
/// leftovers into the next. A pipe that dies with its call cannot do either.
struct Pipe {
    fds: [RawFd; 2],
    reactor: Weak<Reactor>,
}

impl Pipe {
    fn new(reactor: &Rc<Reactor>) -> Result<Self> {
        let mut fds = [0 as RawFd; 2];
        let ok = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if ok < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Pipe {
            fds,
            reactor: Rc::downgrade(reactor),
        })
    }

    fn reader(&self) -> RawFd {
        self.fds[0]
    }

    fn writer(&self) -> RawFd {
        self.fds[1]
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        // Closed on every exit path, including a panic or a cancelled
        // future, so a half-drained pipe is never reachable by anything
        // else -- but not with a raw `libc::close()`. `send_file`'s future
        // can be dropped while suspended at a `collect_rw().await` on an
        // in-flight splice against one of these fds (a `select!`, a
        // timeout, a cancelled task). `Source::drop` only requests an async
        // cancel for a dispatched op and does not wait for the completion,
        // so closing here synchronously would free the fd number before the
        // kernel is done with it -- on this single-threaded reactor, the
        // very next `open`/`socket`/`pipe` could reuse that number while
        // the stale splice is still resolving against it.
        //
        // `GlommioFile::drop` (glommio/src/io/glommio_file.rs) hits the
        // identical hazard and solves it the same way: route the close
        // through the reactor's `async_close`, which queues it as an
        // ordinary op instead of racing the kernel.
        if let Some(r) = self.reactor.upgrade() {
            for fd in self.fds {
                r.sys.async_close(fd);
            }
        } else {
            // The executor is gone, so there is no reactor left to race
            // with and no queue to submit to. Best effort.
            for fd in self.fds {
                unsafe { libc::close(fd) };
            }
        }
    }
}

impl<B: RxBuf + Unpin> TcpStream<B> {
    /// Sends `len` bytes of `file`, starting at `offset`, without the bytes
    /// entering this process.
    ///
    /// The kernel moves page references through a pipe rather than copying
    /// through a user buffer, so the payload never occupies this process's
    /// address space. That is what this method is for; it is not a way to
    /// spend less CPU. See `# Cost` below before reaching for it.
    ///
    /// Returns the number of bytes actually sent, which is short if the file
    /// ends before `len` bytes are available.
    ///
    /// On a mid-transfer error, the count of bytes already sent is lost --
    /// unlike the short-on-EOF case, the error carries no byte count, so the
    /// caller only knows the socket may hold a partial write.
    ///
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
    ///
    /// # Cost
    ///
    /// Measured against `read_at` plus `write_all` in
    /// `examples/send_file_ladder.rs` (Threadripper PRO 9975WX, kernel 7.2.2,
    /// XFS on NVMe), splicing costs *more* total CPU than copying at every
    /// payload from 1 KiB to 8 MiB, except one case:
    ///
    /// | file state | payload | vs. read + write_all |
    /// |------------|--------:|---------------------:|
    /// | in page cache | any size measured | 1.8x - 2.8x more CPU |
    /// | not in page cache | up to 64 KiB | 0.53x - 0.96x, a win |
    /// | not in page cache | above 64 KiB | 1.4x - 1.7x more CPU |
    /// | `O_DIRECT` ([`DmaFile`](crate::io::DmaFile)) | any size measured | 1.4x - 3.5x more CPU |
    ///
    /// The reason is the pipe: it holds 64 KiB, so a payload is spliced in
    /// 64 KiB chunks, two ring operations each, and the file-to-pipe leg
    /// reaches an `iou-wrk` kernel worker every time. A single `read_at`
    /// submits one operation for the whole payload however large it is.
    ///
    /// So: prefer this when the bytes must not enter the process, or when the
    /// file is cold and the payload fits one pipe-load. Prefer `read_at` plus
    /// `write_all` otherwise. The numbers and the mechanism are in
    /// `docs/investigations/io-path/network.md`.
    ///
    /// # Descriptors
    ///
    /// Each call opens its own pipe -- two file descriptors -- and closes it
    /// through the reactor rather than synchronously, so a descriptor is
    /// released a little after the call that owned it returns. There is no
    /// pipe pool. Serving many files concurrently therefore reaches
    /// `RLIMIT_NOFILE` sooner than the number of concurrent sends alone
    /// would suggest: budget two descriptors per in-flight `send_file`, plus
    /// slack for closes still in flight.
    ///
    /// # Alignment
    ///
    /// A [`DmaFile`](crate::io::DmaFile) is opened `O_DIRECT`, which requires
    /// the splice source offset to be a multiple of the device's logical
    /// block size. `offset` that does not satisfy this is refused here,
    /// before anything is submitted, with an error naming both the offset
    /// and the alignment it needed to satisfy -- rather than reaching the
    /// kernel and coming back as a bare `EINVAL` with no indication of what
    /// was wrong or where. A [`BufferedFile`](crate::io::BufferedFile) has no
    /// such requirement and accepts any offset.
    pub async fn send_file<F: SpliceSource>(
        &mut self,
        file: &F,
        offset: u64,
        len: usize,
    ) -> Result<usize> {
        let alignment = file.splice_offset_alignment();
        if alignment > 1 && !offset.is_multiple_of(alignment) {
            return Err(crate::GlommioError::IoError(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "send_file offset {offset} is not a multiple of {alignment}; a file opened \
                     O_DIRECT can only be spliced from an aligned offset"
                ),
            )));
        }

        let reactor = crate::executor().reactor();
        let pipe = Pipe::new(&reactor)?;
        let fd_in = file.splice_fd();
        // `GlommioStream` implements `AsRawFd` only for `NonBuffered`, so reach
        // the socket the way this file's own `AsRawFd` impl does.
        let fd_out = self.stream.stream().as_raw_fd();

        let mut sent = 0usize;
        let mut pos = offset;

        while sent < len {
            let want = std::cmp::min(len - sent, PIPE_CAPACITY) as u32;
            let filled = reactor
                .splice(fd_in, pos as i64, pipe.writer(), want, 0)
                .collect_rw()
                .await?;
            if filled == 0 {
                // End of file: nothing more to send.
                break;
            }

            let mut drained = 0usize;
            while drained < filled {
                let moved = match reactor
                    .splice(
                        pipe.reader(),
                        -1,
                        fd_out,
                        (filled - drained) as u32,
                        SPLICE_F_NONBLOCK,
                    )
                    .collect_rw()
                    .await
                {
                    Ok(moved) => moved,
                    Err(err) if err.raw_os_error() == Some(libc::EAGAIN) => {
                        // The send buffer is full. Park until the socket is
                        // writable again rather than spinning: after a
                        // partial drain by the peer, an immediate retry
                        // still returns EAGAIN.
                        //
                        // Dormant on this repo's development setup (kernel
                        // 7.2.0, io-uring crate 0.7.14): this arm was never
                        // observed to fire in testing, with SPLICE_F_NONBLOCK
                        // set (see its definition above) or without it --
                        // every splice completed with the full requested
                        // length in one shot, never a short count or an
                        // EAGAIN error, even against a genuinely stalled
                        // peer that measurably backpressured the transfer.
                        // Measured, not verified against kernel source: why
                        // is a hypothesis, and there are two candidates.
                        // Either io_uring's poll-based retry for pollable
                        // descriptors resolves the wait before posting a
                        // completion, in which case this arm could still
                        // fire under a different kernel or ring mode; or
                        // IORING_OP_SPLICE punts to an io-wq worker where
                        // -EAGAIN is purely an internal retry signal that
                        // never becomes a CQE, in which case this arm is
                        // unreachable by construction for this op rather
                        // than incidentally dormant. This arm stays either
                        // way because splice(2) documents EAGAIN as a
                        // permitted return and both candidate mechanisms are
                        // implementation details, not a contract; without
                        // this arm a future kernel's EAGAIN would become a
                        // hard mid-transfer error instead of a wait.
                        // Separately, and regardless of which of the above
                        // is true, `a_backpressured_send_does_not_stall_the_
                        // executor` (glommio/tests/send_file.rs) proves the
                        // property this arm exists to protect -- a
                        // neighbour task keeps running while send_file is
                        // backpressured -- so this arm's dormancy costs
                        // nothing observable today either way.
                        reactor.poll_write_ready(fd_out).collect_rw().await?;
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                if moved == 0 {
                    // Neither EOF (this is a pipe, not the source file) nor
                    // EAGAIN (handled above): a zero-length splice here means
                    // no byte moved and nothing was learned that would make
                    // the next attempt different, so looping again would
                    // spin forever instead of making progress.
                    //
                    // Defensive, same as the EAGAIN arm above: not observed
                    // to trigger in testing.
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "splice(pipe -> socket) made no progress",
                    )
                    .into());
                }
                drained += moved;
            }

            pos += filled as u64;
            sent += filled;
        }

        Ok(sent)
    }

    /// Reads one TLS record from a socket with kernel TLS enabled, together
    /// with the record type the kernel attached to it.
    ///
    /// glommio does no TLS itself. This exists because kernel TLS makes an
    /// ordinary read insufficient: once `TLS_RX` is installed, a record that
    /// is not application data can only be read with room for the kernel to
    /// report its type, and a plain [`read`](futures_lite::io::AsyncReadExt)
    /// with such a record at the head of the queue **fails with `EIO`**. A
    /// TLS 1.3 key update is such a record, so a long-lived connection that
    /// only ever calls `read` will eventually see a spurious I/O error and no
    /// way to find out what it was.
    ///
    /// The record type is `None` when the socket has no kernel TLS on it, in
    /// which case this is an ordinary read.
    ///
    /// This bypasses the receive buffer of a [`buffered`](Self::buffered)
    /// stream, so do not mix the two on one connection.
    ///
    /// # The rest of kernel TLS is the caller's
    ///
    /// Enabling it -- `TCP_ULP`, then `TLS_TX`/`TLS_RX` with the keys a
    /// handshake produced -- happens on the raw descriptor, which
    /// [`AsRawFd`] hands over. The [`ktls`](https://docs.rs/ktls) crate does
    /// that against rustls and is the sane way in.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::{TcpStream, tls_record}, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async {
    /// let mut stream = TcpStream::connect("127.0.0.1:443").await.unwrap();
    /// // ... enable kernel TLS on stream.as_raw_fd() ...
    /// let mut buf = [0u8; 4096];
    /// match stream.recv_tls_record(&mut buf).await.unwrap() {
    ///     (read, Some(tls_record::APPLICATION_DATA)) => { /* payload */ }
    ///     (read, Some(tls_record::HANDSHAKE)) => { /* feed it back to rustls */ }
    ///     (read, other) => { /* alert, or a plain socket */ }
    /// }
    /// # });
    /// ```
    pub async fn recv_tls_record(&mut self, buf: &mut [u8]) -> Result<(usize, Option<u8>)> {
        poll_fn(|cx| self.poll_recv_tls_record(cx, buf)).await
    }

    /// Poll version of [`recv_tls_record`](Self::recv_tls_record).
    pub fn poll_recv_tls_record(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<(usize, Option<u8>)>> {
        self.stream
            .poll_recv_record(cx, buf)
            .map(|result| result.map_err(Into::into))
    }
}

impl<B: RxBuf + Unpin> AsyncRead for TcpStream<B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<B: RxBuf + Unpin> AsyncWrite for TcpStream<B> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{channels::shared_channel, enclose, timer::Timer, LocalExecutorBuilder};
    use futures_lite::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
        StreamExt,
    };
    use std::{
        cell::Cell,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    /// An accept that completes while no `accept` future is polling it is
    /// parked in the listener. Dropping the listener must close that socket;
    /// otherwise nobody owns it and the client waits forever.
    #[test]
    fn dropped_listener_closes_an_accepted_socket_nobody_collected() {
        test_executor!(async move {
            for _ in 0..16 {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                {
                    // Arm an io_uring accept, then abandon the future. The
                    // listener keeps the in-flight source for the next call.
                    let accept = listener.accept();
                    futures_lite::pin!(accept);
                    assert!(futures_lite::future::poll_once(accept).await.is_none());
                }
                Timer::new(Duration::from_millis(1)).await;
                let mut client = std::net::TcpStream::connect(addr).unwrap();
                // The reactor reaps the completed accept into that source.
                Timer::new(Duration::from_millis(1)).await;
                drop(listener);

                client
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut byte = [0u8; 1];
                let read = loop {
                    match std::io::Read::read(&mut client, &mut byte) {
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        other => break other,
                    }
                };
                match read {
                    Ok(0) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
                    other => panic!("accepted socket leaked; client read returned {other:?}"),
                }
            }
        });
    }

    #[test]
    fn tcp_listener_ttl() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_ttl(100).unwrap();
            assert_eq!(listener.ttl().unwrap(), 100);
        });
    }

    #[test]
    fn tcp_stream_ttl() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();
            stream.set_ttl(100).unwrap();
            assert_eq!(stream.ttl().unwrap(), 100);
        });
    }

    #[test]
    fn tcp_stream_nodelay() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();
            stream.set_nodelay(true).expect("set_nodelay call failed");
            assert!(stream.nodelay().unwrap());
        });
    }

    #[test]
    fn connect_local_server() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle = crate::spawn_local(enclose! { (coord) async move {
                coord.set(1);
                let stream = listener.accept().await?;
                stream.peer_addr()
            }});

            while coord.get() != 1 {
                crate::executor().yield_task_queue_now().await;
            }
            let stream = TcpStream::connect(addr).await.unwrap();
            assert_eq!(listener_handle.await.unwrap(), stream.local_addr().unwrap());
        });
    }

    #[test]
    fn a_buffered_read_completes_when_data_arrives_later() {
        // The speculative `recv` fails here by construction -- nothing has
        // been sent when the reader starts -- so this exercises the
        // completion read, where the buffer is lent to the kernel across a
        // suspension.
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let reader = crate::spawn_local(async move {
                let mut stream = listener.accept().await.unwrap().buffered();
                let mut buf = [0u8; 16];
                let read = stream.read(&mut buf).await.unwrap();
                buf[..read].to_vec()
            })
            .detach();

            let mut writer = TcpStream::connect(addr).await.unwrap();
            // Long enough that the reader is certainly suspended on the
            // completion, rather than racing it.
            Timer::new(Duration::from_millis(50)).await;
            writer.write_all(b"late").await.unwrap();

            assert_eq!(reader.await.unwrap(), b"late".to_vec());
        });
    }

    #[test]
    fn dropping_a_stream_with_a_read_in_flight_is_safe() {
        // The buffer belongs to the kernel until the completion arrives, so
        // dropping the stream must not take it back. Repeated, because a
        // use-after-free here would be a race rather than a certainty.
        test_executor!(async move {
            for _ in 0..50 {
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();

                let reader = crate::spawn_local(async move {
                    let mut stream = listener.accept().await.unwrap().buffered();
                    let mut buf = [0u8; 16];
                    // Started, then abandoned: the future is dropped with the
                    // read outstanding, and the stream with it.
                    let _ = futures_lite::future::poll_once(stream.read(&mut buf)).await;
                })
                .detach();

                let mut writer = TcpStream::connect(addr).await.unwrap();
                reader.await.unwrap();

                // The peer writes into what the kernel still owns.
                let _ = writer.write_all(b"after the drop").await;
            }

            // Still alive, still working.
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let echo = crate::spawn_local(async move {
                let mut stream = listener.accept().await.unwrap().buffered();
                let mut buf = [0u8; 8];
                let read = stream.read(&mut buf).await.unwrap();
                buf[..read].to_vec()
            })
            .detach();

            let mut writer = TcpStream::connect(addr).await.unwrap();
            writer.write_all(b"alive").await.unwrap();
            assert_eq!(echo.await.unwrap(), b"alive".to_vec());
        });
    }

    #[test]
    fn write_vectored_writes_every_slice() {
        // An HTTP response is a status line, headers and a body: three
        // buffers the caller does not want to concatenate. Without a
        // `poll_write_vectored` of our own, the futures-io default writes the
        // first slice and returns, and the caller pays a syscall per piece.
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let server = crate::spawn_local(async move {
                let mut stream = listener.accept().await.unwrap();
                let mut received = Vec::new();
                let mut buf = [0u8; 64];
                loop {
                    let read = stream.read(&mut buf).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    received.extend_from_slice(&buf[..read]);
                }
                received
            })
            .detach();

            let status = &b"HTTP/1.1 200 OK\r\n"[..];
            let headers = &b"content-length: 2\r\n\r\n"[..];
            let body = &b"hi"[..];
            let whole: Vec<u8> = [status, headers, body].concat();

            let mut client = TcpStream::connect(addr).await.unwrap();
            let written = client
                .write_vectored(&[
                    IoSlice::new(status),
                    IoSlice::new(headers),
                    IoSlice::new(body),
                ])
                .await
                .unwrap();

            assert_eq!(
                written,
                whole.len(),
                "one vectored write should have taken every slice, not just the first"
            );

            client.close().await.unwrap();
            assert_eq!(server.await.unwrap(), whole);
        });
    }

    #[test]
    fn multi_executor_bind_works() {
        test_executor!(async move {
            let addr_getter = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = addr_getter.local_addr().unwrap();
            let (first_sender, first_receiver) = shared_channel::new_bounded(1);
            let (second_sender, second_receiver) = shared_channel::new_bounded(1);

            let ex1 = LocalExecutorBuilder::default()
                .spawn(move || async move {
                    let receiver = first_receiver.connect().await;
                    let _ = TcpListener::bind(addr).unwrap();
                    receiver.recv().await.unwrap();
                })
                .unwrap();

            let ex2 = LocalExecutorBuilder::default()
                .spawn(move || async move {
                    let receiver = second_receiver.connect().await;
                    let _ = TcpListener::bind(addr).unwrap();
                    receiver.recv().await.unwrap();
                })
                .unwrap();

            Timer::new(Duration::from_millis(100)).await;

            let sender = first_sender.connect().await;
            sender.try_send(0).unwrap();
            let sender = second_sender.connect().await;
            sender.try_send(0).unwrap();

            ex1.join().unwrap();
            ex2.join().unwrap();
        });
    }

    #[test]
    fn multi_executor_accept() {
        let (sender, receiver) = shared_channel::new_bounded(1);
        let (addr_sender, addr_receiver) = shared_channel::new_bounded(1);
        let connected = Arc::new(AtomicUsize::new(0));

        let status = connected.clone();
        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                let addr_sender = addr_sender.connect().await;
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                addr_sender.try_send(addr).unwrap();

                status.store(1, Ordering::Relaxed);
                let accepted = listener.shared_accept().await.unwrap();
                sender.try_send(accepted).unwrap();
            })
            .unwrap();

        let status = connected.clone();
        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let accepted = receiver.recv().await.unwrap();
                let _ = accepted.bind_to_executor();
                status.store(2, Ordering::Relaxed);
            })
            .unwrap();

        let ex3 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = addr_receiver.connect().await;
                let addr = receiver.recv().await.unwrap();
                TcpStream::connect(addr).await.unwrap();
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
        ex3.join().unwrap();
        assert_eq!(connected.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn stream_of_connections() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle = crate::spawn_local(enclose! { (coord) async move {
                coord.set(1);
                listener.incoming().take(4).try_for_each(|addr| {
                    addr.map(|_| ())
                }).await
            }});

            while coord.get() != 1 {
                crate::executor().yield_task_queue_now().await;
            }
            let mut handles = Vec::with_capacity(4);
            for _ in 0..4 {
                handles.push(
                    crate::spawn_local(async move { TcpStream::connect(addr).await }).detach(),
                );
            }

            for handle in handles.drain(..) {
                handle.await.unwrap().unwrap();
            }
            listener_handle.await.unwrap();

            let res = TcpStream::connect(addr).await;
            // server is now dead, connection must fail
            assert!(res.is_err())
        });
    }

    #[test]
    fn parallel_accept() {
        test_executor!(async move {
            let listener = Rc::new(TcpListener::bind("127.0.0.1:0").unwrap());
            let addr = listener.local_addr().unwrap();

            let mut handles = Vec::new();

            for _ in 0..128 {
                handles.push(
                    crate::spawn_local(enclose! { (listener) async move {
                        let _accept = listener.accept().await.unwrap();
                    }})
                    .detach(),
                );
            }
            // give it some time to make sure that all tasks above were sent down to
            // the ring
            Timer::new(Duration::from_millis(100)).await;

            // Now we should be able to establish 128 connections and all of that would
            // accept
            for _ in 0..128 {
                handles.push(
                    crate::spawn_local(async move {
                        let _stream = TcpStream::connect(addr).await.unwrap();
                    })
                    .detach(),
                );
            }

            for handle in handles {
                handle.await.unwrap();
            }
        });
    }

    #[test]
    fn connect_and_ping_pong() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle = crate::spawn_local(enclose! { (coord) async move {
                coord.set(1);
                let mut stream = listener.accept().await?;
                let mut byte = [0u8; 1];
                let read = stream.read(&mut byte).await?;
                assert_eq!(read, 1);
                io::Result::Ok(byte[0])
            }})
            .detach();

            while coord.get() != 1 {
                crate::executor().yield_task_queue_now().await;
            }
            let mut stream = TcpStream::connect(addr).await.unwrap();

            let byte = [65u8; 1];
            let b = stream.write(&byte).await.unwrap();
            assert_eq!(b, 1);
            assert_eq!(listener_handle.await.unwrap().unwrap(), 65u8);
        });
    }

    #[test]
    fn test_read_until() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let mut buf = Vec::new();
                stream.read_until(10, &mut buf).await?;
                io::Result::Ok(buf.len())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            let vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            let b = stream.write(&vec).await.unwrap();
            assert_eq!(b, 10);
            assert_eq!(listener_handle.await.unwrap().unwrap(), 10);
        });
    }

    #[test]
    fn test_read_line() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let mut buf = String::new();
                stream.read_line(&mut buf).await?;
                io::Result::Ok(buf.len())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            let b = stream.write(b"line\n").await.unwrap();
            assert_eq!(b, 5);
            assert_eq!(listener_handle.await.unwrap().unwrap(), 5);
        });
    }

    #[test]
    fn test_lines() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let stream = listener.accept().await?.buffered();
                io::Result::Ok(stream.lines().count().await)
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write(b"line1\nline2\nline3\n").await.unwrap();
            stream.write(b"line4\nline5\nline6\n").await.unwrap();
            stream.close().await.unwrap();
            assert_eq!(listener_handle.await.unwrap().unwrap(), 6);
        });
    }

    #[test]
    fn multibuf_fill() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let buf = stream.fill_buf().await?;
                // likely both messages were coalesced together
                assert_eq!(&buf[0..4], b"msg1");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg2");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 0);
                io::Result::Ok(())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            let b = stream.write(b"msg1").await.unwrap();
            assert_eq!(b, 4);
            stream.write(b"msg2").await.unwrap();
            assert_eq!(b, 4);
            stream.close().await.unwrap();
            listener_handle.await.unwrap().unwrap();
        });
    }

    #[test]
    fn overconsume() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 4);
                stream.consume(100);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 0);
                io::Result::Ok(())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write(b"msg1").await.unwrap();
            stream.close().await.unwrap();
            listener_handle.await.unwrap().unwrap();
        });
    }

    #[test]
    fn repeated_fill_before_consume() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg1");
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg1");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert!(buf.is_empty());
                io::Result::Ok(())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write(b"msg1").await.unwrap();
            stream.close().await.unwrap();
            listener_handle.await.unwrap().unwrap();
        });
    }

    #[test]
    fn peek() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?;
                let mut buf = [0u8; 64];
                for _ in 0..10 {
                    let b = stream.peek(&mut buf).await?;
                    assert_eq!(b, 4);
                    assert_eq!(&buf[0..4], b"msg1");
                }
                stream.read(&mut buf).await?;
                stream.peek(&mut buf).await
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write(b"msg1").await.unwrap();
            stream.close().await.unwrap();
            let res = listener_handle.await.unwrap().unwrap();
            assert_eq!(res, 0);
        });
    }

    #[test]
    fn tcp_connect_timeout() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            match TcpStream::connect_timeout(addr, Duration::from_millis(250)).await {
                Ok(_) => {}
                Err(e) => panic!("unexpected error {}", e),
            }
        });
    }

    // adapted from socket2 test:
    // https://docs.rs/socket2/0.3.19/src/socket2/socket.rs.html#971-982
    #[test]
    fn tcp_connect_timeout_error() {
        test_executor!(async move {
            // this IP is unroutable, so connections should always time out
            match TcpStream::connect_timeout("10.255.255.1:80", Duration::from_millis(250)).await {
                Ok(_) => panic!("unexpected success"),
                Err(GlommioError::IoError(ref e)) if e.kind() == io::ErrorKind::TimedOut => {}
                Err(e) => panic!("unexpected error {}", e),
            }
        });
    }

    #[test]
    fn tcp_read_timeout() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let ltask = crate::spawn_local(async move {
                let mut stream = listener.accept().await?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut buf = [0u8; 64];
                let now = Instant::now();
                match stream.read(&mut buf).await {
                    Ok(_) => unreachable!(),
                    Err(x) => {
                        assert_eq!(x.kind(), io::ErrorKind::TimedOut);
                    }
                };
                assert!(now.elapsed().as_secs() >= 1);
                io::Result::Ok(0)
            });

            let _s = TcpStream::connect(addr).await.unwrap();
            ltask.await.unwrap();
        });
    }

    #[test]
    fn tcp_force_poll() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let ltask = crate::spawn_local(async move {
                let mut stream = listener.accept().await?;
                poll_fn(|cx| {
                    let mut buf = [0u8; 64];
                    // try to overflow the amount of wakers possible
                    for _ in 0..64_000 {
                        if Pin::new(&mut stream).poll_read(cx, &mut buf).is_ready() {
                            panic!("should be pending");
                        }
                    }
                    Poll::Ready(())
                })
                .await;
                io::Result::Ok(0)
            });

            let _s = TcpStream::connect(addr).await.unwrap();
            ltask.await.unwrap();
        });
    }

    #[test]
    fn tcp_invalid_timeout() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let ltask = crate::spawn_local(async move {
                let stream = listener.accept().await?;
                stream
                    .set_write_timeout(Some(Duration::from_nanos(0)))
                    .unwrap_err();
                assert!(stream.write_timeout().is_none());
                stream
                    .set_write_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                assert_eq!(stream.write_timeout(), Some(Duration::from_secs(1)));
                io::Result::Ok(0)
            });

            let _s = TcpStream::connect(addr).await.unwrap();
            ltask.await.unwrap();
        });
    }

    #[test]
    fn accepted_tcp_stream_peer_addr() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let peer_addr = crate::spawn_local(async move {
                let accepted = listener.shared_accept().await.unwrap();
                let peer_addr = accepted.peer_addr().unwrap();
                let stream = accepted.bind_to_executor();
                assert_eq!(peer_addr, stream.peer_addr().unwrap());
                peer_addr
            });

            let s = TcpStream::connect(addr).await.unwrap();
            assert_eq!(s.local_addr().unwrap(), peer_addr.await);
        });
    }

    #[test]
    fn tcp_stream_into_raw_fd() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let stream = TcpStream::connect(addr).await.unwrap();
            let original_fd = stream.as_raw_fd();

            let raw_fd = stream.into_raw_fd();
            assert_eq!(original_fd, raw_fd);

            let restored_stream = unsafe { TcpStream::from_raw_fd(raw_fd) };
            assert_eq!(restored_stream.as_raw_fd(), raw_fd);

            std::mem::drop(restored_stream);
        });
    }

    #[test]
    fn tcp_stream_into_raw_fd_with_timeouts() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let stream = TcpStream::connect(addr).await.unwrap();

            // Set timeouts to verify they get cleaned up
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(30)))
                .unwrap();

            assert_eq!(stream.read_timeout(), Some(Duration::from_secs(30)));
            assert_eq!(stream.write_timeout(), Some(Duration::from_secs(30)));

            let original_fd = stream.as_raw_fd();

            let raw_fd = stream.into_raw_fd();
            assert_eq!(original_fd, raw_fd);

            // Create a new stream and verify timeouts are reset
            let restored_stream = unsafe { TcpStream::from_raw_fd(raw_fd) };
            assert_eq!(restored_stream.read_timeout(), None);
            assert_eq!(restored_stream.write_timeout(), None);

            std::mem::drop(restored_stream);
        });
    }

    #[test]
    fn tcp_stream_into_accepted_round_trip() {
        // A connection is accepted and used on one executor, converted back to
        // an AcceptedTcpStream with into_accepted(), migrated to a second
        // executor, and resumed there - all on the same underlying fd.
        let (stream_sender, stream_receiver) = shared_channel::new_bounded(1);
        let (addr_sender, addr_receiver) = shared_channel::new_bounded(1);

        // ex1: accept, do the first exchange, then hand the live connection off.
        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let stream_sender = stream_sender.connect().await;
                let addr_sender = addr_sender.connect().await;
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                addr_sender
                    .try_send(listener.local_addr().unwrap())
                    .unwrap();

                let mut stream = listener.accept().await.unwrap();
                let mut buf = [0u8; 4];
                stream.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"ping");
                stream.write_all(b"pong").await.unwrap();

                // Dispose of glommio state but keep the fd open, then migrate it.
                stream_sender.try_send(stream.into_accepted()).unwrap();
            })
            .unwrap();

        // ex2: bind the migrated connection and resume the same TCP session.
        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let stream_receiver = stream_receiver.connect().await;
                let accepted = stream_receiver.recv().await.unwrap();
                let mut stream = accepted.bind_to_executor();

                let mut buf = [0u8; 5];
                stream.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"hello");
                stream.write_all(b"world").await.unwrap();
            })
            .unwrap();

        // ex3: client driving both stages over a single connection.
        let ex3 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let addr_receiver = addr_receiver.connect().await;
                let addr = addr_receiver.recv().await.unwrap();
                let mut client = TcpStream::connect(addr).await.unwrap();

                client.write_all(b"ping").await.unwrap();
                let mut buf = [0u8; 4];
                client.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"pong");

                client.write_all(b"hello").await.unwrap();
                let mut buf = [0u8; 5];
                client.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"world");
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
        ex3.join().unwrap();
    }
}
