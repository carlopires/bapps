// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! This module provides glommio's networking support.
use crate::sys;
use nix::sys::socket::{MsgFlags, SockaddrLike};
use std::{
    io,
    os::{
        fd::AsRawFd,
        unix::io::{BorrowedFd, RawFd},
    },
};

fn yolo_accept(fd: BorrowedFd<'_>) -> Option<io::Result<RawFd>> {
    // Listeners are put in non-blocking mode when they are created, so this
    // returns `EAGAIN` rather than blocking the executor when the backlog is
    // empty. It used to flip the flag on and back around every call, which
    // cost two extra `fcntl` syscalls per accepted connection and measured at
    // more than half the end-to-end cost of accepting one.
    debug_assert!(
        {
            let flags = nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL).unwrap();
            nix::fcntl::OFlag::from_bits_truncate(flags).contains(nix::fcntl::OFlag::O_NONBLOCK)
        },
        "a listener reached accept in blocking mode: whoever built it skipped \
         set_nonblocking, and this call would park the whole executor"
    );

    match sys::accept_syscall(fd.as_raw_fd()) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_send(fd: RawFd, buf: &[u8]) -> Option<io::Result<usize>> {
    match sys::send_syscall(fd, buf.as_ptr(), buf.len(), MsgFlags::MSG_DONTWAIT.bits()) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_peek(fd: RawFd, buf: &mut [u8]) -> Option<io::Result<usize>> {
    match sys::recv_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        (MsgFlags::MSG_DONTWAIT | MsgFlags::MSG_PEEK).bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

/// The record-aware counterpart of [`yolo_recv`].
fn yolo_recv_record(fd: RawFd, buf: &mut [u8]) -> Option<io::Result<(usize, Option<u8>)>> {
    match sys::recvmsg_record_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_recv(fd: RawFd, buf: &mut [u8]) -> Option<io::Result<usize>> {
    match sys::recv_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_recvmsg<T: SockaddrLike>(
    fd: RawFd,
    buf: &mut [u8],
    flags: MsgFlags,
) -> Option<io::Result<(usize, T)>> {
    match sys::recvmsg_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        (flags | MsgFlags::MSG_DONTWAIT).bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

/// The vectored counterpart of [`yolo_send`].
///
/// `IoSlice` is guaranteed ABI-compatible with `struct iovec`, so the slice is
/// handed to the kernel as-is rather than copied into one.
fn yolo_sendv(fd: RawFd, bufs: &[std::io::IoSlice<'_>]) -> Option<io::Result<usize>> {
    // The kernel rejects anything longer in one call; writing a prefix is
    // allowed and the caller loops, exactly as it does for a short write.
    let len = std::cmp::min(bufs.len(), libc::UIO_MAXIOV as usize);

    match sys::sendmsg_iov_syscall(
        fd,
        bufs.as_ptr() as *const libc::iovec,
        len,
        MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_sendmsg(
    fd: RawFd,
    buf: &[u8],
    addr: &impl nix::sys::socket::SockaddrLike,
) -> Option<io::Result<usize>> {
    match sys::sendmsg_syscall(
        fd,
        buf.as_ptr(),
        buf.len(),
        addr,
        MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

mod datagram;
mod resolve;
mod stream;
mod tcp_socket;
mod udp_socket;
mod unix;
pub use self::{
    resolve::{Resolution, ToSocketAddrs},
    stream::{Buffered, NonBuffered, OwnedRxBuf, Preallocated, RxBuf},
    tcp_socket::{tls_record, AcceptedTcpStream, TcpListener, TcpStream},
    udp_socket::UdpSocket,
    unix::{AcceptedUnixStream, UnixDatagram, UnixListener, UnixStream},
};
