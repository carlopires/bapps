//! Reading TLS records off a socket with kernel TLS enabled.
//!
//! glommio does no TLS. What it has to get right is that a record which is
//! *not* application data can be read at all: once `TLS_RX` is installed, the
//! kernel refuses a plain `recv` with such a record at the head of the queue
//! and returns `EIO`. A TLS 1.3 key update is such a record, so a connection
//! that only ever calls `read` eventually fails for a reason it cannot see.
//!
//! The kTLS half of this file needs the `tls` kernel module. Where it is
//! missing the test says so and passes, rather than pretending to have run --
//! the machine this was written on had `CONFIG_TLS=m` and no module
//! installed, so that path is exercised by CI and by nobody else.

use futures_lite::io::AsyncWriteExt;
use glommio::{
    net::{tls_record, TcpListener, TcpStream},
    LocalExecutor,
};
use std::os::unix::io::{AsRawFd, RawFd};

/// Installs a key on one direction of a socket. The same key goes on both
/// ends: no handshake is needed to exercise the record layer, which is all
/// glommio touches.
fn install_key(fd: RawFd, direction: libc::c_int) -> std::io::Result<()> {
    #[repr(C)]
    struct CryptoInfoAesGcm128 {
        version: u16,
        cipher_type: u16,
        iv: [u8; 8],
        key: [u8; 16],
        salt: [u8; 4],
        rec_seq: [u8; 8],
    }

    const TLS_1_2_VERSION: u16 = 0x0303;
    const TLS_CIPHER_AES_GCM_128: u16 = 51;

    let info = CryptoInfoAesGcm128 {
        version: TLS_1_2_VERSION,
        cipher_type: TLS_CIPHER_AES_GCM_128,
        iv: [0x2b; 8],
        key: [0x2a; 16],
        salt: [0x2c; 4],
        rec_seq: [0; 8],
    };

    let ok = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_TLS,
            direction,
            &info as *const _ as *const libc::c_void,
            std::mem::size_of::<CryptoInfoAesGcm128>() as libc::socklen_t,
        )
    };
    if ok < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn enable_ktls(fd: RawFd) -> std::io::Result<()> {
    let ulp = b"tls\0";
    let ok = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_ULP,
            ulp.as_ptr() as *const libc::c_void,
            3,
        )
    };
    if ok < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Sends one record of a chosen type, the way a TLS 1.3 key update arrives.
fn send_record(fd: RawFd, payload: &[u8], record_type: u8) -> std::io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let mut control = [0u8; 64];
    let mut hdr: libc::msghdr = unsafe { std::mem::zeroed() };
    hdr.msg_iov = &mut iov;
    hdr.msg_iovlen = 1;
    hdr.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    hdr.msg_controllen = unsafe { libc::CMSG_SPACE(1) } as _;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&hdr);
        (*cmsg).cmsg_level = libc::SOL_TLS;
        (*cmsg).cmsg_type = libc::TLS_SET_RECORD_TYPE;
        (*cmsg).cmsg_len = libc::CMSG_LEN(1) as _;
        *libc::CMSG_DATA(cmsg) = record_type;

        if libc::sendmsg(fd, &hdr, 0) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[test]
fn a_plain_socket_reports_no_record_type() {
    // The same call on a socket with no kernel TLS is an ordinary read, which
    // is what makes it usable without knowing whether kTLS is on.
    LocalExecutor::default().run(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let reader = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.unwrap();
            let mut buf = [0u8; 32];
            stream.recv_tls_record(&mut buf).await.unwrap()
        })
        .detach();

        let mut writer = TcpStream::connect(addr).await.unwrap();
        writer.write_all(b"plaintext").await.unwrap();

        let (read, record_type) = reader.await.unwrap();
        assert_eq!(read, b"plaintext".len());
        assert_eq!(
            record_type, None,
            "a socket without kernel TLS has no record type to report"
        );
    });
}

#[test]
fn unrelated_control_messages_are_walked_past() {
    // The kTLS test below cannot run without the kernel module, which leaves
    // the cmsg walk itself untested on such machines. TCP_INQ puts a real
    // control message on an ordinary TCP socket -- one per `recvmsg`, saying
    // how much is left queued -- so the loop runs over genuine kernel-written
    // control data and has to report no record type rather than mistaking a
    // byte count for one.
    //
    // SO_TIMESTAMP was the first attempt and delivered nothing on TCP, which
    // made this test pass while walking an empty control buffer: it agreed
    // with the assertion for the wrong reason.
    LocalExecutor::default().run(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let reader = glommio::spawn_local(async move { listener.accept().await.unwrap() }).detach();
        let mut writer = TcpStream::connect(addr).await.unwrap();
        let mut reader = reader.await.unwrap();

        // Not in libc yet; `include/uapi/linux/tcp.h`.
        const TCP_INQ: libc::c_int = 36;

        let on: libc::c_int = 1;
        let ok = unsafe {
            libc::setsockopt(
                reader.as_raw_fd(),
                libc::IPPROTO_TCP,
                TCP_INQ,
                &on as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        assert_eq!(ok, 0, "TCP_INQ should be settable on a TCP socket");

        writer.write_all(b"timestamped").await.unwrap();

        let mut buf = [0u8; 32];
        let (read, record_type) = reader.recv_tls_record(&mut buf).await.unwrap();
        assert_eq!(&buf[..read], b"timestamped");
        assert_eq!(
            record_type, None,
            "a TCP_INQ control message is not a TLS record type"
        );
    });
}

#[test]
fn a_control_record_is_readable_rather_than_eio() {
    LocalExecutor::default().run(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let accepted =
            glommio::spawn_local(async move { listener.accept().await.unwrap() }).detach();
        let writer = TcpStream::connect(addr).await.unwrap();
        let mut reader = accepted.await.unwrap();

        if enable_ktls(writer.as_raw_fd()).is_err() {
            eprintln!(
                "skipping: no `tls` kernel module, so kernel TLS cannot be enabled here. \
                 This test is real only where CONFIG_TLS is loaded."
            );
            return;
        }
        enable_ktls(reader.as_raw_fd()).unwrap();
        install_key(writer.as_raw_fd(), libc::TLS_TX).unwrap();
        install_key(reader.as_raw_fd(), libc::TLS_RX).unwrap();

        // Application data first: the ordinary case still works.
        send_record(writer.as_raw_fd(), b"payload", tls_record::APPLICATION_DATA).unwrap();
        let mut buf = [0u8; 64];
        let (read, record_type) = reader.recv_tls_record(&mut buf).await.unwrap();
        assert_eq!(&buf[..read], b"payload");
        assert_eq!(record_type, Some(tls_record::APPLICATION_DATA));

        // Then a handshake record, which is what a TLS 1.3 key update is, and
        // what a plain `read` cannot retrieve.
        send_record(
            writer.as_raw_fd(),
            b"\x18\x00\x00\x00",
            tls_record::HANDSHAKE,
        )
        .unwrap();
        let (read, record_type) = reader.recv_tls_record(&mut buf).await.unwrap();
        assert_eq!(
            record_type,
            Some(tls_record::HANDSHAKE),
            "a control record must arrive as a record, not as an error"
        );
        assert_eq!(read, 4);
    });
}
