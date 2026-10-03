//! A socket read reports a timeout when its deadline passes — and not
//! because the reactor happens to have forgotten the timer.
//!
//! The deadline is the thing the caller asked about. Inferring it from whether
//! a timer is still registered conflates "it fired" with "it was cancelled",
//! and ties the socket layer to whichever timer structure is underneath.

use glommio::{
    net::{TcpListener, TcpStream},
    LocalExecutor,
};
use std::{
    io::ErrorKind,
    time::{Duration, Instant},
};

#[test]
fn a_read_that_outlasts_its_timeout_reports_timed_out() {
    let ex = LocalExecutor::default();
    ex.run(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        // Accepted but never written to, so the read has nothing to complete.
        let _server = glommio::spawn_local(async move {
            let stream = listener.accept().await.expect("accept");
            // Hold it open for longer than the client's patience.
            glommio::timer::sleep(Duration::from_secs(30)).await;
            drop(stream);
        })
        .detach();

        let stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(20)))
            .expect("set_read_timeout");

        let started = Instant::now();
        let mut buf = [0u8; 8];
        let err = futures_lite::AsyncReadExt::read(&mut { stream }, &mut buf)
            .await
            .expect_err("a read with nothing to read must time out");

        assert_eq!(err.kind(), ErrorKind::TimedOut, "{err:?}");
        assert!(
            started.elapsed() >= Duration::from_millis(20),
            "reported a timeout after {:?}, before the deadline",
            started.elapsed()
        );
    });
}

#[test]
fn a_read_that_completes_inside_its_timeout_does_not_time_out() {
    let ex = LocalExecutor::default();
    ex.run(async {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let server = glommio::spawn_local(async move {
            let mut stream = listener.accept().await.expect("accept");
            futures_lite::AsyncWriteExt::write_all(&mut stream, &[7u8; 8])
                .await
                .expect("server write");
        });

        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("set_read_timeout");

        let mut buf = [0u8; 8];
        futures_lite::AsyncReadExt::read_exact(&mut stream, &mut buf)
            .await
            .expect("read completes well inside its deadline");
        assert_eq!(buf, [7u8; 8]);

        server.await;
    });
}
