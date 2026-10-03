//! What does glommio's TCP path cost above the kernel it sits on?
//!
//! Same method as the read path: establish a raw io_uring floor, run glommio
//! over the identical workload, attribute the gap. Loopback TCP ping-pong,
//! 64-byte messages, one request in flight — the latency case, where software
//! cost is most visible.
//!
//! Both sides of each measurement use the same stack, so a round trip is two
//! send/recv pairs on that stack. Client and server are pinned to two cores in
//! the same L3 domain so the cache-domain effect measured elsewhere does not
//! confound this.

use io_uring::{opcode, types, IoUring};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Instant;

const MSG: usize = 64;
const ROUNDS: usize = 20_000;
const CPU_A: usize = 0;
const CPU_B: usize = 4;

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn set_nodelay(fd: RawFd) {
    let one: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// std::net blocking sockets, for reference: what the syscalls alone cost.
fn blocking_baseline() -> f64 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        pin(CPU_B);
        let (mut s, _) = listener.accept().unwrap();
        set_nodelay(s.as_raw_fd());
        let mut buf = [0u8; MSG];
        while s.read_exact(&mut buf).is_ok() {
            if s.write_all(&buf).is_err() {
                break;
            }
        }
    });

    pin(CPU_A);
    let mut c = TcpStream::connect(addr).unwrap();
    set_nodelay(c.as_raw_fd());
    let mut buf = [0u8; MSG];

    for _ in 0..2_000 {
        c.write_all(&buf).unwrap();
        c.read_exact(&mut buf).unwrap();
    }
    let start = Instant::now();
    for _ in 0..ROUNDS {
        c.write_all(&buf).unwrap();
        c.read_exact(&mut buf).unwrap();
    }
    let el = start.elapsed();
    drop(c);
    let _ = server.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

/// Raw io_uring on both sides: the floor.
fn raw_uring() -> f64 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        pin(CPU_B);
        let (s, _) = listener.accept().unwrap();
        set_nodelay(s.as_raw_fd());
        let fd = s.as_raw_fd();
        let mut ring = IoUring::new(8).unwrap();
        let mut buf = [0u8; MSG];
        loop {
            if !rw(&mut ring, fd, &mut buf, true) {
                break;
            }
            if !rw(&mut ring, fd, &mut buf, false) {
                break;
            }
        }
    });

    pin(CPU_A);
    let c = TcpStream::connect(addr).unwrap();
    set_nodelay(c.as_raw_fd());
    let fd = c.as_raw_fd();
    let mut ring = IoUring::new(8).unwrap();
    let mut buf = [0u8; MSG];

    for _ in 0..2_000 {
        rw(&mut ring, fd, &mut buf, false);
        rw(&mut ring, fd, &mut buf, true);
    }
    let start = Instant::now();
    for _ in 0..ROUNDS {
        rw(&mut ring, fd, &mut buf, false);
        rw(&mut ring, fd, &mut buf, true);
    }
    let el = start.elapsed();
    drop(c);
    let _ = server.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

/// One recv (`read = true`) or send, submitted and waited on. Returns false on
/// EOF or error, which is how the server loop terminates.
fn rw(ring: &mut IoUring, fd: RawFd, buf: &mut [u8; MSG], read: bool) -> bool {
    let e = if read {
        opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), MSG as u32).build()
    } else {
        opcode::Send::new(types::Fd(fd), buf.as_ptr(), MSG as u32).build()
    }
    .user_data(1);
    // SAFETY: `buf` outlives the operation, which is awaited before returning.
    unsafe { ring.submission().push(&e).unwrap() };
    ring.submit_and_wait(1).unwrap();
    let mut ok = false;
    for cqe in ring.completion() {
        ok = cqe.result() == MSG as i32;
    }
    ok
}

/// glommio over the same workload.
fn glommio_tcp(spin: Option<std::time::Duration>) -> f64 {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use glommio::{
        net::{TcpListener as GListener, TcpStream as GStream},
        LocalExecutorBuilder, Placement,
    };

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mut sb = LocalExecutorBuilder::new(Placement::Fixed(CPU_B));
    if let Some(d) = spin { sb = sb.spin_before_park(d); }
    let server = sb
        .spawn(move || async move {
            let l = GListener::bind(addr).unwrap();
            let mut s = l.accept().await.unwrap();
            s.set_nodelay(true).unwrap();
            let mut buf = [0u8; MSG];
            while s.read_exact(&mut buf).await.is_ok() {
                if s.write_all(&buf).await.is_err() {
                    break;
                }
            }
        })
        .unwrap();

    // give the listener a moment to bind
    std::thread::sleep(std::time::Duration::from_millis(200));

    let mut cb = LocalExecutorBuilder::new(Placement::Fixed(CPU_A));
    if let Some(d) = spin { cb = cb.spin_before_park(d); }
    let client = cb
        .spawn(move || async move {
            let mut c = GStream::connect(addr).await.unwrap();
            c.set_nodelay(true).unwrap();
            let mut buf = [0u8; MSG];

            for _ in 0..2_000 {
                c.write_all(&buf).await.unwrap();
                c.read_exact(&mut buf).await.unwrap();
            }
            let start = Instant::now();
            for _ in 0..ROUNDS {
                c.write_all(&buf).await.unwrap();
                c.read_exact(&mut buf).await.unwrap();
            }
            let el = start.elapsed();
            drop(c);
            el.as_nanos() as f64 / ROUNDS as f64
        })
        .unwrap();

    let ns = client.join().unwrap();
    let _ = server.join();
    ns
}

/// Streaming: the sender never waits for a reply, so the receiver almost always
/// finds data already buffered. That is the regime glommio's readiness-based
/// read path is built for -- `yolo_recv` succeeds on the first try and io_uring
/// is not involved at all.
fn glommio_stream() -> f64 {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use glommio::{
        net::{TcpListener as GListener, TcpStream as GStream},
        LocalExecutorBuilder, Placement,
    };

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = LocalExecutorBuilder::new(Placement::Fixed(CPU_B))
        .spawn(move || async move {
            let l = GListener::bind(addr).unwrap();
            let mut s = l.accept().await.unwrap();
            s.set_nodelay(true).unwrap();
            let mut buf = [0u8; MSG];
            let mut n = 0usize;
            while s.read_exact(&mut buf).await.is_ok() {
                n += 1;
            }
            n
        })
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(200));

    let client = LocalExecutorBuilder::new(Placement::Fixed(CPU_A))
        .spawn(move || async move {
            let mut c = GStream::connect(addr).await.unwrap();
            c.set_nodelay(true).unwrap();
            let buf = [0u8; MSG];
            for _ in 0..2_000 {
                c.write_all(&buf).await.unwrap();
            }
            let start = Instant::now();
            for _ in 0..ROUNDS {
                c.write_all(&buf).await.unwrap();
            }
            let el = start.elapsed();
            drop(c);
            el.as_nanos() as f64 / ROUNDS as f64
        })
        .unwrap();

    let ns = client.join().unwrap();
    let _ = server.join();
    ns
}

/// Same streaming workload on blocking sockets, for scale.
fn blocking_stream() -> f64 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        pin(CPU_B);
        let (mut s, _) = listener.accept().unwrap();
        set_nodelay(s.as_raw_fd());
        let mut buf = [0u8; MSG];
        while s.read_exact(&mut buf).is_ok() {}
    });
    pin(CPU_A);
    let mut c = TcpStream::connect(addr).unwrap();
    set_nodelay(c.as_raw_fd());
    let buf = [0u8; MSG];
    for _ in 0..2_000 { c.write_all(&buf).unwrap(); }
    let start = Instant::now();
    for _ in 0..ROUNDS { c.write_all(&buf).unwrap(); }
    let el = start.elapsed();
    drop(c);
    let _ = server.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

fn main() {
    println!("loopback TCP ping-pong, {MSG}-byte messages, {ROUNDS} round trips");
    println!("client on cpu {CPU_A}, server on cpu {CPU_B} (same L3 domain)\n");

    let block = blocking_baseline();
    let raw = raw_uring();
    let glo = glommio_tcp(None);
    let glo_spin = glommio_tcp(Some(std::time::Duration::from_micros(50)));

    println!("  {:<30} {:>10}", "stack", "round trip");
    println!("  {:<30} {block:>7.0} ns", "std::net blocking");
    println!("  {:<30} {raw:>7.0} ns", "raw io_uring");
    println!("  {:<30} {glo:>7.0} ns", "glommio (parks)");
    println!("  {:<30} {glo_spin:>7.0} ns", "glommio spin_before_park(50us)");
    println!("\n  glommio parking, over raw:  {:+.0} ns ({:+.1}%)",
             glo - raw, (glo - raw) / raw * 100.0);
    println!("  glommio spinning, over raw: {:+.0} ns ({:+.1}%)",
             glo_spin - raw, (glo_spin - raw) / raw * 100.0);

    println!("\n  streaming (sender never waits), per message:");
    let bs = blocking_stream();
    let gs = glommio_stream();
    println!("  {:<30} {bs:>7.0} ns", "std::net blocking");
    println!("  {:<30} {gs:>7.0} ns", "glommio");
}
