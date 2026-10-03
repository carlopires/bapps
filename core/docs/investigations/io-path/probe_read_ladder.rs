//! Is glommio's latency gap caused by its readiness design, or by its
//! implementation of it?
//!
//! The write ladder settled the write side in twenty minutes; this is the same
//! treatment for reads. A constant blocking echo peer, and only the client's
//! read mechanism varies:
//!
//!   1. blocking recv                  -> the floor
//!   2. io_uring Recv (completion)     -> what a completion-based runtime does
//!   3. yolo_recv + PollAdd, by hand   -> glommio's *design*, without glommio
//!   4. glommio                        -> glommio's design *and* implementation
//!
//! If 3 lands near 4, the design costs the gap. If 3 lands near 2, it does not
//! and the cost is somewhere in glommio.
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
        libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t);
    }
}

/// The constant peer: a blocking thread that echoes. Identical for every case,
/// so differences come only from the client.
fn echo_peer(l: TcpListener) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        pin(CPU_B);
        let (mut s, _) = l.accept().unwrap();
        set_nodelay(s.as_raw_fd());
        let mut buf = [0u8; MSG];
        while s.read_exact(&mut buf).is_ok() {
            if s.write_all(&buf).is_err() { break; }
        }
    })
}

fn connect() -> (TcpStream, std::thread::JoinHandle<()>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let peer = echo_peer(l);
    pin(CPU_A);
    let c = TcpStream::connect(addr).unwrap();
    set_nodelay(c.as_raw_fd());
    (c, peer)
}

fn send_raw(fd: RawFd, buf: &[u8; MSG]) {
    let n = unsafe {
        libc::send(fd, buf.as_ptr() as *const libc::c_void, MSG, libc::MSG_DONTWAIT)
    };
    assert_eq!(n, MSG as isize);
}

fn recv_nonblocking(fd: RawFd, buf: &mut [u8; MSG]) -> Option<usize> {
    let n = unsafe {
        libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, MSG, libc::MSG_DONTWAIT)
    };
    if n >= 0 { Some(n as usize) } else { None }
}

/// 1. Blocking sockets.
fn blocking() -> f64 {
    let (mut c, peer) = connect();
    let mut buf = [0u8; MSG];
    for _ in 0..2_000 { c.write_all(&buf).unwrap(); c.read_exact(&mut buf).unwrap(); }
    let start = Instant::now();
    for _ in 0..ROUNDS { c.write_all(&buf).unwrap(); c.read_exact(&mut buf).unwrap(); }
    let el = start.elapsed();
    drop(c); let _ = peer.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

/// 2. Completion-based: one Recv SQE, wait for it.
fn completion() -> f64 {
    let (c, peer) = connect();
    let fd = c.as_raw_fd();
    let mut ring = IoUring::new(8).unwrap();
    let mut buf = [0u8; MSG];
    let mut once = |ring: &mut IoUring, buf: &mut [u8; MSG]| {
        send_raw(fd, buf);
        let e = opcode::Recv::new(types::Fd(fd), buf.as_mut_ptr(), MSG as u32)
            .build().user_data(1);
        unsafe { ring.submission().push(&e).unwrap() };
        ring.submit_and_wait(1).unwrap();
        for cqe in ring.completion() { assert_eq!(cqe.result(), MSG as i32); }
    };
    for _ in 0..2_000 { once(&mut ring, &mut buf); }
    let start = Instant::now();
    for _ in 0..ROUNDS { once(&mut ring, &mut buf); }
    let el = start.elapsed();
    drop(c); let _ = peer.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

/// 3. glommio's readiness design, implemented by hand: try a non-blocking recv
///    first, and on EAGAIN register a PollAdd and wait, then recv again.
fn readiness_by_hand() -> f64 {
    let (c, peer) = connect();
    let fd = c.as_raw_fd();
    let mut ring = IoUring::new(8).unwrap();
    let mut buf = [0u8; MSG];
    let mut once = |ring: &mut IoUring, buf: &mut [u8; MSG]| {
        send_raw(fd, buf);
        loop {
            if let Some(n) = recv_nonblocking(fd, buf) {
                assert_eq!(n, MSG);
                return;
            }
            let e = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
                .build().user_data(2);
            unsafe { ring.submission().push(&e).unwrap() };
            ring.submit_and_wait(1).unwrap();
            for _ in ring.completion() {}
        }
    };
    for _ in 0..2_000 { once(&mut ring, &mut buf); }
    let start = Instant::now();
    for _ in 0..ROUNDS { once(&mut ring, &mut buf); }
    let el = start.elapsed();
    drop(c); let _ = peer.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

/// 4. glommio.
fn glommio_case() -> f64 {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use glommio::{net::TcpStream as GStream, LocalExecutorBuilder, Placement};
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let peer = echo_peer(l);
    let h = LocalExecutorBuilder::new(Placement::Fixed(CPU_A))
        .spawn(move || async move {
            let mut c = GStream::connect(addr).await.unwrap();
            c.set_nodelay(true).unwrap();
            let mut buf = [0u8; MSG];
            for _ in 0..2_000 { c.write_all(&buf).await.unwrap(); c.read_exact(&mut buf).await.unwrap(); }
            glommio::probe_counters::reset();
            let start = Instant::now();
            for _ in 0..ROUNDS { c.write_all(&buf).await.unwrap(); c.read_exact(&mut buf).await.unwrap(); }
            let el = start.elapsed();
            let snap = glommio::probe_counters::snapshot();
            let get = |n: &str| snap.iter().find(|(k,_)| *k==n).unwrap().1 as f64;
            let r = ROUNDS as f64;
            println!("\n  per round trip, glommio internals:");
            for (label, key) in [
                ("reactor loop (awake)", "NS_LOOP"),
                ("  poll_io", "NS_POLL_IO"),
                ("    io_uring_enter", "NS_ENTER"),
                ("    process_one_event", "NS_PROCESS_EVENT"),
                ("      wake_waiters", "NS_WAKE_WAITERS"),
                ("      consume_source", "NS_CONSUME_SOURCE"),
                ("  run_task_queues", "NS_RUN_TQ"),
                ("  Source::new", "NS_NEW_SOURCE"),
                ("  add_source", "NS_ADD_SOURCE"),
            ] {
                println!("    {label:<26} {:>7.0} ns", get(key)/r);
            }
            println!("\n  counts per round trip:");
            for (label, key) in [
                ("io_uring_enter", "N_ENTER"),
                ("completions processed", "N_PROCESS_EVENT"),
                ("Sources created", "N_NEW_SOURCE"),
                ("  of which LinkRings", "C_LINKRINGS"),
                ("  of which PollAdd", "C_POLLADD"),
                ("  of which Timeout", "C_TIMEOUT"),
                ("  of which ForeignNotifier", "C_FOREIGN"),
                ("  of which other", "C_OTHER"),
                ("  user_data 0 (cancel etc)", "C_NODATA"),
            ] {
                println!("    {label:<26} {:>7.2}", get(key)/r);
            }
            drop(c);
            el.as_nanos() as f64 / ROUNDS as f64
        }).unwrap();
    let ns = h.join().unwrap();
    let _ = peer.join();
    ns
}

fn main() {
    println!("TCP echo round trip, constant blocking peer, only the client varies");
    println!("{MSG}-byte messages, {ROUNDS} round trips, TCP_NODELAY throughout\n");
    let b = blocking();
    let cm = completion();
    let rb = readiness_by_hand();
    let g = glommio_case();
    println!("  {:<40} {:>9}", "client", "round trip");
    println!("  {:<40} {b:>6.0} ns", "1. blocking recv");
    println!("  {:<40} {cm:>6.0} ns", "2. io_uring Recv (completion)");
    println!("  {:<40} {rb:>6.0} ns", "3. yolo_recv + PollAdd by hand");
    println!("  {:<40} {g:>6.0} ns", "4. glommio");
    println!("\n  design cost  (3 - 2): {:+.0} ns", rb - cm);
    println!("  glommio over its own design (4 - 3): {:+.0} ns", g - rb);
}
