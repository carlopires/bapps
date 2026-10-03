//! Premise measurement for candidate 3 of the mechanical-sympathy
//! investigation: is IORING_OP_MSG_RING actually cheaper than glommio's
//! eventfd wake path?
//!
//! Two threads, each owning an io_uring, ping-pong a wake-up N times.
//! Only the wake mechanism differs between the two cases:
//!
//!   eventfd  -- write(2) into the peer's eventfd, which is what
//!               SleepNotifier::notify does today; the sleeper is parked in
//!               io_uring_enter with a read queued on its own eventfd.
//!   msg_ring -- submit MSG_RING at the peer's ring fd, which posts a CQE
//!               directly into it. No eventfd, no write(2), no read.
//!
//! Both cases cost one submit-and-wait per direction, so the difference is
//! the wake mechanism itself.

use io_uring::{opcode, types, IoUring};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const ROUNDS: usize = 100_000;
const PING: u64 = 0xAA;
const PONG: u64 = 0xBB;
const READ_TAG: u64 = 0x11;
const SEND_TAG: u64 = 0x22;

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn make_eventfd() -> RawFd {
    let fd = unsafe { libc::eventfd(0, 0) };
    assert!(fd >= 0, "eventfd failed");
    fd
}

fn write_eventfd(fd: RawFd) {
    let v: u64 = 1;
    let n = unsafe { libc::write(fd, &v as *const u64 as *const libc::c_void, 8) };
    assert_eq!(n, 8, "eventfd write failed");
}

/// Park until one CQE arrives whose user_data matches `want`, draining
/// anything else (e.g. our own MSG_RING completion).
fn wait_for(ring: &mut IoUring, want: u64) {
    loop {
        ring.submit_and_wait(1).unwrap();
        let mut found = false;
        let cq = ring.completion();
        for cqe in cq {
            if cqe.user_data() == want {
                found = true;
            }
        }
        if found {
            return;
        }
    }
}

/// Queue a read on our own eventfd so that submit_and_wait parks until the
/// peer writes into it, then park.
fn wait_on_eventfd(ring: &mut IoUring, efd: RawFd, buf: &mut u64) {
    let read = opcode::Read::new(types::Fd(efd), buf as *mut u64 as *mut u8, 8)
        .build()
        .user_data(READ_TAG);
    unsafe {
        ring.submission().push(&read).unwrap();
    }
    wait_for(ring, READ_TAG);
}

fn send_msg_ring(ring: &mut IoUring, peer_ring_fd: RawFd, tag: u64) {
    let msg = opcode::MsgRingData::new(types::Fd(peer_ring_fd), 0, tag, None)
        .build()
        .user_data(SEND_TAG);
    unsafe {
        ring.submission().push(&msg).unwrap();
    }
    ring.submit().unwrap();
}

fn bench_eventfd(cpu_a: usize, cpu_b: usize) -> f64 {
    let efd_a = make_eventfd();
    let efd_b = make_eventfd();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_b = stop.clone();

    let b = thread::spawn(move || {
        pin(cpu_b);
        let mut ring = IoUring::new(64).unwrap();
        let mut buf = 0u64;
        while !stop_b.load(Ordering::Relaxed) {
            wait_on_eventfd(&mut ring, efd_b, &mut buf);
            write_eventfd(efd_a);
        }
    });

    pin(cpu_a);
    let mut ring = IoUring::new(64).unwrap();
    let mut buf = 0u64;

    for _ in 0..5_000 {
        write_eventfd(efd_b);
        wait_on_eventfd(&mut ring, efd_a, &mut buf);
    }

    let start = Instant::now();
    for _ in 0..ROUNDS {
        write_eventfd(efd_b);
        wait_on_eventfd(&mut ring, efd_a, &mut buf);
    }
    let el = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    write_eventfd(efd_b);
    let _ = b.join();
    unsafe {
        libc::close(efd_a);
        libc::close(efd_b);
    }
    el.as_nanos() as f64 / ROUNDS as f64
}

fn bench_msg_ring(cpu_a: usize, cpu_b: usize) -> f64 {
    // Ring fds have to be exchanged before the loop starts.
    let (fd_tx, fd_rx) = std::sync::mpsc::channel::<RawFd>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<RawFd>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_b = stop.clone();

    let b = thread::spawn(move || {
        pin(cpu_b);
        let mut ring = IoUring::new(64).unwrap();
        fd_tx.send(ring.as_raw_fd()).unwrap();
        let peer = go_rx.recv().unwrap();

        while !stop_b.load(Ordering::Relaxed) {
            wait_for(&mut ring, PING);
            send_msg_ring(&mut ring, peer, PONG);
        }
    });

    pin(cpu_a);
    let mut ring = IoUring::new(64).unwrap();
    let peer = fd_rx.recv().unwrap();
    go_tx.send(ring.as_raw_fd()).unwrap();

    for _ in 0..5_000 {
        send_msg_ring(&mut ring, peer, PING);
        wait_for(&mut ring, PONG);
    }

    let start = Instant::now();
    for _ in 0..ROUNDS {
        send_msg_ring(&mut ring, peer, PING);
        wait_for(&mut ring, PONG);
    }
    let el = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    send_msg_ring(&mut ring, peer, PING);
    let _ = b.join();
    el.as_nanos() as f64 / ROUNDS as f64
}

fn main() {
    println!("wake-up round trip between two io_uring shards");
    println!("(L3 domains on this part: 0-7, 8-15, 16-23, 24-31)\n");

    for (label, a, b) in [
        ("same L3  (0 <-> 4)", 0usize, 4usize),
        ("cross L3 (0 <-> 16)", 0, 16),
    ] {
        let ev = (0..3).map(|_| bench_eventfd(a, b)).fold(f64::MAX, f64::min);
        let mr = (0..3).map(|_| bench_msg_ring(a, b)).fold(f64::MAX, f64::min);
        println!("{label}");
        println!("  eventfd write + read : {ev:>8.0} ns/round-trip");
        println!("  MSG_RING             : {mr:>8.0} ns/round-trip");
        println!("  change               : {:>7.1}%\n", (mr - ev) / ev * 100.0);
    }
}
