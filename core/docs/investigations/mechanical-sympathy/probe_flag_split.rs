//! Do the setup flags matter on the ring that *sends* a MSG_RING, on the ring
//! that *receives* it, or both?
//!
//! A symmetric ping-pong cannot answer this: each side is sender for one leg
//! and target for the other. So hold one leg constant and vary only the other.
//!
//!   A -> B   always MSG_RING          <- the leg under test
//!   B -> A   always an eventfd write  <- constant, unaffected by the flags
//!
//! The round trip is (A->B wake) + (B->A wake). Only the first term changes, so
//! differences between rows are attributable to the MSG_RING leg, and within
//! that leg A is unambiguously the sender and B the target.

use io_uring::{opcode, types, IoUring};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const ROUNDS: usize = 50_000;
const PING: u64 = 0xAA;
const READ_TAG: u64 = 0x11;
const SEND_TAG: u64 = 0x22;

fn build(flagged: bool) -> std::io::Result<IoUring> {
    let mut b = IoUring::builder();
    if flagged {
        b.setup_single_issuer();
        b.setup_defer_taskrun();
    }
    b.build(64)
}

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
    assert!(fd >= 0);
    fd
}

fn write_eventfd(fd: RawFd) {
    let v: u64 = 1;
    assert_eq!(
        unsafe { libc::write(fd, &v as *const u64 as *const libc::c_void, 8) },
        8
    );
}

fn wait_for(ring: &mut IoUring, want: u64) {
    loop {
        ring.submit_and_wait(1).unwrap();
        let mut found = false;
        for cqe in ring.completion() {
            if cqe.user_data() == want {
                found = true;
            }
        }
        if found {
            return;
        }
    }
}

fn wait_on_eventfd(ring: &mut IoUring, efd: RawFd, buf: &mut u64) {
    let read = opcode::Read::new(types::Fd(efd), buf as *mut u64 as *mut u8, 8)
        .build()
        .user_data(READ_TAG);
    unsafe { ring.submission().push(&read).unwrap() };
    wait_for(ring, READ_TAG);
}

fn send_msg_ring(ring: &mut IoUring, peer: RawFd) {
    let msg = opcode::MsgRingData::new(types::Fd(peer), 0, PING, None)
        .build()
        .user_data(SEND_TAG);
    unsafe { ring.submission().push(&msg).unwrap() };
    ring.submit().unwrap();
}

/// `sender_flagged` configures A, whose ring submits the MSG_RING.
/// `target_flagged` configures B, whose ring receives it.
fn bench(sender_flagged: bool, target_flagged: bool, cpu_a: usize, cpu_b: usize) -> f64 {
    let efd_a = make_eventfd();
    let (fd_tx, fd_rx) = std::sync::mpsc::channel::<RawFd>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_b = stop.clone();

    let b = thread::spawn(move || {
        pin(cpu_b);
        let mut ring = build(target_flagged).unwrap();
        fd_tx.send(ring.as_raw_fd()).unwrap();
        while !stop_b.load(Ordering::Relaxed) {
            // target: woken by MSG_RING
            wait_for(&mut ring, PING);
            // reply over the constant leg
            write_eventfd(efd_a);
        }
    });

    pin(cpu_a);
    let mut ring = build(sender_flagged).unwrap();
    let peer = fd_rx.recv().unwrap();
    let mut buf = 0u64;

    for _ in 0..3_000 {
        send_msg_ring(&mut ring, peer);
        wait_on_eventfd(&mut ring, efd_a, &mut buf);
    }

    let start = Instant::now();
    for _ in 0..ROUNDS {
        send_msg_ring(&mut ring, peer);
        wait_on_eventfd(&mut ring, efd_a, &mut buf);
    }
    let el = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    send_msg_ring(&mut ring, peer);
    let _ = b.join();
    unsafe { libc::close(efd_a) };
    el.as_nanos() as f64 / ROUNDS as f64
}

fn main() {
    println!("Does SINGLE_ISSUER|DEFER_TASKRUN matter on the sender or the target?");
    println!("A -> B by MSG_RING (under test); B -> A by eventfd (constant).\n");

    for (label, a, b) in [("same L3  (0<->4)", 0usize, 4usize), ("cross L3 (0<->16)", 0, 16)] {
        println!("{label}");
        println!("  {:<34} {:>12}  {:>9}", "sender / target", "round trip", "vs base");
        let mut base = 0.0f64;
        for (i, (sf, tf)) in [(false, false), (true, false), (false, true), (true, true)]
            .into_iter()
            .enumerate()
        {
            let v = (0..3).map(|_| bench(sf, tf, a, b)).fold(f64::MAX, f64::min);
            if i == 0 {
                base = v;
                println!("  {:<34} {v:>9.0} ns  {:>8}", "default / default", "—");
            } else {
                let name = format!(
                    "{} / {}",
                    if sf { "flagged" } else { "default" },
                    if tf { "flagged" } else { "default" }
                );
                println!(
                    "  {name:<34} {v:>9.0} ns  {:>+8.1}%",
                    (v - base) / base * 100.0
                );
            }
        }
        println!();
    }
}
