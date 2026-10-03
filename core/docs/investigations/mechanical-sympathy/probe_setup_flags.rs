//! Premise measurement: do IORING_SETUP_SINGLE_ISSUER and
//! IORING_SETUP_DEFER_TASKRUN help a thread-per-core runtime's wake path?
//!
//! Same two-thread, two-ring ping-pong as probe_msg_ring.rs. The only thing
//! that varies is how the rings are set up. Each ring is owned and submitted to
//! by exactly one thread, which is what SINGLE_ISSUER requires and what glommio
//! guarantees by construction.
//!
//! DEFER_TASKRUN moves completion task work from "whenever it completes,
//! possibly via IPI" to "when the owner next waits", which is the behaviour a
//! shard-per-core design wants. It requires SINGLE_ISSUER.

use io_uring::{opcode, types, IoUring};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const ROUNDS: usize = 50_000;
const PING: u64 = 0xAA;
const PONG: u64 = 0xBB;
const READ_TAG: u64 = 0x11;
const SEND_TAG: u64 = 0x22;

#[derive(Copy, Clone, PartialEq)]
enum Setup {
    Default,
    CoopTaskrun,
    SingleIssuer,
    SingleIssuerDeferTaskrun,
}

impl Setup {
    fn name(self) -> &'static str {
        match self {
            Setup::Default => "default",
            Setup::CoopTaskrun => "COOP_TASKRUN",
            Setup::SingleIssuer => "SINGLE_ISSUER",
            Setup::SingleIssuerDeferTaskrun => "SINGLE_ISSUER|DEFER_TASKRUN",
        }
    }

    fn build(self) -> std::io::Result<IoUring> {
        let mut b = IoUring::builder();
        match self {
            Setup::Default => {}
            Setup::CoopTaskrun => {
                b.setup_coop_taskrun();
            }
            Setup::SingleIssuer => {
                b.setup_single_issuer();
            }
            Setup::SingleIssuerDeferTaskrun => {
                b.setup_single_issuer();
                b.setup_defer_taskrun();
            }
        }
        b.build(64)
    }
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
    let n = unsafe { libc::write(fd, &v as *const u64 as *const libc::c_void, 8) };
    assert_eq!(n, 8);
}

/// Park until a CQE with `want` shows up, draining anything else. Under
/// DEFER_TASKRUN completions are only reaped inside an enter with GETEVENTS,
/// which submit_and_wait performs, so this stays correct across all setups.
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

fn send_msg_ring(ring: &mut IoUring, peer: RawFd, tag: u64) {
    let msg = opcode::MsgRingData::new(types::Fd(peer), 0, tag, None)
        .build()
        .user_data(SEND_TAG);
    unsafe { ring.submission().push(&msg).unwrap() };
    ring.submit().unwrap();
}

fn bench_eventfd(setup: Setup, cpu_a: usize, cpu_b: usize) -> std::io::Result<f64> {
    let efd_a = make_eventfd();
    let efd_b = make_eventfd();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_b = stop.clone();

    let b = thread::spawn(move || -> std::io::Result<()> {
        pin(cpu_b);
        let mut ring = setup.build()?;
        let mut buf = 0u64;
        while !stop_b.load(Ordering::Relaxed) {
            wait_on_eventfd(&mut ring, efd_b, &mut buf);
            write_eventfd(efd_a);
        }
        Ok(())
    });

    pin(cpu_a);
    let mut ring = setup.build()?;
    let mut buf = 0u64;
    for _ in 0..3_000 {
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
    Ok(el.as_nanos() as f64 / ROUNDS as f64)
}

fn bench_msg_ring(setup: Setup, cpu_a: usize, cpu_b: usize) -> std::io::Result<f64> {
    let (fd_tx, fd_rx) = std::sync::mpsc::channel::<RawFd>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<RawFd>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_b = stop.clone();

    let b = thread::spawn(move || -> std::io::Result<()> {
        pin(cpu_b);
        let mut ring = setup.build()?;
        fd_tx.send(ring.as_raw_fd()).unwrap();
        let peer = go_rx.recv().unwrap();
        while !stop_b.load(Ordering::Relaxed) {
            wait_for(&mut ring, PING);
            send_msg_ring(&mut ring, peer, PONG);
        }
        Ok(())
    });

    pin(cpu_a);
    let mut ring = setup.build()?;
    let peer = fd_rx.recv().unwrap();
    go_tx.send(ring.as_raw_fd()).unwrap();

    for _ in 0..3_000 {
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
    Ok(el.as_nanos() as f64 / ROUNDS as f64)
}

fn main() {
    println!("io_uring setup flags vs the wake path");
    println!("(L3 domains: 0-7, 8-15, 16-23, 24-31)\n");

    let setups = [
        Setup::Default,
        Setup::CoopTaskrun,
        Setup::SingleIssuer,
        Setup::SingleIssuerDeferTaskrun,
    ];

    for (label, a, b) in [("same L3  (0<->4)", 0usize, 4usize), ("cross L3 (0<->16)", 0, 16)] {
        println!("{label}");
        println!("  {:<30} {:>12} {:>12}", "setup", "eventfd", "MSG_RING");
        let mut base_ev = 0.0f64;
        let mut base_mr = 0.0f64;
        for (i, s) in setups.iter().enumerate() {
            // best of three, interleaved within the setup
            let ev = (0..3)
                .filter_map(|_| bench_eventfd(*s, a, b).ok())
                .fold(f64::MAX, f64::min);
            let mr = (0..3)
                .filter_map(|_| bench_msg_ring(*s, a, b).ok())
                .fold(f64::MAX, f64::min);
            if i == 0 {
                base_ev = ev;
                base_mr = mr;
                println!("  {:<30} {ev:>9.0} ns {mr:>9.0} ns", s.name());
            } else {
                let d_ev = (ev - base_ev) / base_ev * 100.0;
                let d_mr = (mr - base_mr) / base_mr * 100.0;
                println!(
                    "  {:<30} {ev:>9.0} ns {mr:>9.0} ns   ({d_ev:+.1}% / {d_mr:+.1}%)",
                    s.name()
                );
            }
        }
        println!();
    }
}
