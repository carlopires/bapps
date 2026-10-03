//! What an accept costs, and what multishot could take off it.
//!
//! glommio accepts by speculating: flip the listener to non-blocking, call
//! `accept`, flip it back, and only fall back to an io_uring `Accept` on
//! `EAGAIN`. Under churn the speculation succeeds, so the ring is not
//! involved -- but the flipping is, and it is two `fcntl` calls per
//! connection on top of the `accept` itself.
//!
//! Three rungs, so that the two available wins can be told apart:
//!
//! 1. **toggle** — what glommio does today: `F_GETFL`, `F_SETFL(O_NONBLOCK)`,
//!    `accept`, `F_SETFL(restore)`. Four syscalls.
//! 2. **once** — the listener made non-blocking once, at construction. One
//!    syscall. Needs no io_uring at all.
//! 3. **multishot** — one `IORING_OP_ACCEPT` with `IORING_ACCEPT_MULTISHOT`,
//!    armed once, one CQE per connection. Zero syscalls per connection.
//! 4. **glommio** — the library itself, to check that a rung measured by hand
//!    shows up end to end.
//!
//! Each round is timed **whole** -- filling the backlog and draining it --
//! because multishot accept moves the work: with it armed the kernel accepts
//! as connections arrive, so timing only the drain loop credits it with work
//! it really did, just earlier. The client cost is identical across rungs, so
//! differences remain attributable while nothing hides off the measured path.
//!
//! Run with:
//! ```bash
//! cargo run --release --example accept_ladder
//! ```

use io_uring::{cqueue, opcode, types::Fd, IoUring};
use std::{
    net::{TcpListener, TcpStream},
    os::unix::io::{AsRawFd, RawFd},
    sync::mpsc,
    time::{Duration, Instant},
};

const BURST: usize = 256;
const ROUNDS: usize = 40;
const CONNECTIONS: usize = BURST * ROUNDS;

/// CPU actually burned by this thread.
///
/// Wall clock cannot separate the rungs: the connecting thread is the
/// bottleneck, so every rung finishes at the client's pace regardless of what
/// the server spends. What multishot claims to save is server-side work, and
/// this is the only meter that sees it.
fn thread_cpu() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

fn set_nonblocking(fd: RawFd, on: bool) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        let flags = if on {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        assert_eq!(libc::fcntl(fd, libc::F_SETFL, flags), 0);
    }
}

/// One accept the way `yolo_accept` does it: non-blocking for the duration of
/// the call and back again afterwards.
fn accept_toggling(fd: RawFd) -> RawFd {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        let accepted = libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut());
        libc::fcntl(fd, libc::F_SETFL, flags);
        accepted
    }
}

fn accept_plain(fd: RawFd) -> RawFd {
    unsafe { libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) }
}

/// Fills the backlog with `BURST` connections and says so, so the timed
/// section never waits on the client and never races it either.
struct Connector {
    go: mpsc::Sender<()>,
    ready: mpsc::Receiver<()>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Connector {
    fn new(addr: std::net::SocketAddr) -> Self {
        let (go, wait) = mpsc::channel::<()>();
        let (announce, ready) = mpsc::channel::<()>();
        let handle = std::thread::spawn(move || {
            // Held for the whole run: the peer must still exist when the
            // server accepts, or the cost measured is a teardown.
            let mut held = Vec::with_capacity(BURST * ROUNDS);
            while wait.recv().is_ok() {
                for _ in 0..BURST {
                    held.push(TcpStream::connect(addr).unwrap());
                }
                if announce.send(()).is_err() {
                    break;
                }
            }
        });
        Connector {
            go,
            ready,
            handle: Some(handle),
        }
    }

    /// Fills the backlog and returns once every connection is queued.
    fn fill(&self) {
        self.go.send(()).unwrap();
        self.ready.recv().unwrap();
    }

    fn finish(mut self) {
        drop(std::mem::replace(&mut self.go, mpsc::channel().0));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    // std listens with a backlog of 128; a burst needs more room than that.
    unsafe { libc::listen(listener.as_raw_fd(), 4096) };
    (listener, addr)
}

fn run_syscall_rung(toggling: bool, label: &str) {
    let (listener, addr) = listener();
    let fd = listener.as_raw_fd();
    if !toggling {
        set_nonblocking(fd, true);
    }
    let connector = Connector::new(addr);

    let mut elapsed = Duration::ZERO;
    let mut cpu = Duration::ZERO;
    let mut accepted_fds = Vec::with_capacity(BURST * ROUNDS);
    for _ in 0..ROUNDS {
        let start = Instant::now();
        let cpu_start = thread_cpu();
        connector.fill();

        let mut accepted = 0;
        while accepted < BURST {
            let result = if toggling {
                accept_toggling(fd)
            } else {
                accept_plain(fd)
            };
            if result >= 0 {
                accepted += 1;
                accepted_fds.push(result);
            }
        }
        elapsed += start.elapsed();
        cpu += thread_cpu() - cpu_start;
    }
    // Closing is a syscall of its own and has nothing to do with the rung.
    for fd in accepted_fds {
        unsafe { libc::close(fd) };
    }
    connector.finish();

    report(label, elapsed, cpu);
}

fn run_multishot() {
    let (listener, addr) = listener();
    let fd = listener.as_raw_fd();
    let connector = Connector::new(addr);

    let mut ring = IoUring::new(256).unwrap();
    let armed = opcode::AcceptMulti::new(Fd(fd)).build().user_data(1);
    unsafe { ring.submission().push(&armed).unwrap() };
    ring.submit().unwrap();

    let mut elapsed = Duration::ZERO;
    let mut cpu = Duration::ZERO;
    let mut accepted_fds = Vec::with_capacity(BURST * ROUNDS);
    for _ in 0..ROUNDS {
        let start = Instant::now();
        let cpu_start = thread_cpu();
        connector.fill();

        let mut accepted = 0;
        while accepted < BURST {
            ring.submit_and_wait(1).unwrap();
            let completions: Vec<cqueue::Entry> = ring.completion().collect();
            for completion in completions {
                assert!(
                    cqueue::more(completion.flags()),
                    "multishot accept disarmed: re-arming is the caller's job"
                );
                assert!(completion.result() >= 0, "accept failed");
                accepted_fds.push(completion.result());
                accepted += 1;
            }
        }
        elapsed += start.elapsed();
        cpu += thread_cpu() - cpu_start;
    }
    for fd in accepted_fds {
        unsafe { libc::close(fd) };
    }
    connector.finish();

    report("multishot accept (0 syscalls/conn)", elapsed, cpu);
}

fn run_glommio() {
    let (listener, addr) = listener();
    // The library owns its own listener; this one exists only to reserve the
    // address, and is dropped before glommio binds with SO_REUSEPORT.
    drop(listener);
    let connector = Connector::new(addr);

    let (elapsed, cpu) = glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
        .spawn(move || async move {
            let listener = glommio::net::TcpListener::bind(addr).unwrap();
            let mut elapsed = Duration::ZERO;
            let mut cpu = Duration::ZERO;
            let mut accepted_streams = Vec::with_capacity(BURST * ROUNDS);

            for _ in 0..ROUNDS {
                // Filled outside the timing: unlike multishot, glommio does
                // no work at connect time, so charging it only for the accept
                // loop is fair and isolates the syscalls per accept.
                connector.fill();

                let start = Instant::now();
                let cpu_start = thread_cpu();
                for _ in 0..BURST {
                    accepted_streams.push(listener.accept().await.unwrap());
                }
                elapsed += start.elapsed();
                cpu += thread_cpu() - cpu_start;
            }

            drop(accepted_streams);
            connector.finish();
            (elapsed, cpu)
        })
        .unwrap()
        .join()
        .unwrap();

    report("glommio TcpListener::accept (loop only)", elapsed, cpu);
}

fn report(label: &str, elapsed: Duration, cpu: Duration) {
    println!(
        "{label:<42} {:>8.0} ns wall  {:>8.0} ns cpu   per connection",
        elapsed.as_nanos() as f64 / CONNECTIONS as f64,
        cpu.as_nanos() as f64 / CONNECTIONS as f64,
    );
}

fn main() {
    println!("{CONNECTIONS} connections, connect and accept both inside the timing\n");
    run_syscall_rung(true, "toggle O_NONBLOCK per accept (glommio today)");
    run_syscall_rung(false, "listener non-blocking once");
    run_multishot();
    run_glommio();
}
