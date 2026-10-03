//! Where do the ~950 ns per streaming send go?
//!
//! poll_write's fast path is one send(MSG_DONTWAIT) plus a Weak::upgrade, a
//! timer cancel and an Option::take -- 200-300 ns of work. Streaming measures
//! 1057 ns. A ladder isolates which layer holds the rest.
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Instant;

const MSG: usize = 64;
const OPS: usize = 200_000;
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

fn drain_server(listener: TcpListener) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        pin(CPU_B);
        let (mut s, _) = listener.accept().unwrap();
        let mut buf = [0u8; 65536];
        while let Ok(n) = s.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
    })
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

fn send_once(fd: RawFd, buf: &[u8; MSG]) {
    loop {
        let n = unsafe {
            libc::send(fd, buf.as_ptr() as *const libc::c_void, MSG, libc::MSG_DONTWAIT)
        };
        if n == MSG as isize {
            return;
        }
        std::hint::spin_loop();
    }
}

fn bare_syscall() -> f64 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let srv = drain_server(l);
    pin(CPU_A);
    let c = TcpStream::connect(addr).unwrap();
    set_nodelay(c.as_raw_fd());
    let fd = c.as_raw_fd();
    let buf = [0u8; MSG];
    for _ in 0..20_000 { send_once(fd, &buf); }
    let start = Instant::now();
    for _ in 0..OPS { send_once(fd, &buf); }
    let el = start.elapsed();
    drop(c);
    let _ = srv.join();
    el.as_nanos() as f64 / OPS as f64
}

fn syscall_in_executor() -> f64 {
    use glommio::{LocalExecutorBuilder, Placement};
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let srv = drain_server(l);
    let h = LocalExecutorBuilder::new(Placement::Fixed(CPU_A))
        .spawn(move || async move {
            let c = TcpStream::connect(addr).unwrap();
            set_nodelay(c.as_raw_fd());
            let fd = c.as_raw_fd();
            let buf = [0u8; MSG];
            for _ in 0..20_000 { send_once(fd, &buf); }
            let start = Instant::now();
            for _ in 0..OPS {
                std::future::ready(()).await;
                send_once(fd, &buf);
            }
            let el = start.elapsed();
            drop(c);
            el.as_nanos() as f64 / OPS as f64
        })
        .unwrap();
    let ns = h.join().unwrap();
    let _ = srv.join();
    ns
}

fn glommio_write_all() -> f64 {
    use futures_lite::AsyncWriteExt;
    use glommio::{net::TcpStream as GStream, LocalExecutorBuilder, Placement};
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let srv = drain_server(l);
    let h = LocalExecutorBuilder::new(Placement::Fixed(CPU_A))
        .spawn(move || async move {
            let mut c = GStream::connect(addr).await.unwrap();
            c.set_nodelay(true).unwrap();
            let buf = [0u8; MSG];
            for _ in 0..20_000 { c.write_all(&buf).await.unwrap(); }
            let start = Instant::now();
            for _ in 0..OPS { c.write_all(&buf).await.unwrap(); }
            let el = start.elapsed();
            drop(c);
            el.as_nanos() as f64 / OPS as f64
        })
        .unwrap();
    let ns = h.join().unwrap();
    let _ = srv.join();
    ns
}

fn main() {
    println!("streaming 64-byte sends, {OPS} ops, TCP_NODELAY on all three\n");
    let a = bare_syscall();
    let b = syscall_in_executor();
    let c = glommio_write_all();
    println!("  {:<38} {a:>7.0} ns", "1. bare send(MSG_DONTWAIT)");
    println!("  {:<38} {b:>7.0} ns   {:+.0}", "2. + glommio executor and await", b - a);
    println!("  {:<38} {c:>7.0} ns   {:+.0}", "3. + glommio net layer (write_all)", c - b);
}
