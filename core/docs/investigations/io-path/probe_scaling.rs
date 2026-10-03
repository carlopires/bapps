//! Does glommio scale across shards and across connections?
//!
//! Everything measured so far used one or two shards and one connection. This
//! is the shape a real deployment has. Two questions:
//!
//!   1. N independent shards, no shared state. Does per-shard throughput stay
//!      flat as N grows? Anything else means global contention.
//!   2. One shard, M connections. Does per-message cost stay flat as M grows?
//!
//! Each shard gets its own blocking echo peer on a separate core, so the peers
//! do not contend with the shards.
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{net::TcpStream as GStream, LocalExecutorBuilder, Placement};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::time::Instant;

const MSG: usize = 64;

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn set_nodelay_raw(fd: i32) {
    let one: libc::c_int = 1;
    unsafe {
        libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t);
    }
}

/// Blocking echo peer serving `conns` connections, pinned to `cpu`.
fn echo_peer(l: TcpListener, cpu: usize, conns: usize) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        pin(cpu);
        let mut handles = Vec::new();
        for _ in 0..conns {
            let (mut s, _) = l.accept().unwrap();
            set_nodelay_raw(s.as_raw_fd());
            handles.push(std::thread::spawn(move || {
                let mut buf = [0u8; MSG];
                while s.read_exact(&mut buf).is_ok() {
                    if s.write_all(&buf).is_err() { break; }
                }
            }));
        }
        for h in handles { let _ = h.join(); }
    })
}

/// One shard, `conns` connections, `rounds` round trips spread over them.
/// Returns nanoseconds per round trip.
fn shard(cpu: usize, peer_cpu: usize, conns: usize, rounds: usize) -> f64 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let peer = echo_peer(l, peer_cpu, conns);

    let h = LocalExecutorBuilder::new(Placement::Fixed(cpu))
        .spawn(move || async move {
            let mut streams = Vec::new();
            for _ in 0..conns {
                let c = GStream::connect(addr).await.unwrap();
                c.set_nodelay(true).unwrap();
                streams.push(c);
            }
            let per = rounds / conns;

            // warm up every connection
            for s in streams.iter_mut() {
                let mut buf = [0u8; MSG];
                for _ in 0..200 {
                    s.write_all(&buf).await.unwrap();
                    s.read_exact(&mut buf).await.unwrap();
                }
            }

            let start = Instant::now();
            let tasks: Vec<_> = streams
                .into_iter()
                .map(|mut s| {
                    glommio::spawn_local(async move {
                        let mut buf = [0u8; MSG];
                        for _ in 0..per {
                            s.write_all(&buf).await.unwrap();
                            s.read_exact(&mut buf).await.unwrap();
                        }
                        s
                    })
                })
                .collect();
            let mut keep = Vec::new();
            for t in tasks { keep.push(t.await); }
            let el = start.elapsed();
            drop(keep);
            el.as_nanos() as f64 / (per * conns) as f64
        })
        .unwrap();
    let ns = h.join().unwrap();
    let _ = peer.join();
    ns
}

fn main() {
    println!("loopback TCP echo, {MSG}-byte messages, TCP_NODELAY\n");

    println!("1. N independent shards, one connection each");
    println!("   shards on cpus 0.., peers on cpus 16.. (no shared state between shards)");
    println!("   {:<8} {:>14} {:>16}", "shards", "ns/round trip", "vs 1 shard");
    let mut base = 0.0;
    for n in [1usize, 2, 4, 8] {
        let handles: Vec<_> = (0..n)
            .map(|i| std::thread::spawn(move || shard(i, 16 + i, 1, 20_000)))
            .collect();
        let mut worst: f64 = 0.0;
        for h in handles { worst = worst.max(h.join().unwrap()); }
        if n == 1 { base = worst; }
        println!("   {n:<8} {worst:>11.0} ns {:>15.2}x", worst / base);
    }

    println!("\n2. One shard, M connections");
    println!("   {:<8} {:>14} {:>16}", "conns", "ns/round trip", "vs 1 conn");
    let mut base = 0.0;
    for m in [1usize, 4, 16, 64] {
        let v = shard(0, 16, m, 20_000);
        if m == 1 { base = v; }
        println!("   {m:<8} {v:>11.0} ns {:>15.2}x", v / base);
    }
}
