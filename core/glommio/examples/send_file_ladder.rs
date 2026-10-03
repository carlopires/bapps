//! What `TcpStream::send_file` is worth against reading the file and writing
//! the bytes.
//!
//! `send_file` splices file -> pipe -> socket entirely on the ring, so the
//! payload never enters this process, and it saves two copies. It does not
//! come for free: the pipe holds 65536 bytes, so it costs two ring operations
//! per 64 KiB, while the read-then-write it replaces submits one `read_at`
//! and one `write_all` for the whole payload however large it is. Whether the
//! trade pays is the question -- the copies scale with the payload, and so,
//! at 64 KiB granularity, does the operation count.
//!
//! Two rungs, at each payload size:
//!
//! 1. **read + `write_all`** -- `BufferedFile::read_at` into a glommio buffer,
//!    then `write_all` to the socket. What a caller does today.
//! 2. **`send_file`** -- the splice path.
//!
//! Both rungs send exactly the same bytes over the same kind of connection to
//! the same client, which drains with blocking `read_exact` on a pinned core.
//! Only the server's work differs.
//!
//! ## What the meters are, and why
//!
//! **CPU, not wall clock.** The client is the bottleneck at every size, so
//! wall clock ranks the client. `CLOCK_THREAD_CPUTIME_ID` on the executor
//! thread is the headline number, as in the other ladders here.
//!
//! **Plus an off-thread column, which the other ladders do not need.** An
//! io_uring operation that cannot complete without blocking is punted to an
//! `iou-wrk` kernel worker, and that worker's CPU is charged to a different
//! thread -- invisible to `CLOCK_THREAD_CPUTIME_ID`, which would credit
//! whichever rung punts more. Both rungs can punt (a cold file read, a
//! splice from a cold file), so the ladder also brackets each job with
//! `CLOCK_PROCESS_CPUTIME_ID`, which sums the whole thread group including
//! workers, and subtracts the client thread's own CPU (which the client
//! reports for the same window). What is left is work done for this rung on
//! some thread other than the executor's. If that column is not ~0, the
//! headline column is not the whole cost.
//!
//! Run with:
//! ```bash
//! cargo run --release --example send_file_ladder
//! ```
//!
//! The scratch file is created in the current directory unless
//! `GLOMMIO_LADDER_DIR` says otherwise; it must live on a filesystem that
//! supports `O_DIRECT` for the `DmaFile` group to run.

use futures_lite::io::AsyncWriteExt;
use glommio::{
    io::{BufferedFile, DmaFile, ReadResult},
    LocalExecutorBuilder, Placement,
};
use std::{
    io::Read,
    net::{SocketAddr, TcpStream},
    os::unix::io::{AsRawFd, RawFd},
    path::PathBuf,
    sync::mpsc,
    time::Duration,
};

/// Server and client on distinct cores in the same L3, so the rungs are not
/// ranked by wherever the scheduler happened to put them.
const CPU_SERVER: usize = 0;
const CPU_CLIENT: usize = 4;

const MAX_SIZE: usize = 8 << 20;

/// Payload size, and how many responses to send at that size. The counts are
/// chosen to keep each rung a few hundred milliseconds long: long enough that
/// the timer resolution and the connection setup do not show, short enough
/// that a full pass is tens of seconds.
const SIZES: &[(usize, usize)] = &[
    (1 << 10, 20_000),
    (4 << 10, 20_000),
    (16 << 10, 10_000),
    (32 << 10, 8_000),
    (64 << 10, 4_000),
    (128 << 10, 2_000),
    (256 << 10, 1_000),
    (512 << 10, 512),
    (1 << 20, 256),
    (8 << 20, 64),
];

/// Sizes the cold-cache and `DmaFile` groups use. The full sweep is not
/// repeated for them: they answer "does the ranking change", not "where is
/// the crossover", and the cold group pays a device round trip on every
/// single round.
const COLD_SIZES: &[(usize, usize)] = &[
    (4 << 10, 4_000),
    (16 << 10, 4_000),
    (64 << 10, 4_000),
    (256 << 10, 1_000),
    (1 << 20, 256),
    (8 << 20, 64),
];
const DMA_SIZES: &[(usize, usize)] = &[
    (16 << 10, 4_000),
    (64 << 10, 4_000),
    (256 << 10, 1_000),
    (1 << 20, 256),
    (8 << 20, 64),
];

/// Passes over the whole ladder. Three, so run-to-run spread is visible in
/// the output rather than asserted.
const PASSES: usize = 3;

fn pin(cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

fn clock(which: libc::clockid_t) -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(which, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

fn thread_cpu() -> Duration {
    clock(libc::CLOCK_THREAD_CPUTIME_ID)
}

/// The whole thread group, which is what makes `iou-wrk` workers visible.
fn process_cpu() -> Duration {
    clock(libc::CLOCK_PROCESS_CPUTIME_ID)
}

fn human(bytes: usize) -> String {
    if bytes >= 1 << 20 {
        format!("{} MiB", bytes >> 20)
    } else {
        format!("{} KiB", bytes >> 10)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Rung {
    ReadWrite,
    SendFile,
}

impl Rung {
    fn label(self) -> &'static str {
        match self {
            Rung::ReadWrite => "read + write_all",
            Rung::SendFile => "send_file",
        }
    }
}

/// One job for the draining client: connect here and read this many responses
/// of this size.
struct Job {
    addr: SocketAddr,
    len: usize,
    rounds: usize,
}

/// The client drains as fast as it can with blocking reads, and reports the
/// CPU it spent doing so for the whole job -- which the server subtracts from
/// the process-wide figure to isolate off-thread work.
fn drain(jobs: mpsc::Receiver<Job>, done: mpsc::Sender<Duration>) {
    pin(CPU_CLIENT);
    let mut buf = vec![0u8; MAX_SIZE];
    for job in jobs {
        let mut stream = TcpStream::connect(job.addr).unwrap();
        stream.set_nodelay(true).unwrap();
        let start = thread_cpu();
        for _ in 0..job.rounds {
            stream.read_exact(&mut buf[..job.len]).unwrap();
        }
        done.send(thread_cpu() - start).unwrap();
    }
}

enum Source {
    Buffered(BufferedFile),
    Dma(DmaFile),
}

impl Source {
    fn fd(&self) -> RawFd {
        match self {
            Source::Buffered(f) => f.as_raw_fd(),
            Source::Dma(f) => f.as_raw_fd(),
        }
    }

    async fn read_at(&self, len: usize) -> ReadResult {
        match self {
            Source::Buffered(f) => f.read_at(0, len).await.unwrap(),
            Source::Dma(f) => f.read_at(0, len).await.unwrap(),
        }
    }

    async fn send_file(&self, stream: &mut glommio::net::TcpStream, len: usize) -> usize {
        match self {
            Source::Buffered(f) => stream.send_file(f, 0, len).await.unwrap(),
            Source::Dma(f) => stream.send_file(f, 0, len).await.unwrap(),
        }
    }

    /// Drop this file's page cache, so the next round pays for the data
    /// arriving from the device. Charged to both rungs identically.
    fn evict(&self) {
        unsafe { libc::posix_fadvise(self.fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    }

    async fn close(self) {
        match self {
            Source::Buffered(f) => f.close().await.unwrap(),
            Source::Dma(f) => f.close().await.unwrap(),
        }
    }
}

struct Measured {
    /// CPU on the executor thread, over the counted rounds only.
    per_response: f64,
    /// CPU spent for this rung on any other thread of this process, over the
    /// whole job. Per response, so it is comparable with the column above.
    off_thread: f64,
}

#[allow(clippy::too_many_arguments)]
async fn measure(
    rung: Rung,
    source: &Source,
    len: usize,
    rounds: usize,
    cold: bool,
    jobs: &mpsc::Sender<Job>,
    done: &mpsc::Receiver<Duration>,
) -> Measured {
    // The first rounds fault in buffers, grow the socket's send buffer and
    // warm the file's cache. Charging that to the rung is how a ladder ends
    // up measuring its own first iteration.
    let warmup = std::cmp::max(rounds / 10, 4);

    let listener = glommio::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    jobs.send(Job {
        addr,
        len,
        rounds: warmup + rounds,
    })
    .unwrap();
    let mut stream = listener.accept().await.unwrap();
    stream.set_nodelay(true).unwrap();

    let job_thread = thread_cpu();
    let job_process = process_cpu();
    let mut counted_from = None;

    for round in 0..warmup + rounds {
        if round == warmup {
            counted_from = Some(thread_cpu());
        }
        if cold {
            source.evict();
        }
        match rung {
            Rung::ReadWrite => {
                let buf = source.read_at(len).await;
                assert_eq!(buf.len(), len, "short read at {len}");
                stream.write_all(&buf).await.unwrap();
            }
            Rung::SendFile => {
                let sent = source.send_file(&mut stream, len).await;
                assert_eq!(sent, len, "short send_file at {len}");
            }
        }
    }
    let counted = thread_cpu() - counted_from.unwrap();

    // Wait for the client to finish the job before closing the process-wide
    // window, so the window contains exactly the client CPU the client is
    // about to report. Blocking here costs no CPU and the counted window is
    // already closed.
    let client = done.recv().unwrap();
    let whole_thread = thread_cpu() - job_thread;
    let whole_process = process_cpu() - job_process;
    let off = whole_process.saturating_sub(whole_thread + client);

    Measured {
        per_response: counted.as_nanos() as f64 / rounds as f64,
        off_thread: off.as_nanos() as f64 / (warmup + rounds) as f64,
    }
}

fn header(title: &str) {
    println!("\n{title}");
    println!(
        "{:>9}  {:<18} {:>14} {:>14} {:>8}",
        "payload", "rung", "cpu/response", "off-thread", "ratio"
    );
}

fn row(len: usize, rung: Rung, m: &Measured, ratio: Option<f64>) {
    let ratio = match ratio {
        Some(r) => format!("{r:.2}x"),
        None => String::new(),
    };
    println!(
        "{:>9}  {:<18} {:>11.0} ns {:>11.0} ns {:>8}",
        human(len),
        rung.label(),
        m.per_response,
        m.off_thread,
        ratio
    );
}

async fn group(
    title: &str,
    sizes: &[(usize, usize)],
    cold: bool,
    dma: bool,
    path: &PathBuf,
    jobs: &mpsc::Sender<Job>,
    done: &mpsc::Receiver<Duration>,
) {
    header(title);
    for &(len, rounds) in sizes {
        let mut both = Vec::new();
        for rung in [Rung::ReadWrite, Rung::SendFile] {
            // Opened fresh for every rung so neither inherits the other's
            // file position, readahead state or descriptor flags.
            let source = if dma {
                Source::Dma(DmaFile::open(path).await.unwrap())
            } else {
                Source::Buffered(BufferedFile::open(path).await.unwrap())
            };
            both.push(measure(rung, &source, len, rounds, cold, jobs, done).await);
            source.close().await;
        }
        row(len, Rung::ReadWrite, &both[0], None);
        row(
            len,
            Rung::SendFile,
            &both[1],
            Some(both[1].per_response / both[0].per_response),
        );
    }
}

fn main() {
    pin(CPU_SERVER);

    let dir: PathBuf = std::env::var("GLOMMIO_LADDER_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let path = dir.join(format!(".send_file_ladder-{}.tmp", std::process::id()));
    let mut content = vec![0u8; MAX_SIZE];
    let mut seed = 0x9e3779b9u32;
    for byte in content.iter_mut() {
        seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
        *byte = (seed >> 24) as u8;
    }
    std::fs::write(&path, &content).unwrap();
    drop(content);
    // Written back before anything tries to evict it: POSIX_FADV_DONTNEED
    // only drops clean pages.
    std::fs::File::open(&path).unwrap().sync_all().unwrap();

    let (jobs, take_jobs) = mpsc::channel::<Job>();
    let (report, done) = mpsc::channel::<Duration>();
    let client = std::thread::spawn(move || drain(take_jobs, report));

    let file = path.clone();
    LocalExecutorBuilder::new(Placement::Fixed(CPU_SERVER))
        .spawn(move || async move {
            let dma_works = DmaFile::open(&file).await.is_ok();
            for pass in 1..=PASSES {
                println!("\n=== pass {pass} of {PASSES} ===");
                group(
                    "page cache warm, BufferedFile",
                    SIZES,
                    false,
                    false,
                    &file,
                    &jobs,
                    &done,
                )
                .await;
                group(
                    "page cache dropped before every response, BufferedFile",
                    COLD_SIZES,
                    true,
                    false,
                    &file,
                    &jobs,
                    &done,
                )
                .await;
                if dma_works {
                    group(
                        "O_DIRECT, DmaFile (never cached)",
                        DMA_SIZES,
                        false,
                        true,
                        &file,
                        &jobs,
                        &done,
                    )
                    .await;
                } else {
                    println!("\nO_DIRECT, DmaFile: skipped, this filesystem refuses O_DIRECT");
                }
            }
            drop(jobs);
        })
        .unwrap()
        .join()
        .unwrap();

    let _ = client.join();
    std::fs::remove_file(&path).ok();
}
