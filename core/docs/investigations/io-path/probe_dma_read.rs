//! What does glommio's DMA read path cost above the kernel it sits on?
//!
//! Same method as the wake-path work: establish the floor with raw io_uring,
//! then measure glommio doing the same workload, then attribute the gap. No
//! conclusions from the ratio alone — a gap is only interesting once it is
//! large compared to the floor.
//!
//! Both sides do 4 KiB O_DIRECT random reads over the same file, at several
//! queue depths. Queue depth 1 is the latency case and shows per-operation
//! software overhead most clearly; deeper queues move toward the device limit,
//! where software cost hides behind the SSD.

use io_uring::{opcode, types, IoUring};
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Instant;

const BLOCK: usize = 4096;
const FILE_BYTES: u64 = 1 << 30; // 1 GiB
const OPS: usize = 20_000;

/// Deterministic pseudo-random block offsets, so both sides read the same
/// sequence and neither benefits from a luckier access pattern.
struct Offsets(u64);
impl Offsets {
    fn new() -> Self {
        Offsets(0x2545F4914F6CDD1D)
    }
    fn next(&mut self) -> u64 {
        // xorshift64
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        let blocks = FILE_BYTES / BLOCK as u64;
        (self.0 % blocks) * BLOCK as u64
    }
}

/// A page-aligned buffer, required by O_DIRECT.
struct Aligned(*mut u8, usize);
impl Aligned {
    fn new(len: usize) -> Self {
        let mut p: *mut libc::c_void = std::ptr::null_mut();
        // SAFETY: standard posix_memalign contract; checked below.
        let rc = unsafe { libc::posix_memalign(&mut p, BLOCK, len) };
        assert_eq!(rc, 0, "posix_memalign failed");
        Aligned(p as *mut u8, len)
    }
    fn ptr(&self) -> *mut u8 {
        self.0
    }
}
impl Drop for Aligned {
    fn drop(&mut self) {
        // SAFETY: allocated by posix_memalign, freed once.
        unsafe { libc::free(self.0 as *mut libc::c_void) }
    }
}

fn open_direct(path: &str) -> RawFd {
    let c = std::ffi::CString::new(path).unwrap();
    // SAFETY: valid NUL-terminated path; result checked.
    let fd = unsafe { libc::open(c.as_ptr(), libc::O_RDONLY | libc::O_DIRECT) };
    assert!(fd >= 0, "open O_DIRECT failed: {}", std::io::Error::last_os_error());
    fd
}

/// Raw io_uring: the floor.
fn raw_uring(path: &str, depth: usize) -> f64 {
    let fd = open_direct(path);
    let mut ring = IoUring::new(256).unwrap();
    let bufs: Vec<Aligned> = (0..depth).map(|_| Aligned::new(BLOCK)).collect();
    let mut off = Offsets::new();

    // warmup
    for _ in 0..(OPS / 10).max(depth) {
        one_batch(&mut ring, fd, &bufs, &mut off, depth);
    }

    let start = Instant::now();
    let mut done = 0;
    while done < OPS {
        one_batch(&mut ring, fd, &bufs, &mut off, depth);
        done += depth;
    }
    let el = start.elapsed();
    unsafe { libc::close(fd) };
    el.as_nanos() as f64 / done as f64
}

fn one_batch(ring: &mut IoUring, fd: RawFd, bufs: &[Aligned], off: &mut Offsets, depth: usize) {
    for buf in bufs.iter().take(depth) {
        let e = opcode::Read::new(types::Fd(fd), buf.ptr(), BLOCK as u32)
            .offset(off.next())
            .build()
            .user_data(1);
        // SAFETY: buffer outlives the operation; the ring is drained below.
        unsafe { ring.submission().push(&e).unwrap() };
    }
    ring.submit_and_wait(depth).unwrap();
    let mut got = 0;
    while got < depth {
        for cqe in ring.completion() {
            assert!(cqe.result() >= 0, "read failed: {}", -cqe.result());
            got += 1;
        }
        if got < depth {
            ring.submit_and_wait(1).unwrap();
        }
    }
}

/// glommio's DmaFile doing the same workload.
fn glommio_dma(path: &str, depth: usize) -> f64 {
    use glommio::{io::DmaFile, LocalExecutorBuilder, Placement};
    use std::rc::Rc;

    let path = path.to_string();
    LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(move || async move {
            let file = Rc::new(DmaFile::open(&path).await.unwrap());
            let mut off = Offsets::new();

            async fn run(
                file: &Rc<DmaFile>,
                off: &mut Offsets,
                depth: usize,
                n: usize,
            ) -> usize {
                let mut done = 0;
                while done < n {
                    // Spawn so the reads are genuinely in flight together.
                    // `StreamExt::then` would run them one after another, which
                    // silently pins the queue depth at 1.
                    let handles: Vec<_> = (0..depth)
                        .map(|_| {
                            let pos = off.next();
                            let file = file.clone();
                            glommio::spawn_local(async move {
                                file.read_at_aligned(pos, BLOCK).await.unwrap().len()
                            })
                        })
                        .collect();
                    for h in handles {
                        assert_eq!(h.await, BLOCK);
                    }
                    done += depth;
                }
                done
            }

            let _ = run(&file, &mut off, depth, (OPS / 10).max(depth)).await;
            let start = Instant::now();
            let done = run(&file, &mut off, depth, OPS).await;
            let el = start.elapsed();
            Rc::try_unwrap(file).ok().unwrap().close().await.unwrap();
            el.as_nanos() as f64 / done as f64
        })
        .unwrap()
        .join()
        .unwrap()
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: iopath <file>");

    println!("4 KiB O_DIRECT random reads, {OPS} ops per cell\n");
    println!("  {:<6} {:>14} {:>14} {:>10}", "depth", "raw io_uring", "glommio DMA", "overhead");
    for depth in [1usize, 4, 16, 64] {
        let raw = raw_uring(&path, depth);
        let glo = glommio_dma(&path, depth);
        println!(
            "  {depth:<6} {raw:>11.0} ns {glo:>11.0} ns {:>9.0} ns",
            glo - raw
        );
    }
}
