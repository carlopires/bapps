//! What does one io_uring_enter cost on this box when it does not sleep?
//! Bounds how much glommio's extra per-round-trip enters are worth removing.
use io_uring::{opcode, IoUring};
use std::time::Instant;

const N: usize = 200_000;

fn main() {
    let mut ring = IoUring::new(64).unwrap();

    let mut t = 0u128;
    for _ in 0..N {
        let nop = opcode::Nop::new().build().user_data(1);
        unsafe { ring.submission().push(&nop).unwrap() };
        let s = Instant::now();
        ring.submit().unwrap();
        t += s.elapsed().as_nanos();
        ring.completion().for_each(drop);
    }
    println!("submit() with 1 SQE queued      {:>7.0} ns", t as f64 / N as f64);

    let mut t = 0u128;
    for _ in 0..N {
        let nop = opcode::Nop::new().build().user_data(2);
        unsafe { ring.submission().push(&nop).unwrap() };
        let s = Instant::now();
        ring.submit_and_wait(1).unwrap();
        t += s.elapsed().as_nanos();
        ring.completion().for_each(drop);
    }
    println!("submit_and_wait(1), no sleep    {:>7.0} ns", t as f64 / N as f64);

    let mut t = 0u128;
    for _ in 0..N {
        let s = Instant::now();
        ring.submit().unwrap();
        t += s.elapsed().as_nanos();
    }
    println!("submit() with empty SQ          {:>7.0} ns", t as f64 / N as f64);
}
