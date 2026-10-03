//! Primitive-cost probe for the glommio mechanical-sympathy investigation.
//!
//! Measures, on THIS machine, the per-operation cost of the primitives that
//! appear on glommio's task spawn / wake / schedule path. Everything is
//! uncontended and cache-hot, which is the favourable case -- the real path is
//! never better than this.

use std::cell::{Cell, RefCell};
use std::hint::black_box;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicI16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const ITERS: u64 = 50_000_000;

fn bench<F: FnMut()>(name: &str, ops_per_iter: u64, mut f: F) {
    // warmup
    for _ in 0..1_000_000 {
        f();
    }
    let start = Instant::now();
    for _ in 0..ITERS {
        f();
    }
    let el = start.elapsed();
    let per_op = el.as_nanos() as f64 / (ITERS * ops_per_iter) as f64;
    println!("{name:<44} {per_op:>7.3} ns/op");
}

fn main() {
    println!("iterations: {ITERS} per case, uncontended, cache-hot\n");

    // --- refcount: atomic vs not -------------------------------------------
    let a = AtomicI16::new(1);
    bench("AtomicI16 fetch_add+fetch_sub (Relaxed)", 1, || {
        a.fetch_add(1, Ordering::Relaxed);
        a.fetch_sub(1, Ordering::Relaxed);
        black_box(&a);
    });

    let c = Cell::new(1i16);
    bench("Cell<i16> inc+dec", 1, || {
        c.set(c.get() + 1);
        c.set(c.get() - 1);
        black_box(&c);
    });

    // --- smart pointers -----------------------------------------------------
    let arc = Arc::new(42u64);
    bench("Arc clone+drop", 1, || {
        let x = arc.clone();
        black_box(&x);
    });

    let rc = Rc::new(42u64);
    bench("Rc clone+drop", 1, || {
        let x = rc.clone();
        black_box(&x);
    });

    // --- the schedule closure's captured Weak -------------------------------
    let strong: Rc<RefCell<u64>> = Rc::new(RefCell::new(0));
    let weak: Weak<RefCell<u64>> = Rc::downgrade(&strong);
    bench("Weak::upgrade + drop (Rc)", 1, || {
        let x = weak.upgrade();
        black_box(&x);
    });

    let refc = RefCell::new(0u64);
    bench("RefCell borrow_mut + drop", 1, || {
        let mut b = refc.borrow_mut();
        *b += 1;
        black_box(&*b);
    });

    // --- the notify path ----------------------------------------------------
    let m = Mutex::new(Some(7i32));
    bench("Mutex lock+unlock (uncontended)", 1, || {
        let g = m.lock().unwrap();
        black_box(&*g);
    });

    let ab = AtomicBool::new(true);
    bench("AtomicBool compare_exchange (Relaxed)", 1, || {
        let _ = ab.compare_exchange(true, true, Ordering::Relaxed, Ordering::Relaxed);
        black_box(&ab);
    });

    let raw = AtomicI16::new(3);
    bench("AtomicI16 load (Relaxed)", 1, || {
        black_box(raw.load(Ordering::Relaxed));
    });

    // --- what the guard costs: 2 RMW + waker construction --------------------
    let g = AtomicI16::new(1);
    bench("2x atomic RMW (the schedule guard pair)", 1, || {
        g.fetch_add(1, Ordering::Relaxed);
        g.fetch_sub(1, Ordering::Relaxed);
        black_box(&g);
    });
}
