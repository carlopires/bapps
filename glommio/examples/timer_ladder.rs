//! How many timers a glommio server actually holds, and what it does with
//! them.
//!
//! Reading the source bounds this from above: only two sites register a timer
//! with the reactor (`net/stream.rs` for socket timeouts, `timer_impl.rs` for
//! `Timer`/`sleep`), glommio itself registers none, and socket timeouts are
//! `None` until an application asks for them. So live timers are at most
//! `2 x connections that opted into timeouts` plus whatever `sleep` futures
//! are outstanding.
//!
//! What reading cannot settle is the *ratio*. A connection whose read
//! completes before its deadline cancels its timer; one that stalls lets it
//! fire. Whether a timer structure should be optimised for cancellation or for
//! expiry depends on which of those dominates, and they pull in opposite
//! directions.
//!
//! Two rungs, because they are the two shapes glommio's timers come in:
//!
//! 1. **churn** — N connections with a read timeout set, each doing request
//!    and response cycles that complete well inside the deadline. Every cycle
//!    registers a timer and cancels it. This is what a proxy or a database
//!    front-end looks like.
//! 2. **scheduled** — N concurrent `sleep`s that are allowed to fire. This is
//!    what periodic work looks like.
//!
//! The numbers reported are populations and ratios, not timings. What the
//! structure costs is a separate question, and asking it before knowing the
//! population is how you end up optimising a case nobody runs.
//!
//! Run with:
//! ```bash
//! cargo run --release --features debugging --example timer_ladder
//! ```

#[cfg(not(feature = "debugging"))]
fn main() {
    eprintln!("timer_ladder needs the counters: cargo run --release --features debugging --example timer_ladder");
    std::process::exit(1);
}

#[cfg(feature = "debugging")]
fn main() {
    imp::main()
}

#[cfg(feature = "debugging")]
mod imp {
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use glommio::{
        net::{TcpListener, TcpStream},
        spawn_local,
        timer::{debugging::timer_stats, sleep},
        LocalExecutor,
    };
    use std::time::Duration;

    /// Connection counts to sweep. The top of the range is deliberately past
    /// what most deployments run, to show where the population curve goes.
    const CONNECTIONS: &[usize] = &[64, 256, 1_024, 4_096];

    /// Request/response cycles per connection. Each one registers a timer and
    /// then cancels it.
    const CYCLES: usize = 16;

    /// Long enough that a loopback round trip never reaches it, so every timer
    /// in the churn rung is cancelled rather than fired.
    const READ_TIMEOUT: Duration = Duration::from_secs(30);

    pub fn main() {
        let ex = LocalExecutor::default();
        ex.run(async {
            println!("timer population, per executor\n");
            println!(
                "{:>7}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}",
                "conns", "inserted", "fired", "cancelled", "highwater", "cancel%"
            );

            for &n in CONNECTIONS {
                let before = timer_stats();
                churn_rung(n).await;
                report(n, before);
            }

            println!("\nscheduled work: concurrent sleeps that fire\n");
            println!(
                "{:>7}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}",
                "sleeps", "inserted", "fired", "cancelled", "highwater", "cancel%"
            );

            for &n in CONNECTIONS {
                let before = timer_stats();
                scheduled_rung(n).await;
                report(n, before);
            }
        });
    }

    fn report(n: usize, before: glommio::timer::debugging::TimerStats) {
        let after = timer_stats();
        let inserted = after.inserted - before.inserted;
        let fired = after.fired - before.fired;
        let cancelled = after.cancelled_before_fire - before.cancelled_before_fire;
        let settled = fired + cancelled;
        let pct = if settled > 0 {
            format!("{:.1}", 100.0 * cancelled as f64 / settled as f64)
        } else {
            "-".to_string()
        };

        println!(
            "{n:>7}  {inserted:>10}  {fired:>10}  {cancelled:>10}  {:>10}  {pct:>8}",
            after.live_high_water
        );
    }

    /// N connections, each with a read timeout, exchanging small messages that
    /// always complete before the deadline.
    async fn churn_rung(n: usize) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let server = spawn_local(async move {
            let mut accepted = Vec::with_capacity(n);
            for _ in 0..n {
                accepted.push(listener.accept().await.expect("accept"));
            }
            // Echo every cycle on every connection.
            let mut tasks = Vec::with_capacity(n);
            for mut stream in accepted {
                tasks.push(spawn_local(async move {
                    let mut buf = [0u8; 8];
                    for _ in 0..CYCLES {
                        stream.read_exact(&mut buf).await.expect("server read");
                        stream.write_all(&buf).await.expect("server write");
                    }
                }));
            }
            for t in tasks {
                t.await;
            }
        });

        let mut clients = Vec::with_capacity(n);
        for _ in 0..n {
            let stream = TcpStream::connect(addr).await.expect("connect");
            stream
                .set_read_timeout(Some(READ_TIMEOUT))
                .expect("set_read_timeout");
            clients.push(spawn_local(async move {
                let mut stream = stream;
                let mut buf = [0u8; 8];
                for i in 0..CYCLES {
                    stream
                        .write_all(&(i as u64).to_le_bytes())
                        .await
                        .expect("client write");
                    // Registers a timer on entry; cancels it on completion.
                    stream.read_exact(&mut buf).await.expect("client read");
                }
            }));
        }

        for c in clients {
            c.await;
        }
        server.await;
    }

    /// N concurrent sleeps, all allowed to fire.
    async fn scheduled_rung(n: usize) {
        let mut tasks = Vec::with_capacity(n);
        for _ in 0..n {
            tasks.push(spawn_local(sleep(Duration::from_millis(1))));
        }
        for t in tasks {
            t.await;
        }
    }
}
