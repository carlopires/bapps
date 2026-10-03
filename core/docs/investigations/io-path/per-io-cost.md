# What the ~2 µs Per Blocking I/O Actually Is

**Date:** 2026-08-02
**Status:** attributed — **preempt-timer churn, not the I/O**
**Follows:** [synthesis.md](synthesis.md)

[synthesis.md](synthesis.md) established that glommio costs ~2 µs per operation
that blocks, and that the figure is the same on files and sockets. This
attributes it.

## Counts, per TCP echo round trip

Instrumented inside glommio, running the ladder's glommio case. Absolute times
are inflated by the timers themselves; **the counts are exact and are the
finding**.

| | per round trip |
|---|---:|
| `io_uring_enter` | **5.00** |
| completions processed | **9.00** |
| `Source`s created | 1.00 |

Nine completions to perform one read. Breaking them down by what they were:

| completion | count |
|---|---:|
| `PollAdd` — the actual read-readiness poll | **1.00** |
| `Timeout` — preempt timers | **3.00** |
| `user_data == 0` — cancellations and poll removes | **5.00** |
| `LinkRings` | 0.00 |
| `ForeignNotifier` | 0.00 |
| anything else | 0.00 |

**Eight of the nine completions are preempt-timer traffic.** Three timers firing
or being retired, five cancellations. One completion does the work.

The user-space handling is not what costs: `wake_waiters` measured 93 ns,
`consume_source` 70 ns, `Source::new` 30 ns, `add_source` 82 ns. The cost is the
kernel round trips those eight extra completions imply, and the enters that
submit and cancel them.

## Which explains earlier results

[reactor-loop.md](reactor-loop.md) found four of eight `io_uring_enter` calls on
the latency ring in a shard ping-pong, and that lengthening `preempt_timer` to 10
seconds changed nothing — because the cost is **installing and cancelling** a
timer every loop iteration, not the timer firing.

**Correction.** An earlier version of this document claimed the eventfd read
that wakes a parked shard rides into the kernel on the enter the preempt timer
forces, and that this is why removing the timer deadlocks. **That is not what the
code does.** `SleepableRing::install_eventfd` pushes the read and then calls
`submit_sqes()` itself, so the eventfd reaches the kernel at install time
regardless of the timer.

**The hang is now diagnosed, and it was self-inflicted.** Look at the sleep
condition in `Reactor::react`:

```rust
let should_sleep = preempt_timer().is_none()
    && (woke == 0)
    && poll_ring.can_sleep()
    && main_ring.can_sleep()
    && lat_ring.can_sleep();
```

`preempt_timer().is_none()` is a **precondition for sleeping**, not a
consequence of it. It returns `Some(duration)` while a task queue is running —
meaning "there is something to preempt, do not sleep" — and `None` when the shard
is genuinely idle.

Patching `poll_io(|| Some(...))` to `poll_io(|| None)` therefore tells the
reactor *"always idle, always safe to sleep"* while runnable work still exists,
and simultaneously removes the timer that would have woken it. The shard parks
holding work, with no completion pending. That is the hang.

Verified two ways: with `|| None` every case hangs, including an executor whose
only task is `async {}`; with `|| Some(Duration::from_secs(5))` — a timer so long
it can play no useful role — every case completes normally.

**So the preempt timer is not a load-bearing wakeup source, and removing its
churn does not risk this hang.** Item 1 below does not change what
`preempt_timer()` returns, so `should_sleep` sees exactly what it sees today.
The earlier warning attached to it was based on a broken experiment.

## The shape of a fix

Not "remove the preempt timer" — that was tried and it hangs. The timer exists so
a task queue cannot monopolise the shard, and the eventfd arming currently
depends on it.

What the counts suggest instead:

1. ~~**Do not re-arm an unchanged timer.**~~ **Attempted and it buys nothing.**
   Implemented: track the armed duration, and if `preempt_timer()` asks for the
   same one and the armed timer has not fired, leave it alone. All 433 tests
   pass; the read ladder is unchanged at ~5,700-5,900 ns, against ~5,800 before.

   Counters say why. The reuse condition fires **zero times per round trip**, and
   the reason is always *"no existing timer"*:

   ```
   kept                    0.00 per round trip
   re-armed                1.00 per round trip
     no existing timer     1.00 per round trip
     timer had fired       0.00
     duration changed      0.00
   ```

   There is nothing to reuse, because the timer is cancelled between every pair
   of polls — by the **park path**. `Parker::park()` calls the same code with
   `|| None`, which both allows sleeping and correctly cancels the armed timer,
   since one left armed would wake the sleeper early.

   So the cycle is **arm, run, block, cancel, sleep**, once per round trip, and
   the churn is structural to sleeping rather than redundant re-arming. The
   optimisation targets iterations that poll repeatedly *without* sleeping — a
   busy shard — and a busy shard never pays the 2 µs in the first place.

   Reverted: it added state for no measured benefit.

   **This is not a local change.** The cancellation is entangled with the sleep
   decision:

   ```rust
   // Cancel the old timer regardless of whether we can sleep: ...
   // But if we will sleep, there might be a timer registered that needs
   // to be removed otherwise we'll wake up when it expires.
   drop(self.latency_preemption_timeout_src.take());
   ```

   Cancelling clears a pending submission on the latency ring, which changes
   `lat_ring.can_sleep()`, which feeds `should_sleep` — and `should_sleep` is
   computed *after* the cancel. So "skip the cancel when nothing changed" cannot
   be decided before the value it influences. Doing this means restructuring the
   park decision, in the same function that hangs when the timer is removed
   naively. Budget accordingly; it is not a guard clause.

   A possibly easier start: the **throughput** preemption timer a few lines below
   is `replace`d unconditionally every iteration too, and its comment says it
   does not matter whether the shard sleeps — so it may be separable from the
   sleep decision in a way the latency timer is not. Unverified.
2. **Do not arm a timer for a task queue that is about to block.** This is what
   item 1 should have been. Per round trip the shard arms a preempt timer, runs
   one task, that task blocks immediately, and the timer is cancelled unused. The
   arm was pointless: nothing ran long enough to need preempting.

   The difficulty is that `need_preempt` reads the latency ring's completion
   queue, so the timer has to be armed *before* the task runs — at which point
   whether it will block is unknown. Making this work means either a different
   preemption signal or deferring the arm until a queue has actually been running
   for a while, and both are larger than they sound.
3. ~~Understand the deadlock before removing anything.~~ **Done — see above.**
   The hang was an artefact of the experiment, not a property of the timer. Item
   1 is safe from it by construction. Item 2 changes whether a timer is armed at
   all, but still does not touch `preempt_timer()`'s return value, so it is
   likelier to be safe than it looked — verify rather than assume.

Each needs its own before-and-after measurement, and the ladder in
`probe_read_ladder.rs` is the instrument: it isolates glommio from its own design
so a change can be scored without a runtime rewrite.

## Caveats

**The instrumented run inflates absolute times** by roughly a microsecond of
`Instant::now` calls. Counts are unaffected.

**The ladder's design-cost row is noisy.** It measured −34 ns in one run and
+866 ns in another. The claim that the readiness design is not the problem rests
on it being small and inconsistent in sign, not on the specific figure. The
glommio-over-its-own-design gap was stable at ~2.0–2.5 µs across both.

**Loopback only.** On a real NIC this is a rounding error again, as it is against
an NVMe read. This matters for loopback, IPC-style workloads, and anything else
where the underlying operation is a few microseconds.
