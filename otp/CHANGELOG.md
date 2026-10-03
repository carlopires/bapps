# Changelog

## 0.7.0

Dependency-only: `bapps-trio` 0.7.0.

## 0.6.0 — part of bapps

Renamed from `glommio-otp` to `bapps-otp` (library `bapps_otp`) and moved
into the bapps workspace. No API change.

## 0.5.1

Dependency-only: `bapps-trio` 0.5.1.

## 0.5.0 — supervision trees under the deterministic lab

Uses `bapps-trio` 0.5.0. No API change. `tests/lab.rs` runs OTP scenarios
under Trio's lab over 200 seeds each and requires every run to be clean (root
finished, no deadlock, no leftover task, no pending or leaked obligation):
OneForAll restarts, restart-intensity escalation, a shutdown's recorded exit,
and cancelled producers racing on a small mailbox. The producer scenario hits
196 distinct interleavings; replacing reserve-then-send with build-then-send
fails all 200 seeds. Supervision itself is sequential, so the tree scenarios
have a single interleaving: they check virtual-time behaviour and the
oracles, not ordering.

## 0.4.0 — reserve-then-send mailboxes

Added `LocalSender::reserve_in(scope) -> SendPermit` and `SendPermit::send`.
A permit holds one slot; `send` never waits and returns the value only if the
receiver is gone; dropping an unused permit releases the slot. Reserve before
building a message that carries a reply channel or a claim, so cancellation
while waiting for capacity loses nothing. `send`/`send_in` now reserve
internally. `MailboxSnapshot` has a new `reserved` field. Uses
`bapps-trio` 0.4.0. Tests: `tests/permits.rs`.

## 0.3.0 — generation phases and startup cancellation (G2)

Builds on the carlopires Glommio fork `v0.12.0-ng-cp.2`, which adds a timer
fix: the reactor no longer scans every live timer to find the next deadline
(77% of a busy executor's CPU in a key-value service benchmark;
single-executor throughput ~30.6k -> ~56.4k requests/s).

Added `GenerationPhase` (`Starting`, `Ready`, `Draining`, `Stopped`) with
`ServiceGeneration::phase` and `is_accepting`. The admission linearization
point is the cancellation of the generation's scope: from then on
`operation_scope` fails with `ServiceUnavailable { phase: Draining, .. }` and
the registry stops returning the generation's entries, while the generation
is still alive and cleaning up.

Fixed: **a parent stop during a child's startup was ignored.** Children are
shielded from parent cancellation, and the supervisor only observed shutdown
between starts, so stopping a supervisor whose child was still initializing
waited for a readiness that might never come (the new
`stop_before_readiness_ends_cleanly_without_restart` test hung). A stop is now
forwarded to a child that is still starting, and the supervisor returns
cleanly.

Tests: `tests/lifecycle.rs` (phases and advertisement, stale handle after
restart, stop before readiness, failure before readiness, error during a
requested drain) and registry unit tests for late cleanup by an old owner.

Added cancellation attribution (with `bapps-trio` 0.3.0): supervisors
cancel children with an origin ("supervisor <path>: supervisor shutdown",
"...: restart after <child> exited (<reason>)", "...: stop requested during
startup"), and `ExitRecord::cause` keeps why each generation's cancellation
started. An application's runtime-tree view shows it.

Breaking: `ServiceUnavailable` has a new `phase` field; `ExitRecord` has a new
`cause` field; `Registry::get`, `contains` and `names` hide entries of a
draining generation. Requires `bapps-trio` 0.3.0.

## 0.2.3

Matched with `bapps-trio` 0.2.3. Glommio now comes from the carlopires fork, tag `v0.12.0-ng-cp.1`
(glommio-ng 0.12.0 plus one fix): an accept or open that completed without any
caller collecting it leaked the new descriptor. Dropping a listener while an
abandoned `accept()` was in flight left a racing client connected to a socket
nobody served or closed.

## 0.2.2

Uses `bapps-trio` 0.2.2 (waker-retention and abandoned-stop fixes).

Added `ServiceGeneration::operation_scope(caller)`: fail fast on a stale
generation, otherwise return a scope owned by both caller and generation. This
is the first line of every handle method in a typical service. `MailboxError` now displays
the cancellation reason. The service-pattern guide documents keeping ingress
resources (listeners) outside the service generation.

## 0.2.1 — multicore companion

Add root readiness handshakes, synchronous service-task admission accounting, wait_below, and cancellation checks on mailbox fast paths. Add regressions and multicore integration docs.

## 0.2.0

Built from the validated 0.1 implementation and patterns proven in the first application built on it.

### Added

- bounded shard-local `LocalMailbox<T>` with backpressure and cancellation;
- framework-owned `ServiceGeneration` liveness tokens;
- generation-owned `ServiceTasks` for transient Trio work;
- child shutdown policy: `Graceful(Duration)` or `BrutalKill`;
- bounded graceful shutdown followed by structured force-abort;
- `ExitReason::Killed`;
- live mailbox metrics, active service-task counts, and bounded exit history in
  `RuntimeTree`;
- `ChildContext::mailbox`, `generation`, and `tasks`;
- service-pattern, architecture, migration, and developer documentation.

### Changed

- the supervisor owns generation death publication and registry cleanup;
- applications no longer need a `ServiceLifetime` drop guard;
- service-owned task failure participates in the service failure boundary;
- runtime-tree mailbox instrumentation uses weak references and therefore does
  not extend mailbox/message lifetime;
- child shutdown is now bounded instead of cooperative forever.

### Unchanged by design

- application-specific command enums and state machines remain in applications;
- transient request fan-out remains `bapps-trio`, not OTP children;
- cross-shard/cross-node failure propagation remains an explicit protocol;
- no generic `GenServer` trait yet.
