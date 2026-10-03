# Local semantics inside a multicore node

This crate still governs one Glommio executor. Create a separate local application/nursery graph on each selected CPU with `bapps_app`. The app factory creates local `Rc` state and `!Send` futures on the owning thread. Nothing here makes an arbitrary local object safe to send to another CPU.

Only owned `Send` request/reply data crosses the app layer's bounded transport. Cancellation over that boundary is an explicit protocol with acknowledgement and uncertainty, not `CancelScope::any` shared across OS threads. The latter remains useful for multiple owners **on the same executor**.

A service error/panic belongs to local supervision; a failed shard root/executor causes node-wide stop and joins. There is no automatic restoration of storage contents. Default panic=abort, unsafe/native faults, blocking work and OS-process death remain different failure classes.

Clock injection here remains local. The multicore transport uses real process-local monotonic time and is not a deterministic simulated network. A full deterministic multicore simulator is future work, not a property of `TestClock` alone.

Framework ordering: application-specific data and consistency -> bapps_app CPU/node boundary -> per-shard OTP services -> per-service Trio tasks -> Glommio task queues/I/O. Each layer can be tested without hiding the boundary below it.
