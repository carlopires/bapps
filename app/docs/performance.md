# Performance: measurements before claims

This release adds a real shard-per-core structure, not a demonstrated speedup. There is no lock-free, zero-copy, linear-scaling or Seastar-equivalence claim.

The reference cross-core transport uses `async-channel` bounded MPSC. Each call allocates cancellation/reply channels. This is a deliberate semantic starting point, not the last transport. Compare against Glommio shared/SPSC channels behind the same request/cancellation contract after profiling; do not introduce a full mesh merely because it sounds faster. Full-mesh queues have topology and memory costs.

Run the same release-mode workload with one, two and four explicit physical cores. Keep CPU frequency, NUMA placement, SMT policy, value size, key distribution, client concurrency, node count and payload sizes fixed. Run the load generator on separate resources. A small scripted driver is not proof that the server has been saturated; monitor client CPU before trusting its ceiling.

Measure throughput, p50/p95/p99 end-to-end latency, per-core utilization, task queue pressure, active RPCs, allocations/RSS over time, cross-shard fraction, context switches and network bytes. Include local-hit, forced-cross-core, hot-key skew, scan-heavy and failure-during-load workloads. Separate one-node local sharding from three-node network routing.

An application's own choices -- one TCP connection per RPC, JSONL, in-memory strings, over-fetching scans -- can dominate performance. A Python client and JSON serialization may conceal gains in the runtime.

Trio 0.2.2 removes a wait's registrations when the wait is dropped, and completed nursery task handles are reaped. Application restart soaks should check descriptors after restarts; a long-lived-root allocation/RSS soak is still needed before treating this as production-hardened.

Primary runtime references (dependency remains the user baseline's `glommio-ng` 0.12):
- [Glommio executor builder](https://docs.rs/glommio/latest/glommio/struct.LocalExecutorBuilder.html)
- [Glommio TCP implementation, including reuse-port bind](https://github.com/DataDog/glommio/blob/master/glommio/src/net/tcp_socket.rs)
- [async-channel bounded sender](https://docs.rs/async-channel/latest/async_channel/struct.Sender.html)

Canonical Glommio references describe architecture/API intent; the exact fork must pass the supplied native-Linux build gate. No dependency version was silently upgraded to follow a `latest` page.
