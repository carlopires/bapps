# Adding the app layer to existing local applications

Keep existing local Trio/OTP APIs and use their matched 0.2.1 companion versions. Move construction of local state from the host function into `AppBuilder::run`'s per-shard factory. Build one local OTP application there and finish by awaiting `ShardContext::run_application`.

Define an owned Send request/reply type. Add one OTP child that calls `bapps_app::serve`. Use the provided local client for explicit target-shard calls. Keep same-core service handles local. No local registry is automatically globalized.

Anything formerly assumed node-wide is now per-shard unless deliberately placed on a designated shard. Inspect sockets, disk handles, background jobs, metrics and memory reservations: multiplying all of them by core count may be wrong. A network listener can intentionally bind per core, and an in-memory store can be intentionally partitioned, not duplicated.

Root readiness is additive and explicit. Child initialization must not wait for the global barrier before signalling readiness. Existing `Application::run` can still be used standalone; the multicore host uses the readiness-aware variant.
