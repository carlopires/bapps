//! A small, explicit multicore layer above shard-local OTP and Trio.
//!
//! Each configured CPU owns one executor and one application. Only owned
//! `Send + 'static` request/reply values cross cores. Local futures, registries,
//! service handles and cancel scopes never migrate.
//!
//! This is an educational framework, not an OTP/Seastar compatibility claim.
//! See the developer guide for cancellation and node-failure boundaries.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod cpu;
pub mod lab;
mod rpc;
mod runtime;

pub use cpu::{allowed_cpus, parse_cpu_list, resolve_cpus};
pub use rpc::{
    CallError, CallId, CallOptions, Interruption, RpcLimits, RpcMetrics, ShardClient, ShardInbox,
    serve,
};
pub use runtime::{
    AppBuilder, AppError, NodeControl, ReadyGate, ShardContext, ShardId, ShardResult,
};
