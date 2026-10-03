//! Node ownership of executor threads; no local registry crosses this boundary.
use crate::{RpcLimits, ShardClient, ShardInbox, allowed_cpus, resolve_cpus, rpc::SystemClock};
use async_channel::{Receiver, Sender};
use bapps_otp::Application;
use bapps_trio::{CancelScope, TaskQueues, cancel_on, with_nursery_with_queues};
use glommio::{LocalExecutorBuilder, Placement};
use std::{
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

/// Stable only within one configured node run. CPU ID and shard ID differ.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(pub usize);
impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a shard's future may finish with: `Ok(())`, or `Err` of any error
/// that can be displayed (a `String`, an [`AppError`] from
/// [`ShardContext::run_application`], an application's own error type). The
/// error is reported as [`AppError::ShardFailed`].
pub trait ShardResult {
    /// The outcome, with the error rendered for the node's report.
    ///
    /// # Errors
    ///
    /// The rendered error, when the shard failed.
    fn into_shard_result(self) -> Result<(), String>;
}

impl<E: fmt::Display> ShardResult for Result<(), E> {
    fn into_shard_result(self) -> Result<(), String> {
        self.map_err(|error| error.to_string())
    }
}

/// Why a node could not start, or stopped abnormally.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AppError {
    /// Invalid configuration: RPC limits, timeouts, CPU selection.
    Config(String),
    /// The operating system refused something the node needs (reading CPU
    /// affinity, spawning an executor thread).
    System(String),
    /// Not every shard became ready within the startup timeout.
    StartupTimeout(Duration),
    /// A shard's application failed, panicked or exited on its own; the
    /// whole node was stopped. The first failure is reported.
    ShardFailed {
        /// The shard that failed first.
        shard: ShardId,
        /// What it reported.
        reason: String,
    },
    /// The node stopped before (or while) becoming ready.
    NodeStopped,
    /// The caller's wait was cancelled.
    Cancelled,
}
impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(reason) => write!(f, "invalid configuration: {reason}"),
            Self::System(reason) => write!(f, "system error: {reason}"),
            Self::StartupTimeout(after) => {
                write!(f, "not every shard became ready within {after:?}")
            }
            Self::ShardFailed { shard, reason } => write!(f, "shard {shard} failed: {reason}"),
            Self::NodeStopped => f.write_str("the node stopped before becoming ready"),
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}
impl std::error::Error for AppError {}

const STARTING: u8 = 0;
const RUNNING: u8 = 1;
const STOPPING: u8 = 2;
struct NodeShared {
    phase: AtomicU8,
    stop: Vec<Sender<()>>,
    ready: Sender<()>,
}

/// Node-wide *control-plane* handle. No application state or local cancel
/// scope lives here. Closing a readiness gate wakes all waiters; per-shard
/// stop messages are on dedicated capacity-one channels, not the RPC queues.
#[derive(Clone)]
pub struct NodeControl {
    shared: Arc<NodeShared>,
}
impl NodeControl {
    /// Request a node-wide stop: every shard's application is asked to stop
    /// and readiness waiters are released; [`AppBuilder::run`] returns once
    /// every shard has exited. Does not block; idempotent.
    pub fn shutdown(&self) {
        self.shared.phase.store(STOPPING, Ordering::Release);
        self.shared.ready.close();
        for sender in &self.shared.stop {
            let _ = sender.try_send(());
        }
    }
    /// Whether a stop was requested.
    pub fn is_stopping(&self) -> bool {
        self.shared.phase.load(Ordering::Acquire) == STOPPING
    }
    pub(crate) fn release(&self) {
        if self
            .shared
            .phase
            .compare_exchange(STARTING, RUNNING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.shared.ready.close();
        }
    }
}

/// All local OTP roots must report ready before normal traffic is admitted.
/// Callers must not make cross-shard calls during an init handshake.
#[derive(Clone)]
pub struct ReadyGate {
    signal: Receiver<()>,
    control: NodeControl,
}
impl ReadyGate {
    /// Wait until every shard of the node is ready.
    ///
    /// # Errors
    ///
    /// [`AppError::NodeStopped`] if the node stops first;
    /// [`AppError::Cancelled`] if `scope` is cancelled first.
    pub async fn wait(&self, scope: &CancelScope) -> Result<(), AppError> {
        let _ = cancel_on(scope, self.signal.recv())
            .await
            .map_err(|_| AppError::Cancelled)?;
        if self.control.shared.phase.load(Ordering::Acquire) == RUNNING {
            Ok(())
        } else {
            Err(AppError::NodeStopped)
        }
    }
}

/// Node control and readiness gate for `stop` senders, one per shard.
pub(crate) fn control_plane(stop: Vec<Sender<()>>) -> (NodeControl, ReadyGate) {
    let (ready, signal) = async_channel::bounded(1);
    let control = NodeControl {
        shared: Arc::new(NodeShared {
            phase: AtomicU8::new(STARTING),
            stop,
            ready,
        }),
    };
    let gate = ReadyGate {
        signal,
        control: control.clone(),
    };
    (control, gate)
}

/// An already-open gate for single-executor protocol tests.
#[cfg(test)]
pub(crate) fn open_gate_for_tests() -> ReadyGate {
    let (ready, signal) = async_channel::bounded(1);
    let control = NodeControl {
        shared: Arc::new(NodeShared {
            phase: AtomicU8::new(STARTING),
            stop: Vec::new(),
            ready,
        }),
    };
    control.release();
    ReadyGate { signal, control }
}

pub(crate) enum HostEvent {
    Ready(ShardId),
    Exited(ShardId, Result<(), String>),
}

/// Where shards report readiness and exit: the blocking host thread of
/// [`AppBuilder::run`], or the in-executor host of [`crate::lab::run_node`].
#[derive(Clone)]
pub(crate) enum HostSink {
    Thread(mpsc::Sender<HostEvent>),
    Lab(async_channel::Sender<HostEvent>),
}

impl HostSink {
    pub(crate) fn send(&self, event: HostEvent) -> Result<(), String> {
        match self {
            Self::Thread(sender) => sender.send(event).map_err(|e| e.to_string()),
            Self::Lab(sender) => sender.try_send(event).map_err(|e| e.to_string()),
        }
    }
}

/// Constructed *inside* the destination executor. All contained local objects
/// may be !Send. Use `client()` to cross cores with owned messages, not closures.
pub struct ShardContext<M: Send + 'static, R: Send + 'static> {
    pub(crate) id: ShardId,
    pub(crate) cpu: usize,
    pub(crate) cpus: Arc<Vec<usize>>,
    pub(crate) client: ShardClient<M, R>,
    pub(crate) inbox: ShardInbox<M, R>,
    pub(crate) control: NodeControl,
    pub(crate) gate: ReadyGate,
    pub(crate) stop: Receiver<()>,
    pub(crate) host: HostSink,
}
impl<M: Send + 'static, R: Send + 'static> ShardContext<M, R> {
    /// This shard's index.
    pub fn shard_id(&self) -> ShardId {
        self.id
    }
    /// The CPU this shard's executor is pinned to.
    pub fn cpu_id(&self) -> usize {
        self.cpu
    }
    /// Shards in the node.
    pub fn shard_count(&self) -> usize {
        self.cpus.len()
    }
    /// Every shard's CPU, indexed by shard.
    pub fn cpu_ids(&self) -> &[usize] {
        &self.cpus
    }
    /// A client for calling any shard (this one included).
    pub fn client(&self) -> ShardClient<M, R> {
        self.client.clone()
    }
    /// This shard's inbox; serve it with [`serve`](crate::serve).
    pub fn inbox(&self) -> ShardInbox<M, R> {
        self.inbox.clone()
    }
    /// Node-wide stop control.
    pub fn node_control(&self) -> NodeControl {
        self.control.clone()
    }
    /// The node's readiness gate.
    pub fn ready_gate(&self) -> ReadyGate {
        self.gate.clone()
    }

    /// Run one local OTP root and join its shutdown before the executor exits.
    /// The host barrier opens only after *all* calls to `run_started` succeed.
    /// Service panics belong to OTP; an exhausted root shuts down the node.
    pub async fn run_application(self, application: Application) -> Result<(), AppError> {
        let queues = TaskQueues::storage_defaults();
        let queues_for_app = queues.clone();
        let shutdown = CancelScope::new();
        let shutdown_watcher = shutdown.clone();
        let shutdown_app = shutdown.clone();
        let stop = self.stop;
        let host = self.host;
        let id = self.id;
        let result = with_nursery_with_queues::<String, _, _>(queues, move |nursery| {
            Box::pin(async move {
                nursery
                    .spawn(move |watch_scope| async move {
                        if cancel_on(&watch_scope, stop.recv()).await.is_ok() {
                            shutdown_watcher.cancel();
                        }
                        Ok(())
                    })
                    .map_err(|e| format!("cannot start node-stop watcher: {e:?}"))?;
                let initialized = nursery
                    .start(move |_scope, status| async move {
                        application
                            .run_started(shutdown_app, queues_for_app, status)
                            .await
                            .map_err(|e| e.to_string())
                    })
                    .await;
                if let Err(error) = initialized {
                    shutdown.cancel();
                    nursery.cancellation_scope().cancel();
                    return Err(format!("shard {id} failed to initialize: {error:?}"));
                }
                if let Err(error) = host.send(HostEvent::Ready(id)) {
                    shutdown.cancel();
                    nursery.cancellation_scope().cancel();
                    return Err(error.to_string());
                }
                let group = nursery.cancellation_scope();
                // App failure wakes the group; normal node stop wakes shutdown.
                futures_lite::future::or(shutdown.cancelled(), group.cancelled()).await;
                shutdown.cancel();
                group.cancel(); // retire watcher; OTP still drains its own children
                Ok::<(), String>(())
            })
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason)) => Err(AppError::ShardFailed { shard: id, reason }),
            Err(bapps_trio::NurseryError::Cancelled(_)) if self.control.is_stopping() => Ok(()),
            Err(error) => Err(AppError::ShardFailed {
                shard: id,
                reason: format!("application: {error:?}"),
            }),
        }
    }
}

/// One pinned executor per selected logical CPU. Explicit CPU order is also
/// shard order. No work stealing, implicit all-core allocation or auto NUMA
/// claims. The host thread only coordinates startup/termination and joins.
pub struct AppBuilder {
    shards: usize,
    cpus: Option<Vec<usize>>,
    limits: RpcLimits,
    startup_timeout: Duration,
    name: String,
}
impl Default for AppBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl AppBuilder {
    /// One shard, default limits, a 30 s startup timeout.
    pub fn new() -> Self {
        Self {
            shards: 1,
            cpus: None,
            limits: RpcLimits::default(),
            startup_timeout: Duration::from_secs(30),
            name: "bapps-app".into(),
        }
    }
    /// Run `count` shards on the lowest allowed CPUs.
    pub fn shards(mut self, count: usize) -> Self {
        self.shards = count;
        self
    }
    /// Explicit CPU selection determines the number and order of shards.
    pub fn cpus(mut self, cpus: Vec<usize>) -> Self {
        self.shards = cpus.len();
        self.cpus = Some(cpus);
        self
    }
    /// Cross-shard transport limits.
    pub fn limits(mut self, limits: RpcLimits) -> Self {
        self.limits = limits;
        self
    }
    /// How long every shard has to become ready.
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }
    /// Prefix of the executor thread names (`name-0`, `name-1`, ...).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// The factory is copied into each OS thread. Its local future is created
    /// there and does not need Send. Failures are fail-stop for the whole node;
    /// executor resurrection and data rebalancing are deliberately not implied.
    pub fn run<M, R, F, Fut>(self, factory: F) -> Result<(), AppError>
    where
        M: Send + 'static,
        R: Send + 'static,
        F: Fn(ShardContext<M, R>) -> Fut + Clone + Send + 'static,
        Fut: Future + 'static,
        Fut::Output: ShardResult,
    {
        self.limits.validate()?;
        if self.startup_timeout.is_zero() {
            return Err(AppError::Config("startup timeout must be positive".into()));
        }
        let cpus = Arc::new(resolve_cpus(
            self.shards,
            self.cpus.as_deref(),
            &allowed_cpus()?,
        )?);
        let (senders, receivers) =
            crate::rpc::fabric::<M, R>(self.shards, self.limits.queue_capacity);
        let mut stops = Vec::new();
        let mut stop_receivers = Vec::new();
        for _ in 0..self.shards {
            let (tx, rx) = async_channel::bounded(1);
            stops.push(tx);
            stop_receivers.push(rx);
        }
        let (ready_tx, ready_rx) = async_channel::bounded(1);
        let control = NodeControl {
            shared: Arc::new(NodeShared {
                phase: AtomicU8::new(STARTING),
                stop: stops,
                ready: ready_tx,
            }),
        };
        let gate = ReadyGate {
            signal: ready_rx,
            control: control.clone(),
        };
        let (host_tx, host_rx) = mpsc::channel();
        let mut threads = Vec::new();
        for (index, (receiver, stop)) in receivers.into_iter().zip(stop_receivers).enumerate() {
            let cpu = cpus[index];
            let id = ShardId(index);
            let name = format!("{}-{index}", self.name);
            let cpus = cpus.clone();
            let senders = senders.clone();
            let control_for_thread = control.clone();
            let gate = gate.clone();
            let host = host_tx.clone();
            let factory = factory.clone();
            let limits = self.limits;
            let spawned = thread::Builder::new().name(name.clone()).spawn(move || {
                // Catch setup/teardown panics too, not just application futures.
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    let executor = LocalExecutorBuilder::new(Placement::Fixed(cpu))
                        .name(&name)
                        .make()
                        .map_err(|e| format!("executor {id} on CPU {cpu}: {e:?}"))?;
                    let context = ShardContext {
                        id,
                        cpu,
                        cpus,
                        client: ShardClient::bind(
                            id,
                            senders,
                            limits,
                            gate.clone(),
                            Rc::new(SystemClock),
                        ),
                        inbox: ShardInbox::bind(id, receiver, limits, Rc::new(SystemClock)),
                        control: control_for_thread,
                        gate,
                        stop,
                        host: HostSink::Thread(host.clone()),
                    };
                    executor.run(async move { factory(context).await.into_shard_result() })
                }))
                .unwrap_or_else(|payload| {
                    let text = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_else(|| "non-string panic".into());
                    Err(format!("executor {id} panicked: {text}"))
                });
                let _ = host.send(HostEvent::Exited(id, outcome));
            });
            match spawned {
                Ok(handle) => threads.push(handle),
                Err(error) => {
                    control.shutdown();
                    for handle in threads {
                        let _ = handle.join();
                    }
                    return Err(AppError::System(format!("spawning shard {index}: {error}")));
                }
            }
        }
        drop(host_tx);
        drop(senders); // only executor-local clients retain request senders
        let started = Instant::now();
        let mut ready = vec![false; self.shards];
        let mut exited = 0;
        let mut failure = None;
        while exited < self.shards {
            let was_stopping = control.is_stopping();
            let all_ready = ready.iter().all(|value| *value);
            let event = if all_ready || was_stopping {
                host_rx
                    .recv()
                    .map_err(|_| AppError::System("every shard's event channel closed".into()))
            } else {
                let remaining = self.startup_timeout.saturating_sub(started.elapsed());
                host_rx
                    .recv_timeout(remaining)
                    .map_err(|error| match error {
                        mpsc::RecvTimeoutError::Timeout => {
                            AppError::StartupTimeout(self.startup_timeout)
                        }
                        mpsc::RecvTimeoutError::Disconnected => {
                            AppError::System("every shard's event channel closed".into())
                        }
                    })
            };
            match event {
                Ok(HostEvent::Ready(id)) => {
                    ready[id.0] = true;
                    if ready.iter().all(|value| *value) {
                        control.release();
                    }
                }
                Ok(HostEvent::Exited(id, result)) => {
                    exited += 1;
                    if let Err(reason) = result {
                        failure.get_or_insert(AppError::ShardFailed { shard: id, reason });
                    } else if !control.is_stopping() {
                        failure.get_or_insert_with(|| AppError::ShardFailed {
                            shard: id,
                            reason: "exited on its own".into(),
                        });
                    }
                    control.shutdown();
                }
                Err(error) => {
                    failure.get_or_insert(error);
                    control.shutdown();
                    // On timeout, drain exits; disconnected means no more events.
                    if was_stopping || all_ready {
                        break;
                    }
                }
            }
        }
        for handle in threads {
            if handle.join().is_err() {
                failure.get_or_insert(AppError::System("executor thread join failed".into()));
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
