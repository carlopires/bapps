use std::{
    any::Any,
    cell::RefCell,
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    time::Duration,
};

use bapps_trio::{
    CancelCause, CancelReason, CancelScope, Nursery, NurseryError, OwnedTask, StartError,
    TaskQueues, TaskStatus, current_clock, with_cancel_scope, with_nursery_with_queues,
};
use futures_lite::future::FutureExt;

use crate::{
    ChildContext, ChildSnapshot, ChildSpec, ChildType, ExitReason, NodeStatus, OtpError, Registry,
    Restart, RestartIntensity, RuntimeTree, ServiceGeneration, ServiceTasks, Shutdown, Strategy,
    SupervisorSnapshot,
    error::{map_nursery, map_start},
    mailbox::{ChildExit, ExitMailbox},
    registry::OwnerId,
};

#[derive(Clone)]
pub struct SupervisorSpec {
    name: &'static str,
    strategy: Strategy,
    intensity: RestartIntensity,
    children: Vec<ChildSpec>,
}

impl SupervisorSpec {
    pub fn new(name: &'static str, strategy: Strategy) -> Self {
        Self {
            name,
            strategy,
            intensity: RestartIntensity::default(),
            children: Vec::new(),
        }
    }

    pub fn restart_intensity(mut self, max_restarts: usize, within: Duration) -> Self {
        self.intensity = RestartIntensity {
            max_restarts,
            within,
        };
        self
    }

    pub fn child(mut self, child: ChildSpec) -> Self {
        self.children.push(child);
        self
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn strategy(&self) -> Strategy {
        self.strategy
    }

    pub fn children(&self) -> &[ChildSpec] {
        &self.children
    }

    pub async fn run_root(
        self,
        shutdown: CancelScope,
        queues: TaskQueues,
        registry: Registry,
        tree: RuntimeTree,
        root_path: String,
    ) -> Result<(), OtpError> {
        self.run_internal(shutdown, queues, registry, tree, root_path, None)
            .await
    }

    /// Run a root supervisor and signal readiness only after every child has
    /// completed its startup handshake. Used by a node-wide startup barrier.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_root_started(
        self,
        shutdown: CancelScope,
        queues: TaskQueues,
        registry: Registry,
        tree: RuntimeTree,
        root_path: String,
        started: TaskStatus<()>,
    ) -> Result<(), OtpError> {
        self.run_internal(shutdown, queues, registry, tree, root_path, Some(started))
            .await
    }

    pub(crate) async fn run_as_child(
        self,
        ctx: ChildContext,
        started: TaskStatus<()>,
    ) -> Result<(), OtpError> {
        self.run_internal(
            ctx.scope(),
            ctx.queues(),
            ctx.registry(),
            ctx.tree(),
            ctx.path().to_owned(),
            Some(started),
        )
        .await
    }

    async fn run_internal(
        self,
        shutdown: CancelScope,
        queues: TaskQueues,
        registry: Registry,
        tree: RuntimeTree,
        path: String,
        started: Option<TaskStatus<()>>,
    ) -> Result<(), OtpError> {
        let parent_path = path.rsplit_once('/').map(|(parent, _)| parent.to_owned());
        tree.set_supervisor(SupervisorSnapshot {
            path: path.clone(),
            parent: parent_path,
            name: self.name.to_owned(),
            status: NodeStatus::Starting,
            strategy: self.strategy,
            restart_intensity: self.intensity,
            restart_count: 0,
            last_exit: None,
            active_children: 0,
            recent_exits: Vec::new(),
        });

        // Shield the supervisor's internal nursery from parent cancellation.
        // Parent shutdown is observed explicitly so children can be stopped in
        // reverse dependency order with each child's shutdown policy.
        let internal_scope = CancelScope::new();
        let spec = self.clone();
        let path_for_body = path.clone();
        let tree_for_body = tree.clone();

        let result = with_cancel_scope(
            internal_scope,
            with_nursery_with_queues::<(), _, _>(queues.clone(), move |nursery| {
                let registry = registry.clone();
                let tree = tree_for_body.clone();
                let shutdown = shutdown.clone();
                let path = path_for_body.clone();
                Box::pin(async move {
                    let mailbox = ExitMailbox::new();
                    let runtimes = Rc::new(RefCell::new(
                        spec.children
                            .iter()
                            .cloned()
                            .map(ChildRuntime::new)
                            .collect::<Vec<_>>(),
                    ));
                    let mut restart_times = VecDeque::new();
                    let mut supervisor_restart_count = 0_u64;

                    for index in 0..spec.children.len() {
                        if shutdown.is_cancelled() {
                            shutdown_all(&runtimes, &mailbox, &tree, &path, "supervisor shutdown").await;
                            set_supervisor_status(
                                &tree,
                                &path,
                                NodeStatus::Stopped,
                                supervisor_restart_count,
                                Some(ExitReason::Shutdown),
                            );
                            return Ok::<(), OtpError>(());
                        }

                        if let Err(error) = start_child(
                            nursery,
                            index,
                            &path,
                            &queues,
                            &registry,
                            &tree,
                            &mailbox,
                            &runtimes,
                            &shutdown,
                        )
                        .await
                        {
                            shutdown_all(&runtimes, &mailbox, &tree, &path, "a child failed to start").await;
                            if shutdown.is_cancelled() {
                                // Stop requested while this child was starting.
                                set_supervisor_status(
                                    &tree,
                                    &path,
                                    NodeStatus::Stopped,
                                    supervisor_restart_count,
                                    Some(ExitReason::Shutdown),
                                );
                                return Ok(());
                            }
                            set_supervisor_status(
                                &tree,
                                &path,
                                NodeStatus::Failed,
                                supervisor_restart_count,
                                Some(ExitReason::Failure(error.to_string())),
                            );
                            return Err(error);
                        }
                    }

                    set_supervisor_status(
                        &tree,
                        &path,
                        NodeStatus::Running,
                        supervisor_restart_count,
                        None,
                    );

                    if let Some(started) = started
                        && let Err(error) = started.started(())
                    {
                        shutdown_all(&runtimes, &mailbox, &tree, &path, "parent dropped readiness").await;
                        let error = OtpError::ChildStartProtocol {
                            child: path.clone(),
                            detail: format!("parent dropped readiness receiver: {error:?}"),
                        };
                        set_supervisor_status(
                            &tree,
                            &path,
                            NodeStatus::Failed,
                            supervisor_restart_count,
                            Some(ExitReason::Failure(error.to_string())),
                        );
                        return Err(error);
                    }

                    loop {
                        let next = mailbox
                            .recv_or_cancel(&shutdown)
                            .await
                            .map_err(|cancelled| {
                                OtpError::Application(format!(
                                    "supervisor {path} internal wait cancelled unexpectedly: {cancelled:?}"
                                ))
                            })?;

                        let Some(exit) = next else {
                            set_supervisor_status(
                                &tree,
                                &path,
                                NodeStatus::Stopping,
                                supervisor_restart_count,
                                Some(ExitReason::Shutdown),
                            );
                            shutdown_all(&runtimes, &mailbox, &tree, &path, "supervisor shutdown").await;
                            set_supervisor_status(
                                &tree,
                                &path,
                                NodeStatus::Stopped,
                                supervisor_restart_count,
                                Some(ExitReason::Shutdown),
                            );
                            return Ok(());
                        };

                        if !is_current_exit(&runtimes, &exit) {
                            continue;
                        }

                        if shutdown.is_cancelled() {
                            set_supervisor_status(
                                &tree,
                                &path,
                                NodeStatus::Stopping,
                                supervisor_restart_count,
                                Some(ExitReason::Shutdown),
                            );
                            shutdown_all(&runtimes, &mailbox, &tree, &path, "supervisor shutdown").await;
                            set_supervisor_status(
                                &tree,
                                &path,
                                NodeStatus::Stopped,
                                supervisor_restart_count,
                                Some(ExitReason::Shutdown),
                            );
                            return Ok(());
                        }

                        let (restart, failed_index) = {
                            let children = runtimes.borrow();
                            let runtime = &children[exit.index];
                            (exit.reason.should_restart(runtime.spec.restart), exit.index)
                        };

                        if !restart {
                            reap_runtime_task(&runtimes, failed_index);
                            continue;
                        }

                        let affected =
                            affected_children(spec.strategy, failed_index, runtimes.borrow().len());
                        let restart_indices =
                            restartable_children(&runtimes, failed_index, &affected);

                        // Stop still-running siblings in reverse dependency order.
                        let why = format!(
                            "restart after {} exited ({})",
                            runtimes.borrow()[failed_index].spec.name,
                            exit.reason
                        );
                        for &index in affected.iter().rev() {
                            if index == failed_index {
                                continue;
                            }
                            stop_child(index, &runtimes, &mailbox, &tree, &path, &why).await;
                        }
                        reap_runtime_task(&runtimes, failed_index);

                        for &index in &restart_indices {
                            supervisor_restart_count += 1;
                            if let Err(error) = record_restart(
                                &path,
                                spec.intensity,
                                &mut restart_times,
                            ) {
                                shutdown_all(&runtimes, &mailbox, &tree, &path, "restart intensity exceeded").await;
                                set_supervisor_status(
                                    &tree,
                                    &path,
                                    NodeStatus::Failed,
                                    supervisor_restart_count,
                                    Some(ExitReason::Failure(error.to_string())),
                                );
                                return Err(error);
                            }

                            {
                                let mut children = runtimes.borrow_mut();
                                children[index].restart_count += 1;
                            }

                            if let Err(error) = start_child(
                                nursery,
                                index,
                                &path,
                                &queues,
                                &registry,
                                &tree,
                                &mailbox,
                                &runtimes,
                                &shutdown,
                            )
                            .await
                            {
                                shutdown_all(&runtimes, &mailbox, &tree, &path, "a restarted child failed to start").await;
                                if shutdown.is_cancelled() {
                                    set_supervisor_status(
                                        &tree,
                                        &path,
                                        NodeStatus::Stopped,
                                        supervisor_restart_count,
                                        Some(ExitReason::Shutdown),
                                    );
                                    return Ok(());
                                }
                                set_supervisor_status(
                                    &tree,
                                    &path,
                                    NodeStatus::Failed,
                                    supervisor_restart_count,
                                    Some(ExitReason::Failure(error.to_string())),
                                );
                                return Err(error);
                            }
                        }

                        set_supervisor_status(
                            &tree,
                            &path,
                            NodeStatus::Running,
                            supervisor_restart_count,
                            None,
                        );
                    }
                })
            }),
        )
        .await;

        match result {
            Ok(body) => body,
            Err(error) => Err(map_nursery(error)),
        }
    }
}

struct ChildRuntime {
    spec: ChildSpec,
    generation: u64,
    restart_count: u64,
    scope: Option<CancelScope>,
    task: Option<OwnedTask<()>>,
    running: bool,
    last_exit: Option<ExitReason>,
    path: Option<String>,
}

type Runtimes = Rc<RefCell<Vec<ChildRuntime>>>;

impl ChildRuntime {
    fn new(spec: ChildSpec) -> Self {
        Self {
            spec,
            generation: 0,
            restart_count: 0,
            scope: None,
            task: None,
            running: false,
            last_exit: None,
            path: None,
        }
    }
}

struct ServiceExitGuard {
    finished: bool,
    registry: Registry,
    owner: OwnerId,
    generation_handle: ServiceGeneration,
    runtimes: Runtimes,
    tree: RuntimeTree,
    mailbox: ExitMailbox,
    index: usize,
    generation: u64,
    child_path: String,
}

impl ServiceExitGuard {
    #[allow(clippy::too_many_arguments)]
    fn new(
        registry: Registry,
        owner: OwnerId,
        generation_handle: ServiceGeneration,
        runtimes: Runtimes,
        tree: RuntimeTree,
        mailbox: ExitMailbox,
        index: usize,
        generation: u64,
        child_path: String,
    ) -> Self {
        Self {
            finished: false,
            registry,
            owner,
            generation_handle,
            runtimes,
            tree,
            mailbox,
            index,
            generation,
            child_path,
        }
    }

    fn finish(mut self, reason: ExitReason) {
        self.finished = true;
        self.publish(reason);
    }

    fn publish(&self, reason: ExitReason) {
        // Why the generation's cancellation started, if it was cancelled at
        // all, captured before exit itself cancels the scope.
        let cause = self.generation_handle.scope().cause();
        self.generation_handle.mark_stopped();
        self.registry.remove_owner(self.owner);
        mark_child_exit(
            &self.runtimes,
            &self.tree,
            self.index,
            self.generation,
            &self.child_path,
            reason.clone(),
            cause,
        );
        self.mailbox.push(ChildExit {
            index: self.index,
            generation: self.generation,
            reason,
        });
    }
}

impl Drop for ServiceExitGuard {
    fn drop(&mut self) {
        if !self.finished {
            // The only expected path here is structured force-abort. Keeping
            // cleanup in Drop is what makes framework-owned liveness and
            // registry cleanup survive task cancellation.
            self.publish(ExitReason::Killed);
            self.finished = true;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_child(
    nursery: &Nursery<()>,
    index: usize,
    supervisor_path: &str,
    queues: &TaskQueues,
    registry: &Registry,
    tree: &RuntimeTree,
    mailbox: &ExitMailbox,
    runtimes: &Runtimes,
    shutdown: &CancelScope,
) -> Result<(), OtpError> {
    let (spec, generation, restart_count, child_path) = {
        let mut children = runtimes.borrow_mut();
        let runtime = &mut children[index];
        if let Some(old_task) = runtime.task.take() {
            let _ = old_task.reap();
        }
        runtime.generation += 1;
        runtime.running = false;
        runtime.scope = None;
        runtime.last_exit = None;
        let child_path = format!("{supervisor_path}/{}", runtime.spec.name);
        runtime.path = Some(child_path.clone());
        (
            runtime.spec.clone(),
            runtime.generation,
            runtime.restart_count,
            child_path,
        )
    };

    tree.begin_generation(&child_path);
    set_child_status(
        tree,
        supervisor_path,
        &child_path,
        &spec,
        NodeStatus::Starting,
        generation,
        restart_count,
        None,
    );

    let owner = OwnerId(next_owner_id());
    let generation_root = CancelScope::new();
    let factory = spec.factory.clone();
    let mailbox_for_task = mailbox.clone();
    let registry_for_task = registry.clone();
    let tree_for_task = tree.clone();
    let runtimes_for_task = runtimes.clone();
    let queues_for_task = queues.clone();
    let child_path_for_task = child_path.clone();
    let generation_root_for_task = generation_root.clone();
    let child_name = spec.name;

    // Created before the task so the supervisor can mark readiness on it.
    let generation_handle =
        ServiceGeneration::new(child_path.clone(), generation, generation_root.clone());
    let generation_for_task = generation_handle.clone();

    let start = nursery.start_owned_into(spec.task_class, move |task_scope, status| {
        let service_scope = CancelScope::any([task_scope, generation_root_for_task.clone()]);
        let generation_handle = generation_for_task;
        generation_handle.replace_scope(service_scope.clone());
        let guard = ServiceExitGuard::new(
            registry_for_task.clone(),
            owner,
            generation_handle.clone(),
            runtimes_for_task.clone(),
            tree_for_task.clone(),
            mailbox_for_task.clone(),
            index,
            generation,
            child_path_for_task.clone(),
        );

        let registry = registry_for_task.clone();
        let tree = tree_for_task.clone();
        let queues = queues_for_task.clone();
        let path = child_path_for_task.clone();
        async move {
            let reason = run_service(
                factory,
                child_name,
                path,
                queues,
                registry,
                tree,
                owner,
                generation_handle,
                status,
                service_scope,
            )
            .await;
            guard.finish(reason);
            Ok::<(), ()>(())
        }
    });
    // Children are shielded from parent shutdown so they can be stopped in
    // reverse order with their own policies. A child that has not reached
    // readiness yet is not running, so a parent stop must reach it directly:
    // otherwise shutdown waits for an initialization that may never finish.
    // (Pre-readiness stop is cooperative; there is no task handle to force.)
    let result = {
        let mut start = std::pin::pin!(start);
        let mut stop = std::pin::pin!(shutdown.cancelled());
        let mut forwarded = false;
        std::future::poll_fn(|cx| {
            if !forwarded && stop.as_mut().poll(cx).is_ready() {
                generation_root.cancel_by(
                    CancelReason::NurseryClosing,
                    format!("supervisor {supervisor_path}: stop requested during startup"),
                );
                forwarded = true;
            }
            start.as_mut().poll(cx)
        })
        .await
    };

    match result {
        Ok(((), task)) => {
            let mut children = runtimes.borrow_mut();
            let runtime = &mut children[index];
            if runtime.generation == generation && runtime.last_exit.is_none() {
                generation_handle.mark_ready();
                runtime.scope = Some(generation_root);
                runtime.task = Some(task);
                runtime.running = true;
                if spec.child_type == ChildType::Worker {
                    set_child_status(
                        tree,
                        supervisor_path,
                        &child_path,
                        &spec,
                        NodeStatus::Running,
                        generation,
                        restart_count,
                        None,
                    );
                }
            } else {
                let _ = task.reap();
            }
            Ok(())
        }
        Err(StartError::Exited) => {
            let reason = mailbox
                .take(index, generation)
                .map(|exit| exit.reason)
                .or_else(|| runtimes.borrow()[index].last_exit.clone())
                .unwrap_or_else(|| ExitReason::Failure("exited before readiness".to_owned()));
            Err(OtpError::ChildStartFailed {
                child: child_path,
                reason,
            })
        }
        Err(error) => Err(map_start(&child_path, error)),
    }
}

enum MainOutcome {
    Normal,
    Error(String),
    Panic(String),
}

#[allow(clippy::too_many_arguments)]
async fn run_service(
    factory: crate::ServiceFactory,
    child_name: &'static str,
    path: String,
    queues: TaskQueues,
    registry: Registry,
    tree: RuntimeTree,
    owner: OwnerId,
    generation: ServiceGeneration,
    status: TaskStatus<()>,
    service_scope: CancelScope,
) -> ExitReason {
    let path_for_body = path.clone();
    let tree_for_body = tree.clone();
    let service_scope_for_body = service_scope.clone();
    let generation_for_body = generation.clone();

    let result = with_cancel_scope(
        service_scope.clone(),
        with_nursery_with_queues::<String, _, _>(queues.clone(), move |nursery| {
            let tasks = ServiceTasks::new(nursery.handle());
            let effective_scope =
                CancelScope::any([service_scope_for_body.clone(), nursery.cancellation_scope()]);
            generation_for_body.replace_scope(effective_scope.clone());
            tree_for_body.attach_service_tasks(&path_for_body, tasks.stats());
            let ctx = ChildContext::new(
                child_name,
                path_for_body.clone(),
                effective_scope,
                queues.clone(),
                registry.clone(),
                tree_for_body.clone(),
                owner,
                generation_for_body.clone(),
                tasks.clone(),
            );

            Box::pin(async move {
                let outcome = match catch_unwind(AssertUnwindSafe(|| (factory)(ctx, status))) {
                    Ok(future) => match AssertUnwindSafe(future).catch_unwind().await {
                        Ok(Ok(())) => MainOutcome::Normal,
                        Ok(Err(error)) => MainOutcome::Error(error),
                        Err(payload) => MainOutcome::Panic(panic_message(payload)),
                    },
                    Err(payload) => MainOutcome::Panic(panic_message(payload)),
                };

                // The long-lived service loop has ended. Its transient work is
                // generation-owned and must not survive it.
                tasks.cancel_all();
                outcome
            })
        }),
    )
    .await;

    match result {
        Ok(MainOutcome::Normal) if service_scope.is_cancelled() => ExitReason::Shutdown,
        Ok(MainOutcome::Normal) => ExitReason::Normal,
        Ok(MainOutcome::Error(_)) if service_scope.is_cancelled() => ExitReason::Shutdown,
        Ok(MainOutcome::Error(error)) => ExitReason::Failure(error),
        Ok(MainOutcome::Panic(message)) => ExitReason::Panic(message),
        Err(NurseryError::Child(error)) => {
            ExitReason::Failure(format!("service-owned task failed: {error}"))
        }
        Err(NurseryError::Panicked) => ExitReason::Panic("service-owned task panicked".to_owned()),
        Err(NurseryError::Cancelled(_)) if service_scope.is_cancelled() => ExitReason::Shutdown,
        Err(NurseryError::Cancelled(cancelled)) => ExitReason::Failure(format!(
            "service task group cancelled unexpectedly ({:?})",
            cancelled.reason
        )),
    }
}

fn mark_child_exit(
    runtimes: &Runtimes,
    tree: &RuntimeTree,
    index: usize,
    generation: u64,
    child_path: &str,
    reason: ExitReason,
    cause: Option<CancelCause>,
) {
    let mut children = runtimes.borrow_mut();
    let runtime = &mut children[index];
    if runtime.generation != generation {
        return;
    }
    runtime.running = false;
    runtime.scope = None;
    runtime.last_exit = Some(reason.clone());
    tree.record_exit(
        child_path,
        generation,
        current_clock().now(),
        reason.clone(),
        cause,
    );

    let status = if reason.is_abnormal() {
        NodeStatus::Failed
    } else {
        NodeStatus::Stopped
    };

    if runtime.spec.child_type == ChildType::Supervisor
        && matches!(
            tree.get(child_path),
            Some(crate::TreeNodeSnapshot::Supervisor(_))
        )
    {
        let restart_count = tree
            .supervisor(child_path)
            .map(|snapshot| snapshot.restart_count)
            .unwrap_or(runtime.restart_count);
        set_supervisor_status(tree, child_path, status, restart_count, Some(reason));
    } else {
        set_child_status(
            tree,
            child_path
                .rsplit_once('/')
                .map(|(parent, _)| parent)
                .unwrap_or(""),
            child_path,
            &runtime.spec,
            status,
            runtime.generation,
            runtime.restart_count,
            Some(reason),
        );
    }
}

/// Stop one child with its shutdown policy. `supervisor` and `why` become the
/// origin of the child's cancellation (see `CancelScope::cause`).
async fn stop_child(
    index: usize,
    runtimes: &Runtimes,
    mailbox: &ExitMailbox,
    tree: &RuntimeTree,
    supervisor: &str,
    why: &str,
) {
    let (
        running,
        generation,
        scope,
        task,
        child_type,
        child_path,
        parent_path,
        spec,
        restart_count,
    ) = {
        let mut children = runtimes.borrow_mut();
        let runtime = &mut children[index];
        let child_path = runtime
            .path
            .clone()
            .unwrap_or_else(|| runtime.spec.name.to_owned());
        let parent_path = child_path
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_owned())
            .unwrap_or_default();
        (
            runtime.running,
            runtime.generation,
            runtime.scope.clone(),
            runtime.task.take(),
            runtime.spec.child_type,
            child_path,
            parent_path,
            runtime.spec.clone(),
            runtime.restart_count,
        )
    };

    if !running {
        if let Some(task) = task {
            let _ = task.reap();
        }
        let _ = mailbox.take(index, generation);
        return;
    }

    if child_type == ChildType::Worker {
        set_child_status(
            tree,
            &parent_path,
            &child_path,
            &spec,
            NodeStatus::Stopping,
            generation,
            restart_count,
            None,
        );
    }

    if let Some(scope) = scope {
        scope.cancel_by(
            CancelReason::NurseryClosing,
            format!("supervisor {supervisor}: {why}"),
        );
    }

    if let Some(task) = task {
        match spec.shutdown {
            Shutdown::Graceful(grace) => {
                let _ = task.cancel_and_wait(grace).await;
            }
            Shutdown::BrutalKill => {
                let _ = task.abort().await;
            }
        }
    }

    let _ = mailbox.wait_for(index, generation).await;
}

async fn shutdown_all(
    runtimes: &Runtimes,
    mailbox: &ExitMailbox,
    tree: &RuntimeTree,
    supervisor: &str,
    why: &str,
) {
    let len = runtimes.borrow().len();
    for index in (0..len).rev() {
        stop_child(index, runtimes, mailbox, tree, supervisor, why).await;
    }
}

fn reap_runtime_task(runtimes: &Runtimes, index: usize) {
    let task = runtimes.borrow_mut()[index].task.take();
    if let Some(task) = task {
        let _ = task.reap();
    }
}

fn affected_children(strategy: Strategy, failed_index: usize, len: usize) -> Vec<usize> {
    match strategy {
        Strategy::OneForOne => vec![failed_index],
        Strategy::OneForAll => (0..len).collect(),
        Strategy::RestForOne => (failed_index..len).collect(),
    }
}

fn restartable_children(
    runtimes: &Runtimes,
    failed_index: usize,
    affected: &[usize],
) -> Vec<usize> {
    let children = runtimes.borrow();
    affected
        .iter()
        .copied()
        .filter(|&index| {
            if index == failed_index {
                children[index]
                    .last_exit
                    .as_ref()
                    .is_some_and(|reason| reason.should_restart(children[index].spec.restart))
            } else if children[index].running {
                children[index].spec.restart != Restart::Temporary
            } else {
                children[index]
                    .last_exit
                    .as_ref()
                    .is_some_and(|reason| reason.should_restart(children[index].spec.restart))
            }
        })
        .collect()
}

fn is_current_exit(runtimes: &Runtimes, exit: &ChildExit) -> bool {
    runtimes
        .borrow()
        .get(exit.index)
        .is_some_and(|runtime| runtime.generation == exit.generation)
}

fn record_restart(
    supervisor_path: &str,
    intensity: RestartIntensity,
    restart_times: &mut VecDeque<Duration>,
) -> Result<(), OtpError> {
    let now = current_clock().now();
    restart_times.push_back(now);
    while restart_times
        .front()
        .is_some_and(|oldest| now.saturating_sub(*oldest) > intensity.within)
    {
        restart_times.pop_front();
    }

    if restart_times.len() > intensity.max_restarts {
        return Err(OtpError::RestartIntensityExceeded {
            supervisor: supervisor_path.to_owned(),
            restarts: restart_times.len(),
            max_restarts: intensity.max_restarts,
            within: intensity.within,
        });
    }

    Ok(())
}

fn set_supervisor_status(
    tree: &RuntimeTree,
    path: &str,
    status: NodeStatus,
    restart_count: u64,
    last_exit: Option<ExitReason>,
) {
    let Some(crate::TreeNodeSnapshot::Supervisor(mut snapshot)) = tree.get(path) else {
        return;
    };
    snapshot.status = status;
    snapshot.restart_count = restart_count;
    if last_exit.is_some() {
        snapshot.last_exit = last_exit;
    }
    tree.set_supervisor(snapshot);
}

#[allow(clippy::too_many_arguments)]
fn set_child_status(
    tree: &RuntimeTree,
    parent_path: &str,
    path: &str,
    spec: &ChildSpec,
    status: NodeStatus,
    generation: u64,
    restart_count: u64,
    last_exit: Option<ExitReason>,
) {
    tree.set_child(ChildSnapshot {
        path: path.to_owned(),
        parent: parent_path.to_owned(),
        name: spec.name.to_owned(),
        child_type: spec.child_type,
        status,
        restart: spec.restart,
        shutdown: spec.shutdown,
        task_class: spec.task_class,
        generation,
        restart_count,
        last_exit,
        active_tasks: 0,
        mailboxes: Vec::new(),
        recent_exits: Vec::new(),
    });
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn next_owner_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}
