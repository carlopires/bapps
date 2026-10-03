use std::{fmt, future::Future, pin::Pin, rc::Rc, time::Duration};

use bapps_trio::{CancelScope, TaskClass, TaskQueues, TaskStatus};

use crate::{
    ChildType, LocalMailbox, LocalReceiver, LocalSender, Registry, RegistryError, Restart,
    RuntimeTree, ServiceGeneration, ServiceKey, ServiceTasks, Shutdown, SupervisorSpec,
    registry::OwnerId,
};

pub type ServiceFuture = Pin<Box<dyn Future<Output = Result<(), String>> + 'static>>;
pub type ServiceFactory = Rc<dyn Fn(ChildContext, TaskStatus<()>) -> ServiceFuture>;

#[derive(Clone)]
pub struct ChildContext {
    name: &'static str,
    path: String,
    scope: CancelScope,
    queues: TaskQueues,
    registry: Registry,
    tree: RuntimeTree,
    owner: OwnerId,
    generation: ServiceGeneration,
    tasks: ServiceTasks,
}

impl ChildContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        name: &'static str,
        path: String,
        scope: CancelScope,
        queues: TaskQueues,
        registry: Registry,
        tree: RuntimeTree,
        owner: OwnerId,
        generation: ServiceGeneration,
        tasks: ServiceTasks,
    ) -> Self {
        Self {
            name,
            path,
            scope,
            queues,
            registry,
            tree,
            owner,
            generation,
            tasks,
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn scope(&self) -> CancelScope {
        self.scope.clone()
    }

    pub fn queues(&self) -> TaskQueues {
        self.queues.clone()
    }

    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    pub fn tree(&self) -> RuntimeTree {
        self.tree.clone()
    }

    /// Framework-owned liveness token for this exact service generation.
    /// Cached application handles can clone this token and fail fast after the
    /// supervisor has replaced their generation.
    pub fn generation(&self) -> ServiceGeneration {
        self.generation.clone()
    }

    /// Trio task group whose lifetime is owned by this service generation.
    pub fn tasks(&self) -> ServiceTasks {
        self.tasks.clone()
    }

    /// Construct and automatically expose a bounded shard-local mailbox in the
    /// runtime tree for this service generation.
    pub fn mailbox<T: 'static>(
        &self,
        name: &'static str,
        capacity: usize,
    ) -> (LocalSender<T>, LocalReceiver<T>) {
        let (sender, receiver) = LocalMailbox::<T>::bounded_named(name, capacity);
        self.tree.attach_mailbox(&self.path, sender.stats());
        (sender, receiver)
    }

    pub fn register<T>(&self, key: ServiceKey<T>, value: T) -> Result<(), RegistryError>
    where
        T: Clone + 'static,
    {
        self.registry
            .register_owned(key, value, self.owner, self.generation.clone())
    }

    pub fn service<T>(&self, key: ServiceKey<T>) -> Option<T>
    where
        T: Clone + 'static,
    {
        self.registry.get(key)
    }
}

#[derive(Clone)]
pub struct ChildSpec {
    pub(crate) name: &'static str,
    pub(crate) child_type: ChildType,
    pub(crate) restart: Restart,
    pub(crate) shutdown: Shutdown,
    pub(crate) task_class: TaskClass,
    pub(crate) factory: ServiceFactory,
}

impl ChildSpec {
    pub fn worker<F, Fut, E>(name: &'static str, factory: F) -> Self
    where
        F: Fn(ChildContext, TaskStatus<()>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
        E: fmt::Display + 'static,
    {
        let factory = Rc::new(move |ctx: ChildContext, status: TaskStatus<()>| {
            let future = factory(ctx, status);
            Box::pin(async move { future.await.map_err(|error| error.to_string()) })
                as ServiceFuture
        });

        Self {
            name,
            child_type: ChildType::Worker,
            restart: Restart::Permanent,
            shutdown: Shutdown::default(),
            task_class: TaskClass::Default,
            factory,
        }
    }

    pub fn supervisor(name: &'static str, supervisor: SupervisorSpec) -> Self {
        Self::worker(name, move |ctx, status| {
            let supervisor = supervisor.clone();
            async move { supervisor.run_as_child(ctx, status).await }
        })
        .child_type(ChildType::Supervisor)
    }

    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = restart;
        self
    }

    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    pub fn shutdown_after(self, grace: Duration) -> Self {
        self.shutdown(Shutdown::Graceful(grace))
    }

    pub fn task_class(mut self, task_class: TaskClass) -> Self {
        self.task_class = task_class;
        self
    }

    fn child_type(mut self, child_type: ChildType) -> Self {
        self.child_type = child_type;
        self
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn restart_policy(&self) -> Restart {
        self.restart
    }

    pub fn shutdown_policy(&self) -> Shutdown {
        self.shutdown
    }

    pub fn task_class_value(&self) -> TaskClass {
        self.task_class
    }

    pub fn child_type_value(&self) -> ChildType {
        self.child_type
    }
}
