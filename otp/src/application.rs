use bapps_trio::{CancelScope, TaskQueues};

use crate::{OtpError, Registry, RuntimeTree, SupervisorSpec};

/// One shard's OTP application: a root supervisor with the registry and
/// runtime tree its services share.
pub struct Application {
    name: &'static str,
    root: SupervisorSpec,
    registry: Registry,
    tree: RuntimeTree,
}

impl Application {
    /// An application named `name` with `root` as its root supervisor.
    pub fn new(name: &'static str, root: SupervisorSpec) -> Self {
        Self {
            name,
            root,
            registry: Registry::new(),
            tree: RuntimeTree::new(),
        }
    }

    /// The registry its services register in; for globals set before it runs.
    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }

    /// The live supervision tree, for introspection.
    pub fn tree(&self) -> RuntimeTree {
        self.tree.clone()
    }

    /// Run the root supervisor until `shutdown` is cancelled (an orderly stop)
    /// or the root fails.
    ///
    /// # Errors
    ///
    /// [`OtpError`] when the root supervisor fails: a child could not start,
    /// or restarts exceeded the root's intensity.
    pub async fn run(self, shutdown: CancelScope, queues: TaskQueues) -> Result<(), OtpError> {
        let root_path = format!("{}/{}", self.name, self.root.name());
        self.root
            .run_root(shutdown, queues, self.registry, self.tree, root_path)
            .await
    }
    /// Like `run`, with a root readiness handshake for multicore application
    /// startup. Readiness is local; a node-wide barrier belongs to bapps_app.
    pub async fn run_started(
        self,
        shutdown: CancelScope,
        queues: TaskQueues,
        started: bapps_trio::TaskStatus<()>,
    ) -> Result<(), OtpError> {
        let root_path = format!("{}/{}", self.name, self.root.name());
        self.root
            .run_root_started(
                shutdown,
                queues,
                self.registry,
                self.tree,
                root_path,
                started,
            )
            .await
    }
}
