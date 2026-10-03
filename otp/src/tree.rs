use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
};

use crate::{
    ChildSnapshot, ExitReason, ExitRecord, NodeStatus, SupervisorSnapshot, TreeNodeSnapshot,
    mailbox::MailboxStats, service_tasks::ServiceTaskStats,
};

const HISTORY_LIMIT: usize = 32;

#[derive(Default)]
struct TreeState {
    nodes: HashMap<String, TreeNodeSnapshot>,
    mailboxes: HashMap<String, Vec<MailboxStats>>,
    service_tasks: HashMap<String, ServiceTaskStats>,
    history: HashMap<String, VecDeque<ExitRecord>>,
}

#[derive(Clone, Default)]
pub struct RuntimeTree {
    inner: Rc<RefCell<TreeState>>,
}

impl RuntimeTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<TreeNodeSnapshot> {
        let paths: Vec<String> = self.inner.borrow().nodes.keys().cloned().collect();
        let mut nodes: Vec<_> = paths.iter().filter_map(|path| self.get(path)).collect();
        nodes.sort_by(|a, b| path_of(a).cmp(path_of(b)));
        nodes
    }

    pub fn get(&self, path: &str) -> Option<TreeNodeSnapshot> {
        let state = self.inner.borrow();
        let node = state.nodes.get(path)?.clone();
        Some(enrich(node, &state))
    }

    pub fn child(&self, path: &str) -> Option<ChildSnapshot> {
        match self.get(path) {
            Some(TreeNodeSnapshot::Child(child)) => Some(child),
            _ => None,
        }
    }

    pub fn supervisor(&self, path: &str) -> Option<SupervisorSnapshot> {
        match self.get(path) {
            Some(TreeNodeSnapshot::Supervisor(supervisor)) => Some(supervisor),
            _ => None,
        }
    }

    pub fn history(&self, path: &str) -> Vec<ExitRecord> {
        self.inner
            .borrow()
            .history
            .get(path)
            .map(|history| history.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn set_supervisor(&self, supervisor: SupervisorSnapshot) {
        self.inner.borrow_mut().nodes.insert(
            supervisor.path.clone(),
            TreeNodeSnapshot::Supervisor(supervisor),
        );
    }

    pub(crate) fn set_child(&self, child: ChildSnapshot) {
        self.inner
            .borrow_mut()
            .nodes
            .insert(child.path.clone(), TreeNodeSnapshot::Child(child));
    }

    pub(crate) fn begin_generation(&self, path: &str) {
        let mut state = self.inner.borrow_mut();
        state.mailboxes.remove(path);
        state.service_tasks.remove(path);
    }

    pub(crate) fn attach_mailbox(&self, path: &str, stats: MailboxStats) {
        self.inner
            .borrow_mut()
            .mailboxes
            .entry(path.to_owned())
            .or_default()
            .push(stats);
    }

    pub(crate) fn attach_service_tasks(&self, path: &str, stats: ServiceTaskStats) {
        self.inner
            .borrow_mut()
            .service_tasks
            .insert(path.to_owned(), stats);
    }

    pub(crate) fn record_exit(
        &self,
        path: &str,
        generation: u64,
        at: std::time::Duration,
        reason: ExitReason,
        cause: Option<bapps_trio::CancelCause>,
    ) {
        let mut state = self.inner.borrow_mut();
        let history = state.history.entry(path.to_owned()).or_default();
        if history.len() == HISTORY_LIMIT {
            history.pop_front();
        }
        history.push_back(ExitRecord {
            generation,
            at,
            reason,
            cause,
        });
    }
}

fn enrich(node: TreeNodeSnapshot, state: &TreeState) -> TreeNodeSnapshot {
    match node {
        TreeNodeSnapshot::Child(mut child) => {
            child.active_tasks = state
                .service_tasks
                .get(&child.path)
                .map(ServiceTaskStats::active)
                .unwrap_or(0);
            child.mailboxes = state
                .mailboxes
                .get(&child.path)
                .map(|mailboxes| mailboxes.iter().map(MailboxStats::snapshot).collect())
                .unwrap_or_default();
            child.recent_exits = state
                .history
                .get(&child.path)
                .map(|history| history.iter().cloned().collect())
                .unwrap_or_default();
            TreeNodeSnapshot::Child(child)
        }
        TreeNodeSnapshot::Supervisor(mut supervisor) => {
            supervisor.active_children = state
                .nodes
                .values()
                .filter(|node| direct_parent(node) == Some(supervisor.path.as_str()))
                .filter(|node| {
                    matches!(
                        status_of(node),
                        NodeStatus::Starting | NodeStatus::Running | NodeStatus::Stopping
                    )
                })
                .count();
            supervisor.recent_exits = state
                .history
                .get(&supervisor.path)
                .map(|history| history.iter().cloned().collect())
                .unwrap_or_default();
            TreeNodeSnapshot::Supervisor(supervisor)
        }
    }
}

fn status_of(node: &TreeNodeSnapshot) -> NodeStatus {
    match node {
        TreeNodeSnapshot::Supervisor(node) => node.status,
        TreeNodeSnapshot::Child(node) => node.status,
    }
}

fn direct_parent(node: &TreeNodeSnapshot) -> Option<&str> {
    match node {
        TreeNodeSnapshot::Supervisor(node) => node.parent.as_deref(),
        TreeNodeSnapshot::Child(node) => Some(node.parent.as_str()),
    }
}

fn path_of(node: &TreeNodeSnapshot) -> &str {
    match node {
        TreeNodeSnapshot::Supervisor(node) => &node.path,
        TreeNodeSnapshot::Child(node) => &node.path,
    }
}
