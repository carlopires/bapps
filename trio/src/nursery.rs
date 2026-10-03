//! Structured task ownership on one Glommio executor.
//!
//! v0.2 adds [`OwnedTask`]: an optional control handle for the small set of
//! framework components that must implement "cancel, wait a grace period, then
//! force-abort this exact task" without detaching it from its nursery.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::{Future, poll_fn},
    panic::AssertUnwindSafe,
    pin::Pin,
    rc::Rc,
    task::Poll,
    time::Duration,
};

use futures_lite::future::FutureExt;
use glommio::{GlommioError, ResourceType, Task, channels::oneshot::oneshot, spawn_local_into};

use crate::{
    LocalBoxFuture,
    cancel::{CancelReason, CancelScope, Cancelled, current_cancel_scope, with_cancel_scope},
    sync::Event,
    task_class::{TaskClass, TaskQueues},
    time::{ClockRef, current_clock, with_clock},
};

/// Why a task could not be added to a nursery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SpawnError {
    /// The nursery is closing (its body finished with an error, or it was
    /// cancelled) and admits no new tasks.
    Closing,
    /// The executor refused to spawn the task.
    RuntimeRejected,
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Closing => "nursery is closing and admits no new tasks",
            Self::RuntimeRejected => "executor rejected the task",
        })
    }
}

impl std::error::Error for SpawnError {}

impl<E: std::fmt::Display> std::fmt::Display for NurseryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Child(error) => write!(f, "a child task failed: {error}"),
            Self::Panicked => f.write_str("a child task panicked"),
            Self::Cancelled(cancelled) => write!(f, "the nursery was stopped ({cancelled})"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for NurseryError<E> {}

impl<E: std::fmt::Display> std::fmt::Display for StartError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(error) => write!(f, "the task could not be spawned: {error}"),
            Self::Exited => f.write_str("the task exited before reporting readiness"),
            Self::Child(error) => write!(f, "the task failed before reporting readiness: {error}"),
            Self::Panicked => f.write_str("the task panicked before reporting readiness"),
            Self::Cancelled(cancelled) => {
                write!(
                    f,
                    "the task was stopped before reporting readiness ({cancelled})"
                )
            }
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for StartError<E> {}

/// Why a nursery failed. A nursery fails with the first child error; later
/// ones are dropped, since their siblings were already being cancelled.
#[derive(Debug)]
#[non_exhaustive]
pub enum NurseryError<E> {
    /// A child returned this error (shared: the error is also reported to
    /// whoever else observes the failure).
    Child(Rc<E>),
    /// A child panicked.
    Panicked,
    /// The nursery's scope was cancelled from outside.
    Cancelled(Cancelled),
}

/// Why [`Nursery::start`] returned without the child's readiness value.
#[derive(Debug)]
#[non_exhaustive]
pub enum StartError<E> {
    /// The child could not be spawned.
    Spawn(SpawnError),
    /// The child finished without calling [`TaskStatus::started`].
    Exited,
    /// The child failed with this error before reporting readiness.
    Child(Rc<E>),
    /// The child panicked before reporting readiness.
    Panicked,
    /// The nursery was cancelled before the child reported readiness.
    Cancelled(Cancelled),
}

/// Result of a structured stop request on an [`OwnedTask`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopOutcome {
    /// The task observed cooperative cancellation and exited within the grace
    /// period (or had already finished).
    Graceful,
    /// The grace period expired and the nursery-owned Glommio task handle was
    /// dropped, force-cancelling that exact task.
    Forced,
}

struct TaskControl {
    done: Event,
    finished: Cell<bool>,
}

impl TaskControl {
    fn new() -> Self {
        Self {
            done: Event::new(),
            finished: Cell::new(false),
        }
    }

    fn finish_once(&self) -> bool {
        if self.finished.replace(true) {
            return false;
        }
        self.done.set();
        true
    }
}

/// Records a child's completion when dropped. It is the last field of
/// [`Accounted`], so it runs after the child's own future (and its task-local
/// destructors) have been dropped.
struct Completion<E> {
    id: u64,
    control: Rc<TaskControl>,
    state: Rc<NurseryState<E>>,
}

impl<E> Drop for Completion<E> {
    fn drop(&mut self) {
        if self.control.finish_once() {
            self.state.child_finished();
        }
        self.state.completed_ids.borrow_mut().push(self.id);
    }
}

/// A child future together with its completion record. Struct fields drop in
/// declaration order: first the future, then the completion.
struct Accounted<E> {
    future: LocalBoxFuture<'static, ()>,
    _completion: Completion<E>,
}

impl<E> Future for Accounted<E> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

/// A spawned child: a Glommio task, or a task of the deterministic lab.
/// Dropping either destroys the task; awaiting it joins a finished task.
enum TaskHandle {
    Glommio(Task<()>),
    Lab(crate::lab::LabTask),
}

impl Future for TaskHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        match self.get_mut() {
            Self::Glommio(task) => Pin::new(task).poll(cx),
            Self::Lab(task) => Pin::new(task).poll(cx),
        }
    }
}

fn spawn_task<E: 'static>(
    future: Accounted<E>,
    queues: &TaskQueues,
    class: TaskClass,
) -> Result<TaskHandle, ()> {
    if let Some(lab) = crate::lab::current() {
        return Ok(TaskHandle::Lab(lab.spawn(Box::pin(future))));
    }
    let queue = queues.glommio_queue(class).ok_or(())?;
    spawn_local_into(future, queue)
        .map(TaskHandle::Glommio)
        .map_err(|_| ())
}

struct NurseryState<E> {
    root_scope: CancelScope,
    queues: TaskQueues,
    clock: ClockRef,
    accepting: Cell<bool>,
    body_done: Cell<bool>,
    finished: Cell<bool>,
    active: Cell<usize>,
    all_done: Event,
    next_task_id: Cell<u64>,
    tasks: RefCell<HashMap<u64, TaskHandle>>,
    completed_ids: RefCell<Vec<u64>>,
    first_error: RefCell<Option<Rc<E>>>,
    panicked: Cell<bool>,
}

impl<E> NurseryState<E> {
    /// Drop retained handles of children that already finished. Long-lived
    /// service nurseries must not keep every completed `Task`.
    fn reap_completed(&self) {
        let completed = std::mem::take(&mut *self.completed_ids.borrow_mut());
        let reaped: Vec<_> = {
            let mut tasks = self.tasks.borrow_mut();
            completed
                .into_iter()
                .filter_map(|id| tasks.remove(&id))
                .collect()
        };
        drop(reaped);
    }

    fn begin_child(&self) -> Result<u64, SpawnError> {
        self.reap_completed();
        if !self.accepting.get() {
            return Err(SpawnError::Closing);
        }
        self.active.set(self.active.get() + 1);
        let id = self.next_task_id.get();
        self.next_task_id.set(id.wrapping_add(1));
        Ok(id)
    }

    fn child_finished(&self) {
        self.active.set(self.active.get().saturating_sub(1));
        if self.body_done.get() && self.active.get() == 0 {
            self.accepting.set(false);
            self.all_done.set();
        }
    }

    fn body_finished(&self) {
        self.body_done.set(true);
        if self.active.get() == 0 {
            self.accepting.set(false);
            self.all_done.set();
        }
    }

    fn fail_child(&self, error: E) {
        if self.first_error.borrow().is_none() {
            *self.first_error.borrow_mut() = Some(Rc::new(error));
        }
        self.accepting.set(false);
        self.root_scope.cancel_by(
            CancelReason::NurseryFailure,
            "a sibling task returned an error",
        );
    }

    fn fail_panic(&self) {
        self.panicked.set(true);
        self.accepting.set(false);
        self.root_scope
            .cancel_by(CancelReason::NurseryFailure, "a sibling task panicked");
    }

    fn result(&self) -> Result<(), NurseryError<E>> {
        if let Some(error) = self.first_error.borrow().clone() {
            return Err(NurseryError::Child(error));
        }
        if self.panicked.get() {
            return Err(NurseryError::Panicked);
        }
        if self.root_scope.is_cancelled() {
            return Err(NurseryError::Cancelled(Cancelled {
                reason: self.root_scope.reason(),
            }));
        }
        Ok(())
    }
}

/// A nursery-owned task control handle.
///
/// Dropping this handle does **not** detach or abort the task; the nursery still
/// owns it. The handle merely gives a framework layer a structured way to stop
/// one specific child when supervision policy requires it.
pub struct OwnedTask<E> {
    id: u64,
    scope: CancelScope,
    control: Rc<TaskControl>,
    state: Rc<NurseryState<E>>,
}

impl<E: 'static> OwnedTask<E> {
    /// This task's cancel scope.
    pub fn cancellation_scope(&self) -> CancelScope {
        self.scope.clone()
    }

    /// Whether the task has finished.
    pub fn is_finished(&self) -> bool {
        self.control.finished.get()
    }

    /// Ask this task to stop cooperatively.
    pub fn cancel(&self) {
        self.scope.cancel_with(CancelReason::NurseryClosing);
    }

    /// Force-cancel this exact nursery-owned task and wait until Glommio has
    /// destroyed its future. Returns `true` when a retained task handle was
    /// actually removed.
    ///
    /// This is intentionally asynchronous: framework shutdown code must know
    /// that task-local destructors have run before it treats the task as gone.
    /// Dropping this future early is safe: the task is still destroyed and the
    /// nursery still accounts for it, because that accounting belongs to the
    /// task's own future rather than to this call.
    pub async fn abort(&self) -> bool {
        if self.is_finished() {
            let _ = self.reap();
            return false;
        }
        let task = self.state.tasks.borrow_mut().remove(&self.id);
        let Some(task) = task else {
            return false;
        };
        // Dropping a Glommio task handle cancels the task; the executor then
        // destroys its future, which runs the task's `Completion`.
        drop(task);
        let _ = self.control.done.wait_unchecked().await;
        true
    }

    /// Remove a completed task handle from the nursery's retained-handle set.
    /// This is an optional memory hygiene operation for long-lived nurseries
    /// that create many sequential generations.
    pub fn reap(&self) -> bool {
        if !self.is_finished() {
            return false;
        }
        self.state.tasks.borrow_mut().remove(&self.id).is_some()
    }

    /// Cooperatively cancel the task, wait for `grace`, then force-abort the
    /// same owned task if it still has not returned.
    ///
    /// The grace wait deliberately ignores the caller's current cancellation
    /// scope: this method *is already executing shutdown policy* and must be
    /// able to complete that bounded policy after outer cancellation.
    pub async fn cancel_and_wait(&self, grace: Duration) -> StopOutcome {
        self.cancel();

        if self.is_finished() {
            let _ = self.reap();
            return StopOutcome::Graceful;
        }

        let clock = self.state.clock.clone();
        let deadline = clock.now().saturating_add(grace);
        let mut done = Box::pin(self.control.done.wait_unchecked());
        let mut timer = clock.sleep_until(deadline);

        let graceful = poll_fn(|cx| {
            if done.as_mut().poll(cx).is_ready() {
                return Poll::Ready(true);
            }
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(false);
            }
            Poll::Pending
        })
        .await;

        if graceful {
            let _ = self.reap();
            StopOutcome::Graceful
        } else {
            let _ = self.abort().await;
            StopOutcome::Forced
        }
    }
}

/// Owns a set of executor-local tasks.
pub struct Nursery<E> {
    state: Rc<NurseryState<E>>,
}

impl<E: 'static> Nursery<E> {
    fn new(queues: TaskQueues) -> Self {
        let root_scope = current_cancel_scope()
            .map(|scope| scope.child())
            .unwrap_or_default();
        Self {
            state: Rc::new(NurseryState {
                root_scope,
                queues,
                clock: current_clock(),
                accepting: Cell::new(true),
                body_done: Cell::new(false),
                finished: Cell::new(false),
                active: Cell::new(0),
                all_done: Event::new(),
                next_task_id: Cell::new(1),
                tasks: RefCell::new(HashMap::new()),
                completed_ids: RefCell::new(Vec::new()),
                first_error: RefCell::new(None),
                panicked: Cell::new(false),
            }),
        }
    }

    /// A cloneable handle for spawning into this nursery later or from other
    /// tasks; it cannot outlive the nursery's ability to admit tasks.
    pub fn handle(&self) -> NurseryHandle<E> {
        NurseryHandle {
            state: self.state.clone(),
        }
    }

    /// The nursery's cancel scope: cancelling it cancels every child.
    pub fn cancellation_scope(&self) -> CancelScope {
        self.state.root_scope.clone()
    }

    /// Children still running.
    pub fn active_tasks(&self) -> usize {
        self.state.active.get()
    }

    /// Spawn `task` as a child, scheduled in the current queue. The task gets
    /// its cancel scope; an `Err` it returns fails the nursery and cancels its
    /// siblings.
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn<F, Fut>(&self, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.spawn_into(TaskClass::Default, task)
    }

    /// Like `spawn`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn_into<F, Fut>(&self, class: TaskClass, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.handle().spawn_into(class, task)
    }

    /// Like `spawn`, also returning an [`OwnedTask`] to stop this child
    /// individually (cooperatively, or forcefully after a grace period).
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn_owned<F, Fut>(&self, task: F) -> Result<OwnedTask<E>, SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.spawn_owned_into(TaskClass::Default, task)
    }

    /// Like `spawn_owned`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn_owned_into<F, Fut>(
        &self,
        class: TaskClass,
        task: F,
    ) -> Result<OwnedTask<E>, SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.handle().spawn_owned_into(class, task)
    }

    /// Spawn `task` and wait until it reports readiness with
    /// [`TaskStatus::started`]; returns the value it reported. The task keeps
    /// running in the nursery afterwards.
    ///
    /// # Errors
    ///
    /// [`StartError`]: the spawn failed, or the task exited, failed, panicked or
    /// was cancelled before reporting readiness.
    pub async fn start<T, F, Fut>(&self, task: F) -> Result<T, StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.handle().start(task).await
    }

    /// Like `start`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// As for `start`.
    pub async fn start_into<T, F, Fut>(&self, class: TaskClass, task: F) -> Result<T, StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.handle().start_into(class, task).await
    }

    /// Like `start`, also returning an [`OwnedTask`] for the started child.
    ///
    /// # Errors
    ///
    /// As for `start`.
    pub async fn start_owned<T, F, Fut>(&self, task: F) -> Result<(T, OwnedTask<E>), StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.handle().start_owned(task).await
    }

    /// Like `start_owned`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// As for `start`.
    pub async fn start_owned_into<T, F, Fut>(
        &self,
        class: TaskClass,
        task: F,
    ) -> Result<(T, OwnedTask<E>), StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.handle().start_owned_into(class, task).await
    }

    fn body_finished(&self) {
        self.state.body_finished();
    }

    async fn wait_children(&self) {
        if self.state.active.get() != 0 {
            let _ = self.state.all_done.wait_unchecked().await;
        }

        // At this point active == 0. Any handles not explicitly reaped are
        // complete and can be collected without detaching anything.
        let tasks = std::mem::take(&mut *self.state.tasks.borrow_mut());
        for (_, task) in tasks {
            task.await;
        }
        self.state.finished.set(true);
    }
}

impl<E> Drop for Nursery<E> {
    fn drop(&mut self) {
        if self.state.finished.get() {
            return;
        }

        self.state.accepting.set(false);
        self.state
            .root_scope
            .cancel_by(CancelReason::NurseryClosing, "nursery dropped before exit");

        // Forceful teardown path. Dropping retained Glommio Tasks cancels them.
        // Graceful callers should always exit through `with_nursery`, which waits.
        // Take the handles first: their destruction must not run under a borrow.
        let tasks = std::mem::take(&mut *self.state.tasks.borrow_mut());
        drop(tasks);
    }
}

/// A cloneable way to spawn into a [`Nursery`] from elsewhere (another task,
/// a service). Spawning fails with [`SpawnError::Closing`] once the nursery
/// admits no new tasks.
#[derive(Clone)]
pub struct NurseryHandle<E> {
    state: Rc<NurseryState<E>>,
}

impl<E: 'static> NurseryHandle<E> {
    /// The nursery's cancel scope: cancelling it cancels every child.
    pub fn cancellation_scope(&self) -> CancelScope {
        self.state.root_scope.clone()
    }

    /// Children still running.
    pub fn active_tasks(&self) -> usize {
        self.state.active.get()
    }

    /// Spawn `task` as a child, scheduled in the current queue. The task gets
    /// its cancel scope; an `Err` it returns fails the nursery and cancels its
    /// siblings.
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn<F, Fut>(&self, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.spawn_into(TaskClass::Default, task)
    }

    /// Like `spawn`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn_into<F, Fut>(&self, class: TaskClass, task: F) -> Result<(), SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        let _owned = self.spawn_owned_into(class, task)?;
        Ok(())
    }

    /// Like `spawn`, also returning an [`OwnedTask`] to stop this child
    /// individually (cooperatively, or forcefully after a grace period).
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn_owned<F, Fut>(&self, task: F) -> Result<OwnedTask<E>, SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.spawn_owned_into(TaskClass::Default, task)
    }

    /// Like `spawn_owned`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// [`SpawnError`] when the nursery is closing or the executor refuses.
    pub fn spawn_owned_into<F, Fut>(
        &self,
        class: TaskClass,
        task: F,
    ) -> Result<OwnedTask<E>, SpawnError>
    where
        F: FnOnce(CancelScope) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        let id = self.state.begin_child()?;

        let state = self.state.clone();
        let child_scope = state.root_scope.child();
        let control = Rc::new(TaskControl::new());
        let control_for_task = control.clone();
        let clock = state.clock.clone();
        let queues = state.queues.clone();
        let state_for_task = state.clone();
        let child_scope_for_task = child_scope.clone();

        let future = async move {
            let task_scope = child_scope_for_task.clone();
            let task_future = async move { task(task_scope).await };
            let task_future =
                with_clock(clock, with_cancel_scope(child_scope_for_task, task_future));

            match AssertUnwindSafe(task_future).catch_unwind().await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => state_for_task.fail_child(error),
                Err(_) => state_for_task.fail_panic(),
            }
        };
        // Completion accounting lives *inside* the spawned future, so it runs
        // exactly once however the future ends: returning, or being destroyed
        // by a forced stop, even if whoever requested that stop went away.
        let future = Accounted {
            future: Box::pin(future),
            _completion: Completion {
                id,
                control: control_for_task,
                state: state.clone(),
            },
        };

        match spawn_task(future, &queues, class) {
            Ok(task) => {
                self.state.tasks.borrow_mut().insert(id, task);
                Ok(OwnedTask {
                    id,
                    scope: child_scope,
                    control,
                    state: self.state.clone(),
                })
            }
            // The rejected future was dropped, and its `Completion` with it.
            Err(_) => Err(SpawnError::RuntimeRejected),
        }
    }

    /// Spawn `task` and wait until it reports readiness with
    /// [`TaskStatus::started`]; returns the value it reported. The task keeps
    /// running in the nursery afterwards.
    ///
    /// # Errors
    ///
    /// [`StartError`]: the spawn failed, or the task exited, failed, panicked or
    /// was cancelled before reporting readiness.
    pub async fn start<T, F, Fut>(&self, task: F) -> Result<T, StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.start_into(TaskClass::Default, task).await
    }

    /// Like `start`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// As for `start`.
    pub async fn start_into<T, F, Fut>(&self, class: TaskClass, task: F) -> Result<T, StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.start_owned_into(class, task)
            .await
            .map(|(value, _owned)| value)
    }

    /// Like `start`, also returning an [`OwnedTask`] for the started child.
    ///
    /// # Errors
    ///
    /// As for `start`.
    pub async fn start_owned<T, F, Fut>(&self, task: F) -> Result<(T, OwnedTask<E>), StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        self.start_owned_into(TaskClass::Default, task).await
    }

    /// Like `start_owned`, scheduled in the queue of `class`.
    ///
    /// # Errors
    ///
    /// As for `start`.
    pub async fn start_owned_into<T, F, Fut>(
        &self,
        class: TaskClass,
        task: F,
    ) -> Result<(T, OwnedTask<E>), StartError<E>>
    where
        T: 'static,
        F: FnOnce(CancelScope, TaskStatus<T>) -> Fut + 'static,
        Fut: Future<Output = Result<(), E>> + 'static,
    {
        let (tx, mut rx) = oneshot::<T>();
        let owned = self
            .spawn_owned_into(class, move |scope| {
                task(scope, TaskStatus { sender: Some(tx) })
            })
            .map_err(StartError::Spawn)?;

        let mut cancelled = Box::pin(self.state.root_scope.cancelled());
        let outcome = poll_fn(|cx| {
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(None);
            }
            if let Poll::Ready(value) = Pin::new(&mut rx).poll(cx) {
                return Poll::Ready(Some(value));
            }
            Poll::Pending
        })
        .await;

        match outcome {
            Some(Ok(value)) => Ok((value, owned)),
            Some(Err(_)) | None => {
                if let Some(error) = self.state.first_error.borrow().clone() {
                    Err(StartError::Child(error))
                } else if self.state.panicked.get() {
                    Err(StartError::Panicked)
                } else if self.state.root_scope.is_cancelled() {
                    Err(StartError::Cancelled(Cancelled {
                        reason: self.state.root_scope.reason(),
                    }))
                } else {
                    Err(StartError::Exited)
                }
            }
        }
    }
}

/// One-shot readiness publisher. `started` consumes `self`, so readiness can be
/// published at most once.
pub struct TaskStatus<T> {
    sender: Option<glommio::channels::oneshot::Sender<T>>,
}

impl<T> TaskStatus<T> {
    /// Publish readiness with `value`.
    ///
    /// # Errors
    ///
    /// [`NoWaiter`], handing `value` back, when nobody waits for it any more
    /// (the starter was cancelled or dropped).
    pub fn started(self, value: T) -> Result<(), NoWaiter<T>> {
        match self.sender {
            Some(sender) => sender.send(value).map_err(|error| match error {
                GlommioError::Closed(ResourceType::Channel(value)) => NoWaiter(value),
                other => unreachable!("a oneshot send fails only when closed: {other:?}"),
            }),
            None => unreachable!("TaskStatus is built with a sender and consumed by started"),
        }
    }
}

/// Readiness was published but nobody waits for it any more. Holds the
/// value that was not delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoWaiter<T>(pub T);

impl<T> std::fmt::Display for NoWaiter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("nobody is waiting for this task's readiness")
    }
}

impl<T: std::fmt::Debug> std::error::Error for NoWaiter<T> {}

enum BodyOutcome<R> {
    Body(R),
    Cancelled,
}

/// Open a nursery, run `body` with it, then wait for every child. Returns
/// the body's value once all children finished.
///
/// If a child fails or panics, the remaining children and the body are
/// cancelled (cooperatively) and the nursery fails with the first error.
/// Children are scheduled in the current queue unless spawned `_into` a class.
///
/// # Errors
///
/// [`NurseryError`]: a child failed or panicked, or the nursery was
/// cancelled from outside.
pub async fn with_nursery<E, R, F>(body: F) -> Result<R, NurseryError<E>>
where
    E: 'static,
    F: for<'n> FnOnce(&'n mut Nursery<E>) -> LocalBoxFuture<'n, R>,
{
    with_nursery_with_queues(TaskQueues::current(), body).await
}

/// Like [`with_nursery`], with an explicit set of scheduling queues for the
/// children's task classes.
///
/// # Errors
///
/// As for [`with_nursery`].
pub async fn with_nursery_with_queues<E, R, F>(
    queues: TaskQueues,
    body: F,
) -> Result<R, NurseryError<E>>
where
    E: 'static,
    F: for<'n> FnOnce(&'n mut Nursery<E>) -> LocalBoxFuture<'n, R>,
{
    let mut nursery = Nursery::new(queues);
    let root = nursery.cancellation_scope();
    let clock = nursery.state.clock.clone();

    let body_outcome = {
        let body_future = body(&mut nursery);
        let body_future = with_clock(clock, with_cancel_scope(root.clone(), body_future));
        let mut body_future = Box::pin(body_future);
        let mut cancelled = Box::pin(root.cancelled());

        let first = poll_fn(|cx| {
            if let Poll::Ready(value) = body_future.as_mut().poll(cx) {
                return Poll::Ready(BodyOutcome::Body(value));
            }
            if cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(BodyOutcome::Cancelled);
            }
            Poll::Pending
        })
        .await;

        match first {
            BodyOutcome::Body(value) => value,
            // Cooperative parent cancellation: continue polling the body under
            // the cancelled scope so cancellation-aware operations can unwind.
            BodyOutcome::Cancelled => body_future.await,
        }
    };

    nursery.body_finished();
    nursery.wait_children().await;

    let result = nursery.state.result();
    nursery.state.finished.set(true);
    result?;

    Ok(body_outcome)
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn completed_tasks_are_reaped_on_next_submission() {
        let executor = glommio::LocalExecutorBuilder::new(glommio::Placement::Unbound)
            .make()
            .expect("Glommio executor");
        executor.run(async {
            with_nursery_with_queues::<String, _, _>(TaskQueues::current(), |nursery| {
                Box::pin(async move {
                    for _ in 0..500 {
                        nursery.spawn(|_| async { Ok(()) }).expect("spawn");
                        while nursery.active_tasks() != 0 {
                            futures_lite::future::yield_now().await;
                        }
                        assert!(nursery.state.tasks.borrow().len() <= 1);
                        assert!(nursery.state.completed_ids.borrow().len() <= 1);
                    }
                })
            })
            .await
            .expect("nursery");
        });
    }
}
