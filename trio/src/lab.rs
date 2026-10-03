//! Deterministic lab executor for shard-local (`!Send`, `Rc`) code.
//!
//! [`Lab::run`] drives a future and every task that nurseries spawn under it on
//! a single thread, without Glommio:
//!
//! - **Seeded scheduling.** Whenever several tasks are ready, a seeded RNG picks
//!   the next one. The same seed always gives the same interleaving; different
//!   seeds explore different ones.
//! - **Virtual time.** The root runs under a [`TestClock`]. When no task is
//!   ready the lab jumps the clock to the earliest pending sleep, so deadlines,
//!   grace periods and restart intensity windows cost no wall time.
//! - **Oracles.** The [`LabReport`] says whether the root finished, whether the
//!   run deadlocked (no ready task and no pending timer), how many tasks were
//!   left behind, and which obligations were left pending or leaked.
//! - **Replay.** [`LabReport::trace`] is the order in which tasks were polled; a
//!   failing seed reproduces exactly.
//!
//! What runs: everything built from this crate (nurseries, cancel scopes,
//! `Event`, `Condition`, deadlines, `TestClock`) and from layers built only on
//! it, such as `bapps_otp` supervision trees, mailboxes and registries. What
//! does not: Glommio I/O and Glommio timers, which need a real executor.
//!
//! This is Asupersync's lab idea applied to the task model that Asupersync's
//! own lab excludes: executor-pinned `!Send` tasks.

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::{
    LocalBoxFuture,
    obligation::obligation_stats,
    testing::TestClock,
    time::{Clock, with_clock},
};

thread_local! {
    static CURRENT: RefCell<Option<Weak<LabInner>>> = const { RefCell::new(None) };
}

/// The lab driving the current thread, if any.
pub(crate) fn current() -> Option<LabSpawner> {
    CURRENT
        .with(|current| current.borrow().as_ref().and_then(Weak::upgrade))
        .map(|inner| LabSpawner { inner })
}

pub(crate) fn is_active() -> bool {
    current().is_some()
}

/// Configuration for one lab run.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct LabConfig {
    /// Scheduling seed.
    pub seed: u64,
    /// Upper bound on task polls; a run that reaches it reports `step_limited`.
    pub max_steps: u64,
}

impl LabConfig {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            max_steps: 1_000_000,
        }
    }

    /// Stop a run after this many task polls (a livelock guard; default one
    /// million).
    #[must_use]
    pub fn with_max_steps(mut self, max_steps: u64) -> Self {
        self.max_steps = max_steps;
        self
    }
}

/// Outcome of one lab run.
#[derive(Debug)]
#[non_exhaustive]
pub struct LabReport<T> {
    pub seed: u64,
    /// The root future's output, when it finished.
    pub output: Option<T>,
    /// No task was ready and no timer was pending while the root was unfinished.
    pub deadlocked: bool,
    /// The run stopped at `max_steps`.
    pub step_limited: bool,
    /// Task polls performed.
    pub steps: u64,
    /// Virtual time when the run ended.
    pub virtual_time: Duration,
    /// Tasks still alive when the run ended (destroyed by the lab afterwards).
    pub tasks_left: usize,
    /// Obligations created during the run and still unresolved at its end.
    pub obligations_pending: u64,
    /// Obligations dropped unresolved during the run.
    pub obligations_leaked: u64,
    /// Task ids in poll order; identical for identical seeds.
    pub trace: Vec<u64>,
}

impl<T> LabReport<T> {
    /// The root finished, nothing deadlocked, nothing was left behind and no
    /// obligation was forgotten.
    pub fn is_clean(&self) -> bool {
        self.output.is_some()
            && !self.deadlocked
            && !self.step_limited
            && self.tasks_left == 0
            && self.obligations_pending == 0
            && self.obligations_leaked == 0
    }
}

/// Deterministic single-thread executor. See the module documentation.
pub struct Lab;

impl Lab {
    /// Whether a lab is driving the current thread.
    pub fn is_running() -> bool {
        is_active()
    }

    /// Run `root` to completion (or deadlock, or the step limit) under `config`.
    pub fn run<F, T>(config: LabConfig, root: F) -> LabReport<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        assert!(!is_active(), "labs do not nest");
        let clock = TestClock::new();
        let inner = Rc::new(LabInner {
            tasks: RefCell::new(BTreeMap::new()),
            ready: Arc::new(Mutex::new(BTreeSet::new())),
            next_id: Cell::new(0),
            rng: Cell::new(config.seed ^ 0x9E37_79B9_7F4A_7C15),
        });
        CURRENT.with(|current| *current.borrow_mut() = Some(Rc::downgrade(&inner)));
        let ledger = obligation_stats();

        let output = Rc::new(RefCell::new(None));
        let sink = output.clone();
        let spawner = LabSpawner {
            inner: inner.clone(),
        };
        let root_task = spawner.spawn(Box::pin(with_clock(clock.shared(), async move {
            *sink.borrow_mut() = Some(root.await);
        })));

        let mut trace = Vec::new();
        let mut steps = 0;
        let (mut deadlocked, mut step_limited) = (false, false);
        while output.borrow().is_none() {
            if steps >= config.max_steps {
                step_limited = true;
                break;
            }
            let Some(id) = inner.pick() else {
                match clock.next_deadline() {
                    Some(deadline) => {
                        clock.set(deadline);
                        continue;
                    }
                    None => {
                        deadlocked = true;
                        break;
                    }
                }
            };
            trace.push(id);
            steps += 1;
            inner.poll(id);
        }

        // Oracles are read before teardown: destroying the tasks left behind
        // would otherwise turn their pending obligations into leaks.
        let at_end = obligation_stats();
        drop(root_task);
        // Tasks left behind are destroyed now, while the lab is still current.
        let tasks_left = inner.tasks.borrow().len();
        let leftovers = std::mem::take(&mut *inner.tasks.borrow_mut());
        drop(leftovers);
        CURRENT.with(|current| *current.borrow_mut() = None);

        let output = output.borrow_mut().take();
        LabReport {
            seed: config.seed,
            output,
            deadlocked,
            step_limited,
            steps,
            virtual_time: clock.now(),
            tasks_left,
            obligations_pending: at_end.pending.saturating_sub(ledger.pending),
            obligations_leaked: at_end.leaked - ledger.leaked,
            trace,
        }
    }

    /// Run `make()` under each seed in `seeds`; return the reports that fail
    /// `check`. Use it to sweep interleavings, then replay a failing seed with
    /// [`Lab::run`].
    pub fn explore<F, Fut, T>(
        seeds: std::ops::Range<u64>,
        make: F,
        check: impl Fn(&LabReport<T>) -> bool,
    ) -> Vec<LabReport<T>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = T> + 'static,
        T: 'static,
    {
        seeds
            .map(|seed| Lab::run(LabConfig::new(seed), make()))
            .filter(|report| !check(report))
            .collect()
    }
}

enum Slot {
    Idle(LocalBoxFuture<'static, ()>),
    Running { cancelled: bool },
}

struct LabInner {
    tasks: RefCell<BTreeMap<u64, Slot>>,
    /// Shared with wakers, which must be `Send + Sync`. Single-threaded use.
    ready: Arc<Mutex<BTreeSet<u64>>>,
    next_id: Cell<u64>,
    rng: Cell<u64>,
}

impl LabInner {
    /// SplitMix64: small, fast, and fully determined by the seed.
    fn next_random(&self) -> u64 {
        let mut z = self.rng.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        self.rng.set(z);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn pick(&self) -> Option<u64> {
        let mut ready = self.ready.lock().expect("lab ready set");
        // Tasks woken after they were removed are ignored.
        let tasks = self.tasks.borrow();
        ready.retain(|id| tasks.contains_key(id));
        drop(tasks);
        if ready.is_empty() {
            return None;
        }
        let index = (self.next_random() % ready.len() as u64) as usize;
        let id = *ready.iter().nth(index).expect("index in range");
        ready.remove(&id);
        Some(id)
    }

    fn poll(&self, id: u64) {
        let taken = {
            let mut tasks = self.tasks.borrow_mut();
            match tasks.get_mut(&id) {
                Some(slot @ Slot::Idle(_)) => {
                    match std::mem::replace(slot, Slot::Running { cancelled: false }) {
                        Slot::Idle(future) => Some(future),
                        Slot::Running { .. } => unreachable!(),
                    }
                }
                _ => None,
            }
        };
        let Some(mut future) = taken else { return };
        let waker = Waker::from(Arc::new(LabWaker {
            id,
            ready: self.ready.clone(),
        }));
        let finished = future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready();
        let mut tasks = self.tasks.borrow_mut();
        let cancelled = matches!(tasks.get(&id), Some(Slot::Running { cancelled: true }));
        if finished || cancelled {
            tasks.remove(&id);
            drop(tasks);
            drop(future); // destructors run outside the borrow
        } else {
            tasks.insert(id, Slot::Idle(future));
        }
    }
}

struct LabWaker {
    id: u64,
    ready: Arc<Mutex<BTreeSet<u64>>>,
}

impl Wake for LabWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.lock().expect("lab ready set").insert(self.id);
    }
}

/// Spawns nursery tasks onto the current lab.
#[derive(Clone)]
pub(crate) struct LabSpawner {
    inner: Rc<LabInner>,
}

impl LabSpawner {
    pub(crate) fn spawn(&self, future: LocalBoxFuture<'static, ()>) -> LabTask {
        let id = self.inner.next_id.get();
        self.inner.next_id.set(id + 1);
        self.inner.tasks.borrow_mut().insert(id, Slot::Idle(future));
        self.inner.ready.lock().expect("lab ready set").insert(id);
        LabTask {
            id,
            lab: Rc::downgrade(&self.inner),
        }
    }
}

/// Handle to a lab task. Like a Glommio `Task`, dropping it destroys the task.
pub(crate) struct LabTask {
    id: u64,
    lab: Weak<LabInner>,
}

impl Drop for LabTask {
    fn drop(&mut self) {
        let Some(lab) = self.lab.upgrade() else {
            return;
        };
        let removed = {
            let mut tasks = lab.tasks.borrow_mut();
            match tasks.get_mut(&self.id) {
                Some(Slot::Running { cancelled }) => {
                    // Dropped from inside its own poll: finish the poll first.
                    *cancelled = true;
                    None
                }
                Some(Slot::Idle(_)) => tasks.remove(&self.id),
                None => None,
            }
        };
        drop(removed);
    }
}

/// A finished-or-destroyed lab task, awaited when a nursery joins handles.
impl Future for LabTask {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        // Nurseries join handles only after every child reported completion.
        Poll::Ready(())
    }
}
