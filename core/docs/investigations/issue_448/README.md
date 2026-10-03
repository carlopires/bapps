# Investigation: Issue #448 - Eventfd Leak on Executor Drop

> **Status: fixed (`f28a619`).** The task header now stores `executor_id: usize`
> instead of an `Arc<SleepNotifier>`, and the notifier is looked up only on the
> foreign-wake path. Tasks therefore no longer hold a reference to the eventfd,
> so the leak described below cannot occur even when a task's destructor never
> runs. The same change removed a process-wide `RwLock` read from every spawn.
>
> The analysis is kept because the root cause — tasks that are destroyed
> without their destructors running — is still true of glommio and still
> constrains anything that puts an owning reference in a task header.
>
> Regression test: `glommio/tests/eventfd_leak.rs` (counts FDs process-wide,
> so run it single-threaded).

## Problem Summary

When `LocalExecutor` instances are created and destroyed repeatedly, eventfd file descriptors leak. With 2 executors per test iteration, 12 eventfds remain open after the executors finish.

**Impact:**
- Long-running applications that create/destroy executors will exhaust file descriptors
- Memory leak: ~36.8GB VSZ after 2723 iterations (test crashes with ENOMEM)
- Affects applications using `shared_channel` with short-lived executors

## Root Cause Analysis

### Architecture

1. **SleepNotifier Structure** (`glommio/src/sys/mod.rs:259`)
   ```rust
   pub(crate) struct SleepNotifier {
       id: usize,
       eventfd: std::fs::File,  // ← The leaked resource
       should_notify: AtomicBool,
       foreign_wakes: crossbeam::channel::Receiver<Waker>,
       waker_sender: crossbeam::channel::Sender<Waker>,
   }
   ```

2. **Task Header** (`glommio/src/task/header.rs:25`)
   ```rust
   pub(crate) struct Header {
       pub(crate) notifier: Arc<SleepNotifier>,  // ← Strong reference
       // ... other fields
   }
   ```

3. **Lifecycle Issue**
   - Each executor creates a `SleepNotifier` with an eventfd
   - Tasks hold `Arc<SleepNotifier>` references
   - When executor drops, **non-runnable tasks don't have destructors called**
   - Arc refcount stays positive → SleepNotifier::drop never runs
   - eventfd file descriptor leaks

### Quote from Original Maintainer (Glauber Costa)

> "We keep a clone of the sleep notifier inside task, and there is a problem that we are aware of for a long time now, but has been a minor bother: tasks that are not runnable do not have their destructors run when the executor drops. So that reference count never drops."

> "As a status update, I spent some time trying to fix this, but it is really hard because tasks often get destroyed under our nose. This brought me back to the refcount hell in the task structures. I'll keep looking at it."

## Attempted Solutions

Glauber attempted to fix this but found it "really hard" due to:
1. **Task destruction timing**: Tasks can be destroyed unpredictably
2. **Reference counting complexity**: "Refcount hell in task structures"
3. **State management**: Non-runnable vs runnable task states

## Workarounds

### 1. Use Long-Lived Executors (Recommended)

Instead of creating executors per operation:

```rust
// ❌ BAD: Creates executor per file
for file in files {
    let executor = LocalExecutor::default();
    executor.run(async {
        let file = DmaFile::open(file_path).await?;
        // ...
    });
} // ← Leaks eventfds here!

// ✅ GOOD: Reuse executor
let executor = LocalExecutor::default();
for file in files {
    executor.run(async {
        let file = DmaFile::open(file_path).await?;
        // ...
    });
}
```

### 2. Thread-Local Executor (For Tests)

From @vlovich's workaround:

```rust
thread_local! {
    static EXECUTOR: OnceCell<LocalExecutor> = OnceCell::new();
}

pub fn run_async<T>(future: impl Future<Output = T>) -> T {
    EXECUTOR.with(|cell| {
        let local_ex = cell.get_or_init(|| {
            LocalExecutorBuilder::new(Placement::Fixed(0))
                .make()
                .unwrap()
        });
        local_ex.run(async move { future.await })
    })
}
```

## Proposed Fix Approaches

### Option A: Explicit Task Cleanup on Executor Drop

**Approach:** Walk through all task queues and explicitly drop tasks when executor drops.

**Pros:**
- Comprehensive solution
- Ensures all resources are cleaned up
- Fixes the general "tasks not cleaned up" problem

**Cons:**
- Very complex (Glauber tried and failed)
- Need to handle running/scheduled/blocked tasks carefully
- Risk of introducing new bugs (panics, deadlocks)

**Complexity:** ⭐⭐⭐⭐⭐ (Very Hard)

### Option B: Explicit Eventfd Cleanup

**Approach:** Make eventfd explicitly closeable even while Arc references exist.

```rust
pub(crate) struct SleepNotifier {
    id: usize,
    eventfd: Mutex<Option<std::fs::File>>,  // ← Wrapped in Mutex<Option>
    // ...
}

impl SleepNotifier {
    pub(crate) fn close_eventfd(&self) {
        if let Some(fd) = self.eventfd.lock().unwrap().take() {
            drop(fd); // Explicitly close
        }
    }
}
```

Call from executor drop:
```rust
impl Drop for LocalExecutor {
    fn drop(&mut self) {
        self.notifier.close_eventfd();
    }
}
```

**Pros:**
- Targeted fix for the specific leak
- Simpler than full task cleanup
- eventfd closes immediately on executor drop

**Cons:**
- Doesn't solve general task cleanup issue
- Need to handle closed eventfd in notify() calls
- May cause issues if tasks try to use closed eventfd
- Introduces Mutex overhead on hot path

**Complexity:** ⭐⭐⭐ (Medium-Hard)

### Option C: Weak Task References

**Approach:** Tasks hold `Weak<SleepNotifier>` instead of `Arc`.

**Pros:**
- Elegant - prevents leak at the source
- No explicit cleanup needed

**Cons:**
- Need to upgrade Weak to Arc on every task operation
- Performance overhead
- Need to handle case where notifier is gone
- Extensive changes to task code

**Complexity:** ⭐⭐⭐⭐ (Hard)

## Recommendation

For now, **document the workarounds** and defer the fix until someone has time for deep architectural work.

**Why:**
1. Original maintainer tried and couldn't fix it easily
2. Complex fix has high risk of introducing bugs
3. Workarounds are effective for most use cases
4. The issue primarily affects test code and unusual patterns

**For production:**
- Use long-lived executors (as recommended by Glauber)
- This is more efficient anyway (executor creation is expensive)

**For tests:**
- Use thread-local executor pattern (vlovich's workaround)
- Or increase fd limits: `ulimit -n 65536`

## Testing

Reproduction test provided in `reproduce_issue_448.rs`:
- Creates/destroys executors repeatedly
- Monitors FD count growth
- Confirms leak exists

## References

- **Original Issue:** https://github.com/DataDog/glommio/issues/448
- **Related Code:**
  - `glommio/src/sys/mod.rs:259` - SleepNotifier struct
  - `glommio/src/task/header.rs:25` - Task Header with Arc<SleepNotifier>
  - `glommio/src/executor/mod.rs` - LocalExecutor implementation
