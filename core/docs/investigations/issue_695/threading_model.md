# Deep Dive: Why spawn() is Private

## Understanding Glommio's Threading Model

### Thread-Per-Core Architecture

Glommio is designed for **thread-per-core** applications where:
- Each `LocalExecutor` is bound to a specific CPU core
- Tasks MUST run on the thread that owns the executor
- Cross-thread task execution would violate io_uring guarantees

### The LOCAL_EX Thread-Local Variable

```rust
#[cfg(not(feature = "native-tls"))]
scoped_tls::scoped_thread_local!(static LOCAL_EX: LocalExecutor);

#[cfg(feature = "native-tls")]
#[thread_local]
static mut LOCAL_EX: *const LocalExecutor = std::ptr::null();
```

**Key Insight:** `LOCAL_EX` is ONLY set during `executor.run()`:

```rust
impl LocalExecutor {
    pub fn run<T>(&self, future: impl Future<Output = T>) -> T {
        #[cfg(not(feature = "native-tls"))]
        {
            assert!(
                !LOCAL_EX.is_set(),
                "There is already an LocalExecutor running on this thread"
            );
            LOCAL_EX.set(self, || run(self))  // ← Sets LOCAL_EX here!
        }
        // After run() returns, LOCAL_EX is unset
    }
}
```

## The Lifecycle Problem

### Scenario 1: Using spawn_local() Inside async Code (Works)

```rust
let executor = LocalExecutor::default();

executor.run(async {
    // ✅ LOCAL_EX is set here!
    let task = glommio::spawn_local(async { 42 });
    task.await
});
// LOCAL_EX is unset here
```

**Why it works:** During `.run()`, LOCAL_EX points to the executor, so `spawn_local()` can access it.

### Scenario 2: Using spawn_local() Before run() (Panics)

```rust
let executor = LocalExecutor::default();

// ❌ LOCAL_EX is NOT set yet!
let task = executor.spawn_local(async { 42 });  // PANIC!

executor.run(async move {
    task.await
});
```

**Why it panics:**
- `executor.spawn_local()` tries to access LOCAL_EX
- But LOCAL_EX isn't set until `.run()` is called
- Even though we have `&self`, the method ignores it!

Look at the implementation:
```rust
impl LocalExecutor {
    pub fn spawn_local<T>(&self, future: ...) -> Task<T> {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| Task(local_ex.spawn(future)));
        //     ^^^^^^^^ Uses thread-local, not self!
    }
}
```

## Why spawn() Was Private: The Original Design Intent

### Intent 1: Prevent Wrong-Thread Spawning

The original design wanted to prevent this:

```rust
// Thread 1: Create executor
let executor = LocalExecutor::default();

// Thread 2: Try to spawn on it (BAD!)
std::thread::spawn(move || {
    let task = executor.spawn(async { 42 });  // Should not be allowed!
});
```

**However:** `LocalExecutor` is already `!Send`, so this won't compile anyway!

```rust
impl !Send for LocalExecutor {}  // Cannot be sent between threads
```

The type system already prevents this error at compile time.

### Intent 2: Force Thread-Local Check

By making `spawn()` private and only exposing `spawn_local()` (which checks LOCAL_EX), the API forces a runtime check that you're in the right context.

**The problem:** This check is too strict! It prevents legitimate use cases:

```rust
// ❌ This SHOULD work but panics!
let executor = LocalExecutor::default();
let task = executor.spawn(async { 42 });  // Have instance, same thread
executor.run(async move { task.await });
```

## What Could Go Wrong If spawn() Was Public?

Let's analyze the risks:

### Risk 1: Spawning from Wrong Thread ❌ Already Prevented

```rust
let executor = LocalExecutor::default();

std::thread::spawn(move || {
    executor.spawn(async { 42 });  // Won't compile!
});
```

**Verdict:** Compile error. `LocalExecutor: !Send` prevents this.

### Risk 2: Spawning Before Executor Starts ✅ Actually Fine!

```rust
let executor = LocalExecutor::default();

// Spawn before run() - is this safe?
let task = executor.spawn(async { 42 });

executor.run(async move {
    task.await  // Works fine!
});
```

Looking at the `spawn()` implementation:

```rust
fn spawn<T>(&self, future: impl Future<Output = T>) -> multitask::Task<T> {
    let tq = self
        .queues          // ← Uses self.queues
        .borrow()
        .active_executing
        .clone()
        .or_else(|| self.get_queue(&TaskQueueHandle { index: 0 }))  // ← Fallback to queue 0
        .unwrap();

    let id = self.id;    // ← Uses self.id
    let ex = tq.borrow().ex.clone();
    ex.spawn_and_run(id, tq, future)  // ← Schedules the task
}
```

**Verdict:** Safe! The task is created and scheduled on the correct task queue. It will run when `.run()` is called.

### Risk 3: Spawning After Executor Stops ✅ Also Fine (Task Never Runs)

```rust
let executor = LocalExecutor::default();

executor.run(async {
    // do some work
});

// Executor has finished
let task = executor.spawn(async { 42 });  // Creates task

// task will never run because executor stopped
// But this is fine - task just stays pending
```

**Verdict:** Safe. Task is created but never scheduled. No memory unsafety.

## The Real Reason: Historical Accident?

After analyzing the code, making `spawn()` private seems like **defensive programming that's too defensive**:

1. ✅ Thread-safety already enforced by `!Send`
2. ✅ Spawning before `.run()` is safe
3. ✅ Spawning after `.run()` is safe (task just doesn't run)
4. ❌ But forces confusing API where method ignores `self`

### Comparison to Other Async Runtimes

**Tokio:**
```rust
let runtime = Runtime::new()?;
let handle = runtime.handle();

// Can spawn from handle without runtime check!
handle.spawn(async { 42 });
```

**async-std:**
```rust
// Global spawn, no executor instance needed
task::spawn(async { 42 });
```

**Glommio (current):**
```rust
let executor = LocalExecutor::default();

// ❌ Can't use the instance to spawn!
// Must use free function which checks thread-local
glommio::spawn_local(async { 42 });  // Can panic even with instance!
```

## Recommendation: Make spawn() Public

The benefits outweigh the (non-existent) risks:

### Benefits
1. **More intuitive API** - methods use the instance they're called on
2. **No unexpected panics** - if you have the instance, you can use it
3. **Same thread-safety** - `!Send` still prevents cross-thread usage
4. **Backward compatible** - doesn't change existing spawn_local() behavior

### Updated Investigation Conclusion

The original design made `spawn()` private to force thread-local checking, likely out of an abundance of caution for thread-per-core safety.

However:
- The type system (`!Send`) already prevents wrong-thread usage
- The private `spawn()` is perfectly safe to make public
- The current API is confusing (methods that ignore `self`)

**Making `spawn()` public is safe, intuitive, and solves real user pain points.**
