# Should We Keep spawn_local()?

## Current API Surface

Glommio currently has **three** ways to spawn tasks:

### 1. Free Function: `glommio::spawn_local()`
```rust
pub fn spawn_local<T>(future: impl Future<Output = T> + 'static) -> Task<T> {
    executor().spawn_local(future)
}
```
- Uses thread-local `LOCAL_EX`
- Panics if not called from within executor context
- Convenience for async code

### 2. Method: `LocalExecutor::spawn_local()`
```rust
impl LocalExecutor {
    pub fn spawn_local<T>(&self, future: impl Future<Output = T> + 'static) -> Task<T> {
        LOCAL_EX.with(|local_ex| Task(local_ex.spawn(future)))
    }
}
```
- Takes `&self` but **ignores it** (!!)
- Uses thread-local `LOCAL_EX`
- Panics if not called from within executor context
- **Redundant with free function**

### 3. Private Method: `LocalExecutor::spawn()`
```rust
impl LocalExecutor {
    fn spawn<T>(&self, future: impl Future<Output = T>) -> multitask::Task<T> {
        // Actually uses self!
    }
}
```
- Uses `self` correctly
- Never panics
- **Should be public**

## Analysis: Do We Need All Three?

### Use Case 1: Spawning from Inside Async Code

**Scenario:** You're inside an async task and want to spawn another task

**Current (with spawn_local):**
```rust
executor.run(async {
    // Convenient: no need to capture executor
    let task = glommio::spawn_local(async { 42 });
    task.await
});
```

**Alternative (with public spawn):**
```rust
// Need to capture executor somehow
executor.run(async {
    // How do we get executor reference here?
    let task = executor.spawn(async { 42 });  // `executor` not in scope!
    task.await
});
```

**Verdict:** ✅ **Free function `spawn_local()` is useful** - provides convenience without needing to capture executor

### Use Case 2: Spawning Before run()

**Current (spawn_local panics):**
```rust
let executor = LocalExecutor::default();

// ❌ PANICS - LOCAL_EX not set!
let task = executor.spawn_local(async { 42 });

executor.run(async move {
    task.await
});
```

**With public spawn():**
```rust
let executor = LocalExecutor::default();

// ✅ Works!
let task = executor.spawn(async { 42 });

executor.run(async move {
    task.await
});
```

**Verdict:** ✅ **Method `spawn()` solves this use case**, method `spawn_local()` is redundant

### Use Case 3: Spawning with Executor Instance

**Current (confusing):**
```rust
impl MyApp {
    executor: LocalExecutor,

    fn do_work(&self) {
        self.executor.run(async {
            // Have to use free function, can't use instance!
            let task = glommio::spawn_local(async { 42 });
        });
    }
}
```

**With public spawn():**
```rust
impl MyApp {
    executor: LocalExecutor,

    fn do_work(&self) {
        // Can use instance directly!
        let task = self.executor.spawn(async { 42 });

        self.executor.run(async move {
            task.await
        });
    }
}
```

**Verdict:** ✅ **Method `spawn()` is clearer** - uses the instance you have

## Proposed API: Keep Free Function, Add spawn()

### Keep: Free Function `glommio::spawn_local()`

**Reason:** Convenience for spawning from within async code

```rust
executor.run(async {
    // Convenient - no need to capture executor
    glommio::spawn_local(async { 42 }).await
});
```

### Add: Public Method `LocalExecutor::spawn()`

**Reason:** Use executor instance when you have it

```rust
let executor = LocalExecutor::default();

// Spawn before run() - useful for setup
let task = executor.spawn(async { 42 });

executor.run(async move {
    task.await
});
```

### Deprecate? Method `LocalExecutor::spawn_local()`

**Reason:** Redundant with free function, confusing API

```rust
// Current: method ignores self!
executor.spawn_local(async { 42 });  // Why take &self if not using it?

// Better alternatives:
glommio::spawn_local(async { 42 });  // Free function (convenience)
executor.spawn(async { 42 });         // Method using self (direct)
```

**Decision:** Could be deprecated, but maybe keep for backward compatibility?

## Comparison to Other Runtimes

### Tokio's Approach

```rust
// Free function (on default runtime)
tokio::spawn(async { 42 });

// Or with handle
let handle = runtime.handle();
handle.spawn(async { 42 });
```

**Note:** Tokio has TWO approaches, just like we're proposing!

### async-std's Approach

```rust
// Only has free function
task::spawn(async { 42 });
```

### Proposed Glommio Approach

```rust
// Free function (convenience inside async code)
glommio::spawn_local(async { 42 });

// Method on instance (when you have executor)
executor.spawn(async { 42 });
```

**This matches Tokio's pattern!**

## Why the Free Function is Still Useful

Even with public `spawn()`, the free function serves a purpose:

### Challenge: Capturing Executor in Async Code

When you're inside async code, how do you get a reference to the executor?

**Option 1: Capture in closure (verbose)**
```rust
let executor = LocalExecutor::default();

executor.run(async move {
    let executor_ref = ???;  // How do we get this?
    executor_ref.spawn(async { 42 });
});
```

**Problem:** You can't easily get `&executor` from inside the async code!

**Option 2: Use thread-local (current approach)**
```rust
executor.run(async {
    // Free function uses thread-local
    glommio::spawn_local(async { 42 });
});
```

**This works!** The thread-local `LOCAL_EX` is available during `.run()`

### The Free Function Provides Ergonomics

```rust
// ❌ Awkward: how to capture executor?
executor.run(async {
    let e = ???;
    e.spawn(async { 42 });
});

// ✅ Clean: free function uses thread-local
executor.run(async {
    glommio::spawn_local(async { 42 });
});
```

## Recommendation: Keep Both Patterns

### Keep: Free Function `glommio::spawn_local()`
- **Use case:** Spawning from inside async code
- **Benefit:** Convenience, no need to capture executor
- **Trade-off:** Can panic if called outside executor context

### Add: Public Method `LocalExecutor::spawn()`
- **Use case:** Spawning when you have executor instance
- **Benefit:** Never panics, uses instance directly
- **Trade-off:** Need to have executor reference

### Consider Deprecating: Method `LocalExecutor::spawn_local()`
- **Current:** Takes `&self` but ignores it
- **Problem:** Confusing API, redundant with free function
- **Options:**
  1. Deprecate and recommend alternatives
  2. Keep for backward compatibility
  3. Change implementation to use `self.spawn()` instead of LOCAL_EX

## The Elegant Solution

If we make `spawn()` public, we could **simplify** `spawn_local()`:

### Current Implementation (confusing):
```rust
impl LocalExecutor {
    pub fn spawn_local<T>(&self, future: ...) -> Task<T> {
        // Ignores self!
        LOCAL_EX.with(|local_ex| Task(local_ex.spawn(future)))
    }
}
```

### Simplified Implementation (uses self):
```rust
impl LocalExecutor {
    pub fn spawn_local<T>(&self, future: ...) -> Task<T> {
        // Just delegates to spawn()!
        Task(self.spawn(future))
    }
}
```

**Benefits:**
- ✅ No more thread-local access in method
- ✅ Method actually uses `self`
- ✅ Backward compatible (same signature)
- ✅ Never panics when you have instance

**This might be the best solution!** Keep spawn_local() for compatibility, but make it use spawn() internally.

## Final Recommendation

1. **Make `spawn()` public** - solves the core issue
2. **Keep free function `spawn_local()`** - needed for convenience
3. **Update method `spawn_local()`** - make it delegate to `spawn()` instead of using LOCAL_EX

Result:
- Both patterns work (free function + method)
- No confusing "method ignores self" behavior
- Backward compatible
- Never panics when you have executor instance

This gives users the flexibility to choose the right tool:
- Use free function when inside async code (convenient)
- Use method when you have executor instance (explicit)
