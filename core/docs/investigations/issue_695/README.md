# Investigation: Issue #695 - Non-Panicking spawn_local()

## Problem Summary

The current `spawn_local()` API has a confusing design that can panic even when called on a `LocalExecutor` instance:

```rust
let executor = LocalExecutor::default();
executor.run(async {
    // This can still panic! Even though we have the executor instance!
    let task = glommio::spawn_local(async { 42 });
});
```

**Current Behavior:**
- Free function `glommio::spawn_local()` panics if not in executor context
- Method `LocalExecutor::spawn_local()` **also** panics if not in executor context
- Even though you have a `LocalExecutor` instance, the method ignores `self` and uses thread-local storage!

## Root Cause Analysis

### Current API Structure

There are THREE spawn-related methods, creating confusion:

#### 1. Free Function: `glommio::spawn_local<T>()` (line 1958)
```rust
pub fn spawn_local<T>(future: impl Future<Output = T> + 'static) -> Task<T>
where
    T: 'static,
{
    executor().spawn_local(future)  // Delegates to ExecutorProxy
}
```
- **Purpose:** Convenience function for spawning from within async context
- **Panics:** If called outside an executor context

#### 2. Method: `LocalExecutor::spawn_local<T>()` (line 2632)
```rust
impl LocalExecutor {
    pub fn spawn_local<T>(&self, future: impl Future<Output = T> + 'static) -> Task<T>
    where
        T: 'static,
    {
        #[cfg(not(feature = "native-tls"))]
        return LOCAL_EX.with(|local_ex| Task::<T>(local_ex.spawn(future)));

        #[cfg(feature = "native-tls")]
        return Task::<T>(unsafe {
            LOCAL_EX
                .as_ref()
                .expect("this thread doesn't have a LocalExecutor running")
                .spawn(future)
        });
    }
}
```
- **Problem:** **IGNORES `self`** and uses thread-local `LOCAL_EX` instead!
- **Panics:** Even though you have a `LocalExecutor` instance, this panics if called from wrong thread
- **Confusing:** Why does a method that takes `&self` not use it?

#### 3. Private Method: `LocalExecutor::spawn<T>()` (line 1312)
```rust
impl LocalExecutor {
    fn spawn<T>(&self, future: impl Future<Output = T>) -> multitask::Task<T> {
        let tq = self
            .queues
            .borrow()
            .active_executing
            .clone()
            .or_else(|| self.get_queue(&TaskQueueHandle { index: 0 }))
            .unwrap();

        let id = self.id;
        let ex = tq.borrow().ex.clone();
        ex.spawn_and_run(id, tq, future)
    }
}
```
- **Actually uses `self`!** This is the method that should be public
- **Never panics** (modulo logic bugs) because it uses the executor instance
- **Currently private** - why?

### Why This Design Exists

Looking at the code history, this appears to be for thread-safety:

1. Glommio is thread-per-core - each executor is bound to a specific thread
2. The thread-local `LOCAL_EX` ensures you're spawning on the correct executor
3. The public API prevents accidentally spawning from the wrong thread

**However,** this makes the API confusing when you legitimately have the executor instance!

## Proposed Solutions

### Option A: Make `spawn()` Public (Simplest)

Make the private `spawn()` method public:

```rust
impl LocalExecutor {
    // Change from: fn spawn<T>
    pub fn spawn<T>(&self, future: impl Future<Output = T>) -> multitask::Task<T> {
        // ... existing implementation ...
    }
}
```

**Pros:**
- Simple one-line change
- No API breakage (adds new method, doesn't change existing)
- Allows panic-free spawning when you have the executor instance
- Actually uses `self` (less confusing)

**Cons:**
- Allows spawning from wrong thread if you pass executor between threads
- But `LocalExecutor` is `!Send` so this should be caught at compile time
- Need to wrap return type in `Task<T>` instead of `multitask::Task<T>` for consistency

**Difficulty:** ⭐ (Trivial)

### Option B: Add `try_spawn_local()` to Free Function

Add a new free function that returns `Result`:

```rust
pub fn try_spawn_local<T>(future: impl Future<Output = T> + 'static) -> Result<Task<T>>
where
    T: 'static,
{
    #[cfg(not(feature = "native-tls"))]
    {
        if LOCAL_EX.is_set() {
            Ok(LOCAL_EX.with(|local_ex| Task::<T>(local_ex.spawn(future))))
        } else {
            Err(GlommioError::executor_not_running())
        }
    }

    #[cfg(feature = "native-tls")]
    unsafe {
        LOCAL_EX
            .as_ref()
            .map(|ex| Task::<T>(ex.spawn(future)))
            .ok_or_else(|| GlommioError::executor_not_running())
    }
}
```

**Pros:**
- Follows Rust conventions (try_* for fallible operations)
- Safe even from wrong thread
- Good for libraries that want to handle the error

**Cons:**
- Doesn't solve the core issue (still uses thread-local, not `self`)
- More code to maintain
- Still can't use when you have executor instance but not in its context

**Difficulty:** ⭐⭐ (Easy)

### Option C: Fix `spawn_local()` to Use `self`

Change the public `spawn_local()` to actually use `self`:

```rust
impl LocalExecutor {
    pub fn spawn_local<T>(&self, future: impl Future<Output = T> + 'static) -> Task<T>
    where
        T: 'static,
    {
        Task::<T>(self.spawn(future))  // Use self, not LOCAL_EX!
    }
}
```

**Pros:**
- Fixes the confusing API design
- More intuitive - method uses the instance it's called on
- Can be called safely from anywhere with the instance

**Cons:**
- **BREAKING CHANGE** - behavior changes for existing code
- Code relying on thread-safety check will break
- May cause subtle bugs if executor passed between threads (though compile-time `!Send` should catch this)

**Difficulty:** ⭐⭐⭐ (Medium - breaking change)

## Recommendation

**Implement Option A: Make `spawn()` public as `spawn()`**

**Why:**
1. **Solves the user's problem** - they can spawn without panic when they have the instance
2. **No breaking changes** - adds new API, doesn't modify existing
3. **Trivial implementation** - just change `fn` to `pub fn` and wrap return type
4. **Consistent with Rust patterns** - if you have the instance, you can use it
5. **Thread-safety still enforced** - `LocalExecutor` is `!Send`, can't be passed between threads

**Implementation:**

```rust
impl LocalExecutor {
    /// Spawns a task directly on this executor.
    ///
    /// Unlike `spawn_local()`, this method uses the executor instance directly
    /// and never panics. However, it can only be called from the thread that
    /// owns this executor (enforced by `!Send`).
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::LocalExecutor;
    ///
    /// let executor = LocalExecutor::default();
    /// let task = executor.spawn(async { 42 });
    /// let result = executor.run(async move { task.await });
    /// assert_eq!(result, 42);
    /// ```
    pub fn spawn<T>(&self, future: impl Future<Output = T>) -> Task<T> {
        let tq = self
            .queues
            .borrow()
            .active_executing
            .clone()
            .or_else(|| self.get_queue(&TaskQueueHandle { index: 0 }))
            .unwrap();

        let id = self.id;
        let ex = tq.borrow().ex.clone();
        Task(ex.spawn_and_run(id, tq, future))
    }
}
```

**Additional Consideration:**
- The return type changes from `multitask::Task<T>` to `Task<T>` (our wrapper type)
- Need to wrap the result in `Task(...)`
- This is consistent with other public APIs

## Testing Strategy

Create test that demonstrates:
1. Spawning works even without executor context (just with instance)
2. Can spawn before calling `.run()`
3. Thread-safety still enforced by `!Send`

```rust
#[test]
fn test_spawn_with_instance() {
    let executor = LocalExecutor::default();

    // Can spawn before run() - this would panic with spawn_local()
    let task = executor.spawn(async { 42 });

    // Run and get result
    let result = executor.run(async move { task.await });
    assert_eq!(result, 42);
}
```

## Implementation Plan

1. Change `fn spawn` to `pub fn spawn` in executor/mod.rs:1312
2. Wrap return type in `Task(...)` for API consistency
3. Add documentation explaining the difference from `spawn_local()`
4. Add test case demonstrating usage
5. Update examples if needed

## Related Issues

- Original issue: https://github.com/DataDog/glommio/issues/695
- This is a quality-of-life improvement, not a bug fix
- No security implications (thread-safety still enforced by `!Send`)
