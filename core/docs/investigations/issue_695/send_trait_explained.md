# How !Send is Enforced in Rust's Type System

## The Send Trait

`Send` is an **auto trait** (also called a marker trait) in Rust:

```rust
pub unsafe auto trait Send { }
```

### What Does Send Mean?

A type is `Send` if it's **safe to transfer ownership between threads**.

```rust
// T: Send means:
std::thread::spawn(move || {
    let value: T = ...;  // OK to move T into this thread
});
```

### Auto Trait = Automatic Implementation

Most types automatically implement `Send`:

```rust
struct MyStruct {
    x: i32,
    y: String,
}
// MyStruct is automatically Send because:
// - i32 is Send
// - String is Send
// Therefore MyStruct is Send!
```

The compiler **automatically derives** `Send` if all fields are `Send`.

## How !Send Works

### Negative Impl (Old Way)

**You can't do this in stable Rust:**
```rust
impl !Send for LocalExecutor { }  // ❌ Doesn't compile!
```

Negative trait impls are **unstable** and only available on nightly.

### Type Composition (The Real Way)

Instead, Rust uses **type composition** to make types `!Send`:

**If ANY field is `!Send`, the whole struct becomes `!Send` automatically!**

```rust
struct LocalExecutor {
    queues: Rc<RefCell<ExecutorQueues>>,  // ← Rc is !Send!
    reactor: Rc<reactor::Reactor>,         // ← Rc is !Send!
    stall_detector: RefCell<...>,          // ← RefCell is !Send!
    // ...
}

// LocalExecutor is automatically !Send because its fields are !Send
```

## LocalExecutor's !Send Fields

Let's look at the actual struct from `glommio/src/executor/mod.rs:1143`:

```rust
pub struct LocalExecutor {
    queues: Rc<RefCell<ExecutorQueues>>,      // !Send
    parker: parking::Parker,                    // Implementation detail
    id: usize,                                  // Send
    reactor: Rc<reactor::Reactor>,              // !Send
    stall_detector: RefCell<Option<StallDetector>>,  // !Send
}
```

### Why Rc is !Send

`Rc<T>` (Reference Counted) is **not thread-safe**:

```rust
pub struct Rc<T> {
    // Non-atomic reference count
    ptr: NonNull<RcBox<T>>,
    phantom: PhantomData<RcBox<T>>,
}

// Rc does NOT implement Send!
// Using Rc from multiple threads would cause data races
```

**Why?** The reference count is **not atomic**, so incrementing/decrementing from multiple threads causes data races.

**Thread-safe alternative:** `Arc<T>` (Atomic Reference Counted) - this IS Send

### Why RefCell is !Send

`RefCell<T>` provides **interior mutability without thread safety**:

```rust
pub struct RefCell<T> {
    borrow: Cell<BorrowFlag>,  // ← Uses Cell (not thread-safe)
    value: UnsafeCell<T>,
}

// RefCell does NOT implement Send!
```

**Why?** The borrow checking is done at **runtime** using `Cell<BorrowFlag>`, which is not thread-safe.

**Thread-safe alternative:** `Mutex<T>` or `RwLock<T>`

## How the Compiler Enforces !Send

When you try to send a `!Send` type between threads, you get a **compile error**:

### Example: Trying to Send LocalExecutor

```rust
let executor = LocalExecutor::default();

std::thread::spawn(move || {
    // Try to use executor in another thread
    executor.run(async { println!("Hello"); });
});
```

**Compile Error:**
```
error[E0277]: `Rc<RefCell<ExecutorQueues>>` cannot be sent between threads safely
   --> src/main.rs:4:5
    |
4   |     std::thread::spawn(move || {
    |     ^^^^^^^^^^^^^^^^^^ `Rc<RefCell<ExecutorQueues>>` cannot be sent between threads safely
    |
    = help: within `LocalExecutor`, the trait `Send` is not implemented for `Rc<RefCell<ExecutorQueues>>`
note: required because it appears within the type `LocalExecutor`
   --> src/executor/mod.rs:1143:12
    |
1143| pub struct LocalExecutor {
    |            ^^^^^^^^^^^^^
```

**The compiler:**
1. Sees you're trying to move `LocalExecutor` into a thread
2. Checks if `LocalExecutor` implements `Send`
3. Finds that `Rc<RefCell<...>>` does NOT implement `Send`
4. Therefore `LocalExecutor` does NOT implement `Send`
5. **Compilation fails!**

## Why LocalExecutor Uses !Send Types

### Design Choice: Single-Threaded Executor

Glommio is designed as a **thread-per-core** framework:
- Each executor runs on exactly one thread
- No synchronization needed within an executor
- Uses cheaper, faster non-thread-safe types

### Performance Benefits

**Using `Rc` instead of `Arc`:**
```rust
// Rc: non-atomic increment/decrement
// Faster! No atomic operations
queues: Rc<RefCell<ExecutorQueues>>

// vs Arc: atomic increment/decrement
// Slower, but thread-safe
queues: Arc<Mutex<ExecutorQueues>>
```

**Benchmark (typical):**
- `Rc::clone()`: ~2-5 nanoseconds
- `Arc::clone()`: ~10-20 nanoseconds (atomic operations are expensive!)

### Trade-off: Safety vs Performance

**Glommio's choice:**
- ❌ Cannot send executor between threads (compile-time error)
- ✅ Faster operations (no atomics needed)
- ✅ Perfect for thread-per-core architecture

## The !Send Guarantee for spawn()

When we make `spawn()` public, the !Send guarantee still holds:

### This Won't Compile:

```rust
let executor = LocalExecutor::default();

let handle = std::thread::spawn(move || {
    // ❌ Compile error: LocalExecutor is not Send
    executor.spawn(async { 42 });
});
```

**Error message:**
```
error[E0277]: `Rc<RefCell<ExecutorQueues>>` cannot be sent between threads safely
```

### Even Passing a Reference Won't Work:

```rust
let executor = LocalExecutor::default();

std::thread::scope(|s| {
    s.spawn(|| {
        // ❌ Compile error: &LocalExecutor is not Sync
        // (because LocalExecutor is not Sync either!)
        executor.spawn(async { 42 });
    });
});
```

**The Sync Trait:**
- `T: Sync` means `&T` can be sent between threads
- `LocalExecutor` is also `!Sync` (because `RefCell` is `!Sync`)

## PhantomData: Explicit !Send Marking

Sometimes you want to explicitly mark a type as `!Send` even without !Send fields:

```rust
use std::marker::PhantomData;

struct MyType<T> {
    data: *const T,  // Raw pointer (no auto trait impls!)
    _marker: PhantomData<Rc<()>>,  // ← Forces !Send
}

// Now MyType is !Send because PhantomData<Rc<()>> is !Send
```

**Why?** Raw pointers (`*const T`) don't implement any auto traits, so you need to be explicit.

## Testing !Send at Compile Time

You can verify a type is `!Send` with a compile-time test:

```rust
#[test]
fn test_local_executor_not_send() {
    // This function only compiles if T: Send
    fn assert_send<T: Send>() {}

    // ❌ This will fail to compile!
    assert_send::<LocalExecutor>();
}
```

**Better approach:** Use `static_assertions` crate:

```rust
use static_assertions::assert_not_impl_any;

assert_not_impl_any!(LocalExecutor: Send, Sync);
```

## Summary: The Type System Enforces !Send

**How it works:**

1. **Auto traits** are automatically implemented based on fields
2. **`Rc<T>`** and **`RefCell<T>`** are `!Send` (by design)
3. **LocalExecutor** contains `Rc<RefCell<...>>`
4. **Therefore** LocalExecutor is automatically `!Send`
5. **Compiler** prevents moving LocalExecutor between threads
6. **Compile error** if you try!

**No runtime checks needed!** The type system prevents misuse at compile time.

## Why This Makes spawn() Safe to Expose

When we make `spawn()` public:

```rust
impl LocalExecutor {
    pub fn spawn<T>(&self, future: ...) -> Task<T> { ... }
}
```

**The worry:**
> What if someone passes executor between threads and calls spawn()?

**The answer:**
> **They can't!** The type system prevents it:

```rust
let executor = LocalExecutor::default();

std::thread::spawn(move || {
    executor.spawn(async { 42 });  // ❌ Compile error!
});
```

**Error:**
```
error[E0277]: `Rc<RefCell<ExecutorQueues>>` cannot be sent between threads safely
```

The thread-safety is **enforced at compile time** by the type system, not by runtime checks in `spawn_local()`.

**Therefore:** Making `spawn()` public is safe! The type system guarantees it can only be called from the owning thread.
