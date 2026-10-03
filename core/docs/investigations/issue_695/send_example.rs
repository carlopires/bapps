// Demonstration of how !Send is enforced through type composition
// This file shows compile errors - it's for educational purposes

use std::rc::Rc;
use std::cell::RefCell;

// Example 1: A Send type (works fine)
struct SendType {
    x: i32,
    y: String,
}

fn example_send_works() {
    let value = SendType { x: 42, y: "hello".to_string() };

    // ✅ This compiles! SendType is Send
    std::thread::spawn(move || {
        println!("x = {}, y = {}", value.x, value.y);
    });
}

// Example 2: A !Send type (won't compile)
struct NotSendType {
    data: Rc<i32>,  // ← Rc is !Send
}

fn example_not_send_fails() {
    let value = NotSendType { data: Rc::new(42) };

    // ❌ This won't compile! NotSendType is !Send
    // Uncomment to see error:
    /*
    std::thread::spawn(move || {
        println!("data = {}", value.data);
    });
    */

    // Error message you'd see:
    // error[E0277]: `Rc<i32>` cannot be sent between threads safely
}

// Example 3: How LocalExecutor becomes !Send
struct SimplifiedExecutor {
    queues: Rc<RefCell<Vec<String>>>,  // ← Both Rc and RefCell are !Send
    id: usize,                          // ← usize is Send (but doesn't matter)
}

fn example_executor_not_send() {
    let executor = SimplifiedExecutor {
        queues: Rc::new(RefCell::new(vec![])),
        id: 1,
    };

    // ❌ This won't compile! SimplifiedExecutor is !Send
    // Uncomment to see error:
    /*
    std::thread::spawn(move || {
        executor.queues.borrow_mut().push("task".to_string());
    });
    */
}

// Example 4: Why Rc is !Send - data race demonstration
fn why_rc_is_not_send() {
    // This is what WOULD happen if Rc was Send (hypothetically):

    let rc = Rc::new(42);
    let rc2 = rc.clone();  // Reference count = 2

    // If we could send Rc between threads:
    // Thread 1: drop(rc)  -> decrement count (non-atomic)
    // Thread 2: drop(rc2) -> decrement count (non-atomic)
    // RACE CONDITION! Count could end up wrong, causing:
    // - Memory leak (count never reaches 0)
    // - Use-after-free (count reaches 0 too early)

    // This is why Rc is !Send - it would be UNSAFE!
}

// Example 5: The safe alternative - Arc
use std::sync::Arc;

struct SendExecutor {
    queues: Arc<Vec<String>>,  // ← Arc is Send!
}

fn example_arc_is_send() {
    let executor = SendExecutor {
        queues: Arc::new(vec!["task1".to_string()]),
    };

    // ✅ This compiles! Arc is Send
    std::thread::spawn(move || {
        println!("queues = {:?}", executor.queues);
    });
}

// Example 6: Demonstrating the compile-time check
fn compile_time_check() {
    // This helper function only compiles if T: Send
    fn assert_send<T: Send>() {}

    // ✅ This compiles
    assert_send::<SendType>();
    assert_send::<Arc<i32>>();
    assert_send::<String>();

    // ❌ These won't compile - uncomment to see errors:
    // assert_send::<NotSendType>();
    // assert_send::<Rc<i32>>();
    // assert_send::<RefCell<i32>>();
    // assert_send::<SimplifiedExecutor>();
}

// Example 7: Why RefCell is !Send
fn why_refcell_is_not_send() {
    use std::cell::Cell;

    // RefCell uses Cell internally for borrow tracking:
    // struct RefCell<T> {
    //     borrow: Cell<BorrowFlag>,  // ← Cell is !Send (not thread-safe)
    //     value: UnsafeCell<T>,
    // }

    // If RefCell was Send:
    // Thread 1: let mut b1 = refcell.borrow_mut();
    // Thread 2: let mut b2 = refcell.borrow_mut();
    // BOTH threads think they have exclusive access!
    // DATA RACE!
}

// Example 8: The actual guarantee for LocalExecutor::spawn()
struct RealExecutorExample {
    data: Rc<RefCell<Vec<i32>>>,
}

impl RealExecutorExample {
    pub fn spawn(&self, value: i32) {
        // This method uses &self
        self.data.borrow_mut().push(value);
    }
}

fn spawn_is_safe_because_not_send() {
    let executor = RealExecutorExample {
        data: Rc::new(RefCell::new(vec![])),
    };

    // ✅ Can call spawn from same thread
    executor.spawn(42);

    // ❌ Can't call spawn from different thread - compile error!
    // Uncomment to see:
    /*
    std::thread::spawn(move || {
        executor.spawn(99);
    });
    */

    // The type system prevents misuse!
}

fn main() {
    println!("See the code comments for explanations!");
    println!("Uncomment the examples to see compile errors.");

    example_send_works();
    // example_not_send_fails();  // Won't compile
    // example_executor_not_send();  // Won't compile
    example_arc_is_send();
    compile_time_check();
}
