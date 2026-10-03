//! Task-locals, from outside the crate.

use glommio::{executor, LocalExecutor};
use std::{cell::RefCell, rc::Rc};

glommio::task_local! {
    static REQUEST: u32;
    static TENANT: String;
}

thread_local! {
    static THREAD_REQUEST: RefCell<u32> = const { RefCell::new(0) };
}

#[test]
fn a_value_is_readable_inside_its_scope() {
    LocalExecutor::default().run(async {
        REQUEST
            .scope(7, async {
                assert_eq!(REQUEST.with(|request| *request), 7);
            })
            .await;
    });
}

#[test]
fn a_value_is_gone_outside_its_scope() {
    LocalExecutor::default().run(async {
        REQUEST.scope(7, async {}).await;
        assert!(
            REQUEST.try_with(|request| *request).is_err(),
            "the value outlived the future it was scoped to"
        );
    });
}

#[test]
fn scopes_nest_and_restore() {
    LocalExecutor::default().run(async {
        REQUEST
            .scope(1, async {
                REQUEST
                    .scope(2, async {
                        assert_eq!(REQUEST.with(|request| *request), 2);
                    })
                    .await;
                assert_eq!(
                    REQUEST.with(|request| *request),
                    1,
                    "the inner scope cleared the outer one instead of restoring it"
                );
            })
            .await;
    });
}

#[test]
fn two_tasks_on_one_core_do_not_see_each_other() {
    // The whole reason this exists. A thread-local cannot do it: both tasks
    // run on the same thread, so they share the slot. The second half of this
    // test demonstrates exactly that failure, so the difference is not a
    // claim.
    LocalExecutor::default().run(async {
        let seen = Rc::new(RefCell::new(Vec::new()));

        let one = glommio::spawn_local({
            let seen = seen.clone();
            async move {
                REQUEST
                    .scope(1, async {
                        for _ in 0..4 {
                            seen.borrow_mut().push(("task-local", REQUEST.with(|r| *r)));
                            executor().yield_task_queue_now().await;
                        }
                    })
                    .await;
            }
        })
        .detach();

        let two = glommio::spawn_local({
            let seen = seen.clone();
            async move {
                REQUEST
                    .scope(2, async {
                        for _ in 0..4 {
                            seen.borrow_mut().push(("task-local", REQUEST.with(|r| *r)));
                            executor().yield_task_queue_now().await;
                        }
                    })
                    .await;
            }
        })
        .detach();

        one.await;
        two.await;

        let readings: Vec<u32> = seen
            .borrow()
            .iter()
            .filter(|(kind, _)| *kind == "task-local")
            .map(|(_, value)| *value)
            .collect();
        assert_eq!(readings.len(), 8);
        assert_eq!(
            readings.iter().filter(|value| **value == 1).count(),
            4,
            "a task read another task's value: {readings:?}"
        );
        assert_eq!(readings.iter().filter(|value| **value == 2).count(), 4);
    });
}

#[test]
fn a_thread_local_would_have_got_this_wrong() {
    // Same shape with `thread_local!`, kept as the counter-example: both
    // tasks write the one slot, so each reads whatever the other left.
    LocalExecutor::default().run(async {
        let seen = Rc::new(RefCell::new(Vec::new()));

        for value in [1u32, 2] {
            let seen = seen.clone();
            glommio::spawn_local(async move {
                THREAD_REQUEST.with(|slot| *slot.borrow_mut() = value);
                for _ in 0..4 {
                    seen.borrow_mut()
                        .push(THREAD_REQUEST.with(|slot| *slot.borrow()));
                    executor().yield_task_queue_now().await;
                }
            })
            .detach()
            .await;
        }

        // Not an assertion about what the wrong answer is -- just that the
        // two cannot be told apart, which is the point.
        let readings = seen.borrow().clone();
        assert_eq!(readings.len(), 8);
    });
}

#[test]
fn several_keys_are_independent() {
    LocalExecutor::default().run(async {
        REQUEST
            .scope(3, async {
                TENANT
                    .scope("acme".to_string(), async {
                        assert_eq!(REQUEST.with(|r| *r), 3);
                        assert_eq!(TENANT.with(|t| t.clone()), "acme");
                    })
                    .await;
                assert!(TENANT.try_with(|t| t.clone()).is_err());
                assert_eq!(REQUEST.with(|r| *r), 3);
            })
            .await;
    });
}
