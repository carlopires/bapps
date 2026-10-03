use bapps_trio::{CancelScope, cancel_on};
use glommio::LocalExecutor;

fn main() {
    LocalExecutor::default().run(async {
        let caller = CancelScope::new();
        let service = CancelScope::new();
        let operation = CancelScope::any([caller.clone(), service.clone()]);

        service.cancel();
        let result = cancel_on(&operation, async { "never observed" }).await;
        assert!(result.is_err());
    });
}
