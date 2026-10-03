.PHONY: all validate integration bench clean

PLATFORM := -p bapps-trio -p bapps-otp -p bapps-app
CORE := -p bapps-core -p bapps-core-macros

all: validate

# Format, lints with warnings denied, every test, docs.
validate:
	cargo fmt --all --check
	cargo clippy $(PLATFORM) --all-targets --all-features --locked -- -D warnings
	cargo clippy $(CORE) --all-targets --locked -- -D warnings
	cargo test --workspace --locked
	cargo doc --no-deps $(PLATFORM) --locked

# Real threads on real CPUs: the app crate's multicore suite.
integration:
	timeout 120s cargo test -p bapps-app --test multicore --locked -- --ignored --test-threads=1 --nocapture

bench:
	cargo bench -p bapps-core

clean:
	cargo clean
