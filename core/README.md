# glommio

<!--toc:start-->
- [glommio](#glommio)
  - [New Fork](#new-fork)
  - [What is Glommio?](#what-is-glommio)
  - [Supported Rust Versions](#supported-rust-versions)
  - [Supported Linux kernels](#supported-linux-kernels)
  - [Contributing](#contributing)
  - [License](#license)
<!--toc:end-->

## New Fork

Welcome to the new hard fork of Glommio. Some story why it was forked can be found [here](https://github.com/DataDog/glommio/issues/707),
while TL;DR is - in this fork we are going to keep glommio up to date with fresh versions of io_uring and other dependencies.

## What is Glommio?

Glommio (pronounced glo-mee-jow or |glomjəʊ|) is a Cooperative Thread-per-Core crate for Rust & Linux based
on `io_uring`. Like other rust asynchronous crates, it allows one to write asynchronous code that takes advantage of
rust `async`/`await`, but unlike its counterparts, it doesn't use helper threads anywhere.

Using Glommio is not hard if you are familiar with rust async. All you have to do is:

```rust
use glommio::prelude::*;

LocalExecutorBuilder::default().spawn(|| async move {
    /// your async code here
})
.expect("failed to spawn local executor")
.join();
```

### Versioning

Published versions are this republish's own, not the upstream fork's. Through
`0.10.15` the patch number was ours and the `0.10` mirrored upstream; from
`0.11.0` they diverge, because a breaking change to `GlommioError` required a
minor bump under cargo's rules. Upstream `glommio` remains at `0.10.0`, and a
higher number here means releases, not distance ahead.

### Attribute macros

```rust
#[glommio::main(placement = Fixed(0))]
async fn main() {
    glommio::spawn_local(async { println!("hello") }).await;
}

#[glommio::test]
async fn it_works() {
    assert_eq!(glommio::spawn_local(async { 2 + 2 }).await, 4);
}
```

Both take `placement`; everything else lives on
`LocalExecutorBuilder`. Turning off the default `macros` feature means the
`glommio-macros` crate is not a dependency and not compiled.

If you depend on this crate under another name, e.g.
`glommio = { package = "glommio-ng", version = "0.10" }`, the attribute path
changes along with it: write `#[glommio_ng::main(crate = glommio_ng)]`, not
`#[glommio::main(…)]`. `crate` takes a path, not just a name, so a crate that
reaches this one through a facade can write `crate = ::my_runtime::glommio` and
point at the re-export instead of depending on glommio directly. And
`use glommio::*;` shadows the standard library's
`#[test]` with this crate's — import `main`/`test` by name, or write
`#[glommio::test]`, to keep both available.

For more details check out our [docs page](https://docs.rs/glommio/latest/glommio/) and
an [introductory article.](https://www.datadoghq.com/blog/engineering/introducing-glommio/)

## Recommended allocator

Glommio allocates a block per spawned task and frees it on completion, always on the same thread. That is the pattern a
modern per-thread allocator is built for, and the choice of global allocator is worth more than anything Glommio can do
about task allocation internally. We recommend [mimalloc](https://crates.io/crates/mimalloc):

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

Measured on a 64-core host, median ns per spawn with N tasks live at once:

| live tasks | glibc malloc | jemalloc | mimalloc |
|-----------:|-------------:|---------:|---------:|
| 32         | 38           | 26       | **24**   |
| 128        | 43           | 28       | **24**   |
| 512        | 45           | 30       | **24**   |
| 1024       | 45           | 30       | **24**   |

glibc's per-thread cache holds seven blocks per size class, so it falls off as soon as a handful of tasks are in flight,
while mimalloc stays flat. Reproduce with `glommio/examples/alloc_compare.rs`.

## Supported Rust Versions

Glommio is built against the latest stable release. The minimum supported version is 1.92. The current Glommio version
is not guaranteed to build on Rust versions earlier than the minimum supported version.

## Supported Linux kernels

Glommio requires a kernel with a recent enough `io_uring` support, at least current enough to run discovery probes. The
minimum version at this time is 5.8.

Please also note Glommio requires at least 512 KiB of locked memory for `io_uring` to work. You can increase the
`memlock` resource limit (rlimit) as follows:

```sh
$ vi /etc/security/limits.conf
*    hard    memlock        512
*    soft    memlock        512
```

> Please note that 512 KiB is the minimum needed to spawn a single executor. Spawning multiple executors may require you
> to raise the limit accordingly.

To make the new limits effective, you need to log in to the machine again. You can verify that the limits are updated by
running the following:

```sh
$ ulimit -l
512
```

## Contributing

See [](/CONTRIBUTING.md)

## License

Licensed under either of

* Apache License, Version 2.0 ([](/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([](/LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
