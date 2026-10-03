// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use futures::future::join_all;
use glommio::prelude::*;
use std::io::Result;

async fn hello() {
    let mut tasks = vec![];
    for t in 0..5 {
        tasks.push(glommio::spawn_local(async move {
            println!("{}: Hello {} ...", glommio::executor().id(), t);
            glommio::executor().yield_task_queue_now().await;
            println!("{}: ... {} World!", glommio::executor().id(), t);
        }));
    }
    join_all(tasks).await;
}

// The shortest way to start an executor. Everything below shows what this
// expands to, and when you would want to write it out by hand instead.
#[glommio::main(placement = Fixed(0))]
async fn main() -> Result<()> {
    hello().await;

    // You can still build executors by hand inside an annotated main --
    // spawning one on another thread, for instance.
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    let handle = builder.name("hello").spawn(|| async move {
        hello().await;
    })?;
    handle.join().unwrap();

    Ok(())
}
