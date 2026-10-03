#[glommio_macros::main(placement = if true { 1 } else { 2 })]
async fn placement_expression() {}

fn main() {}
