// Only a body ending in a block may omit the comma, as in a `match` arm. The
// error must name the branch that needs separating, not the next one's pattern.
fn main() {
    let _ = async {
        glommio_macros::select! {
            v = async { 1u32 } => v
            _ = async {} => 0,
        }
    };
}
