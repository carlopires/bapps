// If `crate = …` reaches the expansion, the emitted path names this crate and
// fails to resolve. If the argument were ignored, this would compile against
// the default path -- so the failure below is the proof it is honoured.
fn main() {
    let _ = async {
        glommio_macros::select! {
            crate = definitely_not_a_real_crate;
            v = async { 1u32 } => v,
        }
    };
}
