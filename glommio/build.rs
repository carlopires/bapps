use rustc_version::Channel;

fn main() {
    if rustc_version::version_meta()
        .map(|meta| Channel::Nightly == meta.channel)
        .unwrap_or(false)
    {
        println!("cargo:rustc-cfg=nightly");
    }
}
