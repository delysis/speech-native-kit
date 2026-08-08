use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=DEVELOPER_DIR");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/lib/swift");
    let Ok(output) = Command::new("xcode-select").arg("-p").output() else {
        println!("cargo:warning=unable to discover Xcode's Swift runtime path");
        return;
    };
    if !output.status.success() {
        println!("cargo:warning=xcode-select did not return a Swift runtime path");
        return;
    }
    let developer_dir = String::from_utf8_lossy(&output.stdout);
    let runtime = format!(
        "{}/Toolchains/XcodeDefault.xctoolchain/usr/lib/swift/macosx",
        developer_dir.trim()
    );
    println!("cargo:rustc-link-arg=-Wl,-rpath,{runtime}");
}
