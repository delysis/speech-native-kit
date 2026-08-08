use speech_native_platform::PlatformCapabilityProbe;

#[cfg(target_os = "macos")]
use speech_native_platform::apple::AppleCapabilitySource;

#[tokio::main]
async fn main() {
    let probe = PlatformCapabilityProbe::current();
    #[cfg(target_os = "macos")]
    let probe = {
        let mut probe = probe;
        probe
            .register(std::sync::Arc::new(AppleCapabilitySource))
            .expect("register Apple runtime capability source");
        probe
    };

    let snapshot = probe.probe().await;
    match serde_json::to_string_pretty(&snapshot) {
        Ok(json) => println!("{json}"),
        Err(error) => eprintln!("failed to serialize speech capability snapshot: {error}"),
    }
}
