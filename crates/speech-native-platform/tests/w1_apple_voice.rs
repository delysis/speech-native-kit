#![forbid(unsafe_code)]
#![cfg(all(target_os = "macos", feature = "unstable-w1-vertical-tests"))]

use speech_native_platform::PlatformCapabilityProbe;
use speech_native_platform::apple::AppleCapabilitySource;
use speech_native_platform::apple_backend::AppleSpeechBackend;
use speech_native_types::{
    AlignmentGranularity, AudioOutputFormat, NetworkBehavior, SpeechBackend, SpeechDeadlinePolicy,
    SpeechRequestContext, SpeechRequestId, SpeechRouteSelector, SpeechRoutingPolicy,
    SynthesisEvent, SynthesisInput, SynthesisRequest, VoiceQuality, VoiceSelector,
};
use std::process::Command;
use std::sync::Arc;

const VOICE_ID: &str = "com.apple.eloquence.en-US.Eddy";

fn sw_vers(field: &str) -> String {
    let output = Command::new("sw_vers")
        .arg(format!("-{field}"))
        .output()
        .expect("run sw_vers");
    assert!(output.status.success(), "sw_vers must succeed");
    String::from_utf8(output.stdout)
        .expect("sw_vers output is UTF-8")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn w1_exact_installed_voice_inventory() {
    assert_eq!(sw_vers("productVersion"), "15.6");
    assert_eq!(sw_vers("buildVersion"), "24G84");
    let mut probe = PlatformCapabilityProbe::current();
    probe
        .register(Arc::new(AppleCapabilitySource))
        .expect("register noninteractive Apple inventory");
    let snapshot = probe.probe().await;
    let backend = snapshot
        .source_reports
        .iter()
        .flat_map(|report| &report.backends)
        .find(|backend| backend.id == "apple.av-speech")
        .expect("installed Apple synthesis backend");
    assert!(backend.readiness.is_ready());
    assert_eq!(backend.voices.len(), 191);
    let voice = backend
        .voices
        .iter()
        .find(|voice| voice.id == VOICE_ID)
        .expect("exact installed Eddy voice");
    assert_eq!(voice.language, "en-US");
    assert_eq!(voice.quality, Some(VoiceQuality::Normal));
    assert_eq!(voice.network, NetworkBehavior::Never);
    assert!(voice.installed);
}

fn exact_request() -> SynthesisRequest {
    SynthesisRequest {
        context: SpeechRequestContext {
            request_id: SpeechRequestId("apple-w1-installed-voice".to_owned()),
            client_id: "w1-apple-voice-fixture".to_owned(),
            route: SpeechRouteSelector::ExactBackend {
                backend_id: "apple.av-speech".to_owned(),
                model_id: None,
                voice_id: Some(VOICE_ID.to_owned()),
            },
            routing: SpeechRoutingPolicy::default(),
            deadline: SpeechDeadlinePolicy::default(),
        },
        input: SynthesisInput::Text {
            text: "Native buffer synthesis smoke.".to_owned(),
        },
        voice: VoiceSelector::Exact {
            voice_id: VOICE_ID.to_owned(),
        },
        language: Some("en-US".to_owned()),
        rate: 1.0,
        pitch: 1.0,
        volume: 1.0,
        output: AudioOutputFormat::Wav,
        alignment: AlignmentGranularity::None,
        stream: false,
    }
}

#[tokio::test]
#[ignore = "requires a launched macOS application event loop; replay through the repository Apple W1 smoke"]
async fn w1_exact_installed_voice_synthesizes_invariant_wav() {
    let backend = AppleSpeechBackend::discover()
        .await
        .expect("discover Apple synthesis backend");
    let descriptor = backend.descriptor();
    let voice = descriptor
        .voices
        .iter()
        .find(|voice| voice.id == VOICE_ID)
        .expect("exact voice remains installed");
    assert_eq!(voice.language, "en-US");
    assert_eq!(voice.quality, Some(VoiceQuality::Normal));
    assert_eq!(voice.network, NetworkBehavior::Never);

    let mut ticket = backend
        .synthesize(exact_request())
        .await
        .expect("start exact Apple synthesis");
    let mut events = Vec::new();
    while let Some(event) = ticket.events.recv().await {
        let terminal = event.is_terminal();
        events.push(event);
        if terminal {
            break;
        }
    }
    let response = ticket.final_response().await.expect("complete Apple WAV");
    assert_eq!(response.route.backend_id, "apple.av-speech");
    assert_eq!(response.route.voice_id.as_deref(), Some(VOICE_ID));
    assert_eq!(response.route.network, NetworkBehavior::Never);
    assert!(response.audio.starts_with(b"RIFF"));
    assert_eq!(response.audio.get(8..12), Some(b"WAVE".as_slice()));
    assert!(response.audio.len() > 44);
    assert!(response.duration_ms.is_some_and(|duration| duration > 0));
    assert!(response.usage.real_local_inference);
    assert_eq!(events.iter().filter(|event| event.is_terminal()).count(), 1);
    assert!(matches!(
        events.last(),
        Some(SynthesisEvent::Completed { .. })
    ));
    backend.shutdown().await.expect("join Apple backend");
}
