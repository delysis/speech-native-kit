#![forbid(unsafe_code)]

#[cfg(not(target_os = "macos"))]
compile_error!("the Apple W1 Tauri replay is macOS-only");

#[cfg(target_os = "macos")]
use speech_native_platform::apple_backend::AppleSpeechBackend;
#[cfg(target_os = "macos")]
use speech_native_types::{
    AlignmentGranularity, AudioOutputFormat, NetworkBehavior, SpeechDeadlinePolicy,
    SpeechRequestContext, SpeechRequestId, SpeechRouteSelector, SpeechRoutingPolicy,
    SynthesisEvent, SynthesisInput, SynthesisRequest, VoiceSelector,
};
#[cfg(target_os = "macos")]
use std::sync::Arc;
#[cfg(target_os = "macos")]
use tauri_plugin_speech_native::SpeechNativeExt;

#[cfg(target_os = "macos")]
const VOICE_ID: &str = "com.apple.eloquence.en-US.Eddy";

#[cfg(target_os = "macos")]
fn request() -> SynthesisRequest {
    SynthesisRequest {
        context: SpeechRequestContext {
            request_id: SpeechRequestId("apple-w1-launched-tauri".to_owned()),
            client_id: "w1-apple-launched-tauri".to_owned(),
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

#[cfg(target_os = "macos")]
fn main() {
    let backend = tauri::async_runtime::block_on(AppleSpeechBackend::discover())
        .expect("discover exact Apple voice backend");
    let plugin = tauri_plugin_speech_native::Builder::new()
        .register_speech_backend(Arc::new(backend))
        .expect("register exact Apple voice backend")
        .build();

    tauri::Builder::default()
        .plugin(plugin)
        .setup(|app| {
            let app_handle = app.handle().clone();
            let speech = app.speech_native();
            tauri::async_runtime::spawn(async move {
                let outcome = async {
                    let status = speech.status()?;
                    let descriptor = status
                        .backends
                        .iter()
                        .find(|backend| backend.id == "apple.av-speech")
                        .expect("Apple synthesis descriptor");
                    let voice = descriptor
                        .voices
                        .iter()
                        .find(|voice| voice.id == VOICE_ID)
                        .expect("exact Eddy voice remains installed");
                    assert_eq!(voice.language, "en-US");
                    assert_eq!(format!("{:?}", voice.quality).to_ascii_lowercase(), "some(normal)");
                    assert_eq!(voice.network, NetworkBehavior::Never);

                    let mut ticket = speech.synthesize(request()).await?;
                    let mut events = Vec::new();
                    while let Some(event) = ticket.events.recv().await {
                        let terminal = event.is_terminal();
                        events.push(event);
                        if terminal {
                            break;
                        }
                    }
                    let response = ticket.final_response().await?;
                    assert_eq!(response.route.backend_id, "apple.av-speech");
                    assert_eq!(response.route.voice_id.as_deref(), Some(VOICE_ID));
                    assert_eq!(response.route.network, NetworkBehavior::Never);
                    assert!(response.audio.starts_with(b"RIFF"));
                    assert_eq!(response.audio.get(8..12), Some(b"WAVE".as_slice()));
                    assert!(response.audio.len() > 44);
                    assert!(response.duration_ms.is_some_and(|duration| duration > 0));
                    assert!(response.usage.real_local_inference);
                    assert_eq!(events.iter().filter(|event| event.is_terminal()).count(), 1);
                    assert!(matches!(events.last(), Some(SynthesisEvent::Completed { .. })));
                    speech.shutdown().await?;
                    println!(
                        "APPLE_W1_OK backend=apple.av-speech voice={} language=en-US quality=normal wav_bytes={} terminal_events=1 network=never real_local_inference=true",
                        VOICE_ID,
                        response.audio.len()
                    );
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                }
                .await;
                match outcome {
                    Ok(()) => app_handle.exit(0),
                    Err(error) => {
                        eprintln!("APPLE_W1_FAILED {error}");
                        app_handle.exit(1);
                    }
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("run Apple W1 Tauri application");
}
