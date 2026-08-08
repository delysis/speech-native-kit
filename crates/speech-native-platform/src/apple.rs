//! Runtime-only macOS speech capability inventory.
//!
//! Discovery never requests authorization, activates a microphone, speaks an
//! utterance, or downloads an asset. It only inspects the current authorization
//! state, on-device recognizer support, and available AVSpeech voices.

use crate::{PlatformCapabilitySource, PlatformProbeError};
use async_trait::async_trait;
use avspeechsynthesizer::{
    SpeechSynthesisVoice, SpeechSynthesisVoiceGender, SpeechSynthesisVoiceQuality,
    SpeechSynthesizer,
};
use speech::{AuthorizationStatus, SpeechRecognizer};
use speech_native_types::{
    AcceptedAudio, AudioOutputKind, CapabilityAvailability, CapabilityEvidence, EvidenceKind,
    EvidenceOutcome, NetworkBehavior, PlatformFamily, PlatformTarget, SpeechBackendDescriptor,
    SpeechBackendKind, SpeechBackendReadiness, SpeechCapability, SpeechCapabilityLimits,
    SpeechOperationCapability, SpeechPermission, SynthesisCapabilities, TranscriptionCapabilities,
    VoiceDescriptor, VoiceGender, VoiceQuality,
};
use std::collections::BTreeSet;
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

pub const APPLE_RUNTIME_SOURCE_ID: &str = "apple-runtime";
static APPLE_RUNTIME_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy, Default)]
pub struct AppleCapabilitySource;

#[async_trait]
impl PlatformCapabilitySource for AppleCapabilitySource {
    fn source_id(&self) -> &str {
        APPLE_RUNTIME_SOURCE_ID
    }

    async fn probe(
        &self,
        target: &PlatformTarget,
    ) -> Result<Vec<SpeechBackendDescriptor>, PlatformProbeError> {
        if target.family != PlatformFamily::MacOs {
            return Err(PlatformProbeError::SourceFailed(
                "the macOS speech adapter cannot probe a different platform".to_string(),
            ));
        }
        tokio::task::spawn_blocking(probe_macos)
            .await
            .map_err(|error| {
                PlatformProbeError::SourceFailed(format!(
                    "the macOS speech inventory task failed: {error}"
                ))
            })
    }
}

fn probe_macos() -> Vec<SpeechBackendDescriptor> {
    let _runtime = lock_apple_runtime();
    let observed_at_unix_ms = unix_time_ms();
    vec![
        probe_synthesis(observed_at_unix_ms),
        probe_on_device_recognition(observed_at_unix_ms),
    ]
}

fn probe_synthesis(observed_at_unix_ms: u64) -> SpeechBackendDescriptor {
    let synthesizer = SpeechSynthesizer::new();
    let voices = SpeechSynthesisVoice::speech_voices();
    match (synthesizer, voices) {
        (Ok(_), Ok(voices)) if !voices.is_empty() => {
            let languages = voices
                .iter()
                .map(|voice| voice.language().to_string())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let voice_count = voices.len();
            SpeechBackendDescriptor {
                id: "apple.av-speech".to_string(),
                display_name: "Apple system voices".to_string(),
                kind: SpeechBackendKind::PlatformOnDevice,
                readiness: SpeechBackendReadiness::Ready,
                capabilities: vec![SpeechCapability {
                    id: "apple.av-speech.synthesis".to_string(),
                    backend_id: "apple.av-speech".to_string(),
                    model_id: None,
                    operation: SpeechOperationCapability::Synthesis(SynthesisCapabilities {
                        streaming_audio: false,
                        ssml: true,
                        word_alignment: false,
                        phoneme_alignment: false,
                        pause_resume: false,
                        voice_selection: true,
                        returned_audio: vec![AudioOutputKind::Wav, AudioOutputKind::DirectPlayback],
                    }),
                    availability: CapabilityAvailability::Available,
                    network: NetworkBehavior::Never,
                    languages,
                    limits: SpeechCapabilityLimits {
                        max_concurrent_requests: Some(1),
                        ..SpeechCapabilityLimits::default()
                    },
                    evidence: vec![runtime_evidence(
                        observed_at_unix_ms,
                        format!(
                            "AVSpeechSynthesizer instantiated and enumerated {voice_count} available system voices without speaking"
                        ),
                    )],
                }],
                models: Vec::new(),
                voices: voices.into_iter().map(map_voice).collect(),
            }
        }
        (Err(error), _) => unavailable_backend(
            "apple.av-speech",
            "Apple system voices",
            format!("AVSpeechSynthesizer could not be initialized: {error}"),
        ),
        (_, Err(error)) => unavailable_backend(
            "apple.av-speech",
            "Apple system voices",
            format!("Apple system voices could not be inventoried: {error}"),
        ),
        (_, Ok(_)) => unavailable_backend(
            "apple.av-speech",
            "Apple system voices",
            "No Apple system voices are currently available".to_string(),
        ),
    }
}

fn probe_on_device_recognition(observed_at_unix_ms: u64) -> SpeechBackendDescriptor {
    let authorization = SpeechRecognizer::authorization_status();
    let Some(default_locale) = SpeechRecognizer::default_locale_identifier() else {
        return unavailable_backend(
            "apple.sf-speech",
            "Apple on-device recognition",
            "Apple did not report a default speech-recognition locale".to_string(),
        );
    };

    let mut on_device_languages = BTreeSet::new();
    let mut available_on_device_languages = BTreeSet::new();
    if let Some(recognizer) = SpeechRecognizer::with_locale_checked(&default_locale) {
        if recognizer.supports_on_device_recognition().unwrap_or(false) {
            on_device_languages.insert(default_locale.clone());
            if recognizer.is_available() {
                available_on_device_languages.insert(default_locale);
            }
        }
    }

    let (readiness, availability) = recognition_readiness(
        authorization,
        !available_on_device_languages.is_empty(),
        on_device_languages.len(),
    );
    let languages = on_device_languages.into_iter().collect::<Vec<_>>();
    let available_count = available_on_device_languages.len();
    let supported_count = languages.len();

    SpeechBackendDescriptor {
        id: "apple.sf-speech".to_string(),
        display_name: "Apple on-device recognition".to_string(),
        kind: SpeechBackendKind::PlatformOnDevice,
        readiness,
        capabilities: vec![SpeechCapability {
            id: "apple.sf-speech.on-device-transcription".to_string(),
            backend_id: "apple.sf-speech".to_string(),
            model_id: None,
            operation: SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                streaming: true,
                partial_results: true,
                segment_timestamps: true,
                word_timestamps: true,
                diarization: false,
                translation_to_english: false,
                long_form: false,
                hotwords: true,
                generative: false,
                accepted_audio: vec![AcceptedAudio::Pcm],
            }),
            availability,
            network: NetworkBehavior::Never,
            languages,
            limits: SpeechCapabilityLimits::default(),
            evidence: vec![runtime_evidence(
                observed_at_unix_ms,
                format!(
                    "SFSpeechRecognizer reported {supported_count} on-device locales and {available_count} currently available locales; authorization is {authorization:?}"
                ),
            )],
        }],
        models: Vec::new(),
        voices: Vec::new(),
    }
}

fn recognition_readiness(
    authorization: AuthorizationStatus,
    has_available_locale: bool,
    supported_locale_count: usize,
) -> (SpeechBackendReadiness, CapabilityAvailability) {
    match authorization {
        AuthorizationStatus::Authorized if has_available_locale => (
            SpeechBackendReadiness::Ready,
            CapabilityAvailability::Available,
        ),
        AuthorizationStatus::Authorized => (
            SpeechBackendReadiness::Unavailable {
                reason: format!(
                    "No on-device Apple recognizer is currently available among {supported_locale_count} supported locales"
                ),
            },
            CapabilityAvailability::Unavailable,
        ),
        AuthorizationStatus::NotDetermined => (
            SpeechBackendReadiness::PermissionRequired {
                permissions: vec![SpeechPermission::SpeechRecognition],
            },
            CapabilityAvailability::PermissionRequired,
        ),
        AuthorizationStatus::Denied | AuthorizationStatus::Restricted => (
            SpeechBackendReadiness::Unavailable {
                reason: format!("Apple speech-recognition authorization is {authorization:?}"),
            },
            CapabilityAvailability::Unavailable,
        ),
        _ => (
            SpeechBackendReadiness::Unknown {
                reason: "Apple returned an unknown speech-recognition authorization state"
                    .to_string(),
            },
            CapabilityAvailability::Unknown,
        ),
    }
}

fn map_voice(voice: SpeechSynthesisVoice) -> VoiceDescriptor {
    VoiceDescriptor {
        id: voice.identifier().to_string(),
        name: voice.name().to_string(),
        language: voice.language().to_string(),
        gender: voice.gender().map(map_voice_gender),
        quality: map_voice_quality(voice.quality()),
        expected_latency: None,
        network: NetworkBehavior::Never,
        installed: true,
    }
}

const fn map_voice_gender(gender: SpeechSynthesisVoiceGender) -> VoiceGender {
    match gender {
        SpeechSynthesisVoiceGender::Female => VoiceGender::Female,
        SpeechSynthesisVoiceGender::Male => VoiceGender::Male,
        SpeechSynthesisVoiceGender::Unspecified => VoiceGender::Unspecified,
        SpeechSynthesisVoiceGender::Unknown(_) => VoiceGender::Unspecified,
    }
}

const fn map_voice_quality(quality: SpeechSynthesisVoiceQuality) -> Option<VoiceQuality> {
    match quality {
        SpeechSynthesisVoiceQuality::Default => Some(VoiceQuality::Normal),
        SpeechSynthesisVoiceQuality::Enhanced => Some(VoiceQuality::Enhanced),
        SpeechSynthesisVoiceQuality::Premium => Some(VoiceQuality::Premium),
        SpeechSynthesisVoiceQuality::Unknown(_) => None,
    }
}

fn runtime_evidence(observed_at_unix_ms: u64, detail: String) -> CapabilityEvidence {
    CapabilityEvidence {
        source_id: APPLE_RUNTIME_SOURCE_ID.to_string(),
        source_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        kind: EvidenceKind::RuntimeApi,
        outcome: EvidenceOutcome::Confirmed,
        observed_at_unix_ms,
        detail,
    }
}

fn unavailable_backend(id: &str, display_name: &str, reason: String) -> SpeechBackendDescriptor {
    SpeechBackendDescriptor {
        id: id.to_string(),
        display_name: display_name.to_string(),
        kind: SpeechBackendKind::PlatformOnDevice,
        readiness: SpeechBackendReadiness::Unavailable { reason },
        capabilities: Vec::new(),
        models: Vec::new(),
        voices: Vec::new(),
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub(crate) fn lock_apple_runtime() -> MutexGuard<'static, ()> {
    APPLE_RUNTIME_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PlatformCapabilityProbe;
    use speech_native_router::{SpeechRouteError, SpeechRouter};
    use speech_native_types::{
        AlignmentGranularity, AudioOutputFormat, SpeechDeadlinePolicy, SpeechRequestContext,
        SpeechRequestId, SpeechRouteSelector, SpeechRoutingPolicy, SynthesisInput,
        SynthesisRequest, VoiceSelector,
    };
    use std::sync::Arc;

    #[test]
    fn authorization_is_never_promoted_without_permission_and_availability() {
        for authorization in [
            AuthorizationStatus::NotDetermined,
            AuthorizationStatus::Denied,
            AuthorizationStatus::Restricted,
            AuthorizationStatus::Unknown,
        ] {
            let (readiness, availability) = recognition_readiness(authorization, true, 1);
            assert!(!readiness.is_ready());
            assert_ne!(availability, CapabilityAvailability::Available);
        }
    }

    #[test]
    fn authorized_but_unavailable_recognizer_is_not_ready() {
        let (readiness, availability) =
            recognition_readiness(AuthorizationStatus::Authorized, false, 4);
        assert!(!readiness.is_ready());
        assert_eq!(availability, CapabilityAvailability::Unavailable);
    }

    #[tokio::test]
    async fn real_probe_is_noninteractive_and_well_formed() {
        let target = PlatformTarget::current();
        let backends = AppleCapabilitySource
            .probe(&target)
            .await
            .expect("macOS inventory should return a typed report");
        assert_eq!(backends.len(), 2);
        for backend in backends {
            backend
                .validate()
                .expect("runtime descriptor must validate");
        }
    }

    #[tokio::test]
    async fn real_probe_produces_an_honest_local_tts_route_or_blocker() {
        let mut probe = PlatformCapabilityProbe::current()
            .with_source_timeout(std::time::Duration::from_secs(10));
        probe
            .register(Arc::new(AppleCapabilitySource))
            .expect("register Apple capability source");
        let snapshot = probe.probe().await;
        let request = SynthesisRequest {
            context: SpeechRequestContext {
                request_id: SpeechRequestId("apple-route-smoke".to_string()),
                client_id: "platform-test".to_string(),
                route: SpeechRouteSelector::Auto,
                routing: SpeechRoutingPolicy::default(),
                deadline: SpeechDeadlinePolicy::default(),
            },
            input: SynthesisInput::Text {
                text: "Capability routing smoke.".to_string(),
            },
            voice: VoiceSelector::Auto,
            language: None,
            rate: 1.0,
            pitch: 1.0,
            volume: 1.0,
            output: AudioOutputFormat::Wav,
            alignment: AlignmentGranularity::None,
            stream: false,
        };

        let apple_synthesis_ready = snapshot
            .source_reports
            .iter()
            .flat_map(|report| &report.backends)
            .find(|backend| backend.id == "apple.av-speech")
            .is_some_and(|backend| backend.readiness.is_ready());
        let plan = SpeechRouter.plan_synthesis(&request, &snapshot);

        if apple_synthesis_ready {
            let plan = plan.expect("a ready Apple voice backend must be routable");
            assert_eq!(plan.selected.route.backend_id, "apple.av-speech");
            assert_eq!(plan.selected.route.network, NetworkBehavior::Never);
        } else {
            assert!(
                matches!(plan, Err(SpeechRouteError::NoEligibleRoute { .. })),
                "an unavailable or timed-out OS voice inventory must fail closed"
            );
        }
    }
}
