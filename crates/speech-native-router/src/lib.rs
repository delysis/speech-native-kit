//! Deterministic, fail-closed routing over discovered speech capabilities.
//!
//! The router does not guess quality, privacy, or feature support. Hard gates
//! run before an explicit policy ordering, and every rejected capability is
//! retained in the plan receipt.

use serde::{Deserialize, Serialize};
use speech_native_types::{
    AcceptedAudio, AlignmentGranularity, AudioInput, AudioOutputFormat, AudioOutputKind,
    CapabilityAvailability, EncodedAudioFormat, PlatformCapabilitySnapshot, ProbeSourceStatus,
    SpeechBackendDescriptor, SpeechBackendKind, SpeechCapability, SpeechOperationCapability,
    SpeechOperationKind, SpeechPrivacyPolicy, SpeechRequestContext, SpeechResolvedRoute,
    SpeechRouteProfile, SpeechRouteSelector, SynthesisRequest, TimestampGranularity,
    TranscriptionInput, TranscriptionRequest, TranscriptionTask, VoiceDescriptor, VoiceQuality,
    VoiceSelector,
};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechRoutePlan {
    pub selected: SpeechRouteSelection,
    #[serde(default)]
    pub rejections: Vec<SpeechRouteRejection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechRouteSelection {
    pub route: SpeechResolvedRoute,
    pub capability_id: String,
    pub policy_rank: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechRouteRejection {
    pub backend_id: String,
    pub capability_id: Option<String>,
    pub code: RouteRejectionCode,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteRejectionCode {
    BackendNotReady,
    CapabilityUnavailable,
    OperationMismatch,
    PrivacyMismatch,
    NetworkNotProvenLocal,
    LanguageUnsupported,
    InputFormatUnsupported,
    OutputFormatUnsupported,
    StreamingUnsupported,
    PartialResultsUnsupported,
    TimestampsUnsupported,
    DiarizationUnsupported,
    TranslationUnsupported,
    HotwordsUnsupported,
    SsmlUnsupported,
    AlignmentUnsupported,
    BackendSelectorMismatch,
    ModelSelectorMismatch,
    VoiceSelectorMismatch,
    NamedProfileUnresolved,
}

#[derive(Debug, thiserror::Error, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpeechRouteError {
    #[error("the speech request is invalid ({code}): {detail}")]
    RequestInvalid { code: String, detail: String },
    #[error("the capability snapshot is invalid: {detail}")]
    SnapshotInvalid { detail: String },
    #[error("no eligible {operation:?} route was found")]
    NoEligibleRoute {
        operation: SpeechOperationKind,
        rejections: Vec<SpeechRouteRejection>,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SpeechRouter;

impl SpeechRouter {
    pub fn plan_transcription(
        &self,
        request: &TranscriptionRequest,
        snapshot: &PlatformCapabilitySnapshot,
    ) -> Result<SpeechRoutePlan, SpeechRouteError> {
        request
            .validate()
            .map_err(|error| SpeechRouteError::RequestInvalid {
                code: error.code,
                detail: error.safe_detail,
            })?;
        validate_snapshot(snapshot)?;
        let mut candidates = Vec::new();
        let mut rejections = Vec::new();
        for backend in successful_backends(snapshot) {
            evaluate_backend_basics(
                &request.context,
                backend,
                SpeechOperationKind::Transcription,
                &mut rejections,
            );
            for capability in &backend.capabilities {
                match evaluate_transcription(request, backend, capability) {
                    Ok(()) => match evaluate_common(&request.context, backend, capability) {
                        Ok(()) => {
                            candidates.push(selection(&request.context, backend, capability, None))
                        }
                        Err(rejection) => rejections.push(rejection),
                    },
                    Err(rejection) => rejections.push(rejection),
                }
            }
        }
        finalize(SpeechOperationKind::Transcription, candidates, rejections)
    }

    pub fn plan_synthesis(
        &self,
        request: &SynthesisRequest,
        snapshot: &PlatformCapabilitySnapshot,
    ) -> Result<SpeechRoutePlan, SpeechRouteError> {
        request
            .validate()
            .map_err(|error| SpeechRouteError::RequestInvalid {
                code: error.code,
                detail: error.safe_detail,
            })?;
        validate_snapshot(snapshot)?;
        let mut candidates = Vec::new();
        let mut rejections = Vec::new();
        for backend in successful_backends(snapshot) {
            evaluate_backend_basics(
                &request.context,
                backend,
                SpeechOperationKind::Synthesis,
                &mut rejections,
            );
            for capability in &backend.capabilities {
                match evaluate_synthesis(request, backend, capability) {
                    Ok(voice) => match evaluate_common(&request.context, backend, capability) {
                        Ok(()) => {
                            candidates.push(selection(&request.context, backend, capability, voice))
                        }
                        Err(rejection) => rejections.push(rejection),
                    },
                    Err(rejection) => rejections.push(rejection),
                }
            }
        }
        finalize(SpeechOperationKind::Synthesis, candidates, rejections)
    }
}

fn successful_backends(
    snapshot: &PlatformCapabilitySnapshot,
) -> impl Iterator<Item = &SpeechBackendDescriptor> {
    snapshot
        .source_reports
        .iter()
        .filter(|report| report.status == ProbeSourceStatus::Succeeded)
        .flat_map(|report| report.backends.iter())
}

fn validate_snapshot(snapshot: &PlatformCapabilitySnapshot) -> Result<(), SpeechRouteError> {
    let mut backend_ids = HashSet::new();
    let mut capability_ids = HashSet::new();
    for backend in successful_backends(snapshot) {
        backend
            .validate()
            .map_err(|error| SpeechRouteError::SnapshotInvalid {
                detail: error.to_string(),
            })?;
        if !backend_ids.insert(&backend.id) {
            return Err(SpeechRouteError::SnapshotInvalid {
                detail: format!("backend {} is reported by more than one source", backend.id),
            });
        }
        for capability in &backend.capabilities {
            if !capability_ids.insert(&capability.id) {
                return Err(SpeechRouteError::SnapshotInvalid {
                    detail: format!("capability {} is reported more than once", capability.id),
                });
            }
        }
    }
    Ok(())
}

fn evaluate_backend_basics(
    context: &SpeechRequestContext,
    backend: &SpeechBackendDescriptor,
    operation: SpeechOperationKind,
    rejections: &mut Vec<SpeechRouteRejection>,
) {
    if !backend.readiness.is_ready() {
        rejections.push(rejection(
            backend,
            None,
            RouteRejectionCode::BackendNotReady,
            format!("backend is {:?}", backend.readiness),
        ));
    }
    if backend
        .capabilities
        .iter()
        .all(|capability| capability.operation.kind() != operation)
    {
        rejections.push(rejection(
            backend,
            None,
            RouteRejectionCode::OperationMismatch,
            format!("backend does not advertise {operation:?}"),
        ));
    }
    if let SpeechRouteSelector::ExactBackend { backend_id, .. } = &context.route
        && backend_id != &backend.id
    {
        rejections.push(rejection(
            backend,
            None,
            RouteRejectionCode::BackendSelectorMismatch,
            format!("request selected backend {backend_id}"),
        ));
    }
}

fn evaluate_common(
    context: &SpeechRequestContext,
    backend: &SpeechBackendDescriptor,
    capability: &SpeechCapability,
) -> Result<(), SpeechRouteRejection> {
    if !backend.readiness.is_ready() {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::BackendNotReady,
            "backend is not ready".to_string(),
        ));
    }
    if capability.availability != CapabilityAvailability::Available {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::CapabilityUnavailable,
            format!("capability is {:?}", capability.availability),
        ));
    }
    match context.routing.privacy {
        SpeechPrivacyPolicy::LocalOnly => {
            if backend.kind == SpeechBackendKind::Hosted {
                return Err(rejection(
                    backend,
                    Some(capability),
                    RouteRejectionCode::PrivacyMismatch,
                    "hosted backends are excluded from local-only routing".to_string(),
                ));
            }
            if !capability.eligible_for_local_only() {
                return Err(rejection(
                    backend,
                    Some(capability),
                    RouteRejectionCode::NetworkNotProvenLocal,
                    "local-only routing requires confirmed runtime evidence and network=never"
                        .to_string(),
                ));
            }
        }
        SpeechPrivacyPolicy::HostedOnly if backend.kind != SpeechBackendKind::Hosted => {
            return Err(rejection(
                backend,
                Some(capability),
                RouteRejectionCode::PrivacyMismatch,
                "local backend is excluded from hosted-only routing".to_string(),
            ));
        }
        SpeechPrivacyPolicy::HostedAllowed | SpeechPrivacyPolicy::HostedOnly => {}
    }
    match &context.route {
        SpeechRouteSelector::ExactBackend {
            backend_id,
            model_id,
            ..
        } => {
            if backend_id != &backend.id {
                return Err(rejection(
                    backend,
                    Some(capability),
                    RouteRejectionCode::BackendSelectorMismatch,
                    format!("request selected backend {backend_id}"),
                ));
            }
            if let Some(model_id) = model_id
                && capability.model_id.as_deref() != Some(model_id.as_str())
            {
                return Err(rejection(
                    backend,
                    Some(capability),
                    RouteRejectionCode::ModelSelectorMismatch,
                    format!("request selected model {model_id}"),
                ));
            }
        }
        SpeechRouteSelector::Profile { name } => {
            return Err(rejection(
                backend,
                Some(capability),
                RouteRejectionCode::NamedProfileUnresolved,
                format!("named route profile {name} must be resolved before planning"),
            ));
        }
        SpeechRouteSelector::Auto => {}
    }
    Ok(())
}

fn evaluate_transcription(
    request: &TranscriptionRequest,
    backend: &SpeechBackendDescriptor,
    capability: &SpeechCapability,
) -> Result<(), SpeechRouteRejection> {
    let SpeechOperationCapability::Transcription(features) = &capability.operation else {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::OperationMismatch,
            "capability is not transcription".to_string(),
        ));
    };
    language_supported(request.language.as_deref(), capability, backend)?;
    let accepted = transcription_audio_kind(&request.input);
    if let Some(accepted) = accepted
        && !features.accepted_audio.contains(&accepted)
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::InputFormatUnsupported,
            format!("input format {accepted:?} is not supported"),
        ));
    }
    if matches!(request.input, TranscriptionInput::Stream { .. }) && !features.streaming {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::StreamingUnsupported,
            "live audio streaming is not supported".to_string(),
        ));
    }
    if request.partial_results && !features.partial_results {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::PartialResultsUnsupported,
            "partial transcription results are not supported".to_string(),
        ));
    }
    if (request.timestamps == TimestampGranularity::Segment && !features.segment_timestamps)
        || (request.timestamps == TimestampGranularity::Word && !features.word_timestamps)
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::TimestampsUnsupported,
            format!("{:?} timestamps are not supported", request.timestamps),
        ));
    }
    if !matches!(
        request.diarization,
        speech_native_types::DiarizationPolicy::Disabled
    ) && !features.diarization
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::DiarizationUnsupported,
            "speaker diarization is not supported".to_string(),
        ));
    }
    if request.task == TranscriptionTask::TranslateToEnglish && !features.translation_to_english {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::TranslationUnsupported,
            "translation to English is not supported".to_string(),
        ));
    }
    if !request.hotwords.is_empty() && !features.hotwords {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::HotwordsUnsupported,
            "hotwords are not supported".to_string(),
        ));
    }
    Ok(())
}

fn evaluate_synthesis<'a>(
    request: &SynthesisRequest,
    backend: &'a SpeechBackendDescriptor,
    capability: &SpeechCapability,
) -> Result<Option<&'a VoiceDescriptor>, SpeechRouteRejection> {
    let SpeechOperationCapability::Synthesis(features) = &capability.operation else {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::OperationMismatch,
            "capability is not synthesis".to_string(),
        ));
    };
    language_supported(request.language.as_deref(), capability, backend)?;
    if matches!(
        request.input,
        speech_native_types::SynthesisInput::Ssml { .. }
    ) && !features.ssml
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::SsmlUnsupported,
            "SSML is not supported".to_string(),
        ));
    }
    if request.stream && !features.streaming_audio {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::StreamingUnsupported,
            "streaming audio is not supported".to_string(),
        ));
    }
    let output = synthesis_output_kind(&request.output);
    if !features.returned_audio.contains(&output) {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::OutputFormatUnsupported,
            format!("output format {output:?} is not supported"),
        ));
    }
    if (request.alignment == AlignmentGranularity::Word && !features.word_alignment)
        || (request.alignment == AlignmentGranularity::Phoneme && !features.phoneme_alignment)
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::AlignmentUnsupported,
            format!("{:?} alignment is not supported", request.alignment),
        ));
    }
    select_voice(request, backend, capability)
}

fn language_supported(
    language: Option<&str>,
    capability: &SpeechCapability,
    backend: &SpeechBackendDescriptor,
) -> Result<(), SpeechRouteRejection> {
    if let Some(language) = language
        && !capability.languages.is_empty()
        && !capability.languages.iter().any(|candidate| {
            candidate.eq_ignore_ascii_case(language)
                || language_base(candidate).eq_ignore_ascii_case(language_base(language))
        })
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::LanguageUnsupported,
            format!("language {language} is not supported"),
        ));
    }
    Ok(())
}

fn language_base(language: &str) -> &str {
    language.split_once('-').map_or(language, |(base, _)| base)
}

fn select_voice<'a>(
    request: &SynthesisRequest,
    backend: &'a SpeechBackendDescriptor,
    capability: &SpeechCapability,
) -> Result<Option<&'a VoiceDescriptor>, SpeechRouteRejection> {
    let route_voice = match &request.context.route {
        SpeechRouteSelector::ExactBackend { voice_id, .. } => voice_id.as_deref(),
        _ => None,
    };
    let request_voice = match &request.voice {
        VoiceSelector::Auto => None,
        VoiceSelector::Exact { voice_id } => Some(voice_id.as_str()),
        VoiceSelector::Profile { name } => {
            return Err(rejection(
                backend,
                Some(capability),
                RouteRejectionCode::NamedProfileUnresolved,
                format!("named voice profile {name} must be resolved before planning"),
            ));
        }
    };
    if let (Some(route_voice), Some(request_voice)) = (route_voice, request_voice)
        && route_voice != request_voice
    {
        return Err(rejection(
            backend,
            Some(capability),
            RouteRejectionCode::VoiceSelectorMismatch,
            "route and request select different voices".to_string(),
        ));
    }
    let exact = route_voice.or(request_voice);
    if let Some(exact) = exact {
        return backend
            .voices
            .iter()
            .find(|voice| voice.id == exact && voice.installed)
            .map(Some)
            .ok_or_else(|| {
                rejection(
                    backend,
                    Some(capability),
                    RouteRejectionCode::VoiceSelectorMismatch,
                    format!("voice {exact} is not installed on this backend"),
                )
            });
    }

    // A generic capability descriptor cannot identify the platform's default
    // voice. Leave this unresolved when no language was requested so the
    // selected backend can use its actual runtime default instead of routing
    // to an arbitrary high-quality voice in an unrelated language.
    if request.language.is_none() {
        return Ok(None);
    }

    let mut voices = backend
        .voices
        .iter()
        .filter(|voice| voice.installed)
        .filter(|voice| {
            request.language.as_ref().is_none_or(|language| {
                voice.language.eq_ignore_ascii_case(language)
                    || voice
                        .language
                        .split_once('-')
                        .zip(language.split_once('-'))
                        .is_some_and(|((voice_base, _), (request_base, _))| {
                            voice_base.eq_ignore_ascii_case(request_base)
                        })
            })
        })
        .collect::<Vec<_>>();
    let requested_language = request.language.as_deref();
    voices.sort_by(|left, right| {
        voice_language_rank(left, requested_language)
            .cmp(&voice_language_rank(right, requested_language))
            .then_with(|| voice_quality_rank(right.quality).cmp(&voice_quality_rank(left.quality)))
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(voices.into_iter().next())
}

fn voice_language_rank(voice: &VoiceDescriptor, language: Option<&str>) -> u8 {
    let Some(language) = language else {
        return 0;
    };
    if voice.language.eq_ignore_ascii_case(language) {
        return 0;
    }
    let same_base = voice
        .language
        .split_once('-')
        .zip(language.split_once('-'))
        .is_some_and(|((voice_base, _), (request_base, _))| {
            voice_base.eq_ignore_ascii_case(request_base)
        });
    if same_base { 1 } else { 2 }
}

fn selection(
    context: &SpeechRequestContext,
    backend: &SpeechBackendDescriptor,
    capability: &SpeechCapability,
    voice: Option<&VoiceDescriptor>,
) -> SpeechRouteSelection {
    let rank = policy_rank(context.routing.profile, backend.kind)
        .saturating_mul(10)
        .saturating_add(voice.map_or(0, |voice| 4 - voice_quality_rank(voice.quality)));
    SpeechRouteSelection {
        route: SpeechResolvedRoute {
            backend_id: backend.id.clone(),
            model_id: capability.model_id.clone(),
            voice_id: voice.map(|voice| voice.id.clone()),
            backend_kind: backend.kind,
            network: capability.network,
        },
        capability_id: capability.id.clone(),
        policy_rank: rank,
        reason: format!(
            "selected by {:?} after privacy, readiness, and feature gates",
            context.routing.profile
        ),
    }
}

const fn policy_rank(profile: SpeechRouteProfile, kind: SpeechBackendKind) -> u16 {
    match profile {
        SpeechRouteProfile::PrivateBalanced | SpeechRouteProfile::QualityLocal => match kind {
            SpeechBackendKind::PlatformOnDevice => 0,
            SpeechBackendKind::EmbeddedModel => 1,
            SpeechBackendKind::ResidentMultimodalModel => 2,
            SpeechBackendKind::PlatformService => 3,
            SpeechBackendKind::Hosted => 4,
        },
        SpeechRouteProfile::NativePreferred => match kind {
            SpeechBackendKind::PlatformOnDevice => 0,
            SpeechBackendKind::PlatformService => 1,
            SpeechBackendKind::EmbeddedModel => 2,
            SpeechBackendKind::ResidentMultimodalModel => 3,
            SpeechBackendKind::Hosted => 4,
        },
        SpeechRouteProfile::ConsistentLocal => match kind {
            SpeechBackendKind::EmbeddedModel => 0,
            SpeechBackendKind::ResidentMultimodalModel => 1,
            SpeechBackendKind::PlatformOnDevice => 2,
            SpeechBackendKind::PlatformService => 3,
            SpeechBackendKind::Hosted => 4,
        },
        SpeechRouteProfile::HostedEnabled => match kind {
            SpeechBackendKind::Hosted => 0,
            SpeechBackendKind::PlatformOnDevice => 1,
            SpeechBackendKind::EmbeddedModel => 2,
            SpeechBackendKind::ResidentMultimodalModel => 3,
            SpeechBackendKind::PlatformService => 4,
        },
    }
}

const fn voice_quality_rank(quality: Option<VoiceQuality>) -> u16 {
    match quality {
        Some(VoiceQuality::Premium) => 4,
        Some(VoiceQuality::Enhanced) => 3,
        Some(VoiceQuality::Normal) => 2,
        Some(VoiceQuality::Basic) => 1,
        None => 0,
    }
}

fn finalize(
    operation: SpeechOperationKind,
    mut candidates: Vec<SpeechRouteSelection>,
    mut rejections: Vec<SpeechRouteRejection>,
) -> Result<SpeechRoutePlan, SpeechRouteError> {
    candidates.sort_by(|left, right| {
        left.policy_rank
            .cmp(&right.policy_rank)
            .then_with(|| left.route.backend_id.cmp(&right.route.backend_id))
            .then_with(|| left.capability_id.cmp(&right.capability_id))
    });
    rejections.sort_by(|left, right| {
        left.backend_id
            .cmp(&right.backend_id)
            .then_with(|| left.capability_id.cmp(&right.capability_id))
            .then_with(|| format!("{:?}", left.code).cmp(&format!("{:?}", right.code)))
    });
    let Some(selected) = candidates.into_iter().next() else {
        return Err(SpeechRouteError::NoEligibleRoute {
            operation,
            rejections,
        });
    };
    Ok(SpeechRoutePlan {
        selected,
        rejections,
    })
}

fn rejection(
    backend: &SpeechBackendDescriptor,
    capability: Option<&SpeechCapability>,
    code: RouteRejectionCode,
    detail: String,
) -> SpeechRouteRejection {
    SpeechRouteRejection {
        backend_id: backend.id.clone(),
        capability_id: capability.map(|capability| capability.id.clone()),
        code,
        detail,
    }
}

fn transcription_audio_kind(input: &TranscriptionInput) -> Option<AcceptedAudio> {
    match input {
        TranscriptionInput::Complete { audio } => match audio {
            AudioInput::Pcm { .. } => Some(AcceptedAudio::Pcm),
            AudioInput::Encoded { format, .. } => Some(match format {
                EncodedAudioFormat::Wav => AcceptedAudio::Wav,
                EncodedAudioFormat::Flac => AcceptedAudio::Flac,
                EncodedAudioFormat::Mp3 => AcceptedAudio::Mp3,
                EncodedAudioFormat::M4a => AcceptedAudio::M4a,
                EncodedAudioFormat::OggOpus => AcceptedAudio::OggOpus,
                EncodedAudioFormat::WebmOpus => AcceptedAudio::WebmOpus,
            }),
            AudioInput::Asset { format_hint, .. } => format_hint.map(|format| match format {
                EncodedAudioFormat::Wav => AcceptedAudio::Wav,
                EncodedAudioFormat::Flac => AcceptedAudio::Flac,
                EncodedAudioFormat::Mp3 => AcceptedAudio::Mp3,
                EncodedAudioFormat::M4a => AcceptedAudio::M4a,
                EncodedAudioFormat::OggOpus => AcceptedAudio::OggOpus,
                EncodedAudioFormat::WebmOpus => AcceptedAudio::WebmOpus,
            }),
        },
        TranscriptionInput::Stream { .. } => Some(AcceptedAudio::Pcm),
    }
}

const fn synthesis_output_kind(output: &AudioOutputFormat) -> AudioOutputKind {
    match output {
        AudioOutputFormat::Wav => AudioOutputKind::Wav,
        AudioOutputFormat::Pcm { .. } => AudioOutputKind::Pcm,
        AudioOutputFormat::Mp3 { .. } => AudioOutputKind::Mp3,
        AudioOutputFormat::OggOpus { .. } => AudioOutputKind::OggOpus,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use speech_native_types::{
        ApplicationIdentity, AudioOutputFormat, CapabilityEvidence, CapabilitySourceReport,
        EvidenceKind, EvidenceOutcome, NetworkBehavior, PcmFormat, PcmSampleFormat, PlatformFamily,
        PlatformTarget, SPEECH_CAPABILITY_SCHEMA, SpeechBackendReadiness, SpeechCapabilityLimits,
        SpeechDeadlinePolicy, SpeechRequestId, SpeechRoutingPolicy, SynthesisCapabilities,
        SynthesisInput, TranscriptionCapabilities,
    };

    fn context() -> SpeechRequestContext {
        SpeechRequestContext {
            request_id: SpeechRequestId("route-test".to_string()),
            client_id: "test".to_string(),
            route: SpeechRouteSelector::Auto,
            routing: SpeechRoutingPolicy::default(),
            deadline: SpeechDeadlinePolicy::default(),
        }
    }

    fn capability(
        backend_id: &str,
        operation: SpeechOperationCapability,
        network: NetworkBehavior,
    ) -> SpeechCapability {
        SpeechCapability {
            id: format!("{backend_id}.capability"),
            backend_id: backend_id.to_string(),
            model_id: None,
            operation,
            availability: CapabilityAvailability::Available,
            network,
            languages: vec!["en-US".to_string()],
            limits: SpeechCapabilityLimits::default(),
            evidence: vec![CapabilityEvidence {
                source_id: "fixture".to_string(),
                source_version: Some("1".to_string()),
                kind: EvidenceKind::RuntimeApi,
                outcome: EvidenceOutcome::Confirmed,
                observed_at_unix_ms: 1,
                detail: "fixture".to_string(),
            }],
        }
    }

    fn backend(
        id: &str,
        kind: SpeechBackendKind,
        capability: SpeechCapability,
    ) -> SpeechBackendDescriptor {
        SpeechBackendDescriptor {
            id: id.to_string(),
            display_name: id.to_string(),
            kind,
            readiness: SpeechBackendReadiness::Ready,
            capabilities: vec![capability],
            models: Vec::new(),
            voices: Vec::new(),
        }
    }

    fn snapshot(backends: Vec<SpeechBackendDescriptor>) -> PlatformCapabilitySnapshot {
        PlatformCapabilitySnapshot {
            schema: SPEECH_CAPABILITY_SCHEMA.to_string(),
            captured_at_unix_ms: 1,
            target: PlatformTarget {
                family: PlatformFamily::MacOs,
                os_version: None,
                architecture: "aarch64".to_string(),
                application_identity: ApplicationIdentity::Unknown,
            },
            adapter_candidates: Vec::new(),
            source_reports: vec![CapabilitySourceReport {
                source_id: "fixture".to_string(),
                status: ProbeSourceStatus::Succeeded,
                detail: None,
                backends,
            }],
        }
    }

    fn transcription() -> TranscriptionRequest {
        TranscriptionRequest {
            context: context(),
            input: TranscriptionInput::Complete {
                audio: AudioInput::Pcm {
                    format: PcmFormat {
                        sample_rate_hz: 16_000,
                        channels: 1,
                        sample_format: PcmSampleFormat::I16Le,
                        interleaved: true,
                    },
                    data: vec![0; 32],
                },
            },
            language: Some("en-US".to_string()),
            task: TranscriptionTask::Transcribe,
            timestamps: TimestampGranularity::None,
            diarization: speech_native_types::DiarizationPolicy::Disabled,
            partial_results: false,
            punctuation: true,
            hotwords: Vec::new(),
        }
    }

    #[test]
    fn private_balanced_prefers_ready_native_over_embedded() {
        let native = backend(
            "native.recognizer",
            SpeechBackendKind::PlatformOnDevice,
            capability(
                "native.recognizer",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        let embedded = backend(
            "embedded.parakeet",
            SpeechBackendKind::EmbeddedModel,
            capability(
                "embedded.parakeet",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        let plan = SpeechRouter
            .plan_transcription(&transcription(), &snapshot(vec![embedded, native]))
            .expect("route should exist");
        assert_eq!(plan.selected.route.backend_id, "native.recognizer");
    }

    #[test]
    fn consistent_local_prefers_embedded_backend() {
        let mut request = transcription();
        request.context.routing.profile = SpeechRouteProfile::ConsistentLocal;
        let native = backend(
            "native.recognizer",
            SpeechBackendKind::PlatformOnDevice,
            capability(
                "native.recognizer",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        let embedded = backend(
            "embedded.parakeet",
            SpeechBackendKind::EmbeddedModel,
            capability(
                "embedded.parakeet",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        let plan = SpeechRouter
            .plan_transcription(&request, &snapshot(vec![native, embedded]))
            .expect("route should exist");
        assert_eq!(plan.selected.route.backend_id, "embedded.parakeet");
    }

    #[test]
    fn hosted_backend_cannot_enter_local_only_even_if_it_claims_never_network() {
        let hosted = backend(
            "hosted.bad-claim",
            SpeechBackendKind::Hosted,
            capability(
                "hosted.bad-claim",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        let error = SpeechRouter
            .plan_transcription(&transcription(), &snapshot(vec![hosted]))
            .expect_err("hosted route must fail");
        assert!(matches!(error, SpeechRouteError::NoEligibleRoute { .. }));
    }

    #[test]
    fn requested_features_are_hard_gates() {
        let mut request = transcription();
        request.timestamps = TimestampGranularity::Word;
        let backend = backend(
            "embedded.parakeet",
            SpeechBackendKind::EmbeddedModel,
            capability(
                "embedded.parakeet",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    word_timestamps: false,
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        let error = SpeechRouter
            .plan_transcription(&request, &snapshot(vec![backend]))
            .expect_err("unsupported timestamps must fail");
        let SpeechRouteError::NoEligibleRoute { rejections, .. } = error else {
            panic!("expected no-route error");
        };
        assert!(
            rejections
                .iter()
                .any(|rejection| rejection.code == RouteRejectionCode::TimestampsUnsupported)
        );
    }

    #[test]
    fn synthesis_selects_best_installed_voice_without_invented_latency() {
        let mut backend = backend(
            "native.tts",
            SpeechBackendKind::PlatformOnDevice,
            capability(
                "native.tts",
                SpeechOperationCapability::Synthesis(SynthesisCapabilities {
                    returned_audio: vec![AudioOutputKind::Wav],
                    ..SynthesisCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        backend.voices = vec![
            VoiceDescriptor {
                id: "basic".to_string(),
                name: "Basic".to_string(),
                language: "en-US".to_string(),
                gender: None,
                quality: Some(VoiceQuality::Basic),
                expected_latency: None,
                network: NetworkBehavior::Never,
                installed: true,
            },
            VoiceDescriptor {
                id: "premium".to_string(),
                name: "Premium".to_string(),
                language: "en-US".to_string(),
                gender: None,
                quality: Some(VoiceQuality::Premium),
                expected_latency: None,
                network: NetworkBehavior::Never,
                installed: true,
            },
        ];
        let mut context = context();
        context.routing.profile = SpeechRouteProfile::QualityLocal;
        let request = SynthesisRequest {
            context,
            input: SynthesisInput::Text {
                text: "hello".to_string(),
            },
            voice: VoiceSelector::Auto,
            language: Some("en-US".to_string()),
            rate: 1.0,
            pitch: 1.0,
            volume: 1.0,
            output: AudioOutputFormat::Wav,
            alignment: AlignmentGranularity::None,
            stream: false,
        };
        let plan = SpeechRouter
            .plan_synthesis(&request, &snapshot(vec![backend]))
            .expect("synthesis route should exist");
        assert_eq!(plan.selected.route.voice_id.as_deref(), Some("premium"));
    }

    #[test]
    fn synthesis_prefers_exact_locale_before_related_locale_quality() {
        let mut backend = backend(
            "native.tts",
            SpeechBackendKind::PlatformOnDevice,
            capability(
                "native.tts",
                SpeechOperationCapability::Synthesis(SynthesisCapabilities {
                    returned_audio: vec![AudioOutputKind::Wav],
                    ..SynthesisCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        );
        backend.voices = vec![
            VoiceDescriptor {
                id: "en-gb-premium".to_string(),
                name: "British Premium".to_string(),
                language: "en-GB".to_string(),
                gender: None,
                quality: Some(VoiceQuality::Premium),
                expected_latency: None,
                network: NetworkBehavior::Never,
                installed: true,
            },
            VoiceDescriptor {
                id: "en-us-basic".to_string(),
                name: "US Basic".to_string(),
                language: "en-US".to_string(),
                gender: None,
                quality: Some(VoiceQuality::Basic),
                expected_latency: None,
                network: NetworkBehavior::Never,
                installed: true,
            },
        ];
        let request = SynthesisRequest {
            context: context(),
            input: SynthesisInput::Text {
                text: "hello".to_string(),
            },
            voice: VoiceSelector::Auto,
            language: Some("en-US".to_string()),
            rate: 1.0,
            pitch: 1.0,
            volume: 1.0,
            output: AudioOutputFormat::Wav,
            alignment: AlignmentGranularity::None,
            stream: false,
        };
        let plan = SpeechRouter
            .plan_synthesis(&request, &snapshot(vec![backend]))
            .expect("synthesis route should exist");
        assert_eq!(plan.selected.route.voice_id.as_deref(), Some("en-us-basic"));
    }

    #[test]
    fn duplicate_backend_ids_across_sources_fail_closed() {
        let mut snapshot = snapshot(vec![backend(
            "duplicate.backend",
            SpeechBackendKind::EmbeddedModel,
            capability(
                "duplicate.backend",
                SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                    accepted_audio: vec![AcceptedAudio::Pcm],
                    ..TranscriptionCapabilities::default()
                }),
                NetworkBehavior::Never,
            ),
        )]);
        snapshot
            .source_reports
            .push(snapshot.source_reports[0].clone());
        assert!(matches!(
            SpeechRouter.plan_transcription(&transcription(), &snapshot),
            Err(SpeechRouteError::SnapshotInvalid { .. })
        ));
    }

    #[test]
    fn invalid_requests_are_rejected_before_route_selection() {
        let mut request = transcription();
        request.context.client_id.clear();
        let error = SpeechRouter
            .plan_transcription(&request, &snapshot(Vec::new()))
            .expect_err("an invalid request must fail before route selection");
        assert_eq!(
            error,
            SpeechRouteError::RequestInvalid {
                code: "client_id_empty".to_string(),
                detail: "client_id must not be empty".to_string(),
            }
        );
    }
}
