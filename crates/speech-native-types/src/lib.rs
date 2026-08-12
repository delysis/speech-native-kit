//! Protocol-neutral speech contracts shared by native platform adapters,
//! embedded speech models, hosted providers, loopback protocols, and Tauri.
//!
//! Transcription and synthesis intentionally have different request and event
//! types. A backend cannot claim that generic "speech" support implies
//! streaming ASR, timestamps, SSML, or audio-buffer synthesis.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

mod task_supervisor;

pub use task_supervisor::{
    SupervisedTaskFailure, SupervisedTaskFailureKind, SupervisedTaskFailureSummary, TaskSupervisor,
    TaskSupervisorError, TaskSupervisorSnapshot,
};

pub const SPEECH_CAPABILITY_SCHEMA: &str = "fte.speech.capabilities.v1";
pub const DEFAULT_SPEECH_EVENT_CAPACITY: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[serde(transparent)]
pub struct SpeechRequestId(pub String);

impl SpeechRequestId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for SpeechRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SpeechRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechRequestContext {
    pub request_id: SpeechRequestId,
    pub client_id: String,
    #[serde(default)]
    pub route: SpeechRouteSelector,
    #[serde(default)]
    pub routing: SpeechRoutingPolicy,
    #[serde(default)]
    pub deadline: SpeechDeadlinePolicy,
}

impl SpeechRequestContext {
    fn validate(&self) -> Result<(), SpeechError> {
        if self.request_id.0.trim().is_empty() {
            return Err(SpeechError::invalid_request(
                &self.request_id,
                "request_id_empty",
                "request_id must not be empty",
            ));
        }
        if self.client_id.trim().is_empty() {
            return Err(SpeechError::invalid_request(
                &self.request_id,
                "client_id_empty",
                "client_id must not be empty",
            ));
        }
        if [
            self.deadline.queue_ms,
            self.deadline.model_load_ms,
            self.deadline.first_result_ms,
            self.deadline.idle_stream_ms,
            self.deadline.total_ms,
        ]
        .into_iter()
        .flatten()
        .any(|value| value == 0)
        {
            return Err(SpeechError::invalid_request(
                &self.request_id,
                "deadline_invalid",
                "configured speech deadlines must be greater than zero",
            ));
        }
        match &self.route {
            SpeechRouteSelector::ExactBackend {
                backend_id,
                model_id,
                voice_id,
            } => {
                if backend_id.trim().is_empty() {
                    return Err(SpeechError::invalid_request(
                        &self.request_id,
                        "backend_id_empty",
                        "an exact backend route must name a backend",
                    ));
                }
                if model_id
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty())
                {
                    return Err(SpeechError::invalid_request(
                        &self.request_id,
                        "model_id_empty",
                        "an exact model selection must name a model",
                    ));
                }
                if voice_id
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty())
                {
                    return Err(SpeechError::invalid_request(
                        &self.request_id,
                        "route_voice_id_empty",
                        "an exact route voice selection must name a voice",
                    ));
                }
            }
            SpeechRouteSelector::Profile { name } if name.trim().is_empty() => {
                return Err(SpeechError::invalid_request(
                    &self.request_id,
                    "route_profile_empty",
                    "a speech route profile must have a name",
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpeechRouteSelector {
    #[default]
    Auto,
    ExactBackend {
        backend_id: String,
        model_id: Option<String>,
        voice_id: Option<String>,
    },
    Profile {
        name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TranscriptionRequest {
    pub context: SpeechRequestContext,
    pub input: TranscriptionInput,
    pub language: Option<String>,
    #[serde(default)]
    pub task: TranscriptionTask,
    #[serde(default)]
    pub timestamps: TimestampGranularity,
    #[serde(default)]
    pub diarization: DiarizationPolicy,
    #[serde(default)]
    pub partial_results: bool,
    #[serde(default = "default_true")]
    pub punctuation: bool,
    #[serde(default)]
    pub hotwords: Vec<String>,
}

impl TranscriptionRequest {
    pub fn validate(&self) -> Result<(), SpeechError> {
        self.context.validate()?;
        match &self.input {
            TranscriptionInput::Complete { audio } => audio.validate(&self.context.request_id)?,
            TranscriptionInput::Stream { stream_id, format } => {
                if stream_id.trim().is_empty() {
                    return Err(SpeechError::invalid_request(
                        &self.context.request_id,
                        "audio_stream_id_empty",
                        "stream_id must not be empty",
                    ));
                }
                format.validate(&self.context.request_id)?;
            }
        }
        validate_language(self.language.as_deref(), &self.context.request_id)?;
        if self.hotwords.iter().any(|word| word.trim().is_empty()) {
            return Err(SpeechError::invalid_request(
                &self.context.request_id,
                "hotword_empty",
                "hotwords must not contain empty values",
            ));
        }
        if matches!(
            self.diarization,
            DiarizationPolicy::Bounded { max_speakers: 0 }
        ) {
            return Err(SpeechError::invalid_request(
                &self.context.request_id,
                "diarization_speaker_limit_invalid",
                "bounded diarization must allow at least one speaker",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TranscriptionInput {
    Complete {
        audio: AudioInput,
    },
    Stream {
        stream_id: String,
        format: PcmFormat,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptionTask {
    #[default]
    Transcribe,
    TranslateToEnglish,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampGranularity {
    #[default]
    None,
    Segment,
    Word,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DiarizationPolicy {
    #[default]
    Disabled,
    Auto,
    Bounded {
        max_speakers: u8,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SynthesisRequest {
    pub context: SpeechRequestContext,
    pub input: SynthesisInput,
    #[serde(default)]
    pub voice: VoiceSelector,
    pub language: Option<String>,
    #[serde(default = "default_rate")]
    pub rate: f32,
    #[serde(default = "default_pitch")]
    pub pitch: f32,
    #[serde(default = "default_volume")]
    pub volume: f32,
    #[serde(default)]
    pub output: AudioOutputFormat,
    #[serde(default)]
    pub alignment: AlignmentGranularity,
    #[serde(default = "default_true")]
    pub stream: bool,
}

impl SynthesisRequest {
    pub fn validate(&self) -> Result<(), SpeechError> {
        self.context.validate()?;
        let text = match &self.input {
            SynthesisInput::Text { text } | SynthesisInput::Ssml { ssml: text } => text,
        };
        if text.trim().is_empty() {
            return Err(SpeechError::invalid_request(
                &self.context.request_id,
                "synthesis_input_empty",
                "synthesis input must not be empty",
            ));
        }
        validate_language(self.language.as_deref(), &self.context.request_id)?;
        if !self.rate.is_finite() || self.rate <= 0.0 {
            return Err(SpeechError::invalid_request(
                &self.context.request_id,
                "speech_rate_invalid",
                "speech rate must be finite and greater than zero",
            ));
        }
        if !self.pitch.is_finite() || self.pitch <= 0.0 {
            return Err(SpeechError::invalid_request(
                &self.context.request_id,
                "speech_pitch_invalid",
                "speech pitch must be finite and greater than zero",
            ));
        }
        if !self.volume.is_finite() || !(0.0..=1.0).contains(&self.volume) {
            return Err(SpeechError::invalid_request(
                &self.context.request_id,
                "speech_volume_invalid",
                "speech volume must be between zero and one",
            ));
        }
        match &self.voice {
            VoiceSelector::Exact { voice_id } if voice_id.trim().is_empty() => {
                return Err(SpeechError::invalid_request(
                    &self.context.request_id,
                    "voice_id_empty",
                    "an exact voice selection must name a voice",
                ));
            }
            VoiceSelector::Profile { name } if name.trim().is_empty() => {
                return Err(SpeechError::invalid_request(
                    &self.context.request_id,
                    "voice_profile_empty",
                    "a voice profile must have a name",
                ));
            }
            _ => {}
        }
        self.output.validate(&self.context.request_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SynthesisInput {
    Text { text: String },
    Ssml { ssml: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VoiceSelector {
    #[default]
    Auto,
    Exact {
        voice_id: String,
    },
    Profile {
        name: String,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlignmentGranularity {
    #[default]
    None,
    Word,
    Phoneme,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AudioInput {
    Encoded {
        format: EncodedAudioFormat,
        data: Vec<u8>,
    },
    Pcm {
        format: PcmFormat,
        data: Vec<u8>,
    },
    Asset {
        asset_id: String,
        format_hint: Option<EncodedAudioFormat>,
    },
}

impl AudioInput {
    fn validate(&self, request_id: &SpeechRequestId) -> Result<(), SpeechError> {
        match self {
            Self::Encoded { data, .. } => validate_audio_bytes(data, request_id),
            Self::Pcm { format, data } => {
                format.validate(request_id)?;
                validate_pcm_bytes(format, data, request_id)
            }
            Self::Asset { asset_id, .. } if asset_id.trim().is_empty() => {
                Err(SpeechError::invalid_request(
                    request_id,
                    "audio_asset_id_empty",
                    "asset_id must not be empty",
                ))
            }
            Self::Asset { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EncodedAudioFormat {
    Wav,
    Flac,
    Mp3,
    M4a,
    OggOpus,
    WebmOpus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PcmFormat {
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub sample_format: PcmSampleFormat,
    #[serde(default)]
    pub interleaved: bool,
}

impl PcmFormat {
    fn validate(&self, request_id: &SpeechRequestId) -> Result<(), SpeechError> {
        if !(8_000..=384_000).contains(&self.sample_rate_hz) {
            return Err(SpeechError::invalid_request(
                request_id,
                "sample_rate_invalid",
                "sample_rate_hz must be between 8000 and 384000",
            ));
        }
        if !(1..=32).contains(&self.channels) {
            return Err(SpeechError::invalid_request(
                request_id,
                "channel_count_invalid",
                "channels must be between 1 and 32",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn bytes_per_frame(&self) -> usize {
        let bytes_per_sample = match self.sample_format {
            PcmSampleFormat::I16Le => 2,
            PcmSampleFormat::I24Le => 3,
            PcmSampleFormat::I32Le | PcmSampleFormat::F32Le => 4,
        };
        bytes_per_sample * self.channels as usize
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PcmSampleFormat {
    I16Le,
    I24Le,
    I32Le,
    F32Le,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AudioOutputFormat {
    #[default]
    Wav,
    Pcm {
        format: PcmFormat,
    },
    Mp3 {
        bitrate_kbps: Option<u16>,
    },
    OggOpus {
        bitrate_kbps: Option<u16>,
    },
}

impl AudioOutputFormat {
    fn validate(&self, request_id: &SpeechRequestId) -> Result<(), SpeechError> {
        match self {
            Self::Pcm { format } => format.validate(request_id),
            Self::Mp3 {
                bitrate_kbps: Some(0),
            }
            | Self::OggOpus {
                bitrate_kbps: Some(0),
            } => Err(SpeechError::invalid_request(
                request_id,
                "audio_bitrate_invalid",
                "audio bitrate must be greater than zero",
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AudioChunk {
    pub sequence: u64,
    pub sample_offset: u64,
    pub format: PcmFormat,
    pub data: Vec<u8>,
    #[serde(default)]
    pub end_of_stream: bool,
}

impl AudioChunk {
    pub fn validate(&self, request_id: &SpeechRequestId) -> Result<(), SpeechError> {
        self.format.validate(request_id)?;
        if self.data.is_empty() && self.end_of_stream {
            return Ok(());
        }
        validate_pcm_bytes(&self.format, &self.data, request_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechRoutingPolicy {
    #[serde(default)]
    pub privacy: SpeechPrivacyPolicy,
    #[serde(default)]
    pub profile: SpeechRouteProfile,
    #[serde(default)]
    pub allow_asset_download: bool,
    #[serde(default)]
    pub allow_fallback_before_output: bool,
}

impl Default for SpeechRoutingPolicy {
    fn default() -> Self {
        Self {
            privacy: SpeechPrivacyPolicy::LocalOnly,
            profile: SpeechRouteProfile::PrivateBalanced,
            allow_asset_download: false,
            allow_fallback_before_output: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpeechPrivacyPolicy {
    #[default]
    LocalOnly,
    HostedAllowed,
    HostedOnly,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpeechRouteProfile {
    #[default]
    PrivateBalanced,
    NativePreferred,
    ConsistentLocal,
    QualityLocal,
    HostedEnabled,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechDeadlinePolicy {
    pub queue_ms: Option<u64>,
    pub model_load_ms: Option<u64>,
    pub first_result_ms: Option<u64>,
    pub idle_stream_ms: Option<u64>,
    pub total_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TranscriptionResponse {
    pub request_id: SpeechRequestId,
    pub route: SpeechResolvedRoute,
    pub text: String,
    pub language: Option<String>,
    pub segments: Vec<TranscriptSegment>,
    pub usage: SpeechUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SynthesisResponse {
    pub request_id: SpeechRequestId,
    pub route: SpeechResolvedRoute,
    pub audio: Vec<u8>,
    pub format: AudioOutputFormat,
    pub duration_ms: Option<u64>,
    pub alignments: Vec<SpeechAlignment>,
    pub usage: SpeechUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TranscriptSegment {
    pub index: u32,
    pub text: String,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    pub speaker: Option<String>,
    pub confidence: Option<f32>,
    #[serde(default)]
    pub is_final: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechAlignment {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub kind: AlignmentGranularity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TranscriptionEvent {
    Started {
        request_id: SpeechRequestId,
        route: SpeechResolvedRoute,
    },
    Partial {
        request_id: SpeechRequestId,
        sequence: u64,
        text: String,
    },
    Segment {
        request_id: SpeechRequestId,
        sequence: u64,
        segment: TranscriptSegment,
    },
    UsageUpdated {
        request_id: SpeechRequestId,
        usage: SpeechUsage,
    },
    Warning {
        request_id: SpeechRequestId,
        code: String,
        message: String,
    },
    Completed {
        request_id: SpeechRequestId,
        response: TranscriptionResponse,
    },
    Cancelled {
        request_id: SpeechRequestId,
        usage: SpeechUsage,
    },
    Failed {
        request_id: SpeechRequestId,
        error: SpeechError,
    },
}

impl TranscriptionEvent {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Cancelled { .. } | Self::Failed { .. }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SynthesisEvent {
    Started {
        request_id: SpeechRequestId,
        route: SpeechResolvedRoute,
    },
    Audio {
        request_id: SpeechRequestId,
        chunk: AudioChunk,
    },
    Alignment {
        request_id: SpeechRequestId,
        sequence: u64,
        alignment: SpeechAlignment,
    },
    UsageUpdated {
        request_id: SpeechRequestId,
        usage: SpeechUsage,
    },
    Warning {
        request_id: SpeechRequestId,
        code: String,
        message: String,
    },
    Completed {
        request_id: SpeechRequestId,
        response: SynthesisResponse,
    },
    Cancelled {
        request_id: SpeechRequestId,
        usage: SpeechUsage,
    },
    Failed {
        request_id: SpeechRequestId,
        error: SpeechError,
    },
}

impl SynthesisEvent {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Cancelled { .. } | Self::Failed { .. }
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SpeechUsage {
    pub input_audio_ms: Option<u64>,
    pub output_audio_ms: Option<u64>,
    pub input_characters: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub queue_ms: Option<u64>,
    pub model_load_ms: Option<u64>,
    pub time_to_first_result_ms: Option<u64>,
    pub total_ms: Option<u64>,
    #[serde(default)]
    pub provenance: UsageProvenance,
    #[serde(default)]
    pub real_local_inference: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageProvenance {
    Exact,
    Estimated,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechResolvedRoute {
    pub backend_id: String,
    pub model_id: Option<String>,
    pub voice_id: Option<String>,
    pub backend_kind: SpeechBackendKind,
    pub network: NetworkBehavior,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechBackendDescriptor {
    pub id: String,
    pub display_name: String,
    pub kind: SpeechBackendKind,
    pub readiness: SpeechBackendReadiness,
    #[serde(default)]
    pub capabilities: Vec<SpeechCapability>,
    #[serde(default)]
    pub models: Vec<SpeechModelDescriptor>,
    #[serde(default)]
    pub voices: Vec<VoiceDescriptor>,
}

impl SpeechBackendDescriptor {
    pub fn validate(&self) -> Result<(), CapabilityValidationError> {
        if !valid_identifier(&self.id) {
            return Err(CapabilityValidationError::IdentifierInvalid {
                field: "backend.id".to_string(),
                value: self.id.clone(),
            });
        }
        for capability in &self.capabilities {
            capability.validate()?;
            if capability.backend_id != self.id {
                return Err(CapabilityValidationError::BackendOwnerMismatch {
                    backend_id: self.id.clone(),
                    capability_backend_id: capability.backend_id.clone(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SpeechBackendKind {
    PlatformOnDevice,
    PlatformService,
    EmbeddedModel,
    ResidentMultimodalModel,
    Hosted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SpeechBackendReadiness {
    Ready,
    AssetInstallRequired { assets: Vec<SpeechAsset> },
    PermissionRequired { permissions: Vec<SpeechPermission> },
    NotConfigured { reason: String },
    Unavailable { reason: String },
    Unknown { reason: String },
}

impl SpeechBackendReadiness {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechAsset {
    pub id: String,
    pub display_name: String,
    pub bytes: Option<u64>,
    pub managed_by: AssetManager,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AssetManager {
    OperatingSystem,
    HuggingFaceCache,
    Application,
    Provider,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpeechPermission {
    Microphone,
    SpeechRecognition,
    Network,
    FileRead,
    FileWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechModelDescriptor {
    pub id: String,
    pub display_name: String,
    pub family: String,
    #[serde(default)]
    pub languages: Vec<String>,
    pub resident: bool,
    pub estimated_memory_bytes: Option<u64>,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoiceDescriptor {
    pub id: String,
    pub name: String,
    pub language: String,
    pub gender: Option<VoiceGender>,
    pub quality: Option<VoiceQuality>,
    pub expected_latency: Option<LatencyClass>,
    pub network: NetworkBehavior,
    pub installed: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VoiceGender {
    Female,
    Male,
    Neutral,
    Unspecified,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum VoiceQuality {
    Basic,
    Normal,
    Enhanced,
    Premium,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum LatencyClass {
    VeryLow,
    Low,
    Normal,
    High,
    VeryHigh,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechCapability {
    pub id: String,
    pub backend_id: String,
    pub model_id: Option<String>,
    pub operation: SpeechOperationCapability,
    pub availability: CapabilityAvailability,
    pub network: NetworkBehavior,
    #[serde(default)]
    pub languages: Vec<String>,
    pub limits: SpeechCapabilityLimits,
    #[serde(default)]
    pub evidence: Vec<CapabilityEvidence>,
}

impl SpeechCapability {
    pub fn validate(&self) -> Result<(), CapabilityValidationError> {
        if !valid_identifier(&self.id) {
            return Err(CapabilityValidationError::IdentifierInvalid {
                field: "capability.id".to_string(),
                value: self.id.clone(),
            });
        }
        if !valid_identifier(&self.backend_id) {
            return Err(CapabilityValidationError::IdentifierInvalid {
                field: "capability.backend_id".to_string(),
                value: self.backend_id.clone(),
            });
        }
        if self
            .languages
            .iter()
            .any(|language| language.trim().is_empty())
        {
            return Err(CapabilityValidationError::LanguageEmpty);
        }
        if self
            .evidence
            .iter()
            .any(|evidence| evidence.source_id.trim().is_empty())
        {
            return Err(CapabilityValidationError::EvidenceSourceEmpty);
        }
        Ok(())
    }

    /// A local-only route requires a ready capability, an explicit never-network
    /// assertion, and runtime or real-smoke evidence. Build-target detection and
    /// documentation alone can never promote a backend into a private route.
    #[must_use]
    pub fn eligible_for_local_only(&self) -> bool {
        self.availability == CapabilityAvailability::Available
            && self.network == NetworkBehavior::Never
            && self.evidence.iter().any(CapabilityEvidence::proves_runtime)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpeechOperationCapability {
    Transcription(TranscriptionCapabilities),
    Synthesis(SynthesisCapabilities),
}

impl SpeechOperationCapability {
    #[must_use]
    pub const fn kind(&self) -> SpeechOperationKind {
        match self {
            Self::Transcription(_) => SpeechOperationKind::Transcription,
            Self::Synthesis(_) => SpeechOperationKind::Synthesis,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SpeechOperationKind {
    Transcription,
    Synthesis,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TranscriptionCapabilities {
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub partial_results: bool,
    #[serde(default)]
    pub segment_timestamps: bool,
    #[serde(default)]
    pub word_timestamps: bool,
    #[serde(default)]
    pub diarization: bool,
    #[serde(default)]
    pub translation_to_english: bool,
    #[serde(default)]
    pub long_form: bool,
    #[serde(default)]
    pub hotwords: bool,
    #[serde(default)]
    pub generative: bool,
    #[serde(default)]
    pub accepted_audio: Vec<AcceptedAudio>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SynthesisCapabilities {
    #[serde(default)]
    pub streaming_audio: bool,
    #[serde(default)]
    pub ssml: bool,
    #[serde(default)]
    pub word_alignment: bool,
    #[serde(default)]
    pub phoneme_alignment: bool,
    #[serde(default)]
    pub pause_resume: bool,
    #[serde(default)]
    pub voice_selection: bool,
    #[serde(default)]
    pub returned_audio: Vec<AudioOutputKind>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AcceptedAudio {
    Pcm,
    Wav,
    Flac,
    Mp3,
    M4a,
    OggOpus,
    WebmOpus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AudioOutputKind {
    Pcm,
    Wav,
    Mp3,
    OggOpus,
    DirectPlayback,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechCapabilityLimits {
    pub max_audio_ms: Option<u64>,
    pub max_input_characters: Option<u64>,
    pub max_concurrent_requests: Option<u32>,
    pub max_speakers: Option<u8>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityAvailability {
    Available,
    AssetInstallRequired,
    PermissionRequired,
    Unavailable,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkBehavior {
    Never,
    Optional,
    Required,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityEvidence {
    pub source_id: String,
    pub source_version: Option<String>,
    pub kind: EvidenceKind,
    pub outcome: EvidenceOutcome,
    pub observed_at_unix_ms: u64,
    pub detail: String,
}

impl CapabilityEvidence {
    #[must_use]
    pub fn proves_runtime(&self) -> bool {
        self.outcome == EvidenceOutcome::Confirmed
            && matches!(
                self.kind,
                EvidenceKind::RuntimeApi | EvidenceKind::RealSmoke
            )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Documentation,
    BuildTarget,
    RuntimeApi,
    SystemInventory,
    RealSmoke,
    UserConfiguration,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceOutcome {
    Confirmed,
    Rejected,
    Inconclusive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformTarget {
    pub family: PlatformFamily,
    pub os_version: Option<String>,
    pub architecture: String,
    pub application_identity: ApplicationIdentity,
}

impl PlatformTarget {
    #[must_use]
    pub fn current() -> Self {
        Self {
            family: current_platform_family(),
            os_version: None,
            architecture: std::env::consts::ARCH.to_string(),
            application_identity: ApplicationIdentity::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum PlatformFamily {
    MacOs,
    Ios,
    Windows,
    Android,
    Linux,
    Other(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationIdentity {
    Packaged,
    Unpackaged,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformAdapterCandidate {
    pub id: String,
    pub display_name: String,
    pub operations: Vec<SpeechOperationKind>,
    pub requires_runtime_probe: bool,
    pub privacy_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlatformCapabilitySnapshot {
    pub schema: String,
    pub captured_at_unix_ms: u64,
    pub target: PlatformTarget,
    pub adapter_candidates: Vec<PlatformAdapterCandidate>,
    pub source_reports: Vec<CapabilitySourceReport>,
}

impl PlatformCapabilitySnapshot {
    #[must_use]
    pub fn local_only_capabilities(&self) -> Vec<&SpeechCapability> {
        self.source_reports
            .iter()
            .filter(|report| report.status == ProbeSourceStatus::Succeeded)
            .flat_map(|report| &report.backends)
            .filter(|backend| {
                backend.readiness.is_ready() && backend.kind != SpeechBackendKind::Hosted
            })
            .flat_map(|backend| &backend.capabilities)
            .filter(|capability| capability.eligible_for_local_only())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilitySourceReport {
    pub source_id: String,
    pub status: ProbeSourceStatus,
    pub detail: Option<String>,
    #[serde(default)]
    pub backends: Vec<SpeechBackendDescriptor>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProbeSourceStatus {
    Succeeded,
    Failed,
    TimedOut,
}

#[derive(Debug, thiserror::Error, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CapabilityValidationError {
    #[error("{field} contains an invalid identifier: {value}")]
    IdentifierInvalid { field: String, value: String },
    #[error("capability backend {capability_backend_id} does not match {backend_id}")]
    BackendOwnerMismatch {
        backend_id: String,
        capability_backend_id: String,
    },
    #[error("capability languages must not contain empty values")]
    LanguageEmpty,
    #[error("capability evidence must name its source")]
    EvidenceSourceEmpty,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechError {
    pub code: String,
    pub class: SpeechErrorClass,
    pub retryable: bool,
    pub request_id: SpeechRequestId,
    pub backend_id: Option<String>,
    pub safe_detail: String,
}

impl SpeechError {
    #[must_use]
    pub fn invalid_request(request_id: &SpeechRequestId, code: &str, detail: &str) -> Self {
        Self {
            code: code.to_string(),
            class: SpeechErrorClass::InvalidRequest,
            retryable: false,
            request_id: request_id.clone(),
            backend_id: None,
            safe_detail: detail.to_string(),
        }
    }

    #[must_use]
    pub fn unavailable(request_id: &SpeechRequestId, code: &str, detail: &str) -> Self {
        Self {
            code: code.to_string(),
            class: SpeechErrorClass::Unavailable,
            retryable: true,
            request_id: request_id.clone(),
            backend_id: None,
            safe_detail: detail.to_string(),
        }
    }
}

impl fmt::Display for SpeechError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.safe_detail)
    }
}

impl std::error::Error for SpeechError {}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SpeechErrorClass {
    InvalidRequest,
    Capability,
    Privacy,
    Permission,
    AssetMissing,
    Unavailable,
    Timeout,
    Cancelled,
    Internal,
}

pub trait SpeechCancellation: Send + Sync {
    fn cancel(&self, request_id: &SpeechRequestId) -> usize;
}

/// Backpressured audio input owned by a streaming transcription ticket. The
/// sink must reject chunks after `finish` and must preserve chunk order.
#[async_trait]
pub trait TranscriptionAudioSink: Send + Sync {
    async fn push(&self, chunk: AudioChunk) -> Result<(), SpeechError>;
    async fn finish(&self) -> Result<(), SpeechError>;
}

pub struct TranscriptionTicket {
    pub request_id: SpeechRequestId,
    pub events: mpsc::Receiver<TranscriptionEvent>,
    pub audio_sink: Option<Arc<dyn TranscriptionAudioSink>>,
    final_receiver: Option<oneshot::Receiver<Result<TranscriptionResponse, SpeechError>>>,
    cancellation: Option<Arc<dyn SpeechCancellation>>,
}

impl TranscriptionTicket {
    #[must_use]
    pub fn new(
        request_id: SpeechRequestId,
        events: mpsc::Receiver<TranscriptionEvent>,
        final_receiver: oneshot::Receiver<Result<TranscriptionResponse, SpeechError>>,
        cancellation: Arc<dyn SpeechCancellation>,
        audio_sink: Option<Arc<dyn TranscriptionAudioSink>>,
    ) -> Self {
        Self {
            request_id,
            events,
            audio_sink,
            final_receiver: Some(final_receiver),
            cancellation: Some(cancellation),
        }
    }

    pub async fn final_response(mut self) -> Result<TranscriptionResponse, SpeechError> {
        let receiver = self.final_receiver.take().ok_or_else(|| {
            SpeechError::unavailable(
                &self.request_id,
                "transcription_final_missing",
                "the transcription final response channel is unavailable",
            )
        })?;
        let result = receiver.await.map_err(|_| {
            SpeechError::unavailable(
                &self.request_id,
                "transcription_backend_closed",
                "the transcription backend closed without a final response",
            )
        })?;
        self.cancellation = None;
        result
    }
}

impl Drop for TranscriptionTicket {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.cancellation {
            cancellation.cancel(&self.request_id);
        }
    }
}

pub struct SynthesisTicket {
    pub request_id: SpeechRequestId,
    pub events: mpsc::Receiver<SynthesisEvent>,
    final_receiver: Option<oneshot::Receiver<Result<SynthesisResponse, SpeechError>>>,
    cancellation: Option<Arc<dyn SpeechCancellation>>,
}

impl SynthesisTicket {
    #[must_use]
    pub fn new(
        request_id: SpeechRequestId,
        events: mpsc::Receiver<SynthesisEvent>,
        final_receiver: oneshot::Receiver<Result<SynthesisResponse, SpeechError>>,
        cancellation: Arc<dyn SpeechCancellation>,
    ) -> Self {
        Self {
            request_id,
            events,
            final_receiver: Some(final_receiver),
            cancellation: Some(cancellation),
        }
    }

    pub async fn final_response(mut self) -> Result<SynthesisResponse, SpeechError> {
        let receiver = self.final_receiver.take().ok_or_else(|| {
            SpeechError::unavailable(
                &self.request_id,
                "synthesis_final_missing",
                "the synthesis final response channel is unavailable",
            )
        })?;
        let result = receiver.await.map_err(|_| {
            SpeechError::unavailable(
                &self.request_id,
                "synthesis_backend_closed",
                "the synthesis backend closed without a final response",
            )
        })?;
        self.cancellation = None;
        result
    }
}

impl Drop for SynthesisTicket {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.cancellation {
            cancellation.cancel(&self.request_id);
        }
    }
}

#[async_trait]
pub trait SpeechBackend: Send + Sync {
    fn descriptor(&self) -> SpeechBackendDescriptor;
    fn readiness(&self) -> SpeechBackendReadiness;
    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> Result<TranscriptionTicket, SpeechError>;
    async fn synthesize(&self, request: SynthesisRequest) -> Result<SynthesisTicket, SpeechError>;
    fn cancel(&self, request_id: &SpeechRequestId) -> usize;
    async fn shutdown(&self) -> Result<(), SpeechError>;
}

fn validate_audio_bytes(data: &[u8], request_id: &SpeechRequestId) -> Result<(), SpeechError> {
    if data.is_empty() {
        return Err(SpeechError::invalid_request(
            request_id,
            "audio_data_empty",
            "audio data must not be empty",
        ));
    }
    Ok(())
}

fn validate_pcm_bytes(
    format: &PcmFormat,
    data: &[u8],
    request_id: &SpeechRequestId,
) -> Result<(), SpeechError> {
    validate_audio_bytes(data, request_id)?;
    if !data.len().is_multiple_of(format.bytes_per_frame()) {
        return Err(SpeechError::invalid_request(
            request_id,
            "pcm_frame_incomplete",
            "PCM data must contain only complete audio frames",
        ));
    }
    Ok(())
}

fn validate_language(
    language: Option<&str>,
    request_id: &SpeechRequestId,
) -> Result<(), SpeechError> {
    if language.is_some_and(|language| {
        language.trim().is_empty()
            || !language
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '-')
    }) {
        return Err(SpeechError::invalid_request(
            request_id,
            "language_tag_invalid",
            "language must be a non-empty BCP-47-style tag",
        ));
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
        })
}

const fn default_true() -> bool {
    true
}

const fn default_rate() -> f32 {
    1.0
}

const fn default_pitch() -> f32 {
    1.0
}

const fn default_volume() -> f32 {
    1.0
}

fn current_platform_family() -> PlatformFamily {
    #[cfg(target_os = "macos")]
    {
        PlatformFamily::MacOs
    }
    #[cfg(target_os = "ios")]
    {
        PlatformFamily::Ios
    }
    #[cfg(target_os = "windows")]
    {
        PlatformFamily::Windows
    }
    #[cfg(target_os = "android")]
    {
        PlatformFamily::Android
    }
    #[cfg(target_os = "linux")]
    {
        PlatformFamily::Linux
    }
    #[cfg(not(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "windows",
        target_os = "android",
        target_os = "linux"
    )))]
    {
        PlatformFamily::Other(std::env::consts::OS.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> SpeechRequestContext {
        SpeechRequestContext {
            request_id: SpeechRequestId("request-1".to_string()),
            client_id: "test-client".to_string(),
            route: SpeechRouteSelector::Auto,
            routing: SpeechRoutingPolicy::default(),
            deadline: SpeechDeadlinePolicy::default(),
        }
    }

    fn capability(evidence: EvidenceKind, network: NetworkBehavior) -> SpeechCapability {
        SpeechCapability {
            id: "apple.transcription.en-us".to_string(),
            backend_id: "apple.speech-analyzer".to_string(),
            model_id: None,
            operation: SpeechOperationCapability::Transcription(
                TranscriptionCapabilities::default(),
            ),
            availability: CapabilityAvailability::Available,
            network,
            languages: vec!["en-US".to_string()],
            limits: SpeechCapabilityLimits::default(),
            evidence: vec![CapabilityEvidence {
                source_id: "apple-runtime".to_string(),
                source_version: Some("1".to_string()),
                kind: evidence,
                outcome: EvidenceOutcome::Confirmed,
                observed_at_unix_ms: 1,
                detail: "fixture".to_string(),
            }],
        }
    }

    #[test]
    fn private_route_requires_runtime_evidence_and_never_network() {
        assert!(
            capability(EvidenceKind::RuntimeApi, NetworkBehavior::Never).eligible_for_local_only()
        );
        assert!(
            !capability(EvidenceKind::BuildTarget, NetworkBehavior::Never)
                .eligible_for_local_only()
        );
        assert!(
            !capability(EvidenceKind::RuntimeApi, NetworkBehavior::Unknown)
                .eligible_for_local_only()
        );
        assert!(
            !capability(EvidenceKind::RuntimeApi, NetworkBehavior::Optional)
                .eligible_for_local_only()
        );
    }

    #[test]
    fn empty_audio_fails_before_backend_execution() {
        let request = TranscriptionRequest {
            context: context(),
            input: TranscriptionInput::Complete {
                audio: AudioInput::Encoded {
                    format: EncodedAudioFormat::Wav,
                    data: Vec::new(),
                },
            },
            language: Some("en-US".to_string()),
            task: TranscriptionTask::Transcribe,
            timestamps: TimestampGranularity::None,
            diarization: DiarizationPolicy::Disabled,
            partial_results: false,
            punctuation: true,
            hotwords: Vec::new(),
        };
        let error = request.validate().expect_err("empty audio must fail");
        assert_eq!(error.code, "audio_data_empty");
    }

    #[test]
    fn synthesis_defaults_are_private_and_valid() {
        let request = SynthesisRequest {
            context: context(),
            input: SynthesisInput::Text {
                text: "Hello".to_string(),
            },
            voice: VoiceSelector::Auto,
            language: Some("en-US".to_string()),
            rate: default_rate(),
            pitch: default_pitch(),
            volume: default_volume(),
            output: AudioOutputFormat::Wav,
            alignment: AlignmentGranularity::None,
            stream: true,
        };
        request.validate().expect("valid synthesis request");
        assert_eq!(
            request.context.routing.privacy,
            SpeechPrivacyPolicy::LocalOnly
        );
    }

    #[test]
    fn exact_routes_reject_empty_backend_model_and_voice_ids() {
        for route in [
            SpeechRouteSelector::ExactBackend {
                backend_id: String::new(),
                model_id: None,
                voice_id: None,
            },
            SpeechRouteSelector::ExactBackend {
                backend_id: "embedded.parakeet-asr".to_string(),
                model_id: Some(" ".to_string()),
                voice_id: None,
            },
            SpeechRouteSelector::ExactBackend {
                backend_id: "embedded.kokoro-tts".to_string(),
                model_id: None,
                voice_id: Some(String::new()),
            },
        ] {
            let mut context = context();
            context.route = route;
            let request = SynthesisRequest {
                context,
                input: SynthesisInput::Text {
                    text: "Hello".to_string(),
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
            request.validate().expect_err("empty route id must fail");
        }
    }

    #[test]
    fn event_lifecycles_have_explicit_terminal_states() {
        let request_id = SpeechRequestId("request-1".to_string());
        let event = TranscriptionEvent::Cancelled {
            request_id,
            usage: SpeechUsage::default(),
        };
        assert!(event.is_terminal());
    }

    #[test]
    fn pcm_input_rejects_partial_frames() {
        let request = TranscriptionRequest {
            context: context(),
            input: TranscriptionInput::Complete {
                audio: AudioInput::Pcm {
                    format: PcmFormat {
                        sample_rate_hz: 16_000,
                        channels: 2,
                        sample_format: PcmSampleFormat::I16Le,
                        interleaved: true,
                    },
                    data: vec![0; 3],
                },
            },
            language: None,
            task: TranscriptionTask::Transcribe,
            timestamps: TimestampGranularity::None,
            diarization: DiarizationPolicy::Disabled,
            partial_results: false,
            punctuation: true,
            hotwords: Vec::new(),
        };
        let error = request.validate().expect_err("partial PCM frame must fail");
        assert_eq!(error.code, "pcm_frame_incomplete");
    }

    #[test]
    fn hosted_backends_never_enter_local_only_projection() {
        let mut hosted = SpeechBackendDescriptor {
            id: "hosted.fixture".to_string(),
            display_name: "Hosted fixture".to_string(),
            kind: SpeechBackendKind::Hosted,
            readiness: SpeechBackendReadiness::Ready,
            capabilities: vec![capability(EvidenceKind::RuntimeApi, NetworkBehavior::Never)],
            models: Vec::new(),
            voices: Vec::new(),
        };
        hosted.capabilities[0].backend_id = hosted.id.clone();
        hosted.capabilities[0].id = "hosted.fixture.transcription".to_string();
        let snapshot = PlatformCapabilitySnapshot {
            schema: SPEECH_CAPABILITY_SCHEMA.to_string(),
            captured_at_unix_ms: 1,
            target: PlatformTarget::current(),
            adapter_candidates: Vec::new(),
            source_reports: vec![CapabilitySourceReport {
                source_id: "fixture".to_string(),
                status: ProbeSourceStatus::Succeeded,
                detail: None,
                backends: vec![hosted],
            }],
        };
        assert!(snapshot.local_only_capabilities().is_empty());
    }

    #[test]
    fn capability_snapshot_round_trips_without_losing_privacy_evidence() {
        let snapshot = PlatformCapabilitySnapshot {
            schema: SPEECH_CAPABILITY_SCHEMA.to_string(),
            captured_at_unix_ms: 1,
            target: PlatformTarget::current(),
            adapter_candidates: Vec::new(),
            source_reports: vec![CapabilitySourceReport {
                source_id: "fixture".to_string(),
                status: ProbeSourceStatus::Succeeded,
                detail: None,
                backends: vec![SpeechBackendDescriptor {
                    id: "apple.speech-analyzer".to_string(),
                    display_name: "Apple SpeechAnalyzer".to_string(),
                    kind: SpeechBackendKind::PlatformOnDevice,
                    readiness: SpeechBackendReadiness::Ready,
                    capabilities: vec![capability(
                        EvidenceKind::RuntimeApi,
                        NetworkBehavior::Never,
                    )],
                    models: Vec::new(),
                    voices: Vec::new(),
                }],
            }],
        };
        let encoded = serde_json::to_string(&snapshot).expect("serialize snapshot");
        let decoded: PlatformCapabilitySnapshot =
            serde_json::from_str(&encoded).expect("deserialize snapshot");
        assert_eq!(snapshot, decoded);
        assert_eq!(decoded.local_only_capabilities().len(), 1);
    }
}
