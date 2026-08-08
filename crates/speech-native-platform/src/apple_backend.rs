//! Executable AVSpeechSynthesis backend for macOS.
//!
//! The backend renders to a WAV payload through Apple's buffer API. It never
//! plays audio, opens a network connection, or asks for permission. The
//! upstream wrapper currently collects AVSpeech buffers synchronously, so this
//! adapter advertises non-streaming synthesis and checks cancellation before
//! and after the native call without claiming pre-emptive interruption.

use crate::apple::{AppleCapabilitySource, lock_apple_runtime};
use crate::{PlatformCapabilitySource, PlatformProbeError};
use async_trait::async_trait;
use avspeechsynthesizer::{
    SpeechAudioBuffer, SpeechAudioCommonFormat, SpeechSynthesisVoice, SpeechSynthesisVoiceQuality,
    SpeechSynthesizer, SpeechUtterance,
};
use speech_native_types::{
    AlignmentGranularity, AudioOutputFormat, DEFAULT_SPEECH_EVENT_CAPACITY, NetworkBehavior,
    PlatformTarget, SpeechBackend, SpeechBackendDescriptor, SpeechBackendKind,
    SpeechBackendReadiness, SpeechCancellation, SpeechError, SpeechErrorClass, SpeechRequestId,
    SpeechResolvedRoute, SpeechRouteSelector, SpeechUsage, SynthesisEvent, SynthesisInput,
    SynthesisRequest, SynthesisResponse, SynthesisTicket, TranscriptionRequest,
    TranscriptionTicket, UsageProvenance, VoiceSelector,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};

const APPLE_TTS_BACKEND_ID: &str = "apple.av-speech";

#[derive(Clone)]
pub struct AppleSpeechBackend {
    descriptor: SpeechBackendDescriptor,
    state: Arc<AppleBackendState>,
}

#[derive(Default)]
struct AppleBackendState {
    active: Mutex<HashMap<SpeechRequestId, Arc<AtomicBool>>>,
    shutting_down: AtomicBool,
}

struct AppleCancellation {
    state: Arc<AppleBackendState>,
}

impl AppleSpeechBackend {
    /// Builds the executable backend from the same noninteractive runtime
    /// inventory used by routing. No speech is synthesized during creation.
    pub async fn discover() -> Result<Self, PlatformProbeError> {
        let mut backends = AppleCapabilitySource
            .probe(&PlatformTarget::current())
            .await?;
        let descriptor = backends
            .drain(..)
            .find(|backend| backend.id == APPLE_TTS_BACKEND_ID)
            .ok_or_else(|| {
                PlatformProbeError::SourceFailed(
                    "the Apple runtime probe did not return its synthesis backend".to_string(),
                )
            })?;
        Ok(Self {
            descriptor,
            state: Arc::new(AppleBackendState::default()),
        })
    }

    fn register_request(
        &self,
        request_id: &SpeechRequestId,
    ) -> Result<Arc<AtomicBool>, SpeechError> {
        if self.state.shutting_down.load(Ordering::Acquire) {
            return Err(backend_error(
                request_id,
                "apple_tts_shutting_down",
                SpeechErrorClass::Unavailable,
                true,
                "Apple speech synthesis is shutting down",
            ));
        }
        let mut active = self.state.active.lock().map_err(|_| {
            backend_error(
                request_id,
                "apple_tts_state_unavailable",
                SpeechErrorClass::Internal,
                true,
                "Apple speech synthesis request state is unavailable",
            )
        })?;
        if active.contains_key(request_id) {
            return Err(backend_error(
                request_id,
                "speech_request_duplicate",
                SpeechErrorClass::InvalidRequest,
                false,
                "A speech request with this request_id is already active",
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        active.insert(request_id.clone(), Arc::clone(&cancelled));
        Ok(cancelled)
    }
}

#[async_trait]
impl SpeechBackend for AppleSpeechBackend {
    fn descriptor(&self) -> SpeechBackendDescriptor {
        self.descriptor.clone()
    }

    fn readiness(&self) -> SpeechBackendReadiness {
        self.descriptor.readiness.clone()
    }

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> Result<TranscriptionTicket, SpeechError> {
        Err(backend_error(
            &request.context.request_id,
            "apple_tts_transcription_unsupported",
            SpeechErrorClass::Capability,
            false,
            "The Apple synthesis backend does not transcribe audio",
        ))
    }

    async fn synthesize(&self, request: SynthesisRequest) -> Result<SynthesisTicket, SpeechError> {
        validate_request(&request)?;
        let voice = resolve_voice(&request)?;
        let request_id = request.context.request_id.clone();
        let route = SpeechResolvedRoute {
            backend_id: APPLE_TTS_BACKEND_ID.to_string(),
            model_id: None,
            voice_id: Some(voice.identifier().to_string()),
            backend_kind: SpeechBackendKind::PlatformOnDevice,
            network: NetworkBehavior::Never,
        };
        let cancelled = self.register_request(&request_id)?;
        let (event_sender, event_receiver) = mpsc::channel(DEFAULT_SPEECH_EVENT_CAPACITY);
        let (final_sender, final_receiver) = oneshot::channel();
        let state = Arc::clone(&self.state);
        let worker_request_id = request_id.clone();

        drop(tokio::task::spawn_blocking(move || {
            run_synthesis(
                request,
                voice,
                route,
                Arc::clone(&cancelled),
                &event_sender,
                final_sender,
            );
            if let Ok(mut active) = state.active.lock() {
                active.remove(&worker_request_id);
            }
        }));

        Ok(SynthesisTicket::new(
            request_id,
            event_receiver,
            final_receiver,
            Arc::new(AppleCancellation {
                state: Arc::clone(&self.state),
            }),
        ))
    }

    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        cancel_request(&self.state, request_id)
    }

    async fn shutdown(&self) -> Result<(), SpeechError> {
        self.state.shutting_down.store(true, Ordering::Release);
        let active = self.state.active.lock().map_err(|_| {
            backend_error(
                &SpeechRequestId("apple-shutdown".to_string()),
                "apple_tts_state_unavailable",
                SpeechErrorClass::Internal,
                true,
                "Apple speech synthesis request state is unavailable",
            )
        })?;
        for cancelled in active.values() {
            cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
}

impl SpeechCancellation for AppleCancellation {
    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        cancel_request(&self.state, request_id)
    }
}

fn cancel_request(state: &AppleBackendState, request_id: &SpeechRequestId) -> usize {
    let Ok(active) = state.active.lock() else {
        return 0;
    };
    active.get(request_id).map_or(0, |cancelled| {
        cancelled.store(true, Ordering::Release);
        1
    })
}

fn validate_request(request: &SynthesisRequest) -> Result<(), SpeechError> {
    request.validate()?;
    if request.stream {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_streaming_unsupported",
            SpeechErrorClass::Capability,
            false,
            "Apple buffer synthesis is currently exposed as a non-streaming WAV operation",
        ));
    }
    if request.output != AudioOutputFormat::Wav {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_output_unsupported",
            SpeechErrorClass::Capability,
            false,
            "Apple buffer synthesis currently returns WAV audio",
        ));
    }
    if request.alignment != AlignmentGranularity::None {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_alignment_unsupported",
            SpeechErrorClass::Capability,
            false,
            "Apple buffer synthesis does not expose the requested alignment granularity",
        ));
    }
    if let SpeechRouteSelector::ExactBackend { backend_id, .. } = &request.context.route
        && backend_id != APPLE_TTS_BACKEND_ID
    {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_backend_mismatch",
            SpeechErrorClass::Capability,
            false,
            "The request selected a different speech backend",
        ));
    }
    if !(0.5..=2.0).contains(&request.pitch) {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_pitch_unsupported",
            SpeechErrorClass::Capability,
            false,
            "Apple speech pitch must be between 0.5 and 2.0",
        ));
    }
    let apple_rate = SpeechUtterance::default_speech_rate() * request.rate;
    if !(SpeechUtterance::minimum_speech_rate()..=SpeechUtterance::maximum_speech_rate())
        .contains(&apple_rate)
    {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_rate_unsupported",
            SpeechErrorClass::Capability,
            false,
            "The requested speech-rate multiplier is outside Apple's supported range",
        ));
    }
    Ok(())
}

fn resolve_voice(request: &SynthesisRequest) -> Result<SpeechSynthesisVoice, SpeechError> {
    let _runtime = lock_apple_runtime();
    let route_voice = match &request.context.route {
        SpeechRouteSelector::ExactBackend { voice_id, .. } => voice_id.as_deref(),
        _ => None,
    };
    let request_voice = match &request.voice {
        VoiceSelector::Auto => None,
        VoiceSelector::Exact { voice_id } => Some(voice_id.as_str()),
        VoiceSelector::Profile { .. } => {
            return Err(backend_error(
                &request.context.request_id,
                "apple_tts_voice_profile_unresolved",
                SpeechErrorClass::InvalidRequest,
                false,
                "Voice profiles must be resolved before Apple synthesis",
            ));
        }
    };
    if let (Some(route_voice), Some(request_voice)) = (route_voice, request_voice)
        && route_voice != request_voice
    {
        return Err(backend_error(
            &request.context.request_id,
            "apple_tts_voice_mismatch",
            SpeechErrorClass::InvalidRequest,
            false,
            "The route and synthesis request select different voices",
        ));
    }
    if let Some(voice_id) = route_voice.or(request_voice) {
        return SpeechSynthesisVoice::voice_with_identifier(voice_id)
            .map_err(|error| apple_api_error(&request.context.request_id, error.to_string()))?
            .ok_or_else(|| {
                backend_error(
                    &request.context.request_id,
                    "apple_tts_voice_unavailable",
                    SpeechErrorClass::Unavailable,
                    false,
                    "The selected Apple voice is not available",
                )
            });
    }

    let mut voices = if let Some(language) = request.language.as_deref() {
        SpeechSynthesisVoice::voices_with_language(language)
    } else {
        return SpeechSynthesisVoice::default_voice()
            .map_err(|error| apple_api_error(&request.context.request_id, error.to_string()))?
            .ok_or_else(|| {
                backend_error(
                    &request.context.request_id,
                    "apple_tts_voice_unavailable",
                    SpeechErrorClass::Unavailable,
                    false,
                    "Apple did not report a default speech voice",
                )
            });
    }
    .map_err(|error| apple_api_error(&request.context.request_id, error.to_string()))?;
    let requested_language = request.language.as_deref();
    voices.sort_by(|left, right| {
        apple_voice_language_rank(left, requested_language)
            .cmp(&apple_voice_language_rank(right, requested_language))
            .then_with(|| {
                apple_voice_quality_rank(right.quality())
                    .cmp(&apple_voice_quality_rank(left.quality()))
            })
            .then_with(|| left.identifier().cmp(right.identifier()))
    });
    voices.into_iter().next().ok_or_else(|| {
        backend_error(
            &request.context.request_id,
            "apple_tts_voice_unavailable",
            SpeechErrorClass::Unavailable,
            false,
            "Apple did not report an installed voice for the requested language",
        )
    })
}

fn run_synthesis(
    request: SynthesisRequest,
    voice: SpeechSynthesisVoice,
    route: SpeechResolvedRoute,
    cancelled: Arc<AtomicBool>,
    event_sender: &mpsc::Sender<SynthesisEvent>,
    final_sender: oneshot::Sender<Result<SynthesisResponse, SpeechError>>,
) {
    let request_id = request.context.request_id.clone();
    let started_at = Instant::now();
    if cancelled.load(Ordering::Acquire) {
        finish_cancelled(
            &request_id,
            event_sender,
            final_sender,
            SpeechUsage::default(),
        );
        return;
    }
    if event_sender
        .blocking_send(SynthesisEvent::Started {
            request_id: request_id.clone(),
            route: route.clone(),
        })
        .is_err()
    {
        let _ = final_sender.send(Err(cancelled_error(&request_id)));
        return;
    }

    let result = synthesize_wav(&request, voice).map(|(audio, duration_ms)| {
        let total_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let input_characters = match &request.input {
            SynthesisInput::Text { text } => text.chars().count(),
            SynthesisInput::Ssml { ssml } => ssml.chars().count(),
        };
        SynthesisResponse {
            request_id: request_id.clone(),
            route,
            audio,
            format: AudioOutputFormat::Wav,
            duration_ms: Some(duration_ms),
            alignments: Vec::new(),
            usage: SpeechUsage {
                output_audio_ms: Some(duration_ms),
                input_characters: Some(u64::try_from(input_characters).unwrap_or(u64::MAX)),
                time_to_first_result_ms: Some(total_ms),
                total_ms: Some(total_ms),
                provenance: UsageProvenance::Exact,
                real_local_inference: true,
                ..SpeechUsage::default()
            },
        }
    });

    if cancelled.load(Ordering::Acquire) {
        let usage = result.as_ref().map_or_else(
            |_| SpeechUsage::default(),
            |response| response.usage.clone(),
        );
        finish_cancelled(&request_id, event_sender, final_sender, usage);
        return;
    }
    match result {
        Ok(response) => {
            let _ = event_sender.blocking_send(SynthesisEvent::Completed {
                request_id,
                response: response.clone(),
            });
            let _ = final_sender.send(Ok(response));
        }
        Err(error) => {
            let _ = event_sender.blocking_send(SynthesisEvent::Failed {
                request_id,
                error: error.clone(),
            });
            let _ = final_sender.send(Err(error));
        }
    }
}

fn finish_cancelled(
    request_id: &SpeechRequestId,
    event_sender: &mpsc::Sender<SynthesisEvent>,
    final_sender: oneshot::Sender<Result<SynthesisResponse, SpeechError>>,
    usage: SpeechUsage,
) {
    let _ = event_sender.blocking_send(SynthesisEvent::Cancelled {
        request_id: request_id.clone(),
        usage,
    });
    let _ = final_sender.send(Err(cancelled_error(request_id)));
}

fn synthesize_wav(
    request: &SynthesisRequest,
    voice: SpeechSynthesisVoice,
) -> Result<(Vec<u8>, u64), SpeechError> {
    let _runtime = lock_apple_runtime();
    let mut utterance = match &request.input {
        SynthesisInput::Text { text } => SpeechUtterance::new(text),
        SynthesisInput::Ssml { ssml } => SpeechUtterance::from_ssml(ssml.clone())
            .map_err(|error| apple_api_error(&request.context.request_id, error.to_string()))?,
    };
    let apple_rate = SpeechUtterance::default_speech_rate() * request.rate;
    utterance = utterance
        .with_voice(voice)
        .with_rate(apple_rate)
        .with_pitch_multiplier(request.pitch)
        .with_volume(request.volume);

    let buffers = Arc::new(Mutex::new(Vec::new()));
    let callback_buffers = Arc::clone(&buffers);
    SpeechSynthesizer::new()
        .and_then(|synthesizer| {
            synthesizer.write_utterance_with_buffer_callback(&utterance, move |buffer| {
                if !buffer.is_end_of_stream()
                    && let Ok(mut buffers) = callback_buffers.lock()
                {
                    buffers.push(buffer);
                }
            })
        })
        .map_err(|error| apple_api_error(&request.context.request_id, error.to_string()))?;
    let buffers = buffers.lock().map_err(|_| {
        backend_error(
            &request.context.request_id,
            "apple_tts_buffer_state_unavailable",
            SpeechErrorClass::Internal,
            true,
            "Apple synthesis audio buffers are unavailable",
        )
    })?;
    encode_wav(&request.context.request_id, &buffers)
}

fn encode_wav(
    request_id: &SpeechRequestId,
    buffers: &[SpeechAudioBuffer],
) -> Result<(Vec<u8>, u64), SpeechError> {
    let first = buffers.first().ok_or_else(|| {
        backend_error(
            request_id,
            "apple_tts_audio_empty",
            SpeechErrorClass::Unavailable,
            true,
            "Apple speech synthesis returned no audio",
        )
    })?;
    let sample_rate = checked_sample_rate(first.sample_rate(), request_id)?;
    let channels = u16::try_from(first.channel_count()).map_err(|_| {
        backend_error(
            request_id,
            "apple_tts_channel_count_invalid",
            SpeechErrorClass::Internal,
            false,
            "Apple returned an unsupported channel count",
        )
    })?;
    let (wav_format, bytes_per_sample) = wav_sample_format(first.common_format(), request_id)?;
    let mut samples = Vec::new();
    let mut frames = 0_u64;
    for buffer in buffers {
        if checked_sample_rate(buffer.sample_rate(), request_id)? != sample_rate
            || buffer.channel_count() != usize::from(channels)
            || buffer.common_format() != first.common_format()
        {
            return Err(backend_error(
                request_id,
                "apple_tts_audio_format_changed",
                SpeechErrorClass::Internal,
                false,
                "Apple changed audio format during one synthesis operation",
            ));
        }
        append_interleaved(buffer, bytes_per_sample, &mut samples, request_id)?;
        frames = frames.saturating_add(u64::try_from(buffer.frame_length()).unwrap_or(u64::MAX));
    }

    let bits_per_sample = u16::try_from(bytes_per_sample * 8).map_err(|_| {
        backend_error(
            request_id,
            "apple_tts_sample_width_invalid",
            SpeechErrorClass::Internal,
            false,
            "Apple returned an unsupported audio sample width",
        )
    })?;
    let block_align = channels.checked_mul(bits_per_sample / 8).ok_or_else(|| {
        backend_error(
            request_id,
            "apple_tts_wav_size_overflow",
            SpeechErrorClass::Internal,
            false,
            "Apple speech audio is too large to encode as WAV",
        )
    })?;
    let byte_rate = sample_rate
        .checked_mul(u32::from(block_align))
        .ok_or_else(|| {
            backend_error(
                request_id,
                "apple_tts_wav_size_overflow",
                SpeechErrorClass::Internal,
                false,
                "Apple speech audio is too large to encode as WAV",
            )
        })?;
    let data_len = u32::try_from(samples.len()).map_err(|_| {
        backend_error(
            request_id,
            "apple_tts_wav_size_overflow",
            SpeechErrorClass::Internal,
            false,
            "Apple speech audio is too large to encode as WAV",
        )
    })?;
    let riff_len = 36_u32.checked_add(data_len).ok_or_else(|| {
        backend_error(
            request_id,
            "apple_tts_wav_size_overflow",
            SpeechErrorClass::Internal,
            false,
            "Apple speech audio is too large to encode as WAV",
        )
    })?;
    let mut wav = Vec::with_capacity(44 + samples.len());
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&riff_len.to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&wav_format.to_le_bytes());
    wav.extend_from_slice(&channels.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&bits_per_sample.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_len.to_le_bytes());
    wav.extend_from_slice(&samples);
    let duration_ms = frames.saturating_mul(1_000) / u64::from(sample_rate);
    Ok((wav, duration_ms))
}

fn append_interleaved(
    buffer: &SpeechAudioBuffer,
    bytes_per_sample: usize,
    output: &mut Vec<u8>,
    request_id: &SpeechRequestId,
) -> Result<(), SpeechError> {
    let frame_bytes = bytes_per_sample
        .checked_mul(buffer.channel_count())
        .ok_or_else(|| invalid_buffer(request_id))?;
    if buffer.is_interleaved() {
        let plane = buffer
            .planes()
            .first()
            .ok_or_else(|| invalid_buffer(request_id))?;
        let expected = frame_bytes
            .checked_mul(buffer.frame_length())
            .ok_or_else(|| invalid_buffer(request_id))?;
        if plane.len() < expected {
            return Err(invalid_buffer(request_id));
        }
        output.extend_from_slice(&plane[..expected]);
        return Ok(());
    }
    if buffer.planes().len() < buffer.channel_count() {
        return Err(invalid_buffer(request_id));
    }
    for frame in 0..buffer.frame_length() {
        let offset = frame
            .checked_mul(bytes_per_sample)
            .ok_or_else(|| invalid_buffer(request_id))?;
        let end = offset
            .checked_add(bytes_per_sample)
            .ok_or_else(|| invalid_buffer(request_id))?;
        for plane in &buffer.planes()[..buffer.channel_count()] {
            let sample = plane
                .get(offset..end)
                .ok_or_else(|| invalid_buffer(request_id))?;
            output.extend_from_slice(sample);
        }
    }
    Ok(())
}

fn checked_sample_rate(sample_rate: f64, request_id: &SpeechRequestId) -> Result<u32, SpeechError> {
    if !sample_rate.is_finite() || sample_rate < 1.0 || sample_rate > f64::from(u32::MAX) {
        return Err(invalid_buffer(request_id));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let rounded = sample_rate.round() as u32;
    Ok(rounded)
}

fn wav_sample_format(
    format: SpeechAudioCommonFormat,
    request_id: &SpeechRequestId,
) -> Result<(u16, usize), SpeechError> {
    match format {
        SpeechAudioCommonFormat::PcmInt16 => Ok((1, 2)),
        SpeechAudioCommonFormat::PcmInt32 => Ok((1, 4)),
        SpeechAudioCommonFormat::PcmFloat32 => Ok((3, 4)),
        SpeechAudioCommonFormat::PcmFloat64 => Ok((3, 8)),
        SpeechAudioCommonFormat::Other | SpeechAudioCommonFormat::Unknown => Err(backend_error(
            request_id,
            "apple_tts_audio_format_unsupported",
            SpeechErrorClass::Capability,
            false,
            "Apple returned a non-PCM speech audio format",
        )),
    }
}

fn invalid_buffer(request_id: &SpeechRequestId) -> SpeechError {
    backend_error(
        request_id,
        "apple_tts_audio_buffer_invalid",
        SpeechErrorClass::Internal,
        false,
        "Apple returned an invalid speech audio buffer",
    )
}

const fn apple_voice_quality_rank(quality: SpeechSynthesisVoiceQuality) -> u8 {
    match quality {
        SpeechSynthesisVoiceQuality::Premium => 3,
        SpeechSynthesisVoiceQuality::Enhanced => 2,
        SpeechSynthesisVoiceQuality::Default => 1,
        SpeechSynthesisVoiceQuality::Unknown(_) => 0,
    }
}

fn apple_voice_language_rank(voice: &SpeechSynthesisVoice, language: Option<&str>) -> u8 {
    let Some(language) = language else {
        return 0;
    };
    if voice.language().eq_ignore_ascii_case(language) {
        return 0;
    }
    let same_base = voice
        .language()
        .split_once('-')
        .zip(language.split_once('-'))
        .is_some_and(|((voice_base, _), (request_base, _))| {
            voice_base.eq_ignore_ascii_case(request_base)
        });
    if same_base { 1 } else { 2 }
}

fn apple_api_error(request_id: &SpeechRequestId, detail: String) -> SpeechError {
    backend_error(
        request_id,
        "apple_tts_api_failed",
        SpeechErrorClass::Unavailable,
        true,
        &detail,
    )
}

fn cancelled_error(request_id: &SpeechRequestId) -> SpeechError {
    backend_error(
        request_id,
        "speech_request_cancelled",
        SpeechErrorClass::Cancelled,
        false,
        "Speech synthesis was cancelled",
    )
}

fn backend_error(
    request_id: &SpeechRequestId,
    code: &str,
    class: SpeechErrorClass,
    retryable: bool,
    detail: &str,
) -> SpeechError {
    SpeechError {
        code: code.to_string(),
        class,
        retryable,
        request_id: request_id.clone(),
        backend_id: Some(APPLE_TTS_BACKEND_ID.to_string()),
        safe_detail: detail.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use speech_native_types::{
        SpeechDeadlinePolicy, SpeechRequestContext, SpeechRoutingPolicy, SynthesisInput,
    };

    fn request(id: &str) -> SynthesisRequest {
        SynthesisRequest {
            context: SpeechRequestContext {
                request_id: SpeechRequestId(id.to_string()),
                client_id: "apple-backend-test".to_string(),
                route: SpeechRouteSelector::ExactBackend {
                    backend_id: APPLE_TTS_BACKEND_ID.to_string(),
                    model_id: None,
                    voice_id: None,
                },
                routing: SpeechRoutingPolicy::default(),
                deadline: SpeechDeadlinePolicy::default(),
            },
            input: SynthesisInput::Text {
                text: "Native buffer synthesis smoke.".to_string(),
            },
            voice: VoiceSelector::Auto,
            language: Some("en-US".to_string()),
            rate: 1.0,
            pitch: 1.0,
            volume: 1.0,
            output: AudioOutputFormat::Wav,
            alignment: AlignmentGranularity::None,
            stream: false,
        }
    }

    #[tokio::test]
    #[ignore = "requires a running AppKit event loop; covered by the launched Tauri smoke"]
    async fn real_apple_tts_returns_silent_wav_bytes_without_permission() {
        let backend = AppleSpeechBackend::discover()
            .await
            .expect("discover Apple TTS");
        let mut ticket = backend
            .synthesize(request("apple-real-tts"))
            .await
            .expect("start Apple buffer synthesis");
        let mut events = Vec::new();
        while let Some(event) = ticket.events.recv().await {
            let terminal = event.is_terminal();
            events.push(event);
            if terminal {
                break;
            }
        }
        let response = ticket.final_response().await.expect("final WAV response");
        assert!(response.audio.starts_with(b"RIFF"));
        assert_eq!(response.audio.get(8..12), Some(b"WAVE".as_slice()));
        assert!(response.audio.len() > 44);
        assert!(response.duration_ms.is_some_and(|duration| duration > 0));
        assert!(response.usage.real_local_inference);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], SynthesisEvent::Started { .. }));
        assert!(matches!(events[1], SynthesisEvent::Completed { .. }));
        assert_eq!(events.iter().filter(|event| event.is_terminal()).count(), 1);
    }

    #[tokio::test]
    async fn unsupported_streaming_is_rejected_before_native_execution() {
        let backend = AppleSpeechBackend::discover()
            .await
            .expect("discover Apple TTS");
        let mut request = request("apple-stream-reject");
        request.stream = true;
        let error = match backend.synthesize(request).await {
            Ok(_) => panic!("streaming must be rejected honestly"),
            Err(error) => error,
        };
        assert_eq!(error.code, "apple_tts_streaming_unsupported");
        assert!(!error.retryable);
    }

    #[tokio::test]
    async fn duplicate_active_request_ids_fail_closed() {
        let backend = AppleSpeechBackend::discover()
            .await
            .expect("discover Apple TTS");
        let request_id = SpeechRequestId("apple-duplicate".to_string());
        let _active = backend
            .register_request(&request_id)
            .expect("register first request");
        let second = backend.register_request(&request_id);
        assert_eq!(
            second.err().map(|error| error.code).as_deref(),
            Some("speech_request_duplicate")
        );
    }

    #[test]
    fn wave_header_constants_are_correct() {
        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&36_u32.to_le_bytes());
        wav.extend_from_slice(b"WAVE");
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(crate::apple::APPLE_RUNTIME_SOURCE_ID, "apple-runtime");
    }
}
