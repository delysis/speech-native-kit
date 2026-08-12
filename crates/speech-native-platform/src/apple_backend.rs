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
    SynthesisRequest, SynthesisResponse, SynthesisTicket, TaskSupervisor, TaskSupervisorError,
    TranscriptionRequest, TranscriptionTicket, UsageProvenance, VoiceSelector,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{Notify, mpsc, oneshot};

const APPLE_TTS_BACKEND_ID: &str = "apple.av-speech";

#[derive(Clone)]
pub struct AppleSpeechBackend {
    descriptor: SpeechBackendDescriptor,
    state: Arc<AppleBackendState>,
}

struct AppleBackendState {
    data: Mutex<AppleBackendStateData>,
    tasks: Arc<TaskSupervisor>,
    changed: Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppleBackendPhase {
    Running,
    Quiescing,
    Closed,
}

struct AppleBackendStateData {
    phase: AppleBackendPhase,
    next_nonce: u64,
    active: HashMap<SpeechRequestId, ActiveAppleOperation>,
    shutdown_result: Option<Result<(), SpeechError>>,
}

struct ActiveAppleOperation {
    cancelled: Arc<AtomicBool>,
    nonce: u64,
}

struct AppleCancellation {
    state: Arc<AppleBackendState>,
}

struct AppleOperationLease {
    state: Arc<AppleBackendState>,
    request_id: SpeechRequestId,
    nonce: u64,
}

impl Default for AppleBackendState {
    fn default() -> Self {
        Self {
            data: Mutex::new(AppleBackendStateData {
                phase: AppleBackendPhase::Running,
                next_nonce: 0,
                active: HashMap::new(),
                shutdown_result: None,
            }),
            tasks: Arc::new(TaskSupervisor::default()),
            changed: Notify::new(),
        }
    }
}

impl AppleBackendState {
    fn spawn_operation(
        self: &Arc<Self>,
        request_id: SpeechRequestId,
        cancelled: Arc<AtomicBool>,
        worker: impl FnOnce() + Send + 'static,
    ) -> Result<(), SpeechError> {
        let mut data = self.data.lock().map_err(|_| apple_state_error())?;
        if data.phase != AppleBackendPhase::Running {
            return Err(backend_error(
                &request_id,
                "apple_tts_shutting_down",
                SpeechErrorClass::Unavailable,
                true,
                "Apple speech synthesis is shutting down",
            ));
        }
        if data.active.contains_key(&request_id) {
            return Err(backend_error(
                &request_id,
                "speech_request_duplicate",
                SpeechErrorClass::InvalidRequest,
                false,
                "A speech request with this request_id is already active",
            ));
        }
        let nonce = data.next_nonce;
        data.next_nonce = data.next_nonce.checked_add(1).ok_or_else(|| {
            backend_error(
                &request_id,
                "apple_tts_nonce_exhausted",
                SpeechErrorClass::Internal,
                false,
                "Apple speech request nonce space is exhausted",
            )
        })?;
        let state = Arc::clone(self);
        let worker_request_id = request_id.clone();
        self.tasks
            .spawn_blocking(format!("apple-tts:{}", request_id.0), move || {
                let _lease = AppleOperationLease {
                    state,
                    request_id: worker_request_id,
                    nonce,
                };
                worker();
                Ok(())
            })
            .map_err(|error| task_supervisor_error(&request_id, error))?;
        data.active
            .insert(request_id, ActiveAppleOperation { cancelled, nonce });
        Ok(())
    }
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
        let (event_sender, event_receiver) = mpsc::channel(DEFAULT_SPEECH_EVENT_CAPACITY);
        let (final_sender, final_receiver) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancelled = Arc::clone(&cancelled);
        self.state
            .spawn_operation(request_id.clone(), Arc::clone(&cancelled), move || {
                run_synthesis(
                    request,
                    voice,
                    route,
                    worker_cancelled,
                    &event_sender,
                    final_sender,
                );
            })?;

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
        let start = {
            let mut data = self.state.data.lock().map_err(|_| apple_state_error())?;
            match data.phase {
                AppleBackendPhase::Running => {
                    data.phase = AppleBackendPhase::Quiescing;
                    for operation in data.active.values() {
                        operation.cancelled.store(true, Ordering::Release);
                    }
                    true
                }
                AppleBackendPhase::Quiescing => false,
                AppleBackendPhase::Closed => {
                    return data
                        .shutdown_result
                        .clone()
                        .unwrap_or_else(|| Err(apple_state_error()));
                }
            }
        };
        if start {
            spawn_apple_shutdown(Arc::clone(&self.state));
        }
        wait_for_apple_shutdown(&self.state).await
    }
}

impl SpeechCancellation for AppleCancellation {
    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        cancel_request(&self.state, request_id)
    }
}

fn cancel_request(state: &AppleBackendState, request_id: &SpeechRequestId) -> usize {
    let Ok(data) = state.data.lock() else {
        return 0;
    };
    data.active.get(request_id).map_or(0, |operation| {
        operation.cancelled.store(true, Ordering::Release);
        1
    })
}

impl Drop for AppleOperationLease {
    fn drop(&mut self) {
        if let Ok(mut data) = self.state.data.lock()
            && data
                .active
                .get(&self.request_id)
                .is_some_and(|operation| operation.nonce == self.nonce)
        {
            data.active.remove(&self.request_id);
            self.state.changed.notify_waiters();
        }
    }
}

async fn wait_for_apple_shutdown(state: &AppleBackendState) -> Result<(), SpeechError> {
    loop {
        let changed = state.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let result = {
            let data = state.data.lock().map_err(|_| apple_state_error())?;
            (data.phase == AppleBackendPhase::Closed).then(|| data.shutdown_result.clone())
        };
        if let Some(result) = result {
            return result.unwrap_or_else(|| Err(apple_state_error()));
        }
        changed.await;
    }
}

fn spawn_apple_shutdown(state: Arc<AppleBackendState>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        publish_apple_shutdown(&state, Err(apple_state_error()));
        return;
    };
    runtime.spawn(async move {
        let worker_state = Arc::clone(&state);
        let joined = tokio::spawn(async move { run_apple_shutdown(&worker_state).await }).await;
        let result = joined.unwrap_or_else(|error| {
            Err(backend_error(
                &SpeechRequestId("apple-shutdown".to_owned()),
                "apple_shutdown_panicked",
                SpeechErrorClass::Internal,
                false,
                &format!("Apple shutdown coordinator panicked: {error}"),
            ))
        });
        publish_apple_shutdown(&state, result);
    });
}

async fn run_apple_shutdown(state: &AppleBackendState) -> Result<(), SpeechError> {
    let request_id = SpeechRequestId("apple-shutdown".to_owned());
    state
        .tasks
        .begin_shutdown()
        .map_err(|error| task_supervisor_error(&request_id, error))?;
    state
        .tasks
        .wait_for_idle()
        .await
        .map_err(|error| task_supervisor_error(&request_id, error))?;
    state
        .tasks
        .failure_summary()
        .map_err(|error| task_supervisor_error(&request_id, error))?
        .map_or(Ok(()), |summary| {
            Err(backend_error(
                &request_id,
                "apple_tts_worker_failed",
                SpeechErrorClass::Internal,
                false,
                &format!(
                    "Apple speech worker '{}' failed ({:?}): {}; {} additional failure(s)",
                    summary.first.label,
                    summary.first.kind,
                    summary.first.detail,
                    summary.additional_failures
                ),
            ))
        })
}

fn publish_apple_shutdown(state: &AppleBackendState, result: Result<(), SpeechError>) {
    if let Ok(mut data) = state.data.lock() {
        data.shutdown_result = Some(result);
        data.phase = AppleBackendPhase::Closed;
    }
    state.changed.notify_waiters();
}

fn apple_state_error() -> SpeechError {
    backend_error(
        &SpeechRequestId("apple-shutdown".to_string()),
        "apple_tts_state_unavailable",
        SpeechErrorClass::Internal,
        true,
        "Apple speech synthesis request state is unavailable",
    )
}

fn task_supervisor_error(request_id: &SpeechRequestId, error: TaskSupervisorError) -> SpeechError {
    backend_error(
        request_id,
        "apple_tts_worker_state_unavailable",
        SpeechErrorClass::Internal,
        true,
        &format!("Apple speech worker supervision failed: {error}"),
    )
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
        backend
            .state
            .data
            .lock()
            .expect("lock Apple backend state")
            .active
            .insert(
                request_id.clone(),
                ActiveAppleOperation {
                    cancelled: Arc::new(AtomicBool::new(false)),
                    nonce: 1,
                },
            );
        let second = backend.synthesize(request("apple-duplicate")).await;
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

    #[tokio::test]
    async fn completed_apple_workers_self_reap_task_state() {
        let state = Arc::new(AppleBackendState::default());
        for index in 0..1_000 {
            state
                .spawn_operation(
                    SpeechRequestId(format!("self-reaping-apple-worker-{index}")),
                    Arc::new(AtomicBool::new(false)),
                    || {},
                )
                .expect("spawn fixture worker");
        }
        state
            .tasks
            .wait_for_idle()
            .await
            .expect("wait for fixture workers");

        assert!(state.data.lock().expect("lock state").active.is_empty());
        let task_state = state.tasks.snapshot().expect("read task state");
        assert_eq!(task_state.active, 0);
        assert_eq!(task_state.retained_failures, 0);
        assert!(task_state.expected_worker_ids.len() <= 1_000);
        assert_eq!(
            task_state
                .joined_worker_ids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            task_state
                .expected_worker_ids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_and_joins_the_owned_apple_worker() {
        let backend = AppleSpeechBackend::discover()
            .await
            .expect("discover Apple TTS");
        let request_id = SpeechRequestId("blocking-apple-worker".to_string());
        let cancelled = Arc::new(AtomicBool::new(false));
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        backend
            .state
            .spawn_operation(request_id, Arc::clone(&cancelled), move || {
                started_sender.send(()).expect("signal worker start");
                release_receiver.recv().expect("release Apple worker");
            })
            .expect("spawn blocking Apple fixture worker");
        started_receiver.recv().expect("Apple worker must start");

        let shutdown_backend = backend.clone();
        let mut shutdown = tokio::spawn(async move { shutdown_backend.shutdown().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
                .await
                .is_err()
        );
        assert!(cancelled.load(Ordering::Acquire));
        release_sender.send(()).expect("release Apple worker");
        shutdown
            .await
            .expect("shutdown task joins")
            .expect("Apple backend shutdown succeeds");
        backend
            .shutdown()
            .await
            .expect("repeated shutdown retains success");

        let data = backend.state.data.lock().expect("lock closed state");
        assert_eq!(data.phase, AppleBackendPhase::Closed);
        assert!(data.active.is_empty());
        drop(data);
        let task_state = backend.state.tasks.snapshot().expect("read task state");
        assert_eq!(task_state.active, 0);
        assert_eq!(task_state.retained_failures, 0);
    }

    #[tokio::test]
    async fn aborted_shutdown_caller_and_concurrent_followers_complete() {
        let backend = AppleSpeechBackend::discover()
            .await
            .expect("discover Apple TTS");
        let cancelled = Arc::new(AtomicBool::new(false));
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        backend
            .state
            .spawn_operation(
                SpeechRequestId("abort-safe-apple".to_owned()),
                Arc::clone(&cancelled),
                move || {
                    started_sender.send(()).expect("signal worker start");
                    release_receiver.recv().expect("release blocking worker");
                },
            )
            .expect("spawn blocking Apple worker");
        started_receiver.recv().expect("worker starts");

        let first_backend = backend.clone();
        let first = tokio::spawn(async move { first_backend.shutdown().await });
        tokio::task::yield_now().await;
        first.abort();
        first.await.expect_err("first shutdown caller is aborted");
        let followers = (0..32)
            .map(|_| {
                let follower = backend.clone();
                tokio::spawn(async move { follower.shutdown().await })
            })
            .collect::<Vec<_>>();
        assert!(cancelled.load(Ordering::Acquire));
        release_sender.send(()).expect("release worker");
        for follower in followers {
            tokio::time::timeout(std::time::Duration::from_secs(2), follower)
                .await
                .expect("follower does not miss close notification")
                .expect("follower joins")
                .expect("retained shutdown succeeds");
        }
    }

    #[tokio::test]
    async fn apple_domain_error_self_reaps_without_task_failure() {
        let state = Arc::new(AppleBackendState::default());
        let request_id = SpeechRequestId("apple-domain-error".to_string());
        let worker_request_id = request_id.clone();
        let (result_sender, result_receiver) = std::sync::mpsc::channel();
        state
            .spawn_operation(request_id, Arc::new(AtomicBool::new(false)), move || {
                let result = encode_wav(&worker_request_id, &[]);
                result_sender.send(result).expect("send domain result");
            })
            .expect("spawn domain-error fixture worker");
        state
            .tasks
            .wait_for_idle()
            .await
            .expect("wait for domain-error worker");

        let error = result_receiver
            .recv()
            .expect("receive domain result")
            .expect_err("empty Apple buffers must produce a domain error");
        assert_eq!(error.code, "apple_tts_audio_empty");
        assert!(state.data.lock().expect("lock state").active.is_empty());
        let task_state = state.tasks.snapshot().expect("read task state");
        assert_eq!(task_state.active, 0);
        assert_eq!(task_state.retained_failures, 0);
    }

    #[tokio::test]
    async fn apple_worker_panic_is_preserved_before_reaping() {
        let state = Arc::new(AppleBackendState::default());
        state
            .spawn_operation(
                SpeechRequestId("panicking-apple-worker".to_string()),
                Arc::new(AtomicBool::new(false)),
                || panic!("fixture Apple panic"),
            )
            .expect("spawn panic fixture");
        state.tasks.wait_for_idle().await.expect("wait for panic");

        let failure = state
            .tasks
            .failure_summary()
            .expect("read failure summary")
            .expect("panic is retained")
            .first;
        assert_eq!(
            failure.kind,
            speech_native_types::SupervisedTaskFailureKind::Panic
        );
        assert!(failure.detail.contains("fixture Apple panic"));
        assert!(state.data.lock().expect("lock state").active.is_empty());
    }

    #[tokio::test]
    async fn apple_nonce_exhaustion_fails_closed() {
        let state = Arc::new(AppleBackendState::default());
        state.data.lock().expect("lock state").next_nonce = u64::MAX;
        let error = state
            .spawn_operation(
                SpeechRequestId("nonce-exhausted".to_string()),
                Arc::new(AtomicBool::new(false)),
                || {},
            )
            .expect_err("nonce exhaustion must reject admission");

        assert_eq!(error.code, "apple_tts_nonce_exhausted");
        assert!(state.data.lock().expect("lock state").active.is_empty());
        assert_eq!(state.tasks.snapshot().expect("read task state").active, 0);
    }
}
