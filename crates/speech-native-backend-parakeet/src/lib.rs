//! Embedded, network-free Parakeet speech recognition.
//!
//! The backend loads one Parakeet Realtime EOU ONNX model from the shared
//! Hugging Face cache and creates independent decoder state per request. Model
//! weights are never copied into application storage. The first implementation
//! intentionally advertises only what the 120M model proves: English PCM/WAV
//! transcription with partial streaming results, without timestamps,
//! diarization, translation, or hotword biasing.

use async_trait::async_trait;
use parakeet_rs::{ParakeetEOU, ParakeetEOUHandle};
use speech_native_types::{
    AcceptedAudio, AssetManager, AudioChunk, AudioInput, CapabilityAvailability,
    CapabilityEvidence, DEFAULT_SPEECH_EVENT_CAPACITY, DiarizationPolicy, EncodedAudioFormat,
    EvidenceKind, EvidenceOutcome, NetworkBehavior, PcmFormat, PcmSampleFormat, SpeechAsset,
    SpeechBackend, SpeechBackendDescriptor, SpeechBackendKind, SpeechBackendReadiness,
    SpeechCancellation, SpeechCapability, SpeechCapabilityLimits, SpeechError, SpeechErrorClass,
    SpeechModelDescriptor, SpeechOperationCapability, SpeechRequestId, SpeechResolvedRoute,
    SpeechRouteSelector, SpeechUsage, SynthesisRequest, SynthesisTicket, TimestampGranularity,
    TranscriptSegment, TranscriptionAudioSink, TranscriptionCapabilities, TranscriptionEvent,
    TranscriptionInput, TranscriptionRequest, TranscriptionResponse, TranscriptionTask,
    TranscriptionTicket, UsageProvenance,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

pub const PARAKEET_BACKEND_ID: &str = "parakeet-rs.eou-120m";
pub const PARAKEET_MODEL_ID: &str = "parakeet-realtime-eou-120m-v1-onnx";
pub const PARAKEET_HF_REPOSITORY: &str = "altunenes/parakeet-rs";
pub const PARAKEET_HF_SUBDIRECTORY: &str = "realtime_eou_120m-v1-onnx";

const TARGET_SAMPLE_RATE: u32 = 16_000;
const MODEL_CHUNK_SAMPLES: usize = 2_560;
const MODEL_ASSET_BYTES: u64 = 480_708_981;
const MAX_AUDIO_MS: u64 = 2 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Default)]
pub struct ParakeetBackendConfig {
    pub model_dir: Option<PathBuf>,
}

#[derive(Clone)]
pub struct ParakeetSpeechBackend {
    descriptor: SpeechBackendDescriptor,
    model: Option<Arc<ParakeetEOUHandle>>,
    state: Arc<BackendState>,
}

#[derive(Default)]
struct BackendState {
    active: Mutex<HashMap<SpeechRequestId, Arc<AtomicBool>>>,
    shutting_down: AtomicBool,
}

struct BackendCancellation {
    state: Arc<BackendState>,
}

struct StreamAudioSink {
    request_id: SpeechRequestId,
    format: PcmFormat,
    sender: Mutex<Option<mpsc::Sender<AudioChunk>>>,
    finished: AtomicBool,
}

impl ParakeetSpeechBackend {
    /// Discover and load the model from an explicit path or the standard
    /// Hugging Face cache. Missing weights produce a registered but ineligible
    /// backend so status can explain the exact remediation.
    pub async fn discover(config: ParakeetBackendConfig) -> Self {
        let model_dir = config.model_dir.or_else(discover_eou_model_dir);
        let Some(model_dir) = model_dir else {
            return Self::unavailable(asset_required_descriptor());
        };
        if !model_dir_is_complete(&model_dir) {
            return Self::unavailable(asset_required_descriptor());
        }

        let started = Instant::now();
        let loaded = tokio::task::spawn_blocking(move || {
            ParakeetEOUHandle::from_pretrained(model_dir, None)
        })
        .await;
        let _initialization_ms = elapsed_ms(started);
        match loaded {
            Ok(Ok(handle)) => Self {
                descriptor: ready_descriptor(),
                model: Some(Arc::new(handle)),
                state: Arc::new(BackendState::default()),
            },
            Ok(Err(error)) => Self::unavailable(unavailable_descriptor(format!(
                "Parakeet model loading failed: {error}"
            ))),
            Err(error) => Self::unavailable(unavailable_descriptor(format!(
                "Parakeet model task failed: {error}"
            ))),
        }
    }

    fn unavailable(descriptor: SpeechBackendDescriptor) -> Self {
        Self {
            descriptor,
            model: None,
            state: Arc::new(BackendState::default()),
        }
    }

    fn register_request(
        &self,
        request_id: &SpeechRequestId,
    ) -> Result<Arc<AtomicBool>, SpeechError> {
        if self.state.shutting_down.load(Ordering::Acquire) {
            return Err(backend_error(
                request_id,
                "parakeet_shutting_down",
                SpeechErrorClass::Unavailable,
                true,
                "The embedded Parakeet backend is shutting down",
            ));
        }
        let mut active = self.state.active.lock().map_err(|_| {
            backend_error(
                request_id,
                "parakeet_state_unavailable",
                SpeechErrorClass::Internal,
                true,
                "Parakeet request state is unavailable",
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
impl SpeechBackend for ParakeetSpeechBackend {
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
        validate_request(&request)?;
        let model = self.model.clone().ok_or_else(|| {
            backend_error(
                &request.context.request_id,
                "parakeet_model_unavailable",
                SpeechErrorClass::AssetMissing,
                true,
                "The Parakeet EOU model is not loaded from the Hugging Face cache",
            )
        })?;
        let request_id = request.context.request_id.clone();
        let cancelled = self.register_request(&request_id)?;
        let (event_sender, event_receiver) = mpsc::channel(DEFAULT_SPEECH_EVENT_CAPACITY);
        let (final_sender, final_receiver) = oneshot::channel();
        let state = Arc::clone(&self.state);
        let worker_id = request_id.clone();

        let (audio_sender, audio_receiver, audio_sink) = match &request.input {
            TranscriptionInput::Complete { .. } => (None, None, None),
            TranscriptionInput::Stream { format, .. } => {
                let (sender, receiver) = mpsc::channel(DEFAULT_SPEECH_EVENT_CAPACITY);
                let sink: Arc<dyn TranscriptionAudioSink> = Arc::new(StreamAudioSink {
                    request_id: request_id.clone(),
                    format: *format,
                    sender: Mutex::new(Some(sender.clone())),
                    finished: AtomicBool::new(false),
                });
                (Some(sender), Some(receiver), Some(sink))
            }
        };
        drop(audio_sender);

        drop(tokio::task::spawn_blocking(move || {
            run_transcription(
                request,
                model,
                audio_receiver,
                Arc::clone(&cancelled),
                &event_sender,
                final_sender,
            );
            if let Ok(mut active) = state.active.lock() {
                active.remove(&worker_id);
            }
        }));

        Ok(TranscriptionTicket::new(
            request_id,
            event_receiver,
            final_receiver,
            Arc::new(BackendCancellation {
                state: Arc::clone(&self.state),
            }),
            audio_sink,
        ))
    }

    async fn synthesize(&self, request: SynthesisRequest) -> Result<SynthesisTicket, SpeechError> {
        Err(backend_error(
            &request.context.request_id,
            "parakeet_synthesis_unsupported",
            SpeechErrorClass::Capability,
            false,
            "The Parakeet backend transcribes audio and does not synthesize speech",
        ))
    }

    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        cancel_request(&self.state, request_id)
    }

    async fn shutdown(&self) -> Result<(), SpeechError> {
        self.state.shutting_down.store(true, Ordering::Release);
        let active = self.state.active.lock().map_err(|_| {
            backend_error(
                &SpeechRequestId("parakeet-shutdown".to_string()),
                "parakeet_state_unavailable",
                SpeechErrorClass::Internal,
                true,
                "Parakeet request state is unavailable",
            )
        })?;
        for cancelled in active.values() {
            cancelled.store(true, Ordering::Release);
        }
        Ok(())
    }
}

impl SpeechCancellation for BackendCancellation {
    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        cancel_request(&self.state, request_id)
    }
}

#[async_trait]
impl TranscriptionAudioSink for StreamAudioSink {
    async fn push(&self, chunk: AudioChunk) -> Result<(), SpeechError> {
        if self.finished.load(Ordering::Acquire) {
            return Err(backend_error(
                &self.request_id,
                "audio_stream_finished",
                SpeechErrorClass::InvalidRequest,
                false,
                "Audio cannot be pushed after the stream has finished",
            ));
        }
        chunk.validate(&self.request_id)?;
        if chunk.format != self.format {
            return Err(backend_error(
                &self.request_id,
                "audio_stream_format_changed",
                SpeechErrorClass::InvalidRequest,
                false,
                "A transcription stream must keep one PCM format",
            ));
        }
        let sender = self
            .sender
            .lock()
            .map_err(|_| stream_closed_error(&self.request_id))?
            .clone()
            .ok_or_else(|| stream_closed_error(&self.request_id))?;
        sender
            .send(chunk)
            .await
            .map_err(|_| stream_closed_error(&self.request_id))
    }

    async fn finish(&self) -> Result<(), SpeechError> {
        if self.finished.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.sender
            .lock()
            .map_err(|_| stream_closed_error(&self.request_id))?
            .take();
        Ok(())
    }
}

fn cancel_request(state: &BackendState, request_id: &SpeechRequestId) -> usize {
    let Ok(active) = state.active.lock() else {
        return 0;
    };
    active.get(request_id).map_or(0, |cancelled| {
        cancelled.store(true, Ordering::Release);
        1
    })
}

fn validate_request(request: &TranscriptionRequest) -> Result<(), SpeechError> {
    request.validate()?;
    if request.task != TranscriptionTask::Transcribe {
        return Err(capability_error(
            &request.context.request_id,
            "parakeet_translation_unsupported",
            "The Parakeet EOU model does not translate audio",
        ));
    }
    if request.timestamps != TimestampGranularity::None {
        return Err(capability_error(
            &request.context.request_id,
            "parakeet_timestamps_unsupported",
            "The Parakeet EOU model does not provide timestamp evidence",
        ));
    }
    if request.diarization != DiarizationPolicy::Disabled {
        return Err(capability_error(
            &request.context.request_id,
            "parakeet_diarization_unsupported",
            "The Parakeet EOU model does not perform speaker diarization",
        ));
    }
    if !request.hotwords.is_empty() {
        return Err(capability_error(
            &request.context.request_id,
            "parakeet_hotwords_unsupported",
            "The Parakeet EOU model does not expose hotword biasing",
        ));
    }
    if let Some(language) = request.language.as_deref()
        && !language.eq_ignore_ascii_case("en")
        && !language.to_ascii_lowercase().starts_with("en-")
    {
        return Err(capability_error(
            &request.context.request_id,
            "parakeet_language_unsupported",
            "The bundled Parakeet EOU model currently supports English",
        ));
    }
    if let SpeechRouteSelector::ExactBackend {
        backend_id,
        model_id,
        ..
    } = &request.context.route
    {
        if backend_id != PARAKEET_BACKEND_ID {
            return Err(capability_error(
                &request.context.request_id,
                "parakeet_backend_mismatch",
                "The request selected a different speech backend",
            ));
        }
        if model_id
            .as_deref()
            .is_some_and(|model_id| model_id != PARAKEET_MODEL_ID)
        {
            return Err(capability_error(
                &request.context.request_id,
                "parakeet_model_mismatch",
                "The request selected a different speech model",
            ));
        }
    }
    match &request.input {
        TranscriptionInput::Complete { audio } => match audio {
            AudioInput::Encoded {
                format: EncodedAudioFormat::Wav,
                ..
            }
            | AudioInput::Pcm { .. } => Ok(()),
            AudioInput::Asset { .. } => Err(capability_error(
                &request.context.request_id,
                "parakeet_asset_input_unresolved",
                "Audio assets must be resolved to WAV or PCM before embedded transcription",
            )),
            AudioInput::Encoded { .. } => Err(capability_error(
                &request.context.request_id,
                "parakeet_audio_format_unsupported",
                "The embedded Parakeet backend currently accepts WAV or PCM audio",
            )),
        },
        TranscriptionInput::Stream { .. } => Ok(()),
    }
}

fn run_transcription(
    request: TranscriptionRequest,
    model: Arc<ParakeetEOUHandle>,
    audio_receiver: Option<mpsc::Receiver<AudioChunk>>,
    cancelled: Arc<AtomicBool>,
    event_sender: &mpsc::Sender<TranscriptionEvent>,
    final_sender: oneshot::Sender<Result<TranscriptionResponse, SpeechError>>,
) {
    let request_id = request.context.request_id.clone();
    let route = resolved_route();
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
        .blocking_send(TranscriptionEvent::Started {
            request_id: request_id.clone(),
            route: route.clone(),
        })
        .is_err()
    {
        let _ = final_sender.send(Err(cancelled_error(&request_id)));
        return;
    }

    let mut recognizer = ParakeetEOU::from_shared(&model);
    let result = match (&request.input, audio_receiver) {
        (TranscriptionInput::Complete { audio }, None) => decode_complete(audio, &request_id)
            .and_then(|audio| {
                transcribe_samples(
                    &request,
                    &mut recognizer,
                    &audio,
                    &cancelled,
                    event_sender,
                    started_at,
                    route,
                )
            }),
        (TranscriptionInput::Stream { format, .. }, Some(receiver)) => transcribe_stream(
            &request,
            &mut recognizer,
            *format,
            receiver,
            &cancelled,
            event_sender,
            started_at,
            route,
        ),
        _ => Err(backend_error(
            &request_id,
            "parakeet_audio_stream_missing",
            SpeechErrorClass::Internal,
            true,
            "The streaming transcription input channel is unavailable",
        )),
    };

    match result {
        Ok(response) if cancelled.load(Ordering::Acquire) => {
            finish_cancelled(&request_id, event_sender, final_sender, response.usage);
        }
        Ok(response) => {
            let _ = event_sender.blocking_send(TranscriptionEvent::Completed {
                request_id,
                response: response.clone(),
            });
            let _ = final_sender.send(Ok(response));
        }
        Err(error) if error.class == SpeechErrorClass::Cancelled => {
            finish_cancelled(
                &request_id,
                event_sender,
                final_sender,
                SpeechUsage::default(),
            );
        }
        Err(error) => {
            let _ = event_sender.blocking_send(TranscriptionEvent::Failed {
                request_id,
                error: error.clone(),
            });
            let _ = final_sender.send(Err(error));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn transcribe_samples(
    request: &TranscriptionRequest,
    recognizer: &mut ParakeetEOU,
    audio: &[f32],
    cancelled: &AtomicBool,
    event_sender: &mpsc::Sender<TranscriptionEvent>,
    started_at: Instant,
    route: SpeechResolvedRoute,
) -> Result<TranscriptionResponse, SpeechError> {
    let mut transcript = String::new();
    let mut sequence = 0_u64;
    let mut first_result_ms = None;
    for chunk in audio.chunks(MODEL_CHUNK_SAMPLES) {
        check_cancelled(&request.context.request_id, cancelled)?;
        let mut padded = chunk.to_vec();
        padded.resize(MODEL_CHUNK_SAMPLES, 0.0);
        let delta = recognize_chunk(recognizer, &padded, &request.context.request_id)?;
        append_delta(
            request,
            &delta,
            &mut transcript,
            &mut sequence,
            &mut first_result_ms,
            started_at,
            event_sender,
        );
    }
    build_response(
        request,
        transcript,
        u64::try_from(audio.len()).unwrap_or(u64::MAX) * 1_000 / u64::from(TARGET_SAMPLE_RATE),
        started_at,
        first_result_ms,
        route,
    )
}

#[allow(clippy::too_many_arguments)]
fn transcribe_stream(
    request: &TranscriptionRequest,
    recognizer: &mut ParakeetEOU,
    format: PcmFormat,
    mut receiver: mpsc::Receiver<AudioChunk>,
    cancelled: &AtomicBool,
    event_sender: &mpsc::Sender<TranscriptionEvent>,
    started_at: Instant,
    route: SpeechResolvedRoute,
) -> Result<TranscriptionResponse, SpeechError> {
    let mut normalizer = StreamingNormalizer::new(format);
    let mut pending = Vec::new();
    let mut transcript = String::new();
    let mut sequence = 0_u64;
    let mut next_chunk_sequence = 0_u64;
    let mut next_sample_offset = 0_u64;
    let mut first_result_ms = None;

    while let Some(chunk) = receiver.blocking_recv() {
        check_cancelled(&request.context.request_id, cancelled)?;
        if chunk.sequence != next_chunk_sequence || chunk.sample_offset != next_sample_offset {
            return Err(backend_error(
                &request.context.request_id,
                "audio_stream_order_invalid",
                SpeechErrorClass::InvalidRequest,
                false,
                "Audio chunks must arrive in contiguous sequence and sample order",
            ));
        }
        next_chunk_sequence = next_chunk_sequence.saturating_add(1);
        let frames = chunk.data.len() / format.bytes_per_frame();
        next_sample_offset =
            next_sample_offset.saturating_add(u64::try_from(frames).unwrap_or(u64::MAX));
        pending.extend(normalizer.push(&chunk.data, &request.context.request_id)?);
        consume_model_chunks(
            request,
            recognizer,
            &mut pending,
            &mut transcript,
            &mut sequence,
            &mut first_result_ms,
            started_at,
            cancelled,
            event_sender,
        )?;
        if chunk.end_of_stream {
            break;
        }
    }
    pending.extend(normalizer.finish());
    if !pending.is_empty() {
        pending.resize(MODEL_CHUNK_SAMPLES, 0.0);
        let delta = recognize_chunk(recognizer, &pending, &request.context.request_id)?;
        append_delta(
            request,
            &delta,
            &mut transcript,
            &mut sequence,
            &mut first_result_ms,
            started_at,
            event_sender,
        );
    }
    let input_audio_ms =
        next_sample_offset.saturating_mul(1_000) / u64::from(format.sample_rate_hz);
    build_response(
        request,
        transcript,
        input_audio_ms,
        started_at,
        first_result_ms,
        route,
    )
}

#[allow(clippy::too_many_arguments)]
fn consume_model_chunks(
    request: &TranscriptionRequest,
    recognizer: &mut ParakeetEOU,
    pending: &mut Vec<f32>,
    transcript: &mut String,
    sequence: &mut u64,
    first_result_ms: &mut Option<u64>,
    started_at: Instant,
    cancelled: &AtomicBool,
    event_sender: &mpsc::Sender<TranscriptionEvent>,
) -> Result<(), SpeechError> {
    while pending.len() >= MODEL_CHUNK_SAMPLES {
        check_cancelled(&request.context.request_id, cancelled)?;
        let remainder = pending.split_off(MODEL_CHUNK_SAMPLES);
        let model_chunk = std::mem::replace(pending, remainder);
        let delta = recognize_chunk(recognizer, &model_chunk, &request.context.request_id)?;
        append_delta(
            request,
            &delta,
            transcript,
            sequence,
            first_result_ms,
            started_at,
            event_sender,
        );
    }
    Ok(())
}

fn recognize_chunk(
    recognizer: &mut ParakeetEOU,
    chunk: &[f32],
    request_id: &SpeechRequestId,
) -> Result<String, SpeechError> {
    recognizer.transcribe(chunk, true).map_err(|error| {
        backend_error(
            request_id,
            "parakeet_inference_failed",
            SpeechErrorClass::Unavailable,
            true,
            &format!("Embedded Parakeet inference failed: {error}"),
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn append_delta(
    request: &TranscriptionRequest,
    delta: &str,
    transcript: &mut String,
    sequence: &mut u64,
    first_result_ms: &mut Option<u64>,
    started_at: Instant,
    event_sender: &mpsc::Sender<TranscriptionEvent>,
) {
    // parakeet-rs currently decodes one SentencePiece token at a time for EOU,
    // so its word-boundary marker can survive token decoding. It is model
    // metadata, not transcript text.
    let clean = delta.replace("[EOU]", "").replace('▁', " ");
    if clean.is_empty() {
        return;
    }
    transcript.push_str(&clean);
    let observed = elapsed_ms(started_at);
    first_result_ms.get_or_insert(observed);
    if request.partial_results && event_sender.capacity() > 1 {
        // Partial text is cumulative and may be coalesced under pressure. Keep
        // one bounded slot reserved so a lifecycle terminal can never be
        // blocked behind unconsumed partials.
        let sent = event_sender.try_send(TranscriptionEvent::Partial {
            request_id: request.context.request_id.clone(),
            sequence: *sequence,
            text: transcript.clone(),
        });
        if sent.is_ok() {
            *sequence = sequence.saturating_add(1);
        }
    }
}

fn build_response(
    request: &TranscriptionRequest,
    text: String,
    input_audio_ms: u64,
    started_at: Instant,
    first_result_ms: Option<u64>,
    route: SpeechResolvedRoute,
) -> Result<TranscriptionResponse, SpeechError> {
    if input_audio_ms > MAX_AUDIO_MS {
        return Err(backend_error(
            &request.context.request_id,
            "parakeet_audio_too_long",
            SpeechErrorClass::InvalidRequest,
            false,
            "Audio exceeds the embedded Parakeet two-hour request limit",
        ));
    }
    let text = text.trim().to_string();
    let segments = if text.is_empty() {
        Vec::new()
    } else {
        vec![TranscriptSegment {
            index: 0,
            text: text.clone(),
            start_ms: None,
            end_ms: None,
            speaker: None,
            confidence: None,
            is_final: true,
        }]
    };
    Ok(TranscriptionResponse {
        request_id: request.context.request_id.clone(),
        route,
        text,
        language: Some("en".to_string()),
        segments,
        usage: SpeechUsage {
            input_audio_ms: Some(input_audio_ms),
            // The handle was resident before request admission. The one-time
            // application initialization cost is not charged to every request.
            model_load_ms: Some(0),
            time_to_first_result_ms: first_result_ms,
            total_ms: Some(elapsed_ms(started_at)),
            provenance: UsageProvenance::Exact,
            real_local_inference: true,
            ..SpeechUsage::default()
        },
    })
}

fn decode_complete(
    audio: &AudioInput,
    request_id: &SpeechRequestId,
) -> Result<Vec<f32>, SpeechError> {
    match audio {
        AudioInput::Pcm { format, data } => {
            let mono = decode_pcm(format, data, request_id)?;
            Ok(resample_complete(&mono, format.sample_rate_hz))
        }
        AudioInput::Encoded {
            format: EncodedAudioFormat::Wav,
            data,
        } => decode_wav(data, request_id),
        _ => Err(capability_error(
            request_id,
            "parakeet_audio_format_unsupported",
            "The embedded Parakeet backend currently accepts WAV or PCM audio",
        )),
    }
}

fn decode_wav(data: &[u8], request_id: &SpeechRequestId) -> Result<Vec<f32>, SpeechError> {
    let mut reader = hound::WavReader::new(Cursor::new(data)).map_err(|error| {
        invalid_audio_error(request_id, format!("WAV header is invalid: {error}"))
    })?;
    let spec = reader.spec();
    let channels = usize::from(spec.channels);
    if channels == 0 {
        return Err(invalid_audio_error(
            request_id,
            "WAV channel count is zero".to_string(),
        ));
    }
    let interleaved = match spec.sample_format {
        hound::SampleFormat::Float if spec.bits_per_sample == 32 => reader
            .samples::<f32>()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| invalid_audio_error(request_id, error.to_string()))?,
        hound::SampleFormat::Int if spec.bits_per_sample <= 32 => {
            let scale = 2_f32.powi(i32::from(spec.bits_per_sample.saturating_sub(1)));
            reader
                .samples::<i32>()
                .map(|sample| sample.map(|value| value as f32 / scale))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| invalid_audio_error(request_id, error.to_string()))?
        }
        _ => {
            return Err(invalid_audio_error(
                request_id,
                "WAV sample format is unsupported".to_string(),
            ));
        }
    };
    let mono = downmix_interleaved(&interleaved, channels);
    Ok(resample_complete(&mono, spec.sample_rate))
}

fn decode_pcm(
    format: &PcmFormat,
    data: &[u8],
    request_id: &SpeechRequestId,
) -> Result<Vec<f32>, SpeechError> {
    let bytes_per_sample = format.bytes_per_frame() / usize::from(format.channels);
    let values = data
        .chunks_exact(bytes_per_sample)
        .map(|bytes| decode_sample(format.sample_format, bytes))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|detail| invalid_audio_error(request_id, detail))?;
    let channels = usize::from(format.channels);
    if channels == 1 {
        return Ok(values);
    }
    if format.interleaved {
        Ok(downmix_interleaved(&values, channels))
    } else {
        let frames = values.len() / channels;
        let mut mono = Vec::with_capacity(frames);
        for frame in 0..frames {
            let sum = (0..channels)
                .map(|channel| values[channel * frames + frame])
                .sum::<f32>();
            mono.push(sum / channels as f32);
        }
        Ok(mono)
    }
}

fn decode_sample(format: PcmSampleFormat, bytes: &[u8]) -> Result<f32, String> {
    match format {
        PcmSampleFormat::I16Le => {
            let bytes: [u8; 2] = bytes.try_into().map_err(|_| "invalid i16 PCM sample")?;
            Ok(f32::from(i16::from_le_bytes(bytes)) / 32_768.0)
        }
        PcmSampleFormat::I24Le => {
            let [a, b, c]: [u8; 3] = bytes.try_into().map_err(|_| "invalid i24 PCM sample")?;
            let raw = i32::from(a) | (i32::from(b) << 8) | (i32::from(c) << 16);
            let signed = if raw & 0x80_0000 != 0 {
                raw | !0xFF_FFFF
            } else {
                raw
            };
            Ok(signed as f32 / 8_388_608.0)
        }
        PcmSampleFormat::I32Le => {
            let bytes: [u8; 4] = bytes.try_into().map_err(|_| "invalid i32 PCM sample")?;
            Ok(i32::from_le_bytes(bytes) as f32 / 2_147_483_648.0)
        }
        PcmSampleFormat::F32Le => {
            let bytes: [u8; 4] = bytes.try_into().map_err(|_| "invalid f32 PCM sample")?;
            let value = f32::from_le_bytes(bytes);
            if value.is_finite() {
                Ok(value.clamp(-1.0, 1.0))
            } else {
                Err("PCM contains a non-finite floating-point sample".to_string())
            }
        }
    }
}

fn downmix_interleaved(values: &[f32], channels: usize) -> Vec<f32> {
    values
        .chunks_exact(channels)
        .map(|frame| frame.iter().sum::<f32>() / channels as f32)
        .collect()
}

fn resample_complete(input: &[f32], input_rate: u32) -> Vec<f32> {
    if input_rate == TARGET_SAMPLE_RATE || input.is_empty() {
        return input.to_vec();
    }
    let output_len = u64::try_from(input.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::from(TARGET_SAMPLE_RATE))
        / u64::from(input_rate);
    let step = f64::from(input_rate) / f64::from(TARGET_SAMPLE_RATE);
    (0..usize::try_from(output_len).unwrap_or(usize::MAX))
        .map(|index| interpolate(input, index as f64 * step))
        .collect()
}

fn interpolate(input: &[f32], position: f64) -> f32 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let left = position.floor() as usize;
    let right = left.saturating_add(1).min(input.len().saturating_sub(1));
    let fraction = (position - left as f64) as f32;
    input[left] + (input[right] - input[left]) * fraction
}

struct StreamingNormalizer {
    format: PcmFormat,
    source: Vec<f32>,
    position: f64,
}

impl StreamingNormalizer {
    fn new(format: PcmFormat) -> Self {
        Self {
            format,
            source: Vec::new(),
            position: 0.0,
        }
    }

    fn push(
        &mut self,
        bytes: &[u8],
        request_id: &SpeechRequestId,
    ) -> Result<Vec<f32>, SpeechError> {
        self.source
            .extend(decode_pcm(&self.format, bytes, request_id)?);
        if self.format.sample_rate_hz == TARGET_SAMPLE_RATE {
            self.position = 0.0;
            return Ok(std::mem::take(&mut self.source));
        }
        let step = f64::from(self.format.sample_rate_hz) / f64::from(TARGET_SAMPLE_RATE);
        let mut output = Vec::new();
        while self.position + 1.0 < self.source.len() as f64 {
            output.push(interpolate(&self.source, self.position));
            self.position += step;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let consumed = self.position.floor() as usize;
        if consumed > 0 {
            self.source.drain(..consumed.min(self.source.len()));
            self.position -= consumed as f64;
        }
        Ok(output)
    }

    fn finish(&mut self) -> Vec<f32> {
        if self.source.is_empty() {
            return Vec::new();
        }
        if self.format.sample_rate_hz == TARGET_SAMPLE_RATE {
            return std::mem::take(&mut self.source);
        }
        let step = f64::from(self.format.sample_rate_hz) / f64::from(TARGET_SAMPLE_RATE);
        let mut output = Vec::new();
        while self.position < self.source.len() as f64 {
            output.push(interpolate(&self.source, self.position));
            self.position += step;
        }
        self.source.clear();
        output
    }
}

/// Resolve the EOU model without copying it out of Hugging Face's cache.
#[must_use]
pub fn discover_eou_model_dir() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("SPEECH_NATIVE_PARAKEET_MODEL_DIR")
        .or_else(|| std::env::var_os("FTE_PARAKEET_MODEL_DIR"))
    {
        let explicit = PathBuf::from(explicit);
        if model_dir_is_complete(&explicit) {
            return Some(explicit);
        }
    }
    huggingface_cache_roots()
        .into_iter()
        .find_map(|root| discover_in_hf_root(&root))
}

fn huggingface_cache_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(root) = std::env::var_os("HUGGINGFACE_HUB_CACHE") {
        roots.push(PathBuf::from(root));
    }
    if let Some(home) = std::env::var_os("HF_HOME") {
        roots.push(PathBuf::from(home).join("hub"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        roots.push(PathBuf::from(home).join(".cache/huggingface/hub"));
    }
    roots.sort();
    roots.dedup();
    roots
}

fn discover_in_hf_root(root: &Path) -> Option<PathBuf> {
    let repository = root.join("models--altunenes--parakeet-rs");
    if let Ok(reference) = std::fs::read_to_string(repository.join("refs/main")) {
        let candidate = repository
            .join("snapshots")
            .join(reference.trim())
            .join(PARAKEET_HF_SUBDIRECTORY);
        if model_dir_is_complete(&candidate) {
            return Some(candidate);
        }
    }
    let mut snapshots = std::fs::read_dir(repository.join("snapshots"))
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path().join(PARAKEET_HF_SUBDIRECTORY))
        .filter(|candidate| model_dir_is_complete(candidate))
        .collect::<Vec<_>>();
    snapshots.sort();
    snapshots.pop()
}

fn model_dir_is_complete(path: &Path) -> bool {
    ["encoder.onnx", "decoder_joint.onnx", "tokenizer.json"]
        .iter()
        .all(|file| path.join(file).is_file())
}

fn ready_descriptor() -> SpeechBackendDescriptor {
    descriptor(
        SpeechBackendReadiness::Ready,
        CapabilityAvailability::Available,
        vec![CapabilityEvidence {
            source_id: "parakeet-rs-runtime".to_string(),
            source_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            kind: EvidenceKind::RuntimeApi,
            outcome: EvidenceOutcome::Confirmed,
            observed_at_unix_ms: unix_time_ms(),
            detail: "Parakeet EOU ONNX sessions and tokenizer loaded from the Hugging Face cache"
                .to_string(),
        }],
        true,
    )
}

fn asset_required_descriptor() -> SpeechBackendDescriptor {
    descriptor(
        SpeechBackendReadiness::AssetInstallRequired {
            assets: vec![SpeechAsset {
                id: "hf.altunenes.parakeet-rs.realtime-eou-120m-v1-onnx".to_string(),
                display_name: "Parakeet Realtime EOU 120M ONNX".to_string(),
                bytes: Some(MODEL_ASSET_BYTES),
                managed_by: AssetManager::HuggingFaceCache,
            }],
        },
        CapabilityAvailability::AssetInstallRequired,
        Vec::new(),
        false,
    )
}

fn unavailable_descriptor(reason: String) -> SpeechBackendDescriptor {
    descriptor(
        SpeechBackendReadiness::Unavailable { reason },
        CapabilityAvailability::Unavailable,
        Vec::new(),
        false,
    )
}

fn descriptor(
    readiness: SpeechBackendReadiness,
    availability: CapabilityAvailability,
    evidence: Vec<CapabilityEvidence>,
    resident: bool,
) -> SpeechBackendDescriptor {
    SpeechBackendDescriptor {
        id: PARAKEET_BACKEND_ID.to_string(),
        display_name: "Parakeet Realtime EOU 120M".to_string(),
        kind: SpeechBackendKind::EmbeddedModel,
        readiness,
        capabilities: vec![SpeechCapability {
            id: "parakeet-rs.eou-120m.transcription".to_string(),
            backend_id: PARAKEET_BACKEND_ID.to_string(),
            model_id: Some(PARAKEET_MODEL_ID.to_string()),
            operation: SpeechOperationCapability::Transcription(TranscriptionCapabilities {
                streaming: true,
                partial_results: true,
                segment_timestamps: false,
                word_timestamps: false,
                diarization: false,
                translation_to_english: false,
                long_form: true,
                hotwords: false,
                generative: false,
                accepted_audio: vec![AcceptedAudio::Pcm, AcceptedAudio::Wav],
            }),
            availability,
            network: NetworkBehavior::Never,
            languages: vec!["en".to_string()],
            limits: SpeechCapabilityLimits {
                max_audio_ms: Some(MAX_AUDIO_MS),
                max_concurrent_requests: Some(4),
                ..SpeechCapabilityLimits::default()
            },
            evidence,
        }],
        models: vec![SpeechModelDescriptor {
            id: PARAKEET_MODEL_ID.to_string(),
            display_name: "Parakeet Realtime EOU 120M".to_string(),
            family: "nvidia-parakeet-eou".to_string(),
            languages: vec!["en".to_string()],
            resident,
            estimated_memory_bytes: Some(700_000_000),
            content_hash: None,
        }],
        voices: Vec::new(),
    }
}

fn resolved_route() -> SpeechResolvedRoute {
    SpeechResolvedRoute {
        backend_id: PARAKEET_BACKEND_ID.to_string(),
        model_id: Some(PARAKEET_MODEL_ID.to_string()),
        voice_id: None,
        backend_kind: SpeechBackendKind::EmbeddedModel,
        network: NetworkBehavior::Never,
    }
}

fn finish_cancelled(
    request_id: &SpeechRequestId,
    event_sender: &mpsc::Sender<TranscriptionEvent>,
    final_sender: oneshot::Sender<Result<TranscriptionResponse, SpeechError>>,
    usage: SpeechUsage,
) {
    let _ = event_sender.blocking_send(TranscriptionEvent::Cancelled {
        request_id: request_id.clone(),
        usage,
    });
    let _ = final_sender.send(Err(cancelled_error(request_id)));
}

fn check_cancelled(
    request_id: &SpeechRequestId,
    cancelled: &AtomicBool,
) -> Result<(), SpeechError> {
    if cancelled.load(Ordering::Acquire) {
        Err(cancelled_error(request_id))
    } else {
        Ok(())
    }
}

fn capability_error(request_id: &SpeechRequestId, code: &str, detail: &str) -> SpeechError {
    backend_error(
        request_id,
        code,
        SpeechErrorClass::Capability,
        false,
        detail,
    )
}

fn cancelled_error(request_id: &SpeechRequestId) -> SpeechError {
    backend_error(
        request_id,
        "speech_request_cancelled",
        SpeechErrorClass::Cancelled,
        false,
        "Speech transcription was cancelled",
    )
}

fn stream_closed_error(request_id: &SpeechRequestId) -> SpeechError {
    backend_error(
        request_id,
        "audio_stream_closed",
        SpeechErrorClass::Unavailable,
        false,
        "The transcription audio stream is closed",
    )
}

fn invalid_audio_error(request_id: &SpeechRequestId, detail: String) -> SpeechError {
    backend_error(
        request_id,
        "parakeet_audio_invalid",
        SpeechErrorClass::InvalidRequest,
        false,
        &detail,
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
        backend_id: Some(PARAKEET_BACKEND_ID.to_string()),
        safe_detail: detail.to_string(),
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i24_sign_extension_is_correct() {
        assert_eq!(decode_sample(PcmSampleFormat::I24Le, &[0, 0, 0]), Ok(0.0));
        assert!(
            decode_sample(PcmSampleFormat::I24Le, &[0, 0, 0x80])
                .is_ok_and(|value| (value + 1.0).abs() < f32::EPSILON)
        );
    }

    #[test]
    fn stereo_downmix_and_resampling_are_deterministic() {
        let mono = downmix_interleaved(&[1.0, -1.0, 0.5, 0.5], 2);
        assert_eq!(mono, vec![0.0, 0.5]);
        let resampled = resample_complete(&[0.0, 1.0, 0.0], 8_000);
        assert_eq!(resampled.len(), 6);
        assert_eq!(resampled[0], 0.0);
        assert_eq!(resampled[2], 1.0);
    }

    #[test]
    fn incomplete_model_directory_never_looks_ready() {
        let path = std::env::temp_dir().join(format!(
            "fte-parakeet-missing-{}-{}",
            std::process::id(),
            unix_time_ms()
        ));
        std::fs::create_dir_all(&path).expect("create fixture directory");
        assert!(!model_dir_is_complete(&path));
        std::fs::remove_dir_all(path).expect("remove fixture directory");
    }

    #[test]
    fn asset_blocker_is_hugging_face_cache_managed() {
        let descriptor = asset_required_descriptor();
        assert!(matches!(
            descriptor.readiness,
            SpeechBackendReadiness::AssetInstallRequired { .. }
        ));
        assert!(!descriptor.capabilities[0].eligible_for_local_only());
        assert!(!descriptor.models[0].resident);
    }
}
