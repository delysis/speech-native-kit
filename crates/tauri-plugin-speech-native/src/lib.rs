//! Optional Tauri 2 embedding surface for `speech-native-kit`.

use serde::Serialize;
use speech_native_host::{SpeechHost, SpeechHostError, SpeechHostStatus};
use speech_native_router::SpeechRoutePlan;
use speech_native_types::{
    AudioChunk, SpeechBackend, SpeechError, SpeechRequestId, SynthesisEvent, SynthesisRequest,
    SynthesisResponse, TranscriptionAudioSink, TranscriptionEvent, TranscriptionRequest,
    TranscriptionResponse,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tauri::ipc::Channel;
use tauri::plugin::{Builder as PluginBuilder, TauriPlugin};
use tauri::{Manager, RunEvent, Runtime, State};

pub struct Builder {
    speech: Arc<SpeechHost>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            speech: Arc::new(SpeechHost::default()),
        }
    }

    #[must_use]
    pub fn with_speech_host(mut self, speech: Arc<SpeechHost>) -> Self {
        self.speech = speech;
        self
    }

    pub fn register_speech_backend(
        self,
        backend: Arc<dyn SpeechBackend>,
    ) -> Result<Self, SpeechHostError> {
        self.speech.register_backend(backend)?;
        Ok(self)
    }

    pub fn build<R: Runtime>(self) -> TauriPlugin<R> {
        let speech = self.speech;
        PluginBuilder::new("speech-native")
            .invoke_handler(tauri::generate_handler![
                speech_status,
                speech_plan_transcription,
                speech_plan_synthesis,
                speech_synthesize,
                speech_synthesize_stream,
                speech_transcribe,
                speech_transcribe_stream,
                speech_transcription_audio_push,
                speech_transcription_audio_finish,
                speech_cancel,
            ])
            .setup(move |app, _api| {
                app.manage(SpeechPluginState {
                    speech,
                    inputs: Mutex::new(HashMap::new()),
                });
                Ok(())
            })
            .on_event(|app, event| {
                if matches!(event, RunEvent::Exit)
                    && let Err(error) = shutdown_plugin(app)
                {
                    eprintln!("speech-native shutdown failed at application exit: {error}");
                }
            })
            .on_drop(|app| {
                if let Err(error) = shutdown_plugin(&app) {
                    eprintln!("speech-native shutdown failed while dropping plugin: {error}");
                }
            })
            .build()
    }
}

struct SpeechPluginState {
    speech: Arc<SpeechHost>,
    inputs: Mutex<HashMap<SpeechRequestId, Arc<dyn TranscriptionAudioSink>>>,
}

fn shutdown_plugin<R: Runtime>(app: &tauri::AppHandle<R>) -> Result<(), SpeechHostError> {
    let Some(state) = app.try_state::<SpeechPluginState>() else {
        return Ok(());
    };
    state
        .inputs
        .lock()
        .map_err(|_| {
            speech_input_state_unavailable(&SpeechRequestId("speech-shutdown".to_string()))
        })?
        .clear();
    let speech = Arc::clone(&state.speech);
    tauri::async_runtime::block_on(async move { speech.shutdown().await })
}

pub trait SpeechNativeExt<R: Runtime> {
    fn speech_native(&self) -> Arc<SpeechHost>;
}

impl<R: Runtime, T: Manager<R>> SpeechNativeExt<R> for T {
    fn speech_native(&self) -> Arc<SpeechHost> {
        Arc::clone(&self.state::<SpeechPluginState>().speech)
    }
}

#[derive(Debug, Clone, Serialize)]
struct CancelResult {
    cancelled: usize,
}

#[tauri::command]
fn speech_status(state: State<'_, SpeechPluginState>) -> Result<SpeechHostStatus, SpeechHostError> {
    state.speech.status()
}

#[tauri::command]
fn speech_plan_transcription(
    request: TranscriptionRequest,
    state: State<'_, SpeechPluginState>,
) -> Result<SpeechRoutePlan, SpeechHostError> {
    state.speech.plan_transcription(&request)
}

#[tauri::command]
fn speech_plan_synthesis(
    request: SynthesisRequest,
    state: State<'_, SpeechPluginState>,
) -> Result<SpeechRoutePlan, SpeechHostError> {
    state.speech.plan_synthesis(&request)
}

#[tauri::command]
async fn speech_synthesize(
    request: SynthesisRequest,
    state: State<'_, SpeechPluginState>,
) -> Result<SynthesisResponse, SpeechHostError> {
    let mut ticket = state.speech.synthesize(request).await?;
    while let Some(event) = ticket.events.recv().await {
        if event.is_terminal() {
            break;
        }
    }
    ticket.final_response().await.map_err(Into::into)
}

#[tauri::command]
async fn speech_synthesize_stream(
    request: SynthesisRequest,
    on_event: Channel<SynthesisEvent>,
    state: State<'_, SpeechPluginState>,
) -> Result<SynthesisResponse, SpeechHostError> {
    let request_id = request.context.request_id.clone();
    let mut ticket = state.speech.synthesize(request).await?;
    while let Some(event) = ticket.events.recv().await {
        let terminal = event.is_terminal();
        on_event
            .send(event)
            .map_err(|_| speech_channel_closed(&request_id))?;
        if terminal {
            break;
        }
    }
    ticket.final_response().await.map_err(Into::into)
}

#[tauri::command]
async fn speech_transcribe(
    request: TranscriptionRequest,
    state: State<'_, SpeechPluginState>,
) -> Result<TranscriptionResponse, SpeechHostError> {
    let mut ticket = state.speech.transcribe(request).await?;
    while let Some(event) = ticket.events.recv().await {
        if event.is_terminal() {
            break;
        }
    }
    ticket.final_response().await.map_err(Into::into)
}

#[tauri::command]
async fn speech_transcribe_stream(
    request: TranscriptionRequest,
    on_event: Channel<TranscriptionEvent>,
    state: State<'_, SpeechPluginState>,
) -> Result<TranscriptionResponse, SpeechHostError> {
    let request_id = request.context.request_id.clone();
    let mut ticket = state.speech.transcribe(request).await?;
    if let Some(sink) = ticket.audio_sink.clone() {
        state
            .inputs
            .lock()
            .map_err(|_| speech_input_state_unavailable(&request_id))?
            .insert(request_id.clone(), sink);
    }
    let mut channel_error = None;
    while let Some(event) = ticket.events.recv().await {
        let terminal = event.is_terminal();
        if on_event.send(event).is_err() {
            channel_error = Some(speech_channel_closed(&request_id));
            break;
        }
        if terminal {
            break;
        }
    }
    if let Ok(mut inputs) = state.inputs.lock() {
        inputs.remove(&request_id);
    }
    if let Some(error) = channel_error {
        return Err(error);
    }
    ticket.final_response().await.map_err(Into::into)
}

#[tauri::command]
async fn speech_transcription_audio_push(
    request_id: String,
    chunk: AudioChunk,
    state: State<'_, SpeechPluginState>,
) -> Result<(), SpeechHostError> {
    let request_id = SpeechRequestId(request_id);
    let sink = state
        .inputs
        .lock()
        .map_err(|_| speech_input_state_unavailable(&request_id))?
        .get(&request_id)
        .cloned()
        .ok_or_else(|| speech_input_missing(&request_id))?;
    sink.push(chunk).await.map_err(Into::into)
}

#[tauri::command]
async fn speech_transcription_audio_finish(
    request_id: String,
    state: State<'_, SpeechPluginState>,
) -> Result<(), SpeechHostError> {
    let request_id = SpeechRequestId(request_id);
    let sink = state
        .inputs
        .lock()
        .map_err(|_| speech_input_state_unavailable(&request_id))?
        .get(&request_id)
        .cloned()
        .ok_or_else(|| speech_input_missing(&request_id))?;
    sink.finish().await.map_err(Into::into)
}

#[tauri::command]
fn speech_cancel(request_id: String, state: State<'_, SpeechPluginState>) -> CancelResult {
    CancelResult {
        cancelled: state.speech.cancel(&SpeechRequestId(request_id)),
    }
}

fn speech_channel_closed(request_id: &SpeechRequestId) -> SpeechHostError {
    SpeechError::unavailable(
        request_id,
        "tauri_speech_event_consumer_closed",
        "the Tauri speech event consumer closed before the request completed",
    )
    .into()
}

fn speech_input_state_unavailable(request_id: &SpeechRequestId) -> SpeechHostError {
    SpeechError::unavailable(
        request_id,
        "tauri_speech_input_state_unavailable",
        "the Tauri streaming speech-input registry is unavailable",
    )
    .into()
}

fn speech_input_missing(request_id: &SpeechRequestId) -> SpeechHostError {
    SpeechError::unavailable(
        request_id,
        "tauri_speech_input_missing",
        "no active streaming transcription accepts audio for this request",
    )
    .into()
}
