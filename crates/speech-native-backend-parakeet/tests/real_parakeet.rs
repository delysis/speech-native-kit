use speech_native_backend_parakeet::{
    PARAKEET_BACKEND_ID, PARAKEET_MODEL_ID, ParakeetBackendConfig, ParakeetSpeechBackend,
};
use speech_native_host::SpeechHost;
use speech_native_types::{
    AudioChunk, AudioInput, DiarizationPolicy, EncodedAudioFormat, PcmFormat, PcmSampleFormat,
    SpeechBackend, SpeechBackendReadiness, SpeechDeadlinePolicy, SpeechRequestContext,
    SpeechRequestId, SpeechRouteSelector, SpeechRoutingPolicy, TimestampGranularity,
    TranscriptionEvent, TranscriptionInput, TranscriptionRequest, TranscriptionTask,
    TranscriptionTicket,
};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

fn request(id: &str, wav: Vec<u8>) -> TranscriptionRequest {
    TranscriptionRequest {
        context: SpeechRequestContext {
            request_id: SpeechRequestId(id.to_string()),
            client_id: "real-parakeet-test".to_string(),
            route: SpeechRouteSelector::ExactBackend {
                backend_id: PARAKEET_BACKEND_ID.to_string(),
                model_id: Some(PARAKEET_MODEL_ID.to_string()),
                voice_id: None,
            },
            routing: SpeechRoutingPolicy::default(),
            deadline: SpeechDeadlinePolicy::default(),
        },
        input: TranscriptionInput::Complete {
            audio: AudioInput::Encoded {
                format: EncodedAudioFormat::Wav,
                data: wav,
            },
        },
        language: Some("en-US".to_string()),
        task: TranscriptionTask::Transcribe,
        timestamps: TimestampGranularity::None,
        diarization: DiarizationPolicy::Disabled,
        partial_results: true,
        punctuation: true,
        hotwords: Vec::new(),
    }
}

fn stream_request(id: &str, format: PcmFormat) -> TranscriptionRequest {
    let mut request = request(id, vec![1]);
    request.input = TranscriptionInput::Stream {
        stream_id: format!("{id}-audio"),
        format,
    };
    request
}

async fn drain(
    mut ticket: TranscriptionTicket,
) -> (Vec<TranscriptionEvent>, Result<String, String>) {
    let mut events = Vec::new();
    while let Some(event) = ticket.events.recv().await {
        let terminal = event.is_terminal();
        events.push(event);
        if terminal {
            break;
        }
    }
    let result = ticket
        .final_response()
        .await
        .map(|response| response.text)
        .map_err(|error| error.code);
    (events, result)
}

fn real_wav_path() -> Option<PathBuf> {
    std::env::var_os("SPEECH_NATIVE_TEST_WAV")
        .or_else(|| std::env::var_os("FTE_SPEECH_TEST_WAV"))
        .map(PathBuf::from)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Parakeet weights in the Hugging Face cache and SPEECH_NATIVE_TEST_WAV"]
async fn real_gateway_transcription_and_request_scoped_cancellation() {
    let wav_path = real_wav_path().expect("SPEECH_NATIVE_TEST_WAV must point at real English WAV");
    let wav = std::fs::read(wav_path).expect("read real WAV fixture");
    let backend = ParakeetSpeechBackend::discover(ParakeetBackendConfig::default()).await;
    assert_eq!(backend.readiness(), SpeechBackendReadiness::Ready);
    let gateway = Arc::new(SpeechHost::default());
    gateway
        .register_backend(Arc::new(backend))
        .expect("register Parakeet backend");

    let ticket = gateway
        .transcribe(request("parakeet-real-transcript", wav.clone()))
        .await
        .expect("route real WAV to Parakeet");
    let (events, result) = drain(ticket).await;
    let transcript = result.expect("real transcript must complete");
    assert!(!transcript.trim().is_empty());
    assert!(matches!(
        events.first(),
        Some(TranscriptionEvent::Started { .. })
    ));
    assert!(
        matches!(events.last(), Some(TranscriptionEvent::Completed { response, .. }) if response.usage.real_local_inference && response.usage.model_load_ms == Some(0))
    );
    assert_eq!(events.iter().filter(|event| event.is_terminal()).count(), 1);
    eprintln!("real Parakeet transcript: {transcript}");

    let mut reader = hound::WavReader::new(Cursor::new(wav.clone())).expect("decode smoke WAV");
    let wav_spec = reader.spec();
    assert_eq!(wav_spec.sample_rate, 16_000);
    assert_eq!(wav_spec.channels, 1);
    let samples = reader
        .samples::<f32>()
        .collect::<Result<Vec<_>, _>>()
        .expect("read smoke WAV samples");
    let format = PcmFormat {
        sample_rate_hz: 16_000,
        channels: 1,
        sample_format: PcmSampleFormat::F32Le,
        interleaved: true,
    };
    let stream_ticket = gateway
        .transcribe(stream_request("parakeet-real-stream", format))
        .await
        .expect("start real streaming transcription");
    let sink = stream_ticket
        .audio_sink
        .clone()
        .expect("streaming ticket must expose an audio sink");
    let feeder = tokio::spawn(async move {
        let total_chunks = samples.chunks(2_560).len();
        let mut sample_offset = 0_u64;
        for (sequence, chunk) in samples.chunks(2_560).enumerate() {
            let mut data = Vec::with_capacity(chunk.len() * 4);
            for sample in chunk {
                data.extend_from_slice(&sample.to_le_bytes());
            }
            sink.push(AudioChunk {
                sequence: u64::try_from(sequence).expect("chunk sequence fits u64"),
                sample_offset,
                format,
                data,
                end_of_stream: sequence + 1 == total_chunks,
            })
            .await
            .expect("push real PCM chunk");
            sample_offset = sample_offset
                .saturating_add(u64::try_from(chunk.len()).expect("chunk size fits u64"));
        }
        sink.finish().await.expect("finish real PCM stream");
    });
    let (stream_events, stream_result) = drain(stream_ticket).await;
    feeder.await.expect("audio feeder task");
    let stream_transcript = stream_result.expect("real streaming transcript must complete");
    assert!(!stream_transcript.is_empty());
    assert!(matches!(
        stream_events.last(),
        Some(TranscriptionEvent::Completed { .. })
    ));
    eprintln!("real streaming Parakeet transcript: {stream_transcript}");

    let cancelled_ticket = gateway
        .transcribe(request("parakeet-real-cancelled", wav.clone()))
        .await
        .expect("start cancelled request");
    let peer_ticket = gateway
        .transcribe(request("parakeet-real-peer", wav))
        .await
        .expect("start peer request");
    assert_eq!(
        gateway.cancel(&SpeechRequestId("parakeet-real-cancelled".to_string())),
        1
    );
    let (cancelled_events, cancelled_result) = drain(cancelled_ticket).await;
    let (peer_events, peer_result) = drain(peer_ticket).await;
    assert_eq!(
        cancelled_result,
        Err("speech_request_cancelled".to_string())
    );
    assert!(matches!(
        cancelled_events.last(),
        Some(TranscriptionEvent::Cancelled { .. })
    ));
    assert!(!peer_result.expect("peer request must complete").is_empty());
    assert!(matches!(
        peer_events.last(),
        Some(TranscriptionEvent::Completed { .. })
    ));
    assert_eq!(
        cancelled_events
            .iter()
            .filter(|event| event.is_terminal())
            .count(),
        1
    );
    assert_eq!(
        peer_events
            .iter()
            .filter(|event| event.is_terminal())
            .count(),
        1
    );

    gateway.shutdown().await.expect("shutdown speech gateway");
}
