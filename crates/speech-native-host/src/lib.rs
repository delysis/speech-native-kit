//! Backend registry and execution service for interoperable speech consumers.
//!
//! The service owns no Tauri state. It plans over the descriptors of actually
//! registered backends, pins the resolved backend/model/voice into the request,
//! and then dispatches through the protocol-neutral `SpeechBackend` trait.

use serde::{Deserialize, Serialize};
use speech_native_router::{SpeechRouteError, SpeechRoutePlan, SpeechRouter};
use speech_native_types::{
    CapabilitySourceReport, PlatformCapabilitySnapshot, PlatformTarget, ProbeSourceStatus,
    SPEECH_CAPABILITY_SCHEMA, SpeechBackend, SpeechBackendDescriptor, SpeechError,
    SpeechErrorClass, SpeechRequestId, SpeechRouteSelector, SynthesisRequest, SynthesisTicket,
    TranscriptionRequest, TranscriptionTicket,
};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const REGISTERED_SOURCE_ID: &str = "registered-speech-backends";

#[derive(Debug, thiserror::Error, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpeechHostError {
    #[error("speech route planning failed: {error}")]
    Route { error: SpeechRouteError },
    #[error("speech backend failed: {error}")]
    Backend { error: SpeechError },
    #[error("speech gateway state is unavailable")]
    StateUnavailable,
    #[error("speech backend id is already registered: {backend_id}")]
    BackendDuplicate { backend_id: String },
    #[error("speech backend descriptor is invalid: {detail}")]
    BackendInvalid { detail: String },
    #[error("selected speech backend is no longer registered: {backend_id}")]
    BackendMissing { backend_id: String },
    #[error("one or more speech backends failed during shutdown")]
    Shutdown { failures: Vec<SpeechError> },
}

impl From<SpeechRouteError> for SpeechHostError {
    fn from(error: SpeechRouteError) -> Self {
        Self::Route { error }
    }
}

impl From<SpeechError> for SpeechHostError {
    fn from(error: SpeechError) -> Self {
        Self::Backend { error }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpeechHostStatus {
    pub target: PlatformTarget,
    pub backends: Vec<SpeechBackendDescriptor>,
}

pub struct SpeechHost {
    target: PlatformTarget,
    router: SpeechRouter,
    backends: RwLock<BTreeMap<String, Arc<dyn SpeechBackend>>>,
}

impl std::fmt::Debug for SpeechHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpeechHost")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl Default for SpeechHost {
    fn default() -> Self {
        Self::new(PlatformTarget::current())
    }
}

impl SpeechHost {
    #[must_use]
    pub fn new(target: PlatformTarget) -> Self {
        Self {
            target,
            router: SpeechRouter,
            backends: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn register_backend(&self, backend: Arc<dyn SpeechBackend>) -> Result<(), SpeechHostError> {
        let descriptor = backend.descriptor();
        descriptor
            .validate()
            .map_err(|error| SpeechHostError::BackendInvalid {
                detail: error.to_string(),
            })?;
        let mut backends = self
            .backends
            .write()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        if backends.contains_key(&descriptor.id) {
            return Err(SpeechHostError::BackendDuplicate {
                backend_id: descriptor.id,
            });
        }
        backends.insert(descriptor.id, backend);
        Ok(())
    }

    pub fn status(&self) -> Result<SpeechHostStatus, SpeechHostError> {
        Ok(SpeechHostStatus {
            target: self.target.clone(),
            backends: self.descriptors()?,
        })
    }

    pub fn descriptors(&self) -> Result<Vec<SpeechBackendDescriptor>, SpeechHostError> {
        let backends = self
            .backends
            .read()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        Ok(backends
            .values()
            .map(|backend| backend.descriptor())
            .collect())
    }

    pub fn snapshot(&self) -> Result<PlatformCapabilitySnapshot, SpeechHostError> {
        Ok(PlatformCapabilitySnapshot {
            schema: SPEECH_CAPABILITY_SCHEMA.to_string(),
            captured_at_unix_ms: unix_time_ms(),
            target: self.target.clone(),
            adapter_candidates: Vec::new(),
            source_reports: vec![CapabilitySourceReport {
                source_id: REGISTERED_SOURCE_ID.to_string(),
                status: ProbeSourceStatus::Succeeded,
                detail: None,
                backends: self.descriptors()?,
            }],
        })
    }

    pub fn plan_transcription(
        &self,
        request: &TranscriptionRequest,
    ) -> Result<SpeechRoutePlan, SpeechHostError> {
        Ok(self.router.plan_transcription(request, &self.snapshot()?)?)
    }

    pub fn plan_synthesis(
        &self,
        request: &SynthesisRequest,
    ) -> Result<SpeechRoutePlan, SpeechHostError> {
        Ok(self.router.plan_synthesis(request, &self.snapshot()?)?)
    }

    pub async fn transcribe(
        &self,
        mut request: TranscriptionRequest,
    ) -> Result<TranscriptionTicket, SpeechHostError> {
        let plan = self.plan_transcription(&request)?;
        pin_route(&mut request.context.route, &plan);
        let backend = self.backend(&plan.selected.route.backend_id)?;
        Ok(backend.transcribe(request).await?)
    }

    pub async fn synthesize(
        &self,
        mut request: SynthesisRequest,
    ) -> Result<SynthesisTicket, SpeechHostError> {
        let plan = self.plan_synthesis(&request)?;
        pin_route(&mut request.context.route, &plan);
        let backend = self.backend(&plan.selected.route.backend_id)?;
        Ok(backend.synthesize(request).await?)
    }

    #[must_use]
    pub fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        self.backends.read().map_or(0, |backends| {
            backends
                .values()
                .map(|backend| backend.cancel(request_id))
                .sum()
        })
    }

    pub async fn shutdown(&self) -> Result<(), SpeechHostError> {
        let backends = self
            .backends
            .read()
            .map_err(|_| SpeechHostError::StateUnavailable)?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for backend in backends {
            if let Err(error) = backend.shutdown().await {
                failures.push(error);
            }
        }
        if !failures.is_empty() {
            return Err(SpeechHostError::Shutdown { failures });
        }
        Ok(())
    }

    fn backend(&self, backend_id: &str) -> Result<Arc<dyn SpeechBackend>, SpeechHostError> {
        self.backends
            .read()
            .map_err(|_| SpeechHostError::StateUnavailable)?
            .get(backend_id)
            .cloned()
            .ok_or_else(|| SpeechHostError::BackendMissing {
                backend_id: backend_id.to_string(),
            })
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn pin_route(route: &mut SpeechRouteSelector, plan: &SpeechRoutePlan) {
    *route = SpeechRouteSelector::ExactBackend {
        backend_id: plan.selected.route.backend_id.clone(),
        model_id: plan.selected.route.model_id.clone(),
        voice_id: plan.selected.route.voice_id.clone(),
    };
}

#[must_use]
pub fn service_error(request_id: &SpeechRequestId, error: SpeechHostError) -> SpeechError {
    SpeechError {
        code: "speech_gateway_failed".to_string(),
        class: SpeechErrorClass::Unavailable,
        retryable: false,
        request_id: request_id.clone(),
        backend_id: None,
        safe_detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use speech_native_types::{
        AlignmentGranularity, AudioOutputFormat, AudioOutputKind, CapabilityAvailability,
        CapabilityEvidence, EvidenceKind, EvidenceOutcome, NetworkBehavior, SpeechBackendKind,
        SpeechBackendReadiness, SpeechCancellation, SpeechCapability, SpeechCapabilityLimits,
        SpeechDeadlinePolicy, SpeechOperationCapability, SpeechRequestContext, SpeechResolvedRoute,
        SpeechRoutingPolicy, SpeechUsage, SynthesisCapabilities, SynthesisEvent, SynthesisInput,
        SynthesisResponse, UsageProvenance, VoiceDescriptor, VoiceQuality, VoiceSelector,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{mpsc, oneshot};

    #[derive(Default)]
    struct FixtureCancellation;

    impl SpeechCancellation for FixtureCancellation {
        fn cancel(&self, _request_id: &SpeechRequestId) -> usize {
            0
        }
    }

    struct FixtureBackend {
        descriptor: SpeechBackendDescriptor,
        calls: AtomicUsize,
        shutdown_calls: AtomicUsize,
        fail_shutdown: bool,
    }

    #[async_trait]
    impl SpeechBackend for FixtureBackend {
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
            Err(SpeechError::unavailable(
                &request.context.request_id,
                "fixture_transcription_unsupported",
                "fixture supports only synthesis",
            ))
        }

        async fn synthesize(
            &self,
            request: SynthesisRequest,
        ) -> Result<SynthesisTicket, SpeechError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            let request_id = request.context.request_id.clone();
            let SpeechRouteSelector::ExactBackend {
                backend_id,
                model_id,
                voice_id,
            } = request.context.route
            else {
                return Err(SpeechError::invalid_request(
                    &request_id,
                    "fixture_route_unpinned",
                    "gateway did not pin the selected route",
                ));
            };
            let route = SpeechResolvedRoute {
                backend_id,
                model_id,
                voice_id,
                backend_kind: self.descriptor.kind,
                network: NetworkBehavior::Never,
            };
            let response = SynthesisResponse {
                request_id: request_id.clone(),
                route: route.clone(),
                audio: b"RIFFfixtureWAVE".to_vec(),
                format: AudioOutputFormat::Wav,
                duration_ms: Some(1),
                alignments: Vec::new(),
                usage: SpeechUsage {
                    provenance: UsageProvenance::Exact,
                    real_local_inference: true,
                    ..SpeechUsage::default()
                },
            };
            let (event_sender, event_receiver) = mpsc::channel(4);
            let (final_sender, final_receiver) = oneshot::channel();
            event_sender
                .try_send(SynthesisEvent::Started {
                    request_id: request_id.clone(),
                    route,
                })
                .map_err(|_| {
                    SpeechError::unavailable(
                        &request_id,
                        "fixture_event_failed",
                        "fixture event channel failed",
                    )
                })?;
            event_sender
                .try_send(SynthesisEvent::Completed {
                    request_id: request_id.clone(),
                    response: response.clone(),
                })
                .map_err(|_| {
                    SpeechError::unavailable(
                        &request_id,
                        "fixture_event_failed",
                        "fixture event channel failed",
                    )
                })?;
            drop(event_sender);
            let _ = final_sender.send(Ok(response));
            Ok(SynthesisTicket::new(
                request_id,
                event_receiver,
                final_receiver,
                Arc::new(FixtureCancellation),
            ))
        }

        fn cancel(&self, _request_id: &SpeechRequestId) -> usize {
            0
        }

        async fn shutdown(&self) -> Result<(), SpeechError> {
            self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
            if self.fail_shutdown {
                Err(SpeechError::unavailable(
                    &SpeechRequestId(format!("{}.shutdown", self.descriptor.id)),
                    "fixture_shutdown_failed",
                    "fixture shutdown failed",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn fixture_backend(id: &str) -> Arc<FixtureBackend> {
        Arc::new(FixtureBackend {
            descriptor: SpeechBackendDescriptor {
                id: id.to_string(),
                display_name: id.to_string(),
                kind: SpeechBackendKind::EmbeddedModel,
                readiness: SpeechBackendReadiness::Ready,
                capabilities: vec![SpeechCapability {
                    id: format!("{id}.synthesis"),
                    backend_id: id.to_string(),
                    model_id: Some("fixture-voice-model".to_string()),
                    operation: SpeechOperationCapability::Synthesis(SynthesisCapabilities {
                        returned_audio: vec![AudioOutputKind::Wav],
                        voice_selection: true,
                        ..SynthesisCapabilities::default()
                    }),
                    availability: CapabilityAvailability::Available,
                    network: NetworkBehavior::Never,
                    languages: vec!["en-US".to_string()],
                    limits: SpeechCapabilityLimits::default(),
                    evidence: vec![CapabilityEvidence {
                        source_id: "fixture".to_string(),
                        source_version: Some("1".to_string()),
                        kind: EvidenceKind::RuntimeApi,
                        outcome: EvidenceOutcome::Confirmed,
                        observed_at_unix_ms: 1,
                        detail: "fixture backend".to_string(),
                    }],
                }],
                models: Vec::new(),
                voices: vec![VoiceDescriptor {
                    id: "fixture-voice".to_string(),
                    name: "Fixture".to_string(),
                    language: "en-US".to_string(),
                    gender: None,
                    quality: Some(VoiceQuality::Normal),
                    expected_latency: None,
                    network: NetworkBehavior::Never,
                    installed: true,
                }],
            },
            calls: AtomicUsize::new(0),
            shutdown_calls: AtomicUsize::new(0),
            fail_shutdown: false,
        })
    }

    fn failing_fixture_backend(id: &str) -> Arc<FixtureBackend> {
        let mut backend = fixture_backend(id);
        Arc::get_mut(&mut backend)
            .expect("new fixture backend must be uniquely owned")
            .fail_shutdown = true;
        backend
    }

    fn request() -> SynthesisRequest {
        SynthesisRequest {
            context: SpeechRequestContext {
                request_id: SpeechRequestId("speech-service-test".to_string()),
                client_id: "test".to_string(),
                route: SpeechRouteSelector::Auto,
                routing: SpeechRoutingPolicy::default(),
                deadline: SpeechDeadlinePolicy::default(),
            },
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
        }
    }

    #[tokio::test]
    async fn service_plans_pins_and_executes_registered_backend() {
        let gateway = SpeechHost::default();
        let backend = fixture_backend("fixture.tts");
        gateway
            .register_backend(backend.clone())
            .expect("register fixture backend");
        let mut ticket = gateway
            .synthesize(request())
            .await
            .expect("dispatch synthesis");
        let mut terminal = 0;
        while let Some(event) = ticket.events.recv().await {
            terminal += usize::from(event.is_terminal());
        }
        let response = ticket.final_response().await.expect("final response");
        assert_eq!(response.route.backend_id, "fixture.tts");
        assert_eq!(
            response.route.model_id.as_deref(),
            Some("fixture-voice-model")
        );
        assert_eq!(response.route.voice_id.as_deref(), Some("fixture-voice"));
        assert_eq!(terminal, 1);
        assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn duplicate_backend_registration_fails_closed() {
        let gateway = SpeechHost::default();
        gateway
            .register_backend(fixture_backend("fixture.tts"))
            .expect("first registration");
        assert!(matches!(
            gateway.register_backend(fixture_backend("fixture.tts")),
            Err(SpeechHostError::BackendDuplicate { .. })
        ));
    }

    #[tokio::test]
    async fn shutdown_attempts_every_backend_and_reports_all_failures() {
        let gateway = SpeechHost::default();
        let healthy = fixture_backend("healthy.tts");
        let first_failure = failing_fixture_backend("failure-a.tts");
        let second_failure = failing_fixture_backend("failure-b.tts");
        for backend in [&healthy, &first_failure, &second_failure] {
            gateway
                .register_backend(backend.clone())
                .expect("register shutdown fixture");
        }

        let error = gateway
            .shutdown()
            .await
            .expect_err("shutdown must report failures");
        let SpeechHostError::Shutdown { failures } = error else {
            panic!("expected aggregated shutdown failure");
        };
        assert_eq!(failures.len(), 2);
        assert_eq!(healthy.shutdown_calls.load(Ordering::Acquire), 1);
        assert_eq!(first_failure.shutdown_calls.load(Ordering::Acquire), 1);
        assert_eq!(second_failure.shutdown_calls.load(Ordering::Acquire), 1);
    }
}
