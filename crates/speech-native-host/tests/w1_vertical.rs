#![forbid(unsafe_code)]
#![cfg(feature = "unstable-w1-vertical-tests")]

use async_trait::async_trait;
use platform_vertical_fixtures_v0::{
    EquivalenceProjectionV0, ObservationEnvelopeV0, VerticalFixtureManifestV0, sha256_identity,
    validate_baseline, validate_manifest, verify_prerequisite_chunks,
};
use serde::Deserialize;
use serde_json::json;
use speech_native_host::SpeechHost;
use speech_native_types::{
    AlignmentGranularity, AudioOutputFormat, AudioOutputKind, CapabilityAvailability,
    CapabilityEvidence, EvidenceKind, EvidenceOutcome, NetworkBehavior, SpeechBackend,
    SpeechBackendDescriptor, SpeechBackendKind, SpeechBackendReadiness, SpeechCancellation,
    SpeechCapability, SpeechCapabilityLimits, SpeechDeadlinePolicy, SpeechError, SpeechErrorClass,
    SpeechOperationCapability, SpeechRequestContext, SpeechRequestId, SpeechResolvedRoute,
    SpeechRouteSelector, SpeechRoutingPolicy, SpeechUsage, SynthesisCapabilities, SynthesisEvent,
    SynthesisInput, SynthesisRequest, SynthesisResponse, SynthesisTicket, TranscriptionRequest,
    TranscriptionTicket, VoiceSelector,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

const BASELINE_COMMIT: &str = "89146595df7ba893ddd704a811377f6cb14856bc";
const PRODUCTION_TREE: &[u8] =
    include_bytes!("../../../fixtures/w1/source/speech-production-tree-8914659.json");

#[derive(Deserialize)]
struct ProductionTreeDescriptor {
    commit: String,
    source_roots: BTreeMap<String, String>,
}

fn fixture_bytes(relative_path: &str) -> &'static [u8] {
    match relative_path {
        "apple-installed-voice-inventory-v1.json" => {
            include_bytes!("../../../fixtures/w1/apple-installed-voice-inventory-v1.json")
        }
        "manifests/apple-installed-voice-v0.json" => {
            include_bytes!("../../../fixtures/w1/manifests/apple-installed-voice-v0.json")
        }
        "manifests/current-parakeet-model-audio-v0.json" => {
            include_bytes!("../../../fixtures/w1/manifests/current-parakeet-model-audio-v0.json")
        }
        "manifests/speech-peer-cancellation-v0.json" => {
            include_bytes!("../../../fixtures/w1/manifests/speech-peer-cancellation-v0.json")
        }
        "parakeet-exact-artifacts-v1.json" => {
            include_bytes!("../../../fixtures/w1/parakeet-exact-artifacts-v1.json")
        }
        "projections/apple-installed-voice-v1.json" => {
            include_bytes!("../../../fixtures/w1/projections/apple-installed-voice-v1.json")
        }
        "projections/current-parakeet-model-audio-v1.json" => {
            include_bytes!("../../../fixtures/w1/projections/current-parakeet-model-audio-v1.json")
        }
        "projections/speech-peer-cancellation-v1.json" => {
            include_bytes!("../../../fixtures/w1/projections/speech-peer-cancellation-v1.json")
        }
        "source/speech-production-tree-8914659.json" => PRODUCTION_TREE,
        "speech-peer-cancellation-v1.json" => {
            include_bytes!("../../../fixtures/w1/speech-peer-cancellation-v1.json")
        }
        _ => panic!("unmapped Speech W1 fixture artifact: {relative_path}"),
    }
}

struct NoopCancellation;

impl SpeechCancellation for NoopCancellation {
    fn cancel(&self, _request_id: &SpeechRequestId) -> usize {
        0
    }
}

struct DeferredFinal {
    result: oneshot::Sender<Result<SynthesisResponse, SpeechError>>,
    events: mpsc::Sender<SynthesisEvent>,
}

struct DeterministicBackend {
    descriptor: SpeechBackendDescriptor,
    pending: Mutex<BTreeMap<SpeechRequestId, DeferredFinal>>,
    cancel_calls: AtomicUsize,
    shutdown_calls: AtomicUsize,
}

impl DeterministicBackend {
    fn new(id: &str) -> Arc<Self> {
        Arc::new(Self {
            descriptor: SpeechBackendDescriptor {
                id: id.to_owned(),
                display_name: id.to_owned(),
                kind: SpeechBackendKind::EmbeddedModel,
                readiness: SpeechBackendReadiness::Ready,
                capabilities: vec![SpeechCapability {
                    id: format!("{id}.synthesis"),
                    backend_id: id.to_owned(),
                    model_id: Some("fixture-voice-model".to_owned()),
                    operation: SpeechOperationCapability::Synthesis(SynthesisCapabilities {
                        returned_audio: vec![AudioOutputKind::Wav],
                        voice_selection: false,
                        ..SynthesisCapabilities::default()
                    }),
                    availability: CapabilityAvailability::Available,
                    network: NetworkBehavior::Never,
                    languages: vec!["en-US".to_owned()],
                    limits: SpeechCapabilityLimits::default(),
                    evidence: vec![CapabilityEvidence {
                        source_id: "w1-deterministic-peer".to_owned(),
                        source_version: Some("1".to_owned()),
                        kind: EvidenceKind::RuntimeApi,
                        outcome: EvidenceOutcome::Confirmed,
                        observed_at_unix_ms: 1,
                        detail: "deterministic in-process backend with network=never".to_owned(),
                    }],
                }],
                models: Vec::new(),
                voices: Vec::new(),
            },
            pending: Mutex::new(BTreeMap::new()),
            cancel_calls: AtomicUsize::new(0),
            shutdown_calls: AtomicUsize::new(0),
        })
    }

    fn complete(&self, request_id: &SpeechRequestId) {
        let pending = self
            .pending
            .lock()
            .expect("lock deterministic backend")
            .remove(request_id)
            .expect("request remains pending");
        let response = SynthesisResponse {
            request_id: request_id.clone(),
            route: SpeechResolvedRoute {
                backend_id: self.descriptor.id.clone(),
                model_id: Some("fixture-voice-model".to_owned()),
                voice_id: None,
                backend_kind: SpeechBackendKind::EmbeddedModel,
                network: NetworkBehavior::Never,
            },
            audio: b"RIFFfixtureWAVE".to_vec(),
            format: AudioOutputFormat::Wav,
            duration_ms: Some(1),
            alignments: Vec::new(),
            usage: SpeechUsage::default(),
        };
        pending
            .events
            .try_send(SynthesisEvent::Completed {
                request_id: request_id.clone(),
                response: response.clone(),
            })
            .expect("publish deterministic completion");
        let _ = pending.result.send(Ok(response));
    }
}

#[async_trait]
impl SpeechBackend for DeterministicBackend {
    fn descriptor(&self) -> SpeechBackendDescriptor {
        self.descriptor.clone()
    }

    fn readiness(&self) -> SpeechBackendReadiness {
        SpeechBackendReadiness::Ready
    }

    async fn transcribe(
        &self,
        request: TranscriptionRequest,
    ) -> Result<TranscriptionTicket, SpeechError> {
        Err(SpeechError::unavailable(
            &request.context.request_id,
            "fixture_transcription_unsupported",
            "the deterministic W1 backend supports synthesis only",
        ))
    }

    async fn synthesize(&self, request: SynthesisRequest) -> Result<SynthesisTicket, SpeechError> {
        let request_id = request.context.request_id;
        let (event_sender, event_receiver) = mpsc::channel(2);
        let (final_sender, final_receiver) = oneshot::channel();
        let route = SpeechResolvedRoute {
            backend_id: self.descriptor.id.clone(),
            model_id: Some("fixture-voice-model".to_owned()),
            voice_id: None,
            backend_kind: SpeechBackendKind::EmbeddedModel,
            network: NetworkBehavior::Never,
        };
        event_sender
            .try_send(SynthesisEvent::Started {
                request_id: request_id.clone(),
                route,
            })
            .expect("publish deterministic start");
        self.pending
            .lock()
            .map_err(|_| {
                SpeechError::unavailable(
                    &request_id,
                    "fixture_state_unavailable",
                    "the deterministic W1 backend state is unavailable",
                )
            })?
            .insert(
                request_id.clone(),
                DeferredFinal {
                    result: final_sender,
                    events: event_sender,
                },
            );
        Ok(SynthesisTicket::new(
            request_id,
            event_receiver,
            final_receiver,
            Arc::new(NoopCancellation),
        ))
    }

    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        let Ok(mut pending) = self.pending.lock() else {
            return 0;
        };
        let Some(pending) = pending.remove(request_id) else {
            return 0;
        };
        self.cancel_calls.fetch_add(1, Ordering::AcqRel);
        let error = SpeechError {
            code: "speech_request_cancelled".to_owned(),
            class: SpeechErrorClass::Cancelled,
            retryable: false,
            request_id: request_id.clone(),
            backend_id: Some(self.descriptor.id.clone()),
            safe_detail: "the selected deterministic request was cancelled".to_owned(),
        };
        let _ = pending.events.try_send(SynthesisEvent::Cancelled {
            request_id: request_id.clone(),
            usage: SpeechUsage::default(),
        });
        let _ = pending.result.send(Err(error));
        1
    }

    async fn shutdown(&self) -> Result<(), SpeechError> {
        assert!(
            self.pending.lock().is_ok_and(|pending| pending.is_empty()),
            "shutdown cannot abandon a pending request"
        );
        self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

fn request(request_id: &str, backend_id: &str) -> SynthesisRequest {
    SynthesisRequest {
        context: SpeechRequestContext {
            request_id: SpeechRequestId(request_id.to_owned()),
            client_id: "w1-speech-peer-fixture".to_owned(),
            route: SpeechRouteSelector::ExactBackend {
                backend_id: backend_id.to_owned(),
                model_id: Some("fixture-voice-model".to_owned()),
                voice_id: None,
            },
            routing: SpeechRoutingPolicy::default(),
            deadline: SpeechDeadlinePolicy::default(),
        },
        input: SynthesisInput::Text {
            text: "Deterministic speech fixture.".to_owned(),
        },
        voice: VoiceSelector::Auto,
        language: Some("en-US".to_owned()),
        rate: 1.0,
        pitch: 1.0,
        volume: 1.0,
        output: AudioOutputFormat::Wav,
        alignment: AlignmentGranularity::None,
        stream: false,
    }
}

async fn drain(
    mut ticket: SynthesisTicket,
) -> (Vec<SynthesisEvent>, Result<SynthesisResponse, SpeechError>) {
    let mut events = Vec::new();
    while let Some(event) = ticket.events.recv().await {
        let terminal = event.is_terminal();
        events.push(event);
        if terminal {
            break;
        }
    }
    let final_result = ticket.final_response().await;
    (events, final_result)
}

#[tokio::test]
async fn w1_deterministic_peer_cancellation_projection() {
    let host = SpeechHost::default();
    let cancelled_backend = DeterministicBackend::new("fixture-a.tts");
    let peer_backend = DeterministicBackend::new("fixture-b.tts");
    host.register_backend(cancelled_backend.clone())
        .expect("register cancelled peer backend");
    host.register_backend(peer_backend.clone())
        .expect("register completing peer backend");

    let cancelled_id = SpeechRequestId("speech-peer-cancelled".to_owned());
    let peer_id = SpeechRequestId("speech-peer-completed".to_owned());
    let cancelled_ticket = host
        .synthesize(request(&cancelled_id.0, "fixture-a.tts"))
        .await
        .expect("admit cancelled request");
    let peer_ticket = host
        .synthesize(request(&peer_id.0, "fixture-b.tts"))
        .await
        .expect("admit completing peer");
    assert_eq!(host.cancel(&cancelled_id), 1);
    assert_eq!(cancelled_backend.cancel_calls.load(Ordering::Acquire), 1);
    assert_eq!(peer_backend.cancel_calls.load(Ordering::Acquire), 0);
    peer_backend.complete(&peer_id);

    let (cancelled_events, cancelled_result) = drain(cancelled_ticket).await;
    let (peer_events, peer_result) = drain(peer_ticket).await;
    assert_eq!(
        cancelled_result
            .expect_err("selected request is cancelled")
            .code,
        "speech_request_cancelled"
    );
    let peer_response = peer_result.expect("unaffected peer completes");
    assert_eq!(peer_response.route.backend_id, "fixture-b.tts");
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
    assert!(matches!(
        cancelled_events.last(),
        Some(SynthesisEvent::Cancelled { .. })
    ));
    assert!(matches!(
        peer_events.last(),
        Some(SynthesisEvent::Completed { .. })
    ));

    host.shutdown().await.expect("joined host shutdown");
    assert_eq!(cancelled_backend.shutdown_calls.load(Ordering::Acquire), 1);
    assert_eq!(peer_backend.shutdown_calls.load(Ordering::Acquire), 1);

    let projection: EquivalenceProjectionV0 = serde_json::from_value(json!({
        "ordered_events": [
            {"sequence": 0, "operation_id": "speech-peer-cancelled", "attempt_id": "attempt-1", "correlation_id": "speech-peer-fixture", "kind": "cancelled", "payload": null},
            {"sequence": 1, "operation_id": "speech-peer-completed", "attempt_id": "attempt-1", "correlation_id": "speech-peer-fixture", "kind": "completed", "payload": null}
        ],
        "durable_state": [{
            "state_id": "speech.host.active_routes",
            "schema_id": "speech.host.active_routes.v1",
            "before": {"id": "speech.active_routes.empty.before", "digest": {"algorithm": "sha256", "hex": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}, "length": 2},
            "after": {"id": "speech.active_routes.empty.before", "digest": {"algorithm": "sha256", "hex": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}, "length": 2},
            "disposition": "unchanged"
        }],
        "lifecycle": [
            {"operation_id": "speech-peer-cancelled", "attempt_id": "attempt-1", "correlation_id": "speech-peer-fixture", "terminal": "cancelled", "released": true},
            {"operation_id": "speech-peer-completed", "attempt_id": "attempt-1", "correlation_id": "speech-peer-fixture", "terminal": "completed", "released": true}
        ],
        "ownership": {"active_operations": 0, "retained_tasks": 0, "expected_workers": 0, "joined_workers": 0},
        "output_facts": {
            "cancel_count": {"kind": "integer", "value": 1},
            "cancelled_backend": {"kind": "text", "value": "fixture-a.tts"},
            "cancelled_terminal_count": {"kind": "integer", "value": 1},
            "peer_backend": {"kind": "text", "value": "fixture-b.tts"},
            "peer_completed": {"kind": "boolean", "value": true},
            "peer_terminal_count": {"kind": "integer", "value": 1},
            "shutdown_joined": {"kind": "boolean", "value": true}
        },
        "fail_closed_facts": [
            "cancellation is scoped to the selected request identity",
            "shutdown leaves no active operation or retained task"
        ]
    }))
    .expect("construct production-derived peer projection");
    let expected: EquivalenceProjectionV0 = serde_json::from_slice(include_bytes!(
        "../../../fixtures/w1/projections/speech-peer-cancellation-v1.json"
    ))
    .expect("parse expected peer projection");
    assert_eq!(projection, expected);
}

#[test]
fn w1_canonical_manifests_validate() {
    for bytes in [
        include_bytes!("../../../fixtures/w1/manifests/speech-peer-cancellation-v0.json")
            .as_slice(),
        include_bytes!("../../../fixtures/w1/manifests/current-parakeet-model-audio-v0.json")
            .as_slice(),
        include_bytes!("../../../fixtures/w1/manifests/apple-installed-voice-v0.json").as_slice(),
    ] {
        let manifest: VerticalFixtureManifestV0 =
            serde_json::from_slice(bytes).expect("parse Speech W1 manifest");
        validate_manifest(&manifest).expect("validate Speech W1 manifest");
    }
}

#[test]
fn w1_sha256_ledger_authenticates_every_checked_in_fixture_byte() {
    let ledger = include_str!("../../../fixtures/w1/MANIFEST.sha256");
    let mut paths = BTreeSet::new();
    for line in ledger.lines() {
        let (expected_digest, relative_path) =
            line.split_once("  ").expect("ledger uses sha256sum format");
        assert!(
            paths.insert(relative_path.to_owned()),
            "duplicate ledger path: {relative_path}"
        );
        assert_eq!(
            sha256_identity("speech.w1.ledger", fixture_bytes(relative_path))
                .digest
                .hex,
            expected_digest,
            "fixture drifted: {relative_path}"
        );
    }
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let tracked = Command::new("git")
        .args(["ls-files", "fixtures/w1"])
        .current_dir(repository)
        .output()
        .expect("enumerate tracked Speech W1 fixtures");
    assert!(tracked.status.success());
    let tracked_paths = String::from_utf8(tracked.stdout)
        .expect("tracked fixture paths are UTF-8")
        .lines()
        .filter_map(|path| path.strip_prefix("fixtures/w1/"))
        .filter(|path| *path != "MANIFEST.sha256")
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(paths, tracked_paths, "fixture ledger must be complete");
}

#[test]
fn w1_apple_inventory_baseline_authenticates_invariant_projection() {
    let manifest_bytes =
        include_bytes!("../../../fixtures/w1/manifests/apple-installed-voice-v0.json");
    let projection_bytes =
        include_bytes!("../../../fixtures/w1/projections/apple-installed-voice-v1.json");
    let inventory = include_bytes!("../../../fixtures/w1/apple-installed-voice-inventory-v1.json");
    let manifest: VerticalFixtureManifestV0 =
        serde_json::from_slice(manifest_bytes).expect("parse Apple manifest");
    let case = &manifest.cases[0];
    assert_eq!(
        sha256_identity(case.source.production_tree.id.clone(), PRODUCTION_TREE),
        case.source.production_tree
    );
    assert_eq!(
        sha256_identity(case.inputs[0].identity.id.clone(), inventory),
        case.inputs[0].identity
    );
    assert_eq!(
        sha256_identity(case.expected_projection.id.clone(), projection_bytes),
        case.expected_projection
    );
    let projection: EquivalenceProjectionV0 =
        serde_json::from_slice(projection_bytes).expect("parse Apple projection");
    let observation: ObservationEnvelopeV0 = serde_json::from_value(json!({
        "schema": "delysis.vertical_observation.v0",
        "vertical_id": manifest.vertical_id,
        "case_id": case.case_id,
        "implementation_revision": BASELINE_COMMIT,
        "observed_prerequisites": case.prerequisites,
        "evidence": {
            "schema": "delysis.evidence_claim.v0",
            "tier": "operational",
            "threat_model": "exact installed voice inventory plus launched local Tauri synthesis invariants",
            "exact_source": case.source.production_tree.digest,
            "exact_runtime_or_artifact": sha256_identity("speech.apple.runtime_inventory", inventory).digest,
            "execution_kind": "local_runtime",
            "omitted_claims": manifest.omitted_claims,
            "negative_evidence": []
        },
        "projection": projection
    }))
    .expect("construct Apple observation");
    validate_baseline(
        &manifest,
        &case.case_id,
        projection_bytes,
        &[],
        &observation,
    )
    .expect("central protocol accepts exact Apple invariant baseline");
}

struct ArtifactChunks {
    paths: std::vec::IntoIter<PathBuf>,
    current: Option<File>,
}

impl ArtifactChunks {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self {
            paths: paths.into_iter(),
            current: None,
        }
    }
}

impl Iterator for ArtifactChunks {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                self.current = self.paths.next().map(|path| {
                    File::open(path).expect("open exact external prerequisite artifact")
                });
            }
            let file = self.current.as_mut()?;
            let mut chunk = vec![0_u8; 1024 * 1024];
            let read = file
                .read(&mut chunk)
                .expect("read exact external prerequisite artifact");
            if read == 0 {
                self.current = None;
                continue;
            }
            chunk.truncate(read);
            return Some(chunk);
        }
    }
}

#[test]
#[ignore = "requires the exact external Parakeet model bundle and generated WAV"]
fn w1_parakeet_external_prerequisites_authenticate_baseline() {
    let manifest_bytes =
        include_bytes!("../../../fixtures/w1/manifests/current-parakeet-model-audio-v0.json");
    let projection_bytes =
        include_bytes!("../../../fixtures/w1/projections/current-parakeet-model-audio-v1.json");
    let descriptor = include_bytes!("../../../fixtures/w1/parakeet-exact-artifacts-v1.json");
    let manifest: VerticalFixtureManifestV0 =
        serde_json::from_slice(manifest_bytes).expect("parse Parakeet manifest");
    let case = &manifest.cases[0];
    assert_eq!(
        sha256_identity(case.source.production_tree.id.clone(), PRODUCTION_TREE),
        case.source.production_tree
    );
    assert_eq!(
        sha256_identity(case.inputs[0].identity.id.clone(), descriptor),
        case.inputs[0].identity
    );
    assert_eq!(
        sha256_identity(case.expected_projection.id.clone(), projection_bytes),
        case.expected_projection
    );

    let model_dir = std::env::var_os("SPEECH_NATIVE_PARAKEET_MODEL_DIR")
        .map(PathBuf::from)
        .expect("SPEECH_NATIVE_PARAKEET_MODEL_DIR names exact model bundle");
    let wav = std::env::var_os("SPEECH_NATIVE_TEST_WAV")
        .map(PathBuf::from)
        .expect("SPEECH_NATIVE_TEST_WAV names exact generated audio");
    let model = case
        .prerequisites
        .iter()
        .find(|prerequisite| prerequisite.prerequisite_id == "model.parakeet")
        .expect("Parakeet model prerequisite");
    let audio = case
        .prerequisites
        .iter()
        .find(|prerequisite| prerequisite.prerequisite_id == "audio.input")
        .expect("Parakeet audio prerequisite");
    let verified_model = verify_prerequisite_chunks(
        &model.prerequisite_id,
        &model.identity,
        ArtifactChunks::new(vec![
            model_dir.join("encoder.onnx"),
            model_dir.join("decoder_joint.onnx"),
            model_dir.join("tokenizer.json"),
        ]),
    )
    .expect("stream-authenticate exact Parakeet model bundle");
    let verified_audio = verify_prerequisite_chunks(
        &audio.prerequisite_id,
        &audio.identity,
        ArtifactChunks::new(vec![wav]),
    )
    .expect("stream-authenticate exact Parakeet audio");
    let projection: EquivalenceProjectionV0 =
        serde_json::from_slice(projection_bytes).expect("parse Parakeet projection");
    let observation: ObservationEnvelopeV0 = serde_json::from_value(json!({
        "schema": "delysis.vertical_observation.v0",
        "vertical_id": manifest.vertical_id,
        "case_id": case.case_id,
        "implementation_revision": BASELINE_COMMIT,
        "observed_prerequisites": case.prerequisites,
        "evidence": {
            "schema": "delysis.evidence_claim.v0",
            "tier": "operational",
            "threat_model": "exact local Parakeet bytes and exact generated WAV drive real inference and peer cancellation",
            "exact_source": case.source.production_tree.digest,
            "exact_runtime_or_artifact": sha256_identity("speech.parakeet.runtime_descriptor", descriptor).digest,
            "execution_kind": "local_runtime",
            "omitted_claims": manifest.omitted_claims,
            "negative_evidence": []
        },
        "projection": projection
    }))
    .expect("construct Parakeet observation");
    validate_baseline(
        &manifest,
        &case.case_id,
        projection_bytes,
        &[verified_model, verified_audio],
        &observation,
    )
    .expect("central protocol accepts exact Parakeet baseline");
}

#[test]
fn w1_model_free_baseline_authenticates_exact_bytes() {
    let manifest_bytes =
        include_bytes!("../../../fixtures/w1/manifests/speech-peer-cancellation-v0.json");
    let projection_bytes =
        include_bytes!("../../../fixtures/w1/projections/speech-peer-cancellation-v1.json");
    let manifest: VerticalFixtureManifestV0 =
        serde_json::from_slice(manifest_bytes).expect("parse peer manifest");
    let case = &manifest.cases[0];
    assert_eq!(
        sha256_identity(case.source.production_tree.id.clone(), PRODUCTION_TREE),
        case.source.production_tree
    );
    let input = include_bytes!("../../../fixtures/w1/speech-peer-cancellation-v1.json");
    assert_eq!(
        sha256_identity(case.inputs[0].identity.id.clone(), input),
        case.inputs[0].identity
    );
    assert_eq!(
        sha256_identity(case.expected_projection.id.clone(), projection_bytes),
        case.expected_projection
    );
    let projection: EquivalenceProjectionV0 =
        serde_json::from_slice(projection_bytes).expect("parse peer projection");
    let observation: ObservationEnvelopeV0 = serde_json::from_value(json!({
        "schema": "delysis.vertical_observation.v0",
        "vertical_id": manifest.vertical_id,
        "case_id": case.case_id,
        "implementation_revision": BASELINE_COMMIT,
        "observed_prerequisites": [],
        "evidence": {
            "schema": "delysis.evidence_claim.v0",
            "tier": "reproducible",
            "threat_model": "deterministic in-process peers drive the production SpeechHost",
            "exact_source": case.source.production_tree.digest,
            "exact_runtime_or_artifact": sha256_identity("speech.peer.runtime", input).digest,
            "execution_kind": "fixture",
            "omitted_claims": manifest.omitted_claims,
            "negative_evidence": []
        },
        "projection": projection
    }))
    .expect("construct peer observation");
    validate_baseline(
        &manifest,
        &case.case_id,
        projection_bytes,
        &[],
        &observation,
    )
    .expect("central protocol accepts exact peer baseline");
}

#[test]
fn w1_fixture_descendant_preserves_every_bound_production_source_root() {
    let descriptor: ProductionTreeDescriptor =
        serde_json::from_slice(PRODUCTION_TREE).expect("parse production-tree descriptor");
    assert_eq!(descriptor.commit, BASELINE_COMMIT);
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    assert!(
        Command::new("git")
            .args(["merge-base", "--is-ancestor", BASELINE_COMMIT, "HEAD"])
            .current_dir(repository)
            .status()
            .expect("execute git ancestry proof")
            .success(),
        "fixture commit must descend from baseline"
    );
    for (source_root, expected_oid) in descriptor.source_roots {
        let output = Command::new("git")
            .args(["rev-parse", &format!("HEAD:{source_root}")])
            .current_dir(repository)
            .output()
            .expect("read current source-root identity");
        assert!(
            output.status.success(),
            "missing source root: {source_root}"
        );
        assert_eq!(
            String::from_utf8(output.stdout)
                .expect("git object id is UTF-8")
                .trim(),
            expected_oid,
            "production source changed: {source_root}"
        );
    }
}
