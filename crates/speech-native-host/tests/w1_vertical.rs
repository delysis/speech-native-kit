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
#[cfg(feature = "unstable-w1-contract-tests")]
use speech_native_types::{TaskSupervisor, TaskSupervisorSnapshot};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
#[cfg(feature = "unstable-w1-contract-tests")]
use std::fs::OpenOptions;
use std::io::Read;
#[cfg(feature = "unstable-w1-contract-tests")]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(feature = "unstable-w1-contract-tests")]
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "unstable-w1-contract-tests")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "unstable-w1-contract-tests")]
use tokio::sync::{Notify, Semaphore};
use tokio::sync::{mpsc, oneshot};

const BASELINE_COMMIT: &str = "89146595df7ba893ddd704a811377f6cb14856bc";
const PRODUCTION_TREE: &[u8] =
    include_bytes!("../../../fixtures/w1/source/speech-production-tree-8914659.json");
const QUIT_RELAUNCH_BASELINE_COMMIT: &str = "368196350803ac0a798fab142cd6cdc64b7e6fb6";
const QUIT_RELAUNCH_INPUT: &[u8] =
    include_bytes!("../../../fixtures/w1/speech-quit-relaunch-v1.json");
const QUIT_RELAUNCH_MANIFEST: &[u8] =
    include_bytes!("../../../fixtures/w1/manifests/speech-quit-relaunch-v0.json");
const QUIT_RELAUNCH_PROJECTION: &[u8] =
    include_bytes!("../../../fixtures/w1/projections/speech-quit-relaunch-v1.json");
const QUIT_RELAUNCH_SOURCE: &[u8] =
    include_bytes!("../../../fixtures/w1/source/speech-quit-relaunch-production-tree-3681963.json");
#[cfg(feature = "unstable-w1-contract-tests")]
const RECEIPT_SCHEMA: &str = "delysis.speech.fake_owner_receipt.v0";
#[cfg(feature = "unstable-w1-contract-tests")]
static RECEIPT_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
        "manifests/speech-quit-relaunch-v0.json" => QUIT_RELAUNCH_MANIFEST,
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
        "projections/speech-quit-relaunch-v1.json" => QUIT_RELAUNCH_PROJECTION,
        "source/speech-quit-relaunch-production-tree-3681963.json" => QUIT_RELAUNCH_SOURCE,
        "source/speech-production-tree-8914659.json" => PRODUCTION_TREE,
        "speech-peer-cancellation-v1.json" => {
            include_bytes!("../../../fixtures/w1/speech-peer-cancellation-v1.json")
        }
        "speech-quit-relaunch-v1.json" => QUIT_RELAUNCH_INPUT,
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

#[cfg(feature = "unstable-w1-contract-tests")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuitRelaunchInput {
    schema: String,
    quit_backend_id: String,
    quit_request_id: String,
    relaunch_backend_id: String,
    relaunch_request_id: String,
    receipt_file_name: String,
}

#[cfg(feature = "unstable-w1-contract-tests")]
#[derive(Debug, serde::Serialize)]
struct LifecycleReceipt {
    schema: &'static str,
    sequence: u64,
    epoch: String,
    kind: &'static str,
    operation_id: String,
    terminal: &'static str,
    active_tasks: usize,
    expected_worker_ids: Vec<String>,
    joined_worker_ids: Vec<String>,
    owner_alive: bool,
}

#[cfg(feature = "unstable-w1-contract-tests")]
struct ReceiptDirectory {
    path: PathBuf,
}

#[cfg(feature = "unstable-w1-contract-tests")]
impl ReceiptDirectory {
    fn new() -> std::io::Result<Self> {
        let epoch_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        loop {
            let sequence = RECEIPT_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("speech-w1-quit-relaunch-{epoch_nanos}-{sequence}"));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(feature = "unstable-w1-contract-tests")]
impl Drop for ReceiptDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(feature = "unstable-w1-contract-tests")]
struct ReceiptStore {
    path: PathBuf,
    receipts: usize,
}

#[cfg(feature = "unstable-w1-contract-tests")]
impl ReceiptStore {
    fn open(path: PathBuf) -> Result<Self, String> {
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.to_string()),
        };
        let receipts = if bytes.is_empty() {
            0
        } else {
            let text = std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
            if !text.ends_with('\n') {
                return Err("receipt store has a torn final record".to_owned());
            }
            for (sequence, line) in text.lines().enumerate() {
                let receipt: serde_json::Value =
                    serde_json::from_str(line).map_err(|error| error.to_string())?;
                if receipt.get("schema").and_then(serde_json::Value::as_str) != Some(RECEIPT_SCHEMA)
                    || receipt.get("sequence").and_then(serde_json::Value::as_u64)
                        != u64::try_from(sequence).ok()
                {
                    return Err("receipt store sequence or schema is invalid".to_owned());
                }
            }
            text.lines().count()
        };
        Ok(Self { path, receipts })
    }

    fn append(&mut self, mut receipt: LifecycleReceipt) -> Result<(), String> {
        receipt.sequence =
            u64::try_from(self.receipts).map_err(|_| "receipt sequence overflowed".to_owned())?;
        let encoded = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| error.to_string())?;
        file.write_all(&encoded)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|error| error.to_string())?;
        self.receipts += 1;
        Ok(())
    }

    fn bytes(&self) -> Result<Vec<u8>, String> {
        std::fs::read(&self.path).map_err(|error| error.to_string())
    }
}

#[cfg(feature = "unstable-w1-contract-tests")]
enum LifecycleTerminal {
    Completed,
    Cancelled,
}

#[cfg(feature = "unstable-w1-contract-tests")]
struct LifecycleCommand {
    terminal: LifecycleTerminal,
}

#[cfg(feature = "unstable-w1-contract-tests")]
struct LifecycleBackend {
    descriptor: SpeechBackendDescriptor,
    epoch: String,
    tasks: Arc<TaskSupervisor>,
    commands: Mutex<BTreeMap<SpeechRequestId, oneshot::Sender<LifecycleCommand>>>,
    store: Arc<Mutex<ReceiptStore>>,
    worker_release: Arc<Semaphore>,
    shutdown_release: Arc<Semaphore>,
    shutdown_entered: Arc<Notify>,
    owner_alive: Arc<AtomicBool>,
    shutdown_started: AtomicBool,
}

#[cfg(feature = "unstable-w1-contract-tests")]
impl LifecycleBackend {
    fn new(
        id: &str,
        epoch: &str,
        store: Arc<Mutex<ReceiptStore>>,
        shutdown_release: Arc<Semaphore>,
    ) -> Arc<Self> {
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
                        ..SynthesisCapabilities::default()
                    }),
                    availability: CapabilityAvailability::Available,
                    network: NetworkBehavior::Never,
                    languages: vec!["en-US".to_owned()],
                    limits: SpeechCapabilityLimits::default(),
                    evidence: vec![CapabilityEvidence {
                        source_id: "w1-speech-lifecycle".to_owned(),
                        source_version: Some("1".to_owned()),
                        kind: EvidenceKind::RuntimeApi,
                        outcome: EvidenceOutcome::Confirmed,
                        observed_at_unix_ms: 1,
                        detail: "deterministic owner worker; no model loaded".to_owned(),
                    }],
                }],
                models: Vec::new(),
                voices: Vec::new(),
            },
            epoch: epoch.to_owned(),
            tasks: Arc::new(TaskSupervisor::with_scope(format!(
                "w1-speech-{epoch}-owner"
            ))),
            commands: Mutex::new(BTreeMap::new()),
            store,
            worker_release: Arc::new(Semaphore::new(0)),
            shutdown_release,
            shutdown_entered: Arc::new(Notify::new()),
            owner_alive: Arc::new(AtomicBool::new(false)),
            shutdown_started: AtomicBool::new(false),
        })
    }

    fn signal(&self, request_id: &SpeechRequestId, terminal: LifecycleTerminal) -> usize {
        let sender = self
            .commands
            .lock()
            .ok()
            .and_then(|mut commands| commands.remove(request_id));
        sender.map_or(0, |sender| {
            usize::from(sender.send(LifecycleCommand { terminal }).is_ok())
        })
    }

    fn complete(&self, request_id: &SpeechRequestId) -> usize {
        self.signal(request_id, LifecycleTerminal::Completed)
    }

    fn snapshot(&self) -> TaskSupervisorSnapshot {
        self.tasks
            .snapshot()
            .expect("read backend worker ownership")
    }

    async fn wait_until_shutdown_entered(&self) {
        loop {
            let changed = self.shutdown_entered.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.shutdown_started.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[cfg(feature = "unstable-w1-contract-tests")]
#[async_trait]
impl SpeechBackend for LifecycleBackend {
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
            "the lifecycle fixture supports synthesis only",
        ))
    }

    async fn synthesize(&self, request: SynthesisRequest) -> Result<SynthesisTicket, SpeechError> {
        let request_id = request.context.request_id;
        let (event_sender, event_receiver) = mpsc::channel(2);
        let (final_sender, final_receiver) = oneshot::channel();
        let (command_sender, command_receiver) = oneshot::channel();
        self.commands
            .lock()
            .map_err(|_| {
                SpeechError::unavailable(
                    &request_id,
                    "fixture_state_unavailable",
                    "the lifecycle fixture state is unavailable",
                )
            })?
            .insert(request_id.clone(), command_sender);
        let operation_id = request_id.clone();
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
                route: route.clone(),
            })
            .map_err(|_| {
                SpeechError::unavailable(
                    &request_id,
                    "fixture_event_unavailable",
                    "the lifecycle fixture could not publish start",
                )
            })?;
        let tasks = Arc::clone(&self.tasks);
        let store = Arc::clone(&self.store);
        let epoch = self.epoch.clone();
        let worker_release = Arc::clone(&self.worker_release);
        let owner_alive = Arc::clone(&self.owner_alive);
        owner_alive.store(true, Ordering::Release);
        self.tasks
            .spawn(format!("request:{operation_id}"), async move {
                let command = command_receiver
                    .await
                    .map_err(|_| "lifecycle command sender dropped".to_owned())?;
                let (terminal, event, result) = match command.terminal {
                    LifecycleTerminal::Completed => {
                        let response = SynthesisResponse {
                            request_id: operation_id.clone(),
                            route,
                            audio: b"RIFFpost-relaunchWAVE".to_vec(),
                            format: AudioOutputFormat::Wav,
                            duration_ms: Some(1),
                            alignments: Vec::new(),
                            usage: SpeechUsage::default(),
                        };
                        (
                            "completed",
                            SynthesisEvent::Completed {
                                request_id: operation_id.clone(),
                                response: response.clone(),
                            },
                            Ok(response),
                        )
                    }
                    LifecycleTerminal::Cancelled => {
                        let error = SpeechError {
                            code: "speech_request_cancelled".to_owned(),
                            class: SpeechErrorClass::Cancelled,
                            retryable: false,
                            request_id: operation_id.clone(),
                            backend_id: Some(route.backend_id.clone()),
                            safe_detail: "quit cancelled the owned speech operation".to_owned(),
                        };
                        (
                            "cancelled",
                            SynthesisEvent::Cancelled {
                                request_id: operation_id.clone(),
                                usage: SpeechUsage::default(),
                            },
                            Err(error),
                        )
                    }
                };
                let snapshot = tasks.snapshot().map_err(|error| error.to_string())?;
                store
                    .lock()
                    .map_err(|_| "receipt store unavailable".to_owned())?
                    .append(LifecycleReceipt {
                        schema: RECEIPT_SCHEMA,
                        sequence: 0,
                        epoch,
                        kind: "terminal",
                        operation_id: operation_id.0.clone(),
                        terminal,
                        active_tasks: snapshot.active,
                        expected_worker_ids: snapshot.expected_worker_ids,
                        joined_worker_ids: snapshot.joined_worker_ids,
                        owner_alive: owner_alive.load(Ordering::Acquire),
                    })?;
                event_sender
                    .send(event)
                    .await
                    .map_err(|_| "terminal event consumer dropped".to_owned())?;
                let _consumer_gone = final_sender.send(result).is_err();
                let _release = worker_release
                    .acquire()
                    .await
                    .map_err(|_| "worker release closed".to_owned())?;
                owner_alive.store(false, Ordering::Release);
                Ok(())
            })
            .map_err(|error| {
                SpeechError::unavailable(
                    &request_id,
                    "fixture_worker_unavailable",
                    &error.to_string(),
                )
            })?;
        Ok(SynthesisTicket::new(
            request_id,
            event_receiver,
            final_receiver,
            Arc::new(NoopCancellation),
        ))
    }

    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        self.signal(request_id, LifecycleTerminal::Cancelled)
    }

    async fn shutdown(&self) -> Result<(), SpeechError> {
        let request_id = SpeechRequestId(format!("{}.shutdown", self.descriptor.id));
        self.tasks.begin_shutdown().map_err(|error| {
            SpeechError::unavailable(&request_id, "fixture_shutdown_failed", &error.to_string())
        })?;
        self.shutdown_started.store(true, Ordering::Release);
        self.shutdown_entered.notify_waiters();
        self.worker_release.add_permits(1);
        self.tasks.wait_for_idle().await.map_err(|error| {
            SpeechError::unavailable(&request_id, "fixture_shutdown_failed", &error.to_string())
        })?;
        let _release = self.shutdown_release.acquire().await.map_err(|_| {
            SpeechError::unavailable(
                &request_id,
                "fixture_shutdown_failed",
                "shutdown release closed",
            )
        })?;
        let snapshot = self.tasks.snapshot().map_err(|error| {
            SpeechError::unavailable(&request_id, "fixture_shutdown_failed", &error.to_string())
        })?;
        self.store
            .lock()
            .map_err(|_| {
                SpeechError::unavailable(
                    &request_id,
                    "fixture_shutdown_failed",
                    "receipt store unavailable",
                )
            })?
            .append(LifecycleReceipt {
                schema: RECEIPT_SCHEMA,
                sequence: 0,
                epoch: self.epoch.clone(),
                kind: "joined",
                operation_id: request_id.0,
                terminal: if self.epoch == "quit" {
                    "cancelled"
                } else {
                    "completed"
                },
                active_tasks: snapshot.active,
                expected_worker_ids: snapshot.expected_worker_ids,
                joined_worker_ids: snapshot.joined_worker_ids,
                owner_alive: self.owner_alive.load(Ordering::Acquire),
            })
            .map_err(|detail| {
                SpeechError::unavailable(
                    &SpeechRequestId(format!("{}.shutdown", self.descriptor.id)),
                    "fixture_shutdown_failed",
                    &detail,
                )
            })?;
        Ok(())
    }
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

#[cfg(feature = "unstable-w1-contract-tests")]
fn exact_worker_sets(snapshot: &TaskSupervisorSnapshot) -> bool {
    snapshot.active == 0
        && snapshot.retained_failures == 0
        && snapshot.admitted_tasks == snapshot.completed_tasks
        && snapshot.expected_worker_ids.iter().collect::<BTreeSet<_>>()
            == snapshot.joined_worker_ids.iter().collect::<BTreeSet<_>>()
}

#[cfg(feature = "unstable-w1-contract-tests")]
#[tokio::test]
async fn w1_quit_relaunch_fake_owners_projection() {
    let manifest: VerticalFixtureManifestV0 =
        serde_json::from_slice(QUIT_RELAUNCH_MANIFEST).expect("parse quit/relaunch manifest");
    validate_manifest(&manifest).expect("validate quit/relaunch manifest");
    let input: QuitRelaunchInput =
        serde_json::from_slice(QUIT_RELAUNCH_INPUT).expect("parse quit/relaunch input");
    assert_eq!(input.schema, "delysis.speech.quit_relaunch_input.v0");
    let case = manifest.cases.first().expect("one quit/relaunch case");
    assert_eq!(case.source.commit, QUIT_RELAUNCH_BASELINE_COMMIT);
    assert_eq!(
        sha256_identity(case.source.production_tree.id.clone(), QUIT_RELAUNCH_SOURCE),
        case.source.production_tree
    );
    assert_eq!(
        sha256_identity(case.inputs[0].identity.id.clone(), QUIT_RELAUNCH_INPUT),
        case.inputs[0].identity
    );
    assert_eq!(
        sha256_identity(
            case.expected_projection.id.clone(),
            QUIT_RELAUNCH_PROJECTION,
        ),
        case.expected_projection
    );

    let directory = ReceiptDirectory::new().expect("create isolated receipt directory");
    let store_path = directory.path.join(&input.receipt_file_name);
    let quit_store = Arc::new(Mutex::new(
        ReceiptStore::open(store_path.clone()).expect("open empty receipt store"),
    ));
    let quit_shutdown_release = Arc::new(Semaphore::new(0));
    let quit_backend = LifecycleBackend::new(
        &input.quit_backend_id,
        "quit",
        Arc::clone(&quit_store),
        Arc::clone(&quit_shutdown_release),
    );
    let quit_host = Arc::new(SpeechHost::default());
    quit_host
        .register_backend(quit_backend.clone())
        .expect("register quit backend");
    let quit_ticket = quit_host
        .synthesize(request(&input.quit_request_id, &input.quit_backend_id))
        .await
        .expect("admit operation before quit");
    let first_host = Arc::clone(&quit_host);
    let first_shutdown = tokio::spawn(async move { first_host.shutdown().await });
    quit_backend.wait_until_shutdown_entered().await;
    let (quit_events, quit_result) = drain(quit_ticket).await;
    assert_eq!(
        quit_events
            .iter()
            .filter(|event| event.is_terminal())
            .count(),
        1
    );
    assert!(matches!(
        quit_events.last(),
        Some(SynthesisEvent::Cancelled { .. })
    ));
    assert_eq!(
        quit_result
            .expect_err("quit operation must be cancelled")
            .class,
        SpeechErrorClass::Cancelled
    );
    first_shutdown.abort();
    assert!(
        first_shutdown
            .await
            .expect_err("first shutdown caller is aborted")
            .is_cancelled()
    );
    quit_shutdown_release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), quit_host.shutdown())
        .await
        .expect("detached coordinator must finish")
        .expect("retained shutdown result succeeds");
    let quit_summary = quit_host
        .w1_contract_adapter()
        .closed_summary()
        .expect("project closed quit host");
    quit_summary.validate().expect("validate closed quit host");
    assert_eq!(quit_summary.active_operations, 0);
    assert_eq!(quit_summary.retained_tasks, 0);
    assert_eq!(quit_summary.expected_workers, 1);
    assert_eq!(quit_summary.joined_workers, 1);
    let quit_workers = quit_backend.snapshot();
    assert!(exact_worker_sets(&quit_workers));
    assert!(!quit_backend.owner_alive.load(Ordering::Acquire));
    let quit_store_bytes = quit_store
        .lock()
        .expect("lock quit receipt store")
        .bytes()
        .expect("read post-quit receipt store");
    let quit_store_identity = sha256_identity("speech-receipts-after-quit", &quit_store_bytes);
    assert_eq!(
        quit_store.lock().expect("lock quit receipt count").receipts,
        2
    );
    drop(quit_host);
    drop(quit_backend);
    drop(quit_store);

    let relaunched_store = Arc::new(Mutex::new(
        ReceiptStore::open(store_path).expect("reopen the same durable store"),
    ));
    assert_eq!(
        relaunched_store
            .lock()
            .expect("lock relaunched receipt store")
            .bytes()
            .expect("read reopened receipt store"),
        quit_store_bytes
    );
    let relaunch_shutdown_release = Arc::new(Semaphore::new(0));
    let relaunch_backend = LifecycleBackend::new(
        &input.relaunch_backend_id,
        "relaunch",
        Arc::clone(&relaunched_store),
        Arc::clone(&relaunch_shutdown_release),
    );
    let relaunch_host = SpeechHost::default();
    relaunch_host
        .register_backend(relaunch_backend.clone())
        .expect("register fresh relaunch backend");
    let relaunch_id = SpeechRequestId(input.relaunch_request_id.clone());
    let relaunch_ticket = relaunch_host
        .synthesize(request(
            &input.relaunch_request_id,
            &input.relaunch_backend_id,
        ))
        .await
        .expect("admit post-relaunch operation");
    assert_eq!(relaunch_backend.complete(&relaunch_id), 1);
    let (relaunch_events, relaunch_result) = drain(relaunch_ticket).await;
    assert_eq!(
        relaunch_events
            .iter()
            .filter(|event| event.is_terminal())
            .count(),
        1
    );
    assert!(matches!(
        relaunch_events.last(),
        Some(SynthesisEvent::Completed { .. })
    ));
    assert!(
        !relaunch_result
            .expect("post-relaunch work completes")
            .audio
            .is_empty()
    );
    relaunch_shutdown_release.add_permits(1);
    relaunch_host
        .shutdown()
        .await
        .expect("fresh runtime joins cleanly");
    let relaunch_summary = relaunch_host
        .w1_contract_adapter()
        .closed_summary()
        .expect("project closed relaunch host");
    relaunch_summary
        .validate()
        .expect("validate closed relaunch host");
    assert_eq!(relaunch_summary.active_operations, 0);
    assert_eq!(relaunch_summary.retained_tasks, 0);
    assert_eq!(relaunch_summary.expected_workers, 1);
    assert_eq!(relaunch_summary.joined_workers, 1);
    let relaunch_workers = relaunch_backend.snapshot();
    assert!(exact_worker_sets(&relaunch_workers));
    assert!(!relaunch_backend.owner_alive.load(Ordering::Acquire));
    assert_ne!(
        quit_workers.expected_worker_ids, relaunch_workers.expected_worker_ids,
        "fresh backend owner must have a distinct worker identity"
    );
    let final_store_bytes = relaunched_store
        .lock()
        .expect("lock final receipt store")
        .bytes()
        .expect("read final receipt store");
    let final_store_identity =
        sha256_identity("speech-receipts-after-relaunch", &final_store_bytes);
    assert_eq!(
        relaunched_store
            .lock()
            .expect("lock final receipt count")
            .receipts,
        4
    );

    let expected_workers = quit_summary.expected_workers
        + quit_workers.expected_worker_ids.len()
        + relaunch_summary.expected_workers
        + relaunch_workers.expected_worker_ids.len();
    let joined_workers = quit_summary.joined_workers
        + quit_workers.joined_worker_ids.len()
        + relaunch_summary.joined_workers
        + relaunch_workers.joined_worker_ids.len();
    let projection: EquivalenceProjectionV0 = serde_json::from_value(json!({
        "ordered_events": [
            {"sequence": 0, "operation_id": input.quit_request_id, "attempt_id": "attempt-1", "correlation_id": "speech-quit-relaunch", "kind": "cancelled", "payload": null},
            {"sequence": 1, "operation_id": input.quit_request_id, "attempt_id": "attempt-1", "correlation_id": "speech-quit-relaunch", "kind": "owner_joined", "payload": null},
            {"sequence": 2, "operation_id": input.relaunch_request_id, "attempt_id": "attempt-1", "correlation_id": "speech-quit-relaunch", "kind": "completed", "payload": null},
            {"sequence": 3, "operation_id": input.relaunch_request_id, "attempt_id": "attempt-1", "correlation_id": "speech-quit-relaunch", "kind": "owner_joined", "payload": null}
        ],
        "durable_state": [
            {
                "state_id": "speech-receipts-after-quit",
                "schema_id": "delysis.speech.fake_owner_receipt_store.v0",
                "before": null,
                "after": quit_store_identity,
                "disposition": "created"
            },
            {
                "state_id": "speech-receipts-after-relaunch",
                "schema_id": "delysis.speech.fake_owner_receipt_store.v0",
                "before": quit_store_identity,
                "after": final_store_identity.clone(),
                "disposition": "updated"
            }
        ],
        "lifecycle": [
            {"operation_id": input.quit_request_id, "attempt_id": "attempt-1", "correlation_id": "speech-quit-relaunch", "terminal": "cancelled", "released": true},
            {"operation_id": input.relaunch_request_id, "attempt_id": "attempt-1", "correlation_id": "speech-quit-relaunch", "terminal": "completed", "released": true}
        ],
        "ownership": {
            "active_operations": 0,
            "retained_tasks": 0,
            "expected_workers": expected_workers,
            "joined_workers": joined_workers
        },
        "output_facts": {
            "aborted_shutdown_caller_did_not_own_progress": {"kind": "boolean", "value": true},
            "durable_receipts": {"kind": "integer", "value": 4},
            "fresh_host_and_backend": {"kind": "boolean", "value": true},
            "model_inference_invoked": {"kind": "boolean", "value": false},
            "post_relaunch_completed": {"kind": "boolean", "value": true},
            "same_durable_store_reopened": {"kind": "boolean", "value": true},
            "worker_id_sets_match": {"kind": "boolean", "value": expected_workers == joined_workers}
        },
        "fail_closed_facts": [
            "quit cancellation produces one authoritative terminal before owner join",
            "shutdown acceptance requires every retained worker ID joined and zero orphan owner"
        ]
    }))
    .expect("construct quit/relaunch projection");
    let expected_projection: EquivalenceProjectionV0 =
        serde_json::from_slice(QUIT_RELAUNCH_PROJECTION).expect("parse expected projection");
    assert_eq!(projection, expected_projection);
    let observation: ObservationEnvelopeV0 = serde_json::from_value(json!({
        "schema": "delysis.vertical_observation.v0",
        "vertical_id": manifest.vertical_id,
        "case_id": case.case_id,
        "implementation_revision": QUIT_RELAUNCH_BASELINE_COMMIT,
        "observed_prerequisites": [],
        "evidence": {
            "schema": "delysis.evidence_claim.v0",
            "tier": "reproducible",
            "threat_model": "deterministic SpeechHost and TaskSupervisor ownership with filesystem receipt replay",
            "exact_source": case.source.production_tree.digest,
            "exact_runtime_or_artifact": final_store_identity.digest,
            "execution_kind": "fixture",
            "omitted_claims": manifest.omitted_claims,
            "negative_evidence": []
        },
        "projection": projection
    }))
    .expect("construct quit/relaunch observation");
    validate_baseline(
        &manifest,
        &case.case_id,
        QUIT_RELAUNCH_PROJECTION,
        &[],
        &observation,
    )
    .expect("central protocol accepts quit/relaunch baseline");
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
        QUIT_RELAUNCH_MANIFEST,
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

#[test]
fn w1_quit_relaunch_binds_exact_production_source_roots() {
    let descriptor: ProductionTreeDescriptor = serde_json::from_slice(QUIT_RELAUNCH_SOURCE)
        .expect("parse quit/relaunch source descriptor");
    assert_eq!(descriptor.commit, QUIT_RELAUNCH_BASELINE_COMMIT);
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    assert!(
        Command::new("git")
            .args([
                "merge-base",
                "--is-ancestor",
                QUIT_RELAUNCH_BASELINE_COMMIT,
                "HEAD",
            ])
            .current_dir(repository)
            .status()
            .expect("execute quit/relaunch ancestry proof")
            .success(),
        "quit/relaunch fixture must descend from the observation commit"
    );
    for (source_root, expected_oid) in descriptor.source_roots {
        let output = Command::new("git")
            .args([
                "rev-parse",
                &format!("{QUIT_RELAUNCH_BASELINE_COMMIT}:{source_root}"),
            ])
            .current_dir(repository)
            .output()
            .expect("read observed source-root identity");
        assert!(
            output.status.success(),
            "missing source root: {source_root}"
        );
        assert_eq!(
            String::from_utf8(output.stdout)
                .expect("git object id is UTF-8")
                .trim(),
            expected_oid,
            "observed production source changed: {source_root}"
        );
        let current = Command::new("git")
            .args(["rev-parse", &format!("HEAD:{source_root}")])
            .current_dir(repository)
            .output()
            .expect("read current source-root identity");
        assert!(current.status.success(), "missing current source root");
        assert_eq!(
            String::from_utf8(current.stdout)
                .expect("git object id is UTF-8")
                .trim(),
            expected_oid,
            "row-8 candidate must remain a test-only descendant"
        );
    }
}
