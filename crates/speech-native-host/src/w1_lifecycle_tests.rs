//! Canonical lifecycle conformance over Speech's production owners.

use super::*;
use async_trait::async_trait;
use platform_contract_testkit::compositional_lifecycle::{
    AdmissionQuiesceShutdownBridgeAdapter, AttemptHierarchyAdapter, ConsumerCancellationAdapter,
    PanicShutdownBridgeAdapter, ProgressShutdownBridgeAdapter, RegistryIdentityAdapter,
    ShutdownWitness, StableShutdownAdapter, TaskReapingAdapter, TerminalAuthorityAdapter,
    TransitionChainAdapter, WaiterControlAdapter, run_admission_quiesce_shutdown_bridge_suite,
    run_attempt_hierarchy_suite, run_consumer_cancellation_suite, run_panic_shutdown_bridge_suite,
    run_progress_shutdown_bridge_suite, run_registry_identity_suite, run_stable_shutdown_suite,
    run_task_reaping_suite, run_terminal_authority_suite, run_transition_chain_suite,
    run_waiter_control_suite,
};
use platform_contract_testkit::{
    AttemptIdentity, ClosedFacts, LifecycleCoverageManifest, LifecycleImplementation,
    LifecyclePhase, OperationPhase, OperationSnapshot, ShutdownOutcome, TerminalClass,
    TerminalRecord, WaitObservation,
};
use speech_native_types::{
    AlignmentGranularity, AudioOutputFormat, AudioOutputKind, CapabilityAvailability,
    CapabilityEvidence, EvidenceKind, EvidenceOutcome, NetworkBehavior, SpeechBackendKind,
    SpeechBackendReadiness, SpeechCancellation, SpeechCapability, SpeechCapabilityLimits,
    SpeechDeadlinePolicy, SpeechOperationCapability, SpeechRequestContext, SpeechResolvedRoute,
    SpeechRoutingPolicy, SpeechUsage, SynthesisCapabilities, SynthesisEvent, SynthesisInput,
    SynthesisResponse, TaskSupervisorSnapshot, UsageProvenance, VoiceDescriptor, VoiceQuality,
    VoiceSelector,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc as std_mpsc};
use std::thread;
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, oneshot};

enum SpeechHostLifecycle {}

impl LifecycleImplementation for SpeechHostLifecycle {
    const PRODUCT: &'static str = "speech-native-kit";
    const IMPLEMENTATION: &'static str = "speech-host-v1";
}

#[derive(Clone)]
struct RegistryAdapter {
    registry: operation_lifecycle::OperationRegistry,
}

struct RegistryOperation {
    _consumer: operation_lifecycle::ConsumerGuard,
    lease: operation_lifecycle::OperationLease,
}

impl RegistryAdapter {
    fn with_sequence(next_sequence: u64) -> Self {
        Self {
            registry: operation_lifecycle::OperationRegistry::new(next_sequence, 3),
        }
    }

    fn start_operation(
        &self,
        operation_id: &str,
    ) -> Result<RegistryOperation, operation_lifecycle::RegistryError> {
        let (consumer, lease) = self.registry.reserve(operation_id)?;
        lease.queue()?;
        lease.start()?;
        Ok(RegistryOperation {
            _consumer: consumer,
            lease,
        })
    }
}

fn identity(value: operation_lifecycle::OperationIdentity) -> AttemptIdentity {
    AttemptIdentity {
        operation_id: value.operation_id,
        attempt_id: value.attempt_id,
        sequence: value.sequence,
    }
}

const fn terminal(value: TerminalClass) -> operation_lifecycle::TerminalClass {
    match value {
        TerminalClass::Completed => operation_lifecycle::TerminalClass::Completed,
        TerminalClass::Cancelled => operation_lifecycle::TerminalClass::Cancelled,
        TerminalClass::Failed => operation_lifecycle::TerminalClass::Failed,
    }
}

const fn contract_terminal(value: operation_lifecycle::TerminalClass) -> TerminalClass {
    match value {
        operation_lifecycle::TerminalClass::Completed => TerminalClass::Completed,
        operation_lifecycle::TerminalClass::Cancelled => TerminalClass::Cancelled,
        operation_lifecycle::TerminalClass::Failed => TerminalClass::Failed,
    }
}

fn contract_snapshot(value: operation_lifecycle::OperationSnapshot) -> OperationSnapshot {
    let terminal = value.terminal.map(|class| TerminalRecord {
        class: contract_terminal(class),
        sequence: value.identity.sequence,
    });
    OperationSnapshot {
        identity: identity(value.identity),
        phase: match value.phase {
            operation_lifecycle::OperationPhase::Reserved => OperationPhase::Reserved,
            operation_lifecycle::OperationPhase::Queued => OperationPhase::Queued,
            operation_lifecycle::OperationPhase::Running => OperationPhase::Running,
            operation_lifecycle::OperationPhase::Terminal => OperationPhase::Terminal,
            operation_lifecycle::OperationPhase::Released => OperationPhase::Released,
        },
        cancellation_requested: value.cancellation_requested,
        authoritative_terminal: terminal,
        final_projection: terminal,
        progress_projection: value.progress,
    }
}

impl TransitionChainAdapter for RegistryAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = operation_lifecycle::RegistryError;
    type Operation = RegistryOperation;

    fn deterministic() -> Self {
        Self::with_sequence(1)
    }
    fn reserve(&self, id: &str) -> Result<Self::Operation, Self::Error> {
        let (consumer, lease) = self.registry.reserve(id)?;
        Ok(RegistryOperation {
            _consumer: consumer,
            lease,
        })
    }
    fn phase(&self, op: &Self::Operation) -> Option<OperationPhase> {
        op.lease
            .snapshot()
            .expect("registry available")
            .map(contract_snapshot)
            .map(|s| s.phase)
    }
    fn queue(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.lease.queue()
    }
    fn start(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.lease.start()
    }
    fn terminal(&self, op: &Self::Operation, class: TerminalClass) -> Result<(), Self::Error> {
        op.lease.terminal(terminal(class))
    }
    fn release(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.lease.release()
    }
}

impl RegistryIdentityAdapter for RegistryAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = operation_lifecycle::RegistryError;
    type Guard = operation_lifecycle::ConsumerGuard;
    type Lease = operation_lifecycle::OperationLease;

    fn deterministic(next_sequence: u64) -> Self {
        Self::with_sequence(next_sequence)
    }
    fn reserve(&self, id: &str) -> Result<(Self::Guard, Self::Lease), Self::Error> {
        self.registry.reserve(id)
    }
    fn lease_identity(&self, lease: &Self::Lease) -> AttemptIdentity {
        identity(lease.identity())
    }
    fn complete_and_release(&self, lease: &Self::Lease) -> Result<(), Self::Error> {
        lease.queue()?;
        lease.start()?;
        lease.terminal(operation_lifecycle::TerminalClass::Completed)?;
        lease.release()
    }
    fn active_count(&self) -> usize {
        self.registry.active_count().expect("registry available")
    }
    fn current_identity(&self, id: &str) -> Option<AttemptIdentity> {
        self.registry
            .current(id)
            .expect("registry available")
            .map(|s| identity(s.identity))
    }
}

impl AttemptHierarchyAdapter for RegistryAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = operation_lifecycle::RegistryError;
    type Operation = RegistryOperation;
    type Attempt = operation_lifecycle::AttemptLease;

    fn deterministic() -> Self {
        Self::with_sequence(1)
    }
    fn create_operation(&self, id: &str) -> Result<Self::Operation, Self::Error> {
        self.start_operation(id)
    }
    fn start_attempt(&self, op: &Self::Operation) -> Result<Self::Attempt, Self::Error> {
        op.lease.start_attempt()
    }
    fn attempt_identity(&self, attempt: &Self::Attempt) -> AttemptIdentity {
        identity(attempt.identity())
    }
    fn operation_active(&self, op: &Self::Operation) -> bool {
        op.lease.is_active().expect("registry available")
    }
    fn active_attempts(&self, op: &Self::Operation) -> Vec<AttemptIdentity> {
        op.lease
            .active_attempts()
            .expect("registry available")
            .into_iter()
            .map(identity)
            .collect()
    }
    fn request_operation_cancel(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.lease.request_cancel()
    }
    fn cancellation_requested(&self, attempt: &Self::Attempt) -> bool {
        attempt
            .cancellation_requested()
            .expect("registry available")
    }
    fn finish_attempt(&self, attempt: Self::Attempt) -> Result<(), Self::Error> {
        attempt.finish()
    }
    fn finish_operation(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.lease
            .terminal(operation_lifecycle::TerminalClass::Cancelled)?;
        op.lease.release()
    }
}

impl ConsumerCancellationAdapter for RegistryAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = operation_lifecycle::RegistryError;
    type Ticket = operation_lifecycle::ConsumerGuard;
    type Lease = operation_lifecycle::OperationLease;
    fn deterministic() -> Self {
        Self::with_sequence(1)
    }
    fn start(&self, id: &str) -> Result<(Self::Ticket, Self::Lease), Self::Error> {
        let (consumer, lease) = self.registry.reserve(id)?;
        lease.queue()?;
        lease.start()?;
        Ok((consumer, lease))
    }
    fn ticket_identity(&self, ticket: &Self::Ticket) -> AttemptIdentity {
        identity(ticket.identity())
    }
    fn lease_identity(&self, lease: &Self::Lease) -> AttemptIdentity {
        identity(lease.identity())
    }
    fn active_count(&self) -> usize {
        self.registry.active_count().expect("registry available")
    }
    fn current_snapshot(&self, id: &str) -> Option<OperationSnapshot> {
        self.registry
            .current(id)
            .expect("registry available")
            .map(contract_snapshot)
    }
    fn lease_snapshot(&self, lease: &Self::Lease) -> Option<OperationSnapshot> {
        lease
            .snapshot()
            .expect("registry available")
            .map(contract_snapshot)
    }
    fn cancellation_requested(&self, lease: &Self::Lease) -> bool {
        lease
            .snapshot()
            .expect("registry available")
            .is_some_and(|s| s.cancellation_requested)
    }
    fn explicit_consumer_drop(&self, ticket: Self::Ticket) -> Result<(), Self::Error> {
        ticket.cancel()?;
        drop(ticket);
        Ok(())
    }
    fn finish_cancelled(&self, lease: &Self::Lease) -> Result<(), Self::Error> {
        lease.terminal(operation_lifecycle::TerminalClass::Cancelled)?;
        lease.release()
    }
}

impl TerminalAuthorityAdapter for RegistryAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = operation_lifecycle::RegistryError;
    type Guard = operation_lifecycle::ConsumerGuard;
    type Lease = operation_lifecycle::OperationLease;
    fn deterministic() -> Self {
        Self::with_sequence(1)
    }
    fn start(&self, id: &str) -> Result<(Self::Guard, Self::Lease), Self::Error> {
        ConsumerCancellationAdapter::start(self, id)
    }
    fn terminal(&self, lease: &Self::Lease, class: TerminalClass) -> Result<(), Self::Error> {
        lease.terminal(terminal(class))
    }
    fn snapshot(&self, lease: &Self::Lease) -> Option<OperationSnapshot> {
        lease
            .snapshot()
            .expect("registry available")
            .map(contract_snapshot)
    }
    fn release(&self, lease: &Self::Lease) -> Result<(), Self::Error> {
        lease.release()
    }
}

impl WaiterControlAdapter for RegistryAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = operation_lifecycle::RegistryError;
    type Ticket = operation_lifecycle::ConsumerGuard;
    type Lease = operation_lifecycle::OperationLease;
    fn deterministic() -> Self {
        Self::with_sequence(1)
    }
    fn start(&self, id: &str) -> Result<(Self::Ticket, Self::Lease), Self::Error> {
        ConsumerCancellationAdapter::start(self, id)
    }
    fn snapshot(&self, lease: &Self::Lease) -> Option<OperationSnapshot> {
        lease
            .snapshot()
            .expect("registry available")
            .map(contract_snapshot)
    }
    fn waiter_timeout(&self, _ticket: &Self::Ticket) -> Result<WaitObservation, Self::Error> {
        Ok(WaitObservation::TimedOut)
    }
    fn request_cancel(&self, ticket: &Self::Ticket) -> Result<(), Self::Error> {
        ticket.cancel()
    }
    fn finish_cancelled(&self, lease: &Self::Lease) -> Result<(), Self::Error> {
        lease.terminal(operation_lifecycle::TerminalClass::Cancelled)?;
        lease.release()
    }
}

#[derive(Clone, Copy)]
enum WorkerCommand {
    Completed,
    Cancelled,
    Panic,
}

struct ControlledBackend {
    descriptor: SpeechBackendDescriptor,
    command: Mutex<Option<oneshot::Receiver<WorkerCommand>>>,
    allow_exit: Arc<Semaphore>,
    tasks: Arc<TaskSupervisor>,
    events: Arc<Mutex<Option<mpsc::Sender<SynthesisEvent>>>>,
    event_capacity: usize,
    cancelled: AtomicBool,
}

#[derive(Default)]
struct NoopCancellation;

impl SpeechCancellation for NoopCancellation {
    fn cancel(&self, _request_id: &SpeechRequestId) -> usize {
        0
    }
}

#[async_trait]
impl SpeechBackend for ControlledBackend {
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
            "contract_transcription_unsupported",
            "controlled contract backend supports synthesis only",
        ))
    }

    async fn synthesize(&self, request: SynthesisRequest) -> Result<SynthesisTicket, SpeechError> {
        let request_id = request.context.request_id;
        let command = self
            .command
            .lock()
            .map_err(|_| backend_error(&request_id, "command state unavailable"))?
            .take()
            .ok_or_else(|| backend_error(&request_id, "backend already executed"))?;
        let (event_tx, event_rx) = mpsc::channel(self.event_capacity.max(1));
        *self
            .events
            .lock()
            .map_err(|_| backend_error(&request_id, "event state unavailable"))? = Some(event_tx);
        let (final_tx, final_rx) = oneshot::channel();
        let allow_exit = Arc::clone(&self.allow_exit);
        let worker_request_id = request_id.clone();
        let descriptor = self.descriptor.clone();
        self.tasks
            .spawn("controlled-synthesis", async move {
                let result = match command.await {
                    Ok(WorkerCommand::Completed) => {
                        Ok(controlled_response(&worker_request_id, &descriptor))
                    }
                    Ok(WorkerCommand::Cancelled) => Err(SpeechError {
                        code: "controlled_cancelled".to_owned(),
                        class: SpeechErrorClass::Cancelled,
                        retryable: false,
                        request_id: worker_request_id.clone(),
                        backend_id: Some(descriptor.id.clone()),
                        safe_detail: "controlled operation cancelled".to_owned(),
                    }),
                    Ok(WorkerCommand::Panic) => {
                        let panic =
                            tokio::spawn(async { panic!("controlled speech executor panic") })
                                .await;
                        Err(SpeechError {
                            code: "controlled_executor_panicked".to_owned(),
                            class: SpeechErrorClass::Internal,
                            retryable: false,
                            request_id: worker_request_id.clone(),
                            backend_id: Some(descriptor.id.clone()),
                            safe_detail: match panic {
                                Ok(()) => "controlled executor unexpectedly returned".to_owned(),
                                Err(error) => {
                                    format!("controlled executor panic was caught: {error}")
                                }
                            },
                        })
                    }
                    Err(_) => Err(backend_error(
                        &worker_request_id,
                        "completion command disconnected",
                    )),
                };
                let _consumer_gone = final_tx.send(result).is_err();
                let _permit = allow_exit
                    .acquire()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .map_err(|error| backend_error(&request_id, &error.to_string()))?;
        Ok(SynthesisTicket::new(
            request_id,
            event_rx,
            final_rx,
            Arc::new(NoopCancellation),
        ))
    }

    fn cancel(&self, _request_id: &SpeechRequestId) -> usize {
        usize::from(!self.cancelled.swap(true, Ordering::AcqRel))
    }

    async fn shutdown(&self) -> Result<(), SpeechError> {
        self.tasks.begin_shutdown().map_err(|error| {
            backend_error(
                &SpeechRequestId("controlled-shutdown".to_owned()),
                &error.to_string(),
            )
        })?;
        self.tasks.wait_for_idle().await.map_err(|error| {
            backend_error(
                &SpeechRequestId("controlled-shutdown".to_owned()),
                &error.to_string(),
            )
        })?;
        if self
            .tasks
            .failure_summary()
            .map_err(|error| {
                backend_error(
                    &SpeechRequestId("controlled-shutdown".to_owned()),
                    &error.to_string(),
                )
            })?
            .is_some()
        {
            return Err(backend_error(
                &SpeechRequestId("controlled-shutdown".to_owned()),
                "controlled worker failed",
            ));
        }
        Ok(())
    }
}

fn backend_error(request_id: &SpeechRequestId, detail: &str) -> SpeechError {
    SpeechError::unavailable(request_id, "contract_backend_failed", detail)
}

fn controlled_response(
    request_id: &SpeechRequestId,
    descriptor: &SpeechBackendDescriptor,
) -> SynthesisResponse {
    SynthesisResponse {
        request_id: request_id.clone(),
        route: SpeechResolvedRoute {
            backend_id: descriptor.id.clone(),
            model_id: Some("contract-model".to_owned()),
            voice_id: Some("contract-voice".to_owned()),
            backend_kind: descriptor.kind,
            network: NetworkBehavior::Never,
        },
        audio: b"RIFFcontractWAVE".to_vec(),
        format: AudioOutputFormat::Wav,
        duration_ms: Some(1),
        alignments: Vec::new(),
        usage: SpeechUsage {
            provenance: UsageProvenance::Exact,
            real_local_inference: false,
            ..SpeechUsage::default()
        },
    }
}

fn controlled_descriptor(id: &str) -> SpeechBackendDescriptor {
    SpeechBackendDescriptor {
        id: id.to_owned(),
        display_name: id.to_owned(),
        kind: SpeechBackendKind::EmbeddedModel,
        readiness: SpeechBackendReadiness::Ready,
        capabilities: vec![SpeechCapability {
            id: format!("{id}.synthesis"),
            backend_id: id.to_owned(),
            model_id: Some("contract-model".to_owned()),
            operation: SpeechOperationCapability::Synthesis(SynthesisCapabilities {
                streaming_audio: true,
                returned_audio: vec![AudioOutputKind::Wav],
                voice_selection: true,
                ..SynthesisCapabilities::default()
            }),
            availability: CapabilityAvailability::Available,
            network: NetworkBehavior::Never,
            languages: vec!["en-US".to_owned()],
            limits: SpeechCapabilityLimits::default(),
            evidence: vec![CapabilityEvidence {
                source_id: "w1-contract-fixture".to_owned(),
                source_version: Some("1".to_owned()),
                kind: EvidenceKind::RuntimeApi,
                outcome: EvidenceOutcome::Confirmed,
                observed_at_unix_ms: 1,
                detail: "deterministic local backend".to_owned(),
            }],
        }],
        models: Vec::new(),
        voices: vec![VoiceDescriptor {
            id: "contract-voice".to_owned(),
            name: "Contract".to_owned(),
            language: "en-US".to_owned(),
            gender: None,
            quality: Some(VoiceQuality::Normal),
            expected_latency: None,
            network: NetworkBehavior::Never,
            installed: true,
        }],
    }
}

fn controlled_request(operation_id: &str, backend_id: &str) -> SynthesisRequest {
    SynthesisRequest {
        context: SpeechRequestContext {
            request_id: SpeechRequestId(operation_id.to_owned()),
            client_id: "w1-contract".to_owned(),
            route: SpeechRouteSelector::ExactBackend {
                backend_id: backend_id.to_owned(),
                model_id: Some("contract-model".to_owned()),
                voice_id: Some("contract-voice".to_owned()),
            },
            routing: SpeechRoutingPolicy::default(),
            deadline: SpeechDeadlinePolicy::default(),
        },
        input: SynthesisInput::Text {
            text: "hello".to_owned(),
        },
        voice: VoiceSelector::Auto,
        language: Some("en-US".to_owned()),
        rate: 1.0,
        pitch: 1.0,
        volume: 1.0,
        output: AudioOutputFormat::Wav,
        alignment: AlignmentGranularity::None,
        stream: true,
    }
}

#[derive(Clone)]
struct SpeechBridgeAdapter {
    host: Arc<SpeechHost>,
    runtime: Arc<tokio::runtime::Runtime>,
    next_backend: Arc<AtomicU64>,
    backends: Arc<Mutex<Vec<Arc<ControlledBackend>>>>,
    event_capacity: usize,
}

struct ControlledOperation {
    request_id: SpeechRequestId,
    ticket: Mutex<Option<SynthesisTicket>>,
    command: Mutex<Option<oneshot::Sender<WorkerCommand>>>,
    allow_exit: Arc<Semaphore>,
    events: Arc<Mutex<Option<mpsc::Sender<SynthesisEvent>>>>,
    lifecycle: operation_lifecycle::OperationLease,
    backend: Arc<ControlledBackend>,
}

impl SpeechBridgeAdapter {
    fn deterministic_with_capacity(event_capacity: usize) -> Self {
        Self {
            host: Arc::new(SpeechHost::default()),
            runtime: Arc::new(tokio::runtime::Runtime::new().expect("contract runtime")),
            next_backend: Arc::new(AtomicU64::new(1)),
            backends: Arc::new(Mutex::new(Vec::new())),
            event_capacity,
        }
    }

    fn start_operation(&self, operation_id: &str) -> Result<ControlledOperation, String> {
        let sequence = self.next_backend.fetch_add(1, Ordering::AcqRel);
        let backend_id = format!("controlled-speech-{sequence}");
        let (command_tx, command_rx) = oneshot::channel();
        let allow_exit = Arc::new(Semaphore::new(0));
        let events = Arc::new(Mutex::new(None));
        let backend = Arc::new(ControlledBackend {
            descriptor: controlled_descriptor(&backend_id),
            command: Mutex::new(Some(command_rx)),
            allow_exit: Arc::clone(&allow_exit),
            tasks: Arc::new(TaskSupervisor::with_scope(format!(
                "speech-backend-{sequence}"
            ))),
            events: Arc::clone(&events),
            event_capacity: self.event_capacity,
            cancelled: AtomicBool::new(false),
        });
        self.host
            .register_backend(backend.clone())
            .map_err(|error| error.to_string())?;
        self.backends
            .lock()
            .map_err(|_| "backend registry unavailable".to_owned())?
            .push(backend.clone());
        let request_id = SpeechRequestId(operation_id.to_owned());
        let ticket = self
            .runtime
            .block_on(
                self.host
                    .synthesize(controlled_request(operation_id, &backend_id)),
            )
            .map_err(|error| error.to_string())?;
        let lifecycle = self
            .host
            .lifecycle
            .operations
            .current_lease(operation_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "production operation missing".to_owned())?;
        Ok(ControlledOperation {
            request_id,
            ticket: Mutex::new(Some(ticket)),
            command: Mutex::new(Some(command_tx)),
            allow_exit,
            events,
            lifecycle,
            backend,
        })
    }

    fn send(&self, operation: &ControlledOperation, command: WorkerCommand) -> Result<(), String> {
        operation
            .command
            .lock()
            .map_err(|_| "command state unavailable".to_owned())?
            .take()
            .ok_or_else(|| "command already sent".to_owned())?
            .send(command)
            .map_err(|_| "worker command disconnected".to_owned())
    }

    fn wait_released(
        &self,
        operation: &ControlledOperation,
        timeout: Duration,
    ) -> Result<OperationSnapshot, String> {
        let ticket = operation
            .ticket
            .lock()
            .map_err(|_| "ticket state unavailable".to_owned())?
            .take()
            .ok_or_else(|| "result already observed".to_owned())?;
        let expected = self
            .runtime
            .block_on(async { tokio::time::timeout(timeout, ticket.final_response()).await })
            .map_err(|_| "result timeout".to_owned())?;
        let class = match expected {
            Ok(_) => TerminalClass::Completed,
            Err(error) if error.class == SpeechErrorClass::Cancelled => TerminalClass::Cancelled,
            Err(_) => TerminalClass::Failed,
        };
        let snapshot = operation
            .lifecycle
            .snapshot()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "released snapshot missing".to_owned())?;
        let snapshot = contract_snapshot(snapshot);
        if snapshot
            .authoritative_terminal
            .is_none_or(|value| value.class != class)
        {
            return Err("terminal projection mismatch".to_owned());
        }
        self.runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                loop {
                    let suffix = format!(":host-final-relay:{}", operation.request_id);
                    let state = self
                        .host
                        .lifecycle
                        .tasks
                        .snapshot()
                        .map_err(|error| error.to_string())?;
                    if state
                        .joined_worker_ids
                        .iter()
                        .any(|id| id.ends_with(&suffix))
                    {
                        return Ok::<(), String>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| "host final relay did not reap".to_owned())?
        })?;
        Ok(snapshot)
    }

    fn retained_tasks(&self) -> Result<usize, String> {
        let host = self
            .host
            .lifecycle
            .tasks
            .snapshot()
            .map_err(|error| error.to_string())?
            .active;
        let backend = self
            .backends
            .lock()
            .map_err(|_| "backend registry unavailable".to_owned())?
            .iter()
            .try_fold(0usize, |total, backend| {
                let active = backend
                    .tasks
                    .snapshot()
                    .map_err(|error| error.to_string())?
                    .active;
                total
                    .checked_add(active)
                    .ok_or_else(|| "retained task count overflow".to_owned())
            })?;
        host.checked_add(backend)
            .ok_or_else(|| "retained task count overflow".to_owned())
    }

    fn backend_retained_tasks(&self) -> Result<usize, String> {
        self.backends
            .lock()
            .map_err(|_| "backend registry unavailable".to_owned())?
            .iter()
            .try_fold(0usize, |total, backend| {
                let active = backend
                    .tasks
                    .snapshot()
                    .map_err(|error| error.to_string())?
                    .active;
                total
                    .checked_add(active)
                    .ok_or_else(|| "retained task count overflow".to_owned())
            })
    }

    fn shutdown_witness(&self) -> SpeechShutdownWitness {
        speech_shutdown_witness(
            Arc::clone(&self.host),
            Arc::clone(&self.runtime),
            Arc::clone(&self.backends),
        )
    }
}

struct SpeechShutdownWitness {
    started: Mutex<(Option<std_mpsc::Receiver<()>>, bool)>,
    result: Mutex<ShutdownWitnessResult>,
    thread: Option<thread::JoinHandle<()>>,
}

type ShutdownWitnessResult = (
    std_mpsc::Receiver<Result<ShutdownOutcome, String>>,
    Option<ShutdownOutcome>,
);

impl ShutdownWitness for SpeechShutdownWitness {
    type Error = String;
    fn wait_started(&self, timeout: Duration) -> Result<(), Self::Error> {
        let mut started = self
            .started
            .lock()
            .map_err(|_| "start witness unavailable".to_owned())?;
        if started.1 {
            return Ok(());
        }
        started
            .0
            .take()
            .ok_or_else(|| "start witness already consumed".to_owned())?
            .recv_timeout(timeout)
            .map_err(|error| error.to_string())?;
        started.1 = true;
        Ok(())
    }
    fn try_complete(&self) -> Result<Option<ShutdownOutcome>, Self::Error> {
        let mut result = self
            .result
            .lock()
            .map_err(|_| "result witness unavailable".to_owned())?;
        if result.1.is_none() {
            match result.0.try_recv() {
                Ok(Ok(outcome)) => result.1 = Some(outcome),
                Ok(Err(error)) => return Err(error),
                Err(std_mpsc::TryRecvError::Empty) => {}
                Err(std_mpsc::TryRecvError::Disconnected) => {
                    return Err("shutdown result disconnected".to_owned());
                }
            }
        }
        Ok(result.1.clone())
    }
    fn wait(mut self, timeout: Duration) -> Result<ShutdownOutcome, Self::Error> {
        let outcome = {
            let mut result = self
                .result
                .lock()
                .map_err(|_| "result witness unavailable".to_owned())?;
            match result.1.take() {
                Some(outcome) => outcome,
                None => result
                    .0
                    .recv_timeout(timeout)
                    .map_err(|error| error.to_string())??,
            }
        };
        self.thread
            .take()
            .ok_or_else(|| "shutdown thread already joined".to_owned())?
            .join()
            .map_err(|_| "shutdown witness panicked".to_owned())?;
        Ok(outcome)
    }
}

fn lifecycle_phase(phase: HostPhase) -> LifecyclePhase {
    match phase {
        HostPhase::Running => LifecyclePhase::Running,
        HostPhase::Quiescing => LifecyclePhase::Quiescing,
        HostPhase::Closed => LifecyclePhase::Closed,
    }
}

fn task_snapshots(
    host: &SpeechHost,
    backends: &[Arc<ControlledBackend>],
) -> Result<Vec<TaskSupervisorSnapshot>, String> {
    let mut snapshots = vec![
        host.lifecycle
            .tasks
            .snapshot()
            .map_err(|error| error.to_string())?,
    ];
    for backend in backends {
        snapshots.push(
            backend
                .tasks
                .snapshot()
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(snapshots)
}

fn shutdown_outcome(
    host: &SpeechHost,
    backends: &[Arc<ControlledBackend>],
) -> Result<ShutdownOutcome, String> {
    let phase = host
        .lifecycle
        .state
        .lock()
        .map_err(|_| "host state unavailable".to_owned())?
        .phase;
    let snapshots = task_snapshots(host, backends)?;
    let expected_worker_ids = snapshots
        .iter()
        .flat_map(|snapshot| snapshot.expected_worker_ids.clone())
        .collect::<Vec<_>>();
    let joined_worker_ids = snapshots
        .iter()
        .flat_map(|snapshot| snapshot.joined_worker_ids.clone())
        .collect::<Vec<_>>();
    let retained_tasks = snapshots.iter().try_fold(0usize, |total, snapshot| {
        total
            .checked_add(snapshot.active)
            .ok_or_else(|| "retained task overflow".to_owned())
    })?;
    Ok(ShutdownOutcome {
        facts: ClosedFacts {
            lifecycle: lifecycle_phase(phase),
            active_operations: host
                .lifecycle
                .operations
                .active_count()
                .map_err(|error| error.to_string())?,
            retained_tasks,
            expected_workers: expected_worker_ids.len(),
            joined_workers: joined_worker_ids.len(),
        },
        expected_worker_ids,
        joined_worker_ids,
    })
}

fn speech_shutdown_witness(
    host: Arc<SpeechHost>,
    runtime: Arc<tokio::runtime::Runtime>,
    backends: Arc<Mutex<Vec<Arc<ControlledBackend>>>>,
) -> SpeechShutdownWitness {
    let (started_tx, started_rx) = std_mpsc::sync_channel(0);
    let (result_tx, result_rx) = std_mpsc::sync_channel(1);
    let thread = thread::spawn(move || {
        runtime.block_on(async move {
            let shutdown_host = Arc::clone(&host);
            let shutdown = tokio::spawn(async move { shutdown_host.shutdown().await });
            loop {
                let phase = host
                    .lifecycle
                    .state
                    .lock()
                    .map(|state| state.phase)
                    .map_err(|_| "host state unavailable".to_owned());
                match phase {
                    Ok(HostPhase::Running) => tokio::task::yield_now().await,
                    Ok(_) => break,
                    Err(error) => {
                        let _send_failed = result_tx.send(Err(error)).is_err();
                        return;
                    }
                }
            }
            let _start_gone = started_tx.send(()).is_err();
            let result = match shutdown.await {
                Ok(Ok(())) => {
                    let backends = backends
                        .lock()
                        .map_err(|_| "backend registry unavailable".to_owned())
                        .map(|items| items.clone());
                    backends.and_then(|backends| shutdown_outcome(&host, &backends))
                }
                Ok(Err(error)) => Err(error.to_string()),
                Err(error) => Err(error.to_string()),
            };
            let _result_gone = result_tx.send(result).is_err();
        })
    });
    SpeechShutdownWitness {
        started: Mutex::new((Some(started_rx), false)),
        result: Mutex::new((result_rx, None)),
        thread: Some(thread),
    }
}

impl StableShutdownAdapter for SpeechBridgeAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = String;
    type Operation = ControlledOperation;
    type ShutdownWitness = SpeechShutdownWitness;
    fn deterministic() -> Self {
        Self::deterministic_with_capacity(1)
    }
    fn start(&self, id: &str) -> Result<Self::Operation, Self::Error> {
        self.start_operation(id)
    }
    fn request_completed_release(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        self.send(op, WorkerCommand::Completed)
    }
    fn wait_released(
        &self,
        op: &Self::Operation,
        timeout: Duration,
    ) -> Result<OperationSnapshot, Self::Error> {
        self.wait_released(op, timeout)
    }
    fn allow_worker_exit(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.allow_exit.add_permits(1);
        Ok(())
    }
    fn begin_shutdown(&self) -> Self::ShutdownWitness {
        self.shutdown_witness()
    }
}

#[derive(Clone)]
struct AdmissionAdapter {
    inner: SpeechBridgeAdapter,
    pending: Arc<Mutex<Option<SpeechShutdownWitness>>>,
}

impl AdmissionQuiesceShutdownBridgeAdapter for AdmissionAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = String;
    type Operation = ControlledOperation;
    type ShutdownWitness = SpeechShutdownWitness;
    fn deterministic() -> Self {
        Self {
            inner: SpeechBridgeAdapter::deterministic_with_capacity(1),
            pending: Arc::new(Mutex::new(None)),
        }
    }
    fn reserve(&self, id: &str) -> Result<Self::Operation, Self::Error> {
        self.inner.start_operation(id)
    }
    fn quiesce(&self) {
        self.inner.host.quiesce().expect("host quiesces");
    }
    fn phase(&self) -> LifecyclePhase {
        lifecycle_phase(
            self.inner
                .host
                .lifecycle
                .state
                .lock()
                .expect("host state")
                .phase,
        )
    }
    fn active_count(&self) -> usize {
        self.inner
            .host
            .lifecycle
            .operations
            .active_count()
            .expect("operation registry")
    }
    fn retained_task_count(&self) -> usize {
        self.inner.retained_tasks().expect("task snapshots")
    }
    fn cancellation_requested(&self, id: &str) -> bool {
        self.inner
            .host
            .lifecycle
            .operations
            .current(id)
            .expect("operation registry")
            .is_some_and(|snapshot| snapshot.cancellation_requested)
    }
    fn request_cancelled_release(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        self.inner.send(op, WorkerCommand::Cancelled)
    }
    fn wait_released(
        &self,
        op: &Self::Operation,
        timeout: Duration,
    ) -> Result<OperationSnapshot, Self::Error> {
        self.inner.wait_released(op, timeout)
    }
    fn allow_worker_exit(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.allow_exit.add_permits(1);
        Ok(())
    }
    fn begin_shutdown(&self) -> Self::ShutdownWitness {
        self.pending
            .lock()
            .expect("pending witness")
            .take()
            .unwrap_or_else(|| self.inner.shutdown_witness())
    }
    fn shutdown(&self) -> ClosedFacts {
        self.inner
            .runtime
            .block_on(self.inner.host.shutdown())
            .expect("repeated shutdown");
        let backends = self
            .inner
            .backends
            .lock()
            .expect("backend registry")
            .clone();
        shutdown_outcome(&self.inner.host, &backends)
            .expect("closed outcome")
            .facts
    }
}

#[derive(Clone)]
struct ProgressAdapter {
    inner: SpeechBridgeAdapter,
    capacity: usize,
}

impl ProgressShutdownBridgeAdapter for ProgressAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = String;
    type UnreadProgress = ();
    type Operation = Arc<ControlledOperation>;
    type ShutdownWitness = SpeechShutdownWitness;
    fn deterministic(progress_capacity: usize) -> Self {
        let inner = SpeechBridgeAdapter::deterministic_with_capacity(progress_capacity);
        let capacity = inner.host.lifecycle.operations.progress_capacity();
        Self { inner, capacity }
    }
    fn start(&self, id: &str) -> Result<(Self::UnreadProgress, Self::Operation), Self::Error> {
        Ok(((), Arc::new(self.inner.start_operation(id)?)))
    }
    fn publish_progress(&self, op: &Self::Operation, sequence: u64) -> Result<(), Self::Error> {
        op.lifecycle
            .publish_progress(sequence)
            .map_err(|error| error.to_string())?;
        let sender = op
            .events
            .lock()
            .map_err(|_| "event state unavailable".to_owned())?
            .clone()
            .ok_or_else(|| "event sender missing".to_owned())?;
        match sender.try_send(SynthesisEvent::Warning {
            request_id: op.request_id.clone(),
            code: "contract-progress".to_owned(),
            message: sequence.to_string(),
        }) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err("progress channel closed".to_owned()),
        }
    }
    fn snapshot(&self, op: &Self::Operation) -> Option<OperationSnapshot> {
        op.lifecycle
            .snapshot()
            .expect("operation registry")
            .map(contract_snapshot)
    }
    fn begin_shutdown(&self) -> Self::ShutdownWitness {
        self.inner.shutdown_witness()
    }
    fn request_completed_release(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        self.inner.send(op, WorkerCommand::Completed)
    }
    fn wait_released(
        &self,
        op: &Self::Operation,
        timeout: Duration,
    ) -> Result<OperationSnapshot, Self::Error> {
        self.inner.wait_released(op, timeout)
    }
    fn allow_worker_exit(&self, op: &Self::Operation) -> Result<(), Self::Error> {
        op.allow_exit.add_permits(1);
        Ok(())
    }
    fn progress_capacity(&self) -> usize {
        self.capacity
    }
}

#[derive(Clone)]
struct PanicAdapter {
    inner: SpeechBridgeAdapter,
}
impl PanicShutdownBridgeAdapter for PanicAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = String;
    type Operation = ControlledOperation;
    type ShutdownWitness = SpeechShutdownWitness;
    fn deterministic() -> Self {
        Self {
            inner: SpeechBridgeAdapter::deterministic_with_capacity(1),
        }
    }
    fn run_controlled_panicking_operation(&self, id: &str) -> Result<Self::Operation, Self::Error> {
        let operation = self.inner.start_operation(id)?;
        self.inner.send(&operation, WorkerCommand::Panic)?;
        Ok(operation)
    }
    fn wait_failed_release(
        &self,
        op: &Self::Operation,
        timeout: Duration,
    ) -> Result<OperationSnapshot, Self::Error> {
        let snapshot = self.inner.wait_released(op, timeout)?;
        op.allow_exit.add_permits(1);
        Ok(snapshot)
    }
    fn begin_shutdown(&self) -> Self::ShutdownWitness {
        self.inner.shutdown_witness()
    }
}

impl TaskReapingAdapter for SpeechBridgeAdapter {
    type Implementation = SpeechHostLifecycle;
    type Error = String;
    type Operation = ControlledOperation;
    fn deterministic() -> Self {
        Self::deterministic_with_capacity(1)
    }
    fn start(&self, id: &str) -> Result<Self::Operation, Self::Error> {
        self.start_operation(id)
    }
    fn finish(&self, op: Self::Operation) -> Result<(), Self::Error> {
        self.send(&op, WorkerCommand::Completed)?;
        self.wait_released(&op, Duration::from_secs(5))?;
        op.allow_exit.add_permits(1);
        self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), op.backend.tasks.wait_for_idle())
                .await
                .map_err(|_| "worker did not reap before timeout".to_owned())?
                .map_err(|error| error.to_string())
        })
    }
    fn active_count(&self) -> usize {
        self.host
            .lifecycle
            .operations
            .active_count()
            .expect("operation registry")
    }
    fn retained_task_count(&self) -> usize {
        self.backend_retained_tasks()
            .expect("backend task snapshots")
    }
    fn shutdown(&self) -> ClosedFacts {
        self.runtime
            .block_on(self.host.shutdown())
            .expect("shutdown");
        let backends = self.backends.lock().expect("backend registry").clone();
        shutdown_outcome(&self.host, &backends)
            .expect("closed outcome")
            .facts
    }
}

#[test]
fn full_speech_lifecycle_manifest_covers_all_eighteen_invariants() {
    let evidence = vec![
        run_transition_chain_suite::<RegistryAdapter>("operation-state-machine"),
        run_registry_identity_suite::<RegistryAdapter>("operation-registry"),
        run_attempt_hierarchy_suite::<RegistryAdapter>("backend-attempts"),
        run_consumer_cancellation_suite::<RegistryAdapter>("speech-ticket-control"),
        run_terminal_authority_suite::<RegistryAdapter>("executor-terminal-owner"),
        run_waiter_control_suite::<RegistryAdapter>("ticket-waiter"),
        run_admission_quiesce_shutdown_bridge_suite::<AdmissionAdapter>("host-shutdown-bridge"),
        run_progress_shutdown_bridge_suite::<ProgressAdapter>("bounded-event-bridge"),
        run_panic_shutdown_bridge_suite::<PanicAdapter>("panic-bridge"),
        run_stable_shutdown_suite::<SpeechBridgeAdapter>("stable-host-shutdown"),
        run_task_reaping_suite::<SpeechBridgeAdapter>("host-and-backend-supervisors"),
    ];
    let manifest = LifecycleCoverageManifest::<SpeechHostLifecycle>::accept(evidence)
        .expect("complete Speech lifecycle coverage");
    assert_eq!(manifest.covered().count(), 18);
    assert_eq!(manifest.components().count(), 11);
    assert_eq!(manifest.product(), "speech-native-kit");
    assert_eq!(manifest.implementation(), "speech-host-v1");
}
