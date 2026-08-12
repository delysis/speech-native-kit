//! Backend registry and execution service for interoperable speech consumers.
//!
//! The service owns no Tauri state. It plans over the descriptors of actually
//! registered backends, pins the resolved backend/model/voice into the request,
//! and then dispatches through the protocol-neutral `SpeechBackend` trait.

use serde::{Deserialize, Serialize};
use speech_native_router::{SpeechRouteError, SpeechRoutePlan, SpeechRouter};
use speech_native_types::{
    CapabilitySourceReport, PlatformCapabilitySnapshot, PlatformTarget, ProbeSourceStatus,
    SPEECH_CAPABILITY_SCHEMA, SpeechBackend, SpeechBackendDescriptor, SpeechCancellation,
    SpeechError, SpeechErrorClass, SpeechRequestId, SpeechRouteSelector, SynthesisRequest,
    SynthesisTicket, TaskSupervisor, TaskSupervisorError, TranscriptionRequest,
    TranscriptionTicket,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, mpsc, oneshot};

#[cfg(feature = "unstable-w1-contract-tests")]
mod w1_contracts;
#[cfg(feature = "unstable-w1-contract-tests")]
pub use w1_contracts::SpeechW1ContractAdapter;
#[cfg(all(test, feature = "unstable-w1-contract-tests"))]
mod w1_lifecycle_tests;

pub mod operation_lifecycle;

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
    #[error("speech host admission is closed")]
    AdmissionClosed,
    #[error("speech request id is already active: {request_id}")]
    RequestDuplicate { request_id: SpeechRequestId },
    #[error("speech request nonce space is exhausted")]
    NonceExhausted,
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
    lifecycle: Arc<HostLifecycle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostPhase {
    Running,
    Quiescing,
    Closed,
}

struct ActiveRoute {
    backend: Arc<dyn SpeechBackend>,
    identity: operation_lifecycle::OperationIdentity,
}

struct HostState {
    phase: HostPhase,
    shutdown_started: bool,
    backends: BTreeMap<String, Arc<dyn SpeechBackend>>,
    routes: BTreeMap<SpeechRequestId, ActiveRoute>,
    shutdown_result: Option<Result<(), SpeechHostError>>,
    #[cfg(feature = "unstable-w1-contract-tests")]
    shutdown_facts: Option<HostShutdownFacts>,
}

#[cfg(feature = "unstable-w1-contract-tests")]
#[derive(Clone)]
struct HostShutdownFacts {
    backends: Vec<BackendShutdownFact>,
    tasks: speech_native_types::TaskSupervisorSnapshot,
    monitor_failure: Option<speech_native_types::SupervisedTaskFailureSummary>,
}

#[cfg(feature = "unstable-w1-contract-tests")]
#[derive(Clone)]
struct BackendShutdownFact {
    backend_id: String,
    error: Option<SpeechError>,
}

struct HostLifecycle {
    state: Mutex<HostState>,
    operations: operation_lifecycle::OperationRegistry,
    faulted: AtomicBool,
    changed: Notify,
    tasks: Arc<TaskSupervisor>,
}

struct HostCancellation {
    lifecycle: Weak<HostLifecycle>,
    request_id: SpeechRequestId,
    identity: operation_lifecycle::OperationIdentity,
    _consumer: operation_lifecycle::ConsumerGuard,
}

struct ReservedRoute {
    plan: SpeechRoutePlan,
    backend: Arc<dyn SpeechBackend>,
    consumer: operation_lifecycle::ConsumerGuard,
    operation: operation_lifecycle::OperationLease,
}

struct ExecutorOperation {
    lifecycle: Arc<HostLifecycle>,
    request_id: SpeechRequestId,
    operation: operation_lifecycle::OperationLease,
    attempt: Option<operation_lifecycle::AttemptLease>,
    finished: bool,
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
            lifecycle: Arc::new(HostLifecycle {
                state: Mutex::new(HostState {
                    phase: HostPhase::Running,
                    shutdown_started: false,
                    backends: BTreeMap::new(),
                    routes: BTreeMap::new(),
                    shutdown_result: None,
                    #[cfg(feature = "unstable-w1-contract-tests")]
                    shutdown_facts: None,
                }),
                operations: operation_lifecycle::OperationRegistry::default(),
                faulted: AtomicBool::new(false),
                changed: Notify::new(),
                tasks: Arc::new(TaskSupervisor::default()),
            }),
        }
    }

    pub fn register_backend(&self, backend: Arc<dyn SpeechBackend>) -> Result<(), SpeechHostError> {
        let descriptor = backend.descriptor();
        descriptor
            .validate()
            .map_err(|error| SpeechHostError::BackendInvalid {
                detail: error.to_string(),
            })?;
        let mut state = self.lifecycle.state.lock().map_err(|_| {
            self.lifecycle.mark_faulted();
            SpeechHostError::StateUnavailable
        })?;
        if state.phase != HostPhase::Running {
            return Err(SpeechHostError::AdmissionClosed);
        }
        if state.backends.contains_key(&descriptor.id) {
            return Err(SpeechHostError::BackendDuplicate {
                backend_id: descriptor.id,
            });
        }
        state.backends.insert(descriptor.id, backend);
        Ok(())
    }

    pub fn status(&self) -> Result<SpeechHostStatus, SpeechHostError> {
        Ok(SpeechHostStatus {
            target: self.target.clone(),
            backends: self.descriptors()?,
        })
    }

    pub fn descriptors(&self) -> Result<Vec<SpeechBackendDescriptor>, SpeechHostError> {
        let state = self.lifecycle.state.lock().map_err(|_| {
            self.lifecycle.mark_faulted();
            SpeechHostError::StateUnavailable
        })?;
        Ok(state
            .backends
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
        let request_id = request.context.request_id.clone();
        let ReservedRoute {
            plan,
            backend,
            consumer,
            operation,
        } = self.reserve_transcription(&request)?;
        pin_route(&mut request.context.route, &plan);
        let attempt = match operation
            .queue()
            .and_then(|()| operation.start())
            .and_then(|()| operation.start_attempt())
        {
            Ok(attempt) => attempt,
            Err(error) => {
                return Err(self.rollback_setup(&request_id, &operation, error));
            }
        };
        let mut executor = ExecutorOperation {
            lifecycle: Arc::clone(&self.lifecycle),
            request_id: request_id.clone(),
            operation: operation.clone(),
            attempt: Some(attempt),
            finished: false,
        };
        let mut backend_ticket = match backend.transcribe(request).await {
            Ok(ticket) => ticket,
            Err(error) => {
                executor
                    .finish(terminal_for_error(&error))
                    .map_err(map_registry_error)?;
                return Err(error.into());
            }
        };
        if operation
            .snapshot()
            .map_err(map_registry_error)?
            .is_some_and(|snapshot| snapshot.cancellation_requested)
        {
            backend.cancel(&request_id);
        }

        let events = std::mem::replace(&mut backend_ticket.events, mpsc::channel(1).1);
        let audio_sink = backend_ticket.audio_sink.take();
        let (final_sender, final_receiver) = oneshot::channel();
        let cancellation = Arc::new(HostCancellation {
            lifecycle: Arc::downgrade(&self.lifecycle),
            request_id: request_id.clone(),
            identity: operation.identity(),
            _consumer: consumer,
        });
        let monitor_request_id = request_id.clone();
        self.spawn_monitor(format!("host-final-relay:{request_id}"), async move {
            let result = backend_ticket.final_response().await;
            let finalized = executor.finish(terminal_for_result(&result));
            let delivered = match finalized {
                Ok(()) => result,
                Err(ref error) => Err(lifecycle_speech_error(&monitor_request_id, error)),
            };
            let _consumer_gone = final_sender.send(delivered).is_err();
            finalized.map_err(|error| error.to_string())
        })?;
        Ok(TranscriptionTicket::new(
            request_id,
            events,
            final_receiver,
            cancellation,
            audio_sink,
        ))
    }

    pub async fn synthesize(
        &self,
        mut request: SynthesisRequest,
    ) -> Result<SynthesisTicket, SpeechHostError> {
        let request_id = request.context.request_id.clone();
        let ReservedRoute {
            plan,
            backend,
            consumer,
            operation,
        } = self.reserve_synthesis(&request)?;
        pin_route(&mut request.context.route, &plan);
        let attempt = match operation
            .queue()
            .and_then(|()| operation.start())
            .and_then(|()| operation.start_attempt())
        {
            Ok(attempt) => attempt,
            Err(error) => {
                return Err(self.rollback_setup(&request_id, &operation, error));
            }
        };
        let mut executor = ExecutorOperation {
            lifecycle: Arc::clone(&self.lifecycle),
            request_id: request_id.clone(),
            operation: operation.clone(),
            attempt: Some(attempt),
            finished: false,
        };
        let mut backend_ticket = match backend.synthesize(request).await {
            Ok(ticket) => ticket,
            Err(error) => {
                executor
                    .finish(terminal_for_error(&error))
                    .map_err(map_registry_error)?;
                return Err(error.into());
            }
        };
        if operation
            .snapshot()
            .map_err(map_registry_error)?
            .is_some_and(|snapshot| snapshot.cancellation_requested)
        {
            backend.cancel(&request_id);
        }

        let events = std::mem::replace(&mut backend_ticket.events, mpsc::channel(1).1);
        let (final_sender, final_receiver) = oneshot::channel();
        let cancellation = Arc::new(HostCancellation {
            lifecycle: Arc::downgrade(&self.lifecycle),
            request_id: request_id.clone(),
            identity: operation.identity(),
            _consumer: consumer,
        });
        let monitor_request_id = request_id.clone();
        self.spawn_monitor(format!("host-final-relay:{request_id}"), async move {
            let result = backend_ticket.final_response().await;
            let finalized = executor.finish(terminal_for_result(&result));
            let delivered = match finalized {
                Ok(()) => result,
                Err(ref error) => Err(lifecycle_speech_error(&monitor_request_id, error)),
            };
            let _consumer_gone = final_sender.send(delivered).is_err();
            finalized.map_err(|error| error.to_string())
        })?;
        Ok(SynthesisTicket::new(
            request_id,
            events,
            final_receiver,
            cancellation,
        ))
    }

    #[must_use]
    pub fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        self.lifecycle.cancel(request_id, None)
    }

    pub fn quiesce(&self) -> Result<(), SpeechHostError> {
        let active = {
            let mut state = self.lifecycle.state.lock().map_err(|_| {
                self.lifecycle.mark_faulted();
                SpeechHostError::StateUnavailable
            })?;
            if state.phase != HostPhase::Running {
                return Ok(());
            }
            let cancelled = self
                .lifecycle
                .operations
                .request_cancel_all()
                .map_err(map_registry_error)?;
            state.phase = HostPhase::Quiescing;
            cancelled
                .into_iter()
                .filter_map(|operation_id| {
                    let request_id = SpeechRequestId(operation_id);
                    state
                        .routes
                        .get(&request_id)
                        .map(|route| (request_id, Arc::clone(&route.backend)))
                })
                .collect::<Vec<_>>()
        };
        for (request_id, backend) in active {
            backend.cancel(&request_id);
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), SpeechHostError> {
        let (leader, backends, active) = {
            let mut state = self.lifecycle.state.lock().map_err(|_| {
                self.lifecycle.mark_faulted();
                SpeechHostError::StateUnavailable
            })?;
            match state.phase {
                HostPhase::Running => {
                    let cancelled = self
                        .lifecycle
                        .operations
                        .request_cancel_all()
                        .map_err(map_registry_error)?;
                    state.phase = HostPhase::Quiescing;
                    state.shutdown_started = true;
                    (
                        true,
                        state.backends.values().cloned().collect::<Vec<_>>(),
                        cancelled
                            .into_iter()
                            .filter_map(|operation_id| {
                                let request_id = SpeechRequestId(operation_id);
                                state
                                    .routes
                                    .get(&request_id)
                                    .map(|route| (request_id, Arc::clone(&route.backend)))
                            })
                            .collect::<Vec<_>>(),
                    )
                }
                HostPhase::Quiescing if !state.shutdown_started => {
                    state.shutdown_started = true;
                    (
                        true,
                        state.backends.values().cloned().collect::<Vec<_>>(),
                        state
                            .routes
                            .iter()
                            .map(|(request_id, route)| {
                                (request_id.clone(), Arc::clone(&route.backend))
                            })
                            .collect::<Vec<_>>(),
                    )
                }
                HostPhase::Quiescing => (false, Vec::new(), Vec::new()),
                HostPhase::Closed => {
                    return state
                        .shutdown_result
                        .clone()
                        .unwrap_or(Err(SpeechHostError::StateUnavailable));
                }
            }
        };
        if !leader {
            return self.wait_for_shutdown().await;
        }

        let mut infrastructure_error = self
            .lifecycle
            .tasks
            .begin_shutdown()
            .err()
            .map(map_task_supervisor_error);

        for (request_id, backend) in active {
            backend.cancel(&request_id);
        }
        let mut failures = Vec::new();
        #[cfg(feature = "unstable-w1-contract-tests")]
        let mut backend_facts = Vec::with_capacity(backends.len());
        for backend in backends {
            #[cfg(feature = "unstable-w1-contract-tests")]
            let backend_id = backend.descriptor().id;
            let error = backend.shutdown().await.err();
            failures.extend(error.clone());
            #[cfg(feature = "unstable-w1-contract-tests")]
            backend_facts.push(BackendShutdownFact { backend_id, error });
        }
        if let Err(error) = self.wait_for_active_empty().await {
            infrastructure_error.get_or_insert(error);
        }
        if let Err(error) = self.lifecycle.tasks.wait_for_idle().await {
            infrastructure_error.get_or_insert(map_task_supervisor_error(error));
        }
        let monitor_failure = match self.lifecycle.tasks.failure_summary() {
            Ok(failure) => failure,
            Err(error) => {
                infrastructure_error.get_or_insert(map_task_supervisor_error(error));
                None
            }
        };
        if let Some(summary) = &monitor_failure {
            let additional = summary.additional_failures;
            let first = &summary.first;
            failures.push(SpeechError::unavailable(
                &SpeechRequestId("speech-host-monitor".to_string()),
                "speech_host_monitor_failed",
                &format!(
                    "speech host monitor '{}' failed ({:?}): {}; {additional} additional failure(s)",
                    first.label, first.kind, first.detail
                ),
            ));
        }
        if self.lifecycle.faulted.load(Ordering::Acquire)
            || self.lifecycle.operations.diagnostic_faulted()
        {
            failures.push(SpeechError::unavailable(
                &SpeechRequestId("speech-host-lifecycle".to_owned()),
                "speech_host_lifecycle_failed",
                "speech host lifecycle ownership encountered an internal state failure",
            ));
        }
        let result = if let Some(error) = infrastructure_error {
            Err(error)
        } else if failures.is_empty() {
            Ok(())
        } else {
            Err(SpeechHostError::Shutdown { failures })
        };
        #[cfg(feature = "unstable-w1-contract-tests")]
        let (result, task_snapshot) = match self.lifecycle.tasks.snapshot() {
            Ok(snapshot) => (result, Some(snapshot)),
            Err(error) => (Err(map_task_supervisor_error(error)), None),
        };
        {
            let mut state = self.lifecycle.state.lock().map_err(|_| {
                self.lifecycle.mark_faulted();
                SpeechHostError::StateUnavailable
            })?;
            state.shutdown_result = Some(result.clone());
            #[cfg(feature = "unstable-w1-contract-tests")]
            {
                state.shutdown_facts = task_snapshot.map(|tasks| HostShutdownFacts {
                    backends: backend_facts,
                    tasks,
                    monitor_failure,
                });
            }
            state.phase = HostPhase::Closed;
        }
        self.lifecycle.changed.notify_waiters();
        result
    }

    fn reserve_transcription(
        &self,
        request: &TranscriptionRequest,
    ) -> Result<ReservedRoute, SpeechHostError> {
        self.reserve(request.context.request_id.clone(), |snapshot| {
            self.router.plan_transcription(request, snapshot)
        })
    }

    fn reserve_synthesis(
        &self,
        request: &SynthesisRequest,
    ) -> Result<ReservedRoute, SpeechHostError> {
        self.reserve(request.context.request_id.clone(), |snapshot| {
            self.router.plan_synthesis(request, snapshot)
        })
    }

    fn reserve(
        &self,
        request_id: SpeechRequestId,
        plan: impl FnOnce(&PlatformCapabilitySnapshot) -> Result<SpeechRoutePlan, SpeechRouteError>,
    ) -> Result<ReservedRoute, SpeechHostError> {
        let mut state = self.lifecycle.state.lock().map_err(|_| {
            self.lifecycle.mark_faulted();
            SpeechHostError::StateUnavailable
        })?;
        if state.phase != HostPhase::Running {
            return Err(SpeechHostError::AdmissionClosed);
        }
        if state.routes.contains_key(&request_id) {
            return Err(SpeechHostError::RequestDuplicate { request_id });
        }
        let snapshot = snapshot_from_backends(self.target.clone(), &state.backends);
        let route = plan(&snapshot)?;
        let backend_id = &route.selected.route.backend_id;
        let backend = state.backends.get(backend_id).cloned().ok_or_else(|| {
            SpeechHostError::BackendMissing {
                backend_id: backend_id.clone(),
            }
        })?;
        let (consumer, operation) = self
            .lifecycle
            .operations
            .reserve(&request_id.0)
            .map_err(map_registry_error)?;
        state.routes.insert(
            request_id,
            ActiveRoute {
                backend: Arc::clone(&backend),
                identity: operation.identity(),
            },
        );
        Ok(ReservedRoute {
            plan: route,
            backend,
            consumer,
            operation,
        })
    }

    fn spawn_monitor(
        &self,
        label: String,
        monitor: impl Future<Output = Result<(), String>> + Send + 'static,
    ) -> Result<(), SpeechHostError> {
        self.lifecycle
            .tasks
            .spawn(label, monitor)
            .map_err(map_task_supervisor_error)
    }

    fn rollback_setup(
        &self,
        request_id: &SpeechRequestId,
        operation: &operation_lifecycle::OperationLease,
        setup_error: operation_lifecycle::RegistryError,
    ) -> SpeechHostError {
        self.lifecycle.mark_faulted();
        match operation.fail_setup_and_release() {
            Ok(_) => {
                if self
                    .lifecycle
                    .release_route(request_id, &operation.identity())
                    .is_ok()
                {
                    map_registry_error(setup_error)
                } else {
                    SpeechHostError::StateUnavailable
                }
            }
            Err(_) => SpeechHostError::StateUnavailable,
        }
    }

    async fn wait_for_active_empty(&self) -> Result<(), SpeechHostError> {
        loop {
            let changed = self.lifecycle.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.lifecycle.is_faulted() {
                return Err(SpeechHostError::StateUnavailable);
            }
            if self
                .lifecycle
                .operations
                .active_count()
                .map_err(map_registry_error)?
                == 0
            {
                return Ok(());
            }
            changed.await;
        }
    }

    async fn wait_for_shutdown(&self) -> Result<(), SpeechHostError> {
        loop {
            let changed = self.lifecycle.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let result = {
                let state = self.lifecycle.state.lock().map_err(|_| {
                    self.lifecycle.mark_faulted();
                    SpeechHostError::StateUnavailable
                })?;
                (state.phase == HostPhase::Closed).then(|| state.shutdown_result.clone())
            };
            if let Some(result) = result {
                return result.unwrap_or(Err(SpeechHostError::StateUnavailable));
            }
            if self.lifecycle.is_faulted() {
                return Err(SpeechHostError::StateUnavailable);
            }
            changed.await;
        }
    }
}

fn map_task_supervisor_error(error: TaskSupervisorError) -> SpeechHostError {
    match error {
        TaskSupervisorError::AdmissionClosed => SpeechHostError::AdmissionClosed,
        TaskSupervisorError::StateUnavailable | TaskSupervisorError::RuntimeUnavailable => {
            SpeechHostError::StateUnavailable
        }
    }
}

fn map_registry_error(error: operation_lifecycle::RegistryError) -> SpeechHostError {
    match error {
        operation_lifecycle::RegistryError::Duplicate => SpeechHostError::StateUnavailable,
        operation_lifecycle::RegistryError::Exhausted => SpeechHostError::NonceExhausted,
        operation_lifecycle::RegistryError::Stale
        | operation_lifecycle::RegistryError::InvalidTransition
        | operation_lifecycle::RegistryError::StateUnavailable => SpeechHostError::StateUnavailable,
    }
}

fn lifecycle_speech_error(
    request_id: &SpeechRequestId,
    error: &operation_lifecycle::RegistryError,
) -> SpeechError {
    SpeechError::unavailable(
        request_id,
        "speech_lifecycle_finalization_failed",
        &format!("speech lifecycle finalization failed: {error}"),
    )
}

impl HostLifecycle {
    fn mark_faulted(&self) {
        self.faulted.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn is_faulted(&self) -> bool {
        self.faulted.load(Ordering::Acquire)
            || self.operations.diagnostic_faulted()
            || self.tasks.diagnostic_faulted()
    }

    fn cancel(
        &self,
        request_id: &SpeechRequestId,
        identity: Option<&operation_lifecycle::OperationIdentity>,
    ) -> usize {
        let route = match self.state.lock() {
            Ok(state) => state.routes.get(request_id).and_then(|route| {
                identity
                    .is_none_or(|identity| identity == &route.identity)
                    .then(|| (Arc::clone(&route.backend), route.identity.clone()))
            }),
            Err(_) => {
                self.mark_faulted();
                return 0;
            }
        };
        let Some((backend, route_identity)) = route else {
            return 0;
        };
        let operation = match self.operations.current_lease(&request_id.0) {
            Ok(Some(operation)) => operation,
            Ok(None) => return 0,
            Err(_) => {
                self.mark_faulted();
                return 0;
            }
        };
        if route_identity != operation.identity() {
            self.mark_faulted();
            return 0;
        }
        if identity.is_some_and(|identity| identity != &operation.identity())
            || operation.request_cancel().is_err()
        {
            if self.operations.diagnostic_faulted() {
                self.mark_faulted();
            }
            return 0;
        }
        backend.cancel(request_id);
        1
    }

    fn release_route(
        &self,
        request_id: &SpeechRequestId,
        identity: &operation_lifecycle::OperationIdentity,
    ) -> Result<(), SpeechHostError> {
        let mut state = self.state.lock().map_err(|_| {
            self.mark_faulted();
            SpeechHostError::StateUnavailable
        })?;
        if !state
            .routes
            .get(request_id)
            .is_some_and(|route| &route.identity == identity)
        {
            self.mark_faulted();
            return Err(SpeechHostError::StateUnavailable);
        }
        state.routes.remove(request_id);
        self.changed.notify_waiters();
        Ok(())
    }
}

impl SpeechCancellation for HostCancellation {
    fn cancel(&self, request_id: &SpeechRequestId) -> usize {
        if request_id != &self.request_id {
            return 0;
        }
        let Some(lifecycle) = self.lifecycle.upgrade() else {
            return 0;
        };
        lifecycle.cancel(request_id, Some(&self.identity))
    }
}

impl ExecutorOperation {
    fn finish(
        &mut self,
        terminal: operation_lifecycle::TerminalClass,
    ) -> Result<(), operation_lifecycle::RegistryError> {
        if self.finished {
            return Err(operation_lifecycle::RegistryError::Stale);
        }
        let attempt = self
            .attempt
            .as_ref()
            .ok_or(operation_lifecycle::RegistryError::Stale)?;
        if let Err(error) = self.operation.finish_attempt_and_release(attempt, terminal) {
            self.lifecycle.mark_faulted();
            return Err(error);
        }
        self.attempt.take();
        let identity = self.operation.identity();
        self.lifecycle
            .release_route(&self.request_id, &identity)
            .map_err(|_| operation_lifecycle::RegistryError::StateUnavailable)?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for ExecutorOperation {
    fn drop(&mut self) {
        if !self.finished {
            self.lifecycle.mark_faulted();
            if self
                .finish(operation_lifecycle::TerminalClass::Failed)
                .is_err()
            {
                self.lifecycle.mark_faulted();
            }
        }
    }
}

fn terminal_for_result<T>(result: &Result<T, SpeechError>) -> operation_lifecycle::TerminalClass {
    match result {
        Ok(_) => operation_lifecycle::TerminalClass::Completed,
        Err(error) => terminal_for_error(error),
    }
}

const fn terminal_for_error(error: &SpeechError) -> operation_lifecycle::TerminalClass {
    if matches!(error.class, SpeechErrorClass::Cancelled) {
        operation_lifecycle::TerminalClass::Cancelled
    } else {
        operation_lifecycle::TerminalClass::Failed
    }
}

fn snapshot_from_backends(
    target: PlatformTarget,
    backends: &BTreeMap<String, Arc<dyn SpeechBackend>>,
) -> PlatformCapabilitySnapshot {
    PlatformCapabilitySnapshot {
        schema: SPEECH_CAPABILITY_SCHEMA.to_string(),
        captured_at_unix_ms: unix_time_ms(),
        target,
        adapter_candidates: Vec::new(),
        source_reports: vec![CapabilitySourceReport {
            source_id: REGISTERED_SOURCE_ID.to_string(),
            status: ProbeSourceStatus::Succeeded,
            detail: None,
            backends: backends
                .values()
                .map(|backend| backend.descriptor())
                .collect(),
        }],
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

    struct DeferredCancellation {
        calls: Arc<AtomicUsize>,
    }

    impl SpeechCancellation for DeferredCancellation {
        fn cancel(&self, _request_id: &SpeechRequestId) -> usize {
            self.calls.fetch_add(1, Ordering::AcqRel);
            1
        }
    }

    struct DeferredBackend {
        descriptor: SpeechBackendDescriptor,
        finals: Mutex<BTreeMap<SpeechRequestId, oneshot::Sender<SynthesisResponse>>>,
        cancel_calls: Arc<AtomicUsize>,
        shutdown_calls: AtomicUsize,
    }

    impl DeferredBackend {
        fn complete(&self, request_id: &SpeechRequestId) {
            let sender = self
                .finals
                .lock()
                .expect("lock deferred finals")
                .remove(request_id)
                .expect("deferred request must exist");
            let _ = sender.send(deferred_response(request_id, &self.descriptor.id));
        }
    }

    #[async_trait]
    impl SpeechBackend for DeferredBackend {
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
            let request_id = request.context.request_id;
            let (event_sender, event_receiver) = mpsc::channel(2);
            drop(event_sender);
            let (backend_final_sender, backend_final_receiver) = oneshot::channel();
            let (release_sender, release_receiver) = oneshot::channel();
            self.finals
                .lock()
                .map_err(|_| {
                    SpeechError::unavailable(
                        &request_id,
                        "fixture_state_unavailable",
                        "fixture state is unavailable",
                    )
                })?
                .insert(request_id.clone(), release_sender);
            let response_id = request_id.clone();
            let backend_id = self.descriptor.id.clone();
            tokio::spawn(async move {
                let result = release_receiver.await.map_or_else(
                    |_| {
                        Err(SpeechError::unavailable(
                            &response_id,
                            "fixture_release_closed",
                            "fixture release closed",
                        ))
                    },
                    Ok,
                );
                let _ = backend_final_sender.send(result);
                drop(backend_id);
            });
            Ok(SynthesisTicket::new(
                request_id,
                event_receiver,
                backend_final_receiver,
                Arc::new(DeferredCancellation {
                    calls: Arc::clone(&self.cancel_calls),
                }),
            ))
        }

        fn cancel(&self, request_id: &SpeechRequestId) -> usize {
            if self
                .finals
                .lock()
                .is_ok_and(|finals| finals.contains_key(request_id))
            {
                self.cancel_calls.fetch_add(1, Ordering::AcqRel);
                1
            } else {
                0
            }
        }

        async fn shutdown(&self) -> Result<(), SpeechError> {
            self.shutdown_calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
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

    fn deferred_backend(id: &str) -> Arc<DeferredBackend> {
        let descriptor = fixture_backend(id).descriptor();
        Arc::new(DeferredBackend {
            descriptor,
            finals: Mutex::new(BTreeMap::new()),
            cancel_calls: Arc::new(AtomicUsize::new(0)),
            shutdown_calls: AtomicUsize::new(0),
        })
    }

    fn deferred_response(request_id: &SpeechRequestId, backend_id: &str) -> SynthesisResponse {
        SynthesisResponse {
            request_id: request_id.clone(),
            route: SpeechResolvedRoute {
                backend_id: backend_id.to_string(),
                model_id: Some("fixture-voice-model".to_string()),
                voice_id: Some("fixture-voice".to_string()),
                backend_kind: SpeechBackendKind::EmbeddedModel,
                network: NetworkBehavior::Never,
            },
            audio: b"RIFFfixtureWAVE".to_vec(),
            format: AudioOutputFormat::Wav,
            duration_ms: Some(1),
            alignments: Vec::new(),
            usage: SpeechUsage::default(),
        }
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

    fn exact_request(request_id: &str, backend_id: &str) -> SynthesisRequest {
        let mut request = request();
        request.context.request_id = SpeechRequestId(request_id.to_string());
        request.context.route = SpeechRouteSelector::ExactBackend {
            backend_id: backend_id.to_string(),
            model_id: Some("fixture-voice-model".to_string()),
            voice_id: Some("fixture-voice".to_string()),
        };
        request
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

    #[tokio::test]
    async fn host_owns_request_identity_until_backend_final() {
        let host = SpeechHost::default();
        let backend_a = deferred_backend("deferred-a.tts");
        let backend_b = deferred_backend("deferred-b.tts");
        host.register_backend(backend_a.clone())
            .expect("register backend A");
        host.register_backend(backend_b.clone())
            .expect("register backend B");

        let request_id = SpeechRequestId("global-request-id".to_string());
        let ticket = host
            .synthesize(exact_request(&request_id.0, "deferred-a.tts"))
            .await
            .expect("start deferred request");
        drop(ticket);
        assert_eq!(backend_a.cancel_calls.load(Ordering::Acquire), 1);
        assert_eq!(backend_b.cancel_calls.load(Ordering::Acquire), 0);

        assert!(matches!(
            host.synthesize(exact_request(&request_id.0, "deferred-b.tts"))
                .await,
            Err(SpeechHostError::RequestDuplicate { .. })
        ));
        assert_eq!(host.cancel(&request_id), 1);
        assert_eq!(backend_a.cancel_calls.load(Ordering::Acquire), 2);
        assert_eq!(backend_b.cancel_calls.load(Ordering::Acquire), 0);

        backend_a.complete(&request_id);
        loop {
            let changed = host.lifecycle.changed.notified();
            if host
                .lifecycle
                .operations
                .active_count()
                .expect("read active operations")
                == 0
            {
                break;
            }
            changed.await;
        }

        let ticket = host
            .synthesize(exact_request(&request_id.0, "deferred-b.tts"))
            .await
            .expect("request id is reusable after backend final");
        backend_b.complete(&request_id);
        let response = ticket.final_response().await.expect("deferred final");
        assert_eq!(response.route.backend_id, "deferred-b.tts");
    }

    #[tokio::test]
    async fn shutdown_waits_for_backend_final_and_retains_result() {
        let host = Arc::new(SpeechHost::default());
        let backend = deferred_backend("deferred.tts");
        host.register_backend(backend.clone())
            .expect("register deferred backend");
        let request_id = SpeechRequestId("shutdown-held-request".to_string());
        let ticket = host
            .synthesize(exact_request(&request_id.0, "deferred.tts"))
            .await
            .expect("start deferred request");
        drop(ticket);

        let leader_host = Arc::clone(&host);
        let mut leader = tokio::spawn(async move { leader_host.shutdown().await });
        let waiter_host = Arc::clone(&host);
        let mut waiter = tokio::spawn(async move { waiter_host.shutdown().await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut leader)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiter)
                .await
                .is_err()
        );

        backend.complete(&request_id);
        leader
            .await
            .expect("leader task joins")
            .expect("leader result");
        waiter
            .await
            .expect("waiter task joins")
            .expect("waiter result");
        host.shutdown().await.expect("retained shutdown result");
        assert_eq!(backend.shutdown_calls.load(Ordering::Acquire), 1);
        assert!(matches!(
            host.register_backend(fixture_backend("late.tts")),
            Err(SpeechHostError::AdmissionClosed)
        ));
        assert!(matches!(
            host.synthesize(exact_request("late", "deferred.tts")).await,
            Err(SpeechHostError::AdmissionClosed)
        ));
    }

    #[tokio::test]
    async fn poisoned_finalization_fails_consumer_and_shutdown_without_hanging() {
        let host = Arc::new(SpeechHost::default());
        let backend = deferred_backend("poisoned-finalization.tts");
        host.register_backend(backend.clone())
            .expect("register deferred backend");
        let request_id = SpeechRequestId("poisoned-finalization".to_owned());
        let ticket = host
            .synthesize(exact_request(&request_id.0, "poisoned-finalization.tts"))
            .await
            .expect("start deferred request");
        host.lifecycle
            .operations
            .current_lease(&request_id.0)
            .expect("read production operation registry")
            .expect("operation is active")
            .poison_released_slot_for_test();

        let leader_host = Arc::clone(&host);
        let leader = tokio::spawn(async move { leader_host.shutdown().await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if host
                    .lifecycle
                    .state
                    .lock()
                    .expect("read shutdown state")
                    .shutdown_started
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("leader enters shutdown");
        let follower_host = Arc::clone(&host);
        let follower = tokio::spawn(async move { follower_host.shutdown().await });

        backend.complete(&request_id);
        let consumer_error =
            tokio::time::timeout(std::time::Duration::from_secs(1), ticket.final_response())
                .await
                .expect("consumer finalization must not hang")
                .expect_err("poisoned finalization must fail the consumer");
        assert_eq!(consumer_error.code, "speech_lifecycle_finalization_failed");

        let (leader_result, follower_result) =
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                tokio::join!(leader, follower)
            })
            .await
            .expect("leader and follower must not hang on poisoned finalization");
        assert_eq!(
            leader_result.expect("leader joins"),
            Err(SpeechHostError::StateUnavailable)
        );
        assert_eq!(
            follower_result.expect("follower joins"),
            Err(SpeechHostError::StateUnavailable)
        );
        assert_eq!(backend.shutdown_calls.load(Ordering::Acquire), 1);
    }

    #[cfg(feature = "unstable-w1-contract-tests")]
    #[test]
    fn w1_capability_envelope_projects_real_registered_descriptors() {
        let host = SpeechHost::default();
        host.register_backend(fixture_backend("fixture.tts"))
            .expect("register real host descriptor");

        let snapshot = host
            .w1_contract_adapter()
            .capability_snapshot()
            .expect("project canonical capability snapshot");
        platform_contract_testkit::validate_capability_snapshot_v0(&snapshot)
            .expect("canonical capability snapshot validates");
        let speech = snapshot.services.values().next().expect("speech service");
        assert!(
            speech
                .iter()
                .any(|entry| entry.backend_or_resource_id == "fixture.tts.synthesis")
        );
    }

    #[cfg(feature = "unstable-w1-contract-tests")]
    #[tokio::test]
    async fn w1_closed_summary_uses_real_registry_task_and_backend_shutdown_facts() {
        let host = SpeechHost::default();
        let healthy = fixture_backend("healthy.tts");
        let first_failure = failing_fixture_backend("failure-a.tts");
        let second_failure = failing_fixture_backend("failure-b.tts");
        for backend in [&healthy, &first_failure, &second_failure] {
            host.register_backend(backend.clone())
                .expect("register shutdown backend");
        }

        assert!(host.shutdown().await.is_err());
        let summary = host
            .w1_contract_adapter()
            .closed_summary()
            .expect("project retained real shutdown facts");
        summary
            .validate()
            .expect("canonical closed summary validates");
        assert_eq!(summary.active_operations, 0);
        assert_eq!(summary.retained_tasks, 0);
        assert_eq!(summary.failures.len(), 2);
        assert_eq!(
            summary
                .resources
                .iter()
                .filter(|resource| {
                    resource.kind == platform_contracts_v0::ShutdownResourceKind::Backend
                })
                .count(),
            3
        );
        assert_eq!(healthy.shutdown_calls.load(Ordering::Acquire), 1);
        assert_eq!(first_failure.shutdown_calls.load(Ordering::Acquire), 1);
        assert_eq!(second_failure.shutdown_calls.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn ten_thousand_fixture_operations_self_reap_task_state() {
        let host = SpeechHost::default();
        host.register_backend(fixture_backend("fixture.tts"))
            .expect("register fixture backend");

        for index in 0..10_000 {
            let ticket = host
                .synthesize(exact_request(
                    &format!("bounded-task-{index}"),
                    "fixture.tts",
                ))
                .await
                .expect("fixture request must be admitted");
            ticket
                .final_response()
                .await
                .expect("every fixture request has a final response");
        }

        host.lifecycle
            .tasks
            .wait_for_idle()
            .await
            .expect("task supervisor remains available");
        assert_eq!(
            host.lifecycle
                .operations
                .active_count()
                .expect("read active operations"),
            0
        );
        let task_state = host
            .lifecycle
            .tasks
            .snapshot()
            .expect("task supervisor remains available");
        assert_eq!(task_state.active, 0);
        assert_eq!(task_state.retained_failures, 0);
    }

    #[tokio::test]
    async fn monitor_panic_is_preserved_in_shutdown_evidence() {
        let host = SpeechHost::default();
        host.spawn_monitor("fixture-monitor-panic".to_owned(), async {
            panic!("fixture monitor panic")
        })
        .expect("spawn fixture monitor");

        let error = host
            .shutdown()
            .await
            .expect_err("monitor panic must fail shutdown");
        let SpeechHostError::Shutdown { failures } = error else {
            panic!("expected shutdown failure");
        };
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].code, "speech_host_monitor_failed");
        assert!(failures[0].safe_detail.contains("fixture monitor panic"));
    }

    #[test]
    fn host_nonce_exhaustion_fails_closed() {
        let host = SpeechHost::default();
        host.register_backend(fixture_backend("fixture.tts"))
            .expect("register fixture backend");
        host.lifecycle
            .operations
            .set_next_sequence_for_test(u64::MAX)
            .expect("set exhausted sequence");

        assert!(matches!(
            host.reserve_synthesis(&exact_request("nonce-exhausted", "fixture.tts")),
            Err(SpeechHostError::NonceExhausted)
        ));
        assert!(
            host.lifecycle
                .operations
                .active_count()
                .expect("read active operations")
                == 0
        );
    }

    #[test]
    fn setup_transition_failure_rolls_back_registry_and_route() {
        let host = SpeechHost::default();
        host.register_backend(fixture_backend("fixture.tts"))
            .expect("register fixture backend");
        let request_id = SpeechRequestId("setup-rollback".to_owned());
        let reserved = host
            .reserve_synthesis(&exact_request(&request_id.0, "fixture.tts"))
            .expect("reserve setup fixture");
        reserved.operation.queue().expect("queue once");
        let setup_error = reserved
            .operation
            .queue()
            .expect_err("duplicate queue fails");

        assert!(matches!(
            host.rollback_setup(&request_id, &reserved.operation, setup_error),
            SpeechHostError::StateUnavailable
        ));
        assert_eq!(
            host.lifecycle.operations.active_count(),
            Ok(0),
            "rollback releases the production registry"
        );
        assert!(
            host.lifecycle
                .state
                .lock()
                .expect("host state")
                .routes
                .is_empty(),
            "rollback removes the matching production route"
        );
        assert!(host.lifecycle.faulted.load(Ordering::Acquire));
    }

    #[test]
    fn mismatched_route_release_is_observable_and_preserves_route() {
        let host = SpeechHost::default();
        host.register_backend(fixture_backend("fixture.tts"))
            .expect("register fixture backend");
        let request_id = SpeechRequestId("route-mismatch".to_owned());
        let reserved = host
            .reserve_synthesis(&exact_request(&request_id.0, "fixture.tts"))
            .expect("reserve route fixture");
        let mut wrong = reserved.operation.identity();
        wrong.sequence = wrong
            .sequence
            .checked_add(1)
            .expect("fixture sequence room");

        assert_eq!(
            host.lifecycle.release_route(&request_id, &wrong),
            Err(SpeechHostError::StateUnavailable)
        );
        assert!(
            host.lifecycle
                .state
                .lock()
                .expect("host state")
                .routes
                .contains_key(&request_id)
        );
        assert!(host.lifecycle.faulted.load(Ordering::Acquire));
    }

    #[test]
    fn cancellation_rejects_route_registry_identity_mismatch() {
        let host = SpeechHost::default();
        let backend = deferred_backend("cancel-identity.tts");
        host.register_backend(backend.clone())
            .expect("register deferred backend");
        let request_id = SpeechRequestId("cancel-identity".to_owned());
        let reserved = host
            .reserve_synthesis(&exact_request(&request_id.0, "cancel-identity.tts"))
            .expect("reserve route fixture");
        {
            let mut state = host.lifecycle.state.lock().expect("host state");
            let route = state.routes.get_mut(&request_id).expect("active route");
            route.identity.sequence = route
                .identity
                .sequence
                .checked_add(1)
                .expect("fixture sequence room");
        }

        assert_eq!(host.cancel(&request_id), 0);
        assert_eq!(backend.cancel_calls.load(Ordering::Acquire), 0);
        assert!(host.lifecycle.faulted.load(Ordering::Acquire));
        drop(reserved);
    }
}
