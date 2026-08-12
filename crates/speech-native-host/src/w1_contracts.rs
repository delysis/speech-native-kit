//! Temporary W1 contract projections over real Speech host state.
//!
//! This module is feature-gated test evidence. It does not add a runtime API or
//! duplicate lifecycle registry.

use super::{HostPhase, SpeechHost, SpeechHostError};
use platform_contracts_v0::capability::CAPABILITY_SCHEMA_V0;
use platform_contracts_v0::error::SERVICE_ERROR_SCHEMA_V0;
use platform_contracts_v0::shutdown::CLOSED_SUMMARY_SCHEMA_V0;
use platform_contracts_v0::{
    CapabilityEntryV0, CapabilitySnapshotV0, CapabilitySourceReportV0, ClosedSummaryV0,
    ContentDigest, ErrorClass, Readiness, RetryAdvice, ServiceErrorV0, ServiceId,
    ShutdownFailureV0, ShutdownResourceKind, ShutdownResourceState, ShutdownResourceV0,
    SupervisorPhase, TriState,
};
use sha2::{Digest, Sha256};
use speech_native_types::{
    CapabilityAvailability, NetworkBehavior, SpeechBackendReadiness, SpeechCapability,
    SpeechCapabilityLimits, SpeechError, SpeechErrorClass, SpeechOperationCapability,
};
use std::collections::BTreeMap;

/// Feature-gated test adapter over a real `SpeechHost` lifecycle.
pub struct SpeechW1ContractAdapter<'a> {
    host: &'a SpeechHost,
}

/// Feature-gated closed-host facts that retain exact host worker identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeechW1ClosedFacts {
    pub summary: ClosedSummaryV0,
    pub host_expected_worker_ids: Vec<String>,
    pub host_joined_worker_ids: Vec<String>,
}

impl<'a> SpeechW1ContractAdapter<'a> {
    #[must_use]
    pub const fn new(host: &'a SpeechHost) -> Self {
        Self { host }
    }

    pub fn capability_snapshot(&self) -> Result<CapabilitySnapshotV0, SpeechHostError> {
        self.host.w1_capability_snapshot()
    }

    pub fn closed_summary(&self) -> Result<ClosedSummaryV0, SpeechHostError> {
        self.host.w1_closed_summary()
    }

    pub fn closed_facts(&self) -> Result<SpeechW1ClosedFacts, SpeechHostError> {
        self.host.w1_closed_facts()
    }
}

impl SpeechHost {
    #[must_use]
    pub const fn w1_contract_adapter(&self) -> SpeechW1ContractAdapter<'_> {
        SpeechW1ContractAdapter::new(self)
    }

    /// Project real registered-backend discovery into the canonical v0 envelope.
    pub fn w1_capability_snapshot(&self) -> Result<CapabilitySnapshotV0, SpeechHostError> {
        let source = self.snapshot()?;
        let encoded = serde_json::to_vec(&source).map_err(|_| SpeechHostError::StateUnavailable)?;
        let digest = ContentDigest::sha256(format!("{:x}", Sha256::digest(encoded)))
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        let mut entries = Vec::new();
        for report in &source.source_reports {
            for backend in &report.backends {
                if backend.capabilities.is_empty() {
                    entries.push(readiness_entry(backend, source.captured_at_unix_ms));
                } else {
                    entries.extend(backend.capabilities.iter().map(|capability| {
                        capability_entry(backend, capability, source.captured_at_unix_ms)
                    }));
                }
            }
        }
        let services = BTreeMap::from([(
            ServiceId::new("speech").map_err(|_| SpeechHostError::StateUnavailable)?,
            entries,
        )]);
        let reports = source
            .source_reports
            .iter()
            .map(|report| CapabilitySourceReportV0 {
                source_id: report.source_id.clone(),
                outcome: format!("{:?}", report.status).to_ascii_lowercase(),
                safe_detail: report.detail.clone(),
            })
            .collect();
        let snapshot = CapabilitySnapshotV0 {
            schema: CAPABILITY_SCHEMA_V0.to_owned(),
            snapshot_id: digest,
            target: format!(
                "speech:{:?}:{}",
                source.target.family, source.target.architecture
            )
            .to_ascii_lowercase(),
            services,
            reports,
        };
        snapshot
            .validate()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        Ok(snapshot)
    }

    /// Project the retained real shutdown result and task counters into v0.
    ///
    /// Backend resources prove that shutdown was attempted and returned. Their
    /// internal native worker joins remain backend-owned evidence because the
    /// host has no access to those private worker registries.
    pub fn w1_closed_summary(&self) -> Result<ClosedSummaryV0, SpeechHostError> {
        let state = self
            .lifecycle
            .state
            .lock()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        let active_operations = self
            .lifecycle
            .operations
            .active_count()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        if state.phase != HostPhase::Closed
            || active_operations != 0
            || !state.routes.is_empty()
            || self
                .lifecycle
                .faulted
                .load(std::sync::atomic::Ordering::Acquire)
            || self.lifecycle.operations.diagnostic_faulted()
        {
            return Err(SpeechHostError::StateUnavailable);
        }
        let facts = state
            .shutdown_facts
            .as_ref()
            .ok_or(SpeechHostError::StateUnavailable)?;
        let mut resources = Vec::new();
        let monitor_failed = facts.monitor_failure.is_some();
        if !facts.tasks.expected_worker_ids.is_empty() {
            resources.push(ShutdownResourceV0 {
                resource_id: "speech.host.final-relays".to_owned(),
                service: service_id("speech-host")?,
                kind: ShutdownResourceKind::TaskSupervisor,
                state: if monitor_failed {
                    ShutdownResourceState::Failed
                } else {
                    ShutdownResourceState::Stopped
                },
                expected_workers: facts.tasks.expected_worker_ids.len(),
                joined_workers: facts.tasks.joined_worker_ids.len(),
            });
        }

        let mut failures = Vec::new();
        if let Some(summary) = &facts.monitor_failure {
            failures.push(shutdown_failure(
                "speech.host.monitor.failure",
                "speech.host.final-relays",
                "speech-host",
                &SpeechError::unavailable(
                    &speech_native_types::SpeechRequestId("speech-host-monitor".to_owned()),
                    "speech_host_monitor_failed",
                    &format!(
                        "monitor '{}' failed; {} additional failure(s)",
                        summary.first.label, summary.additional_failures
                    ),
                ),
            )?);
        }
        for (index, backend) in facts.backends.iter().enumerate() {
            let resource_id = format!(
                "speech.backend.{index}.{}",
                opaque_component(&backend.backend_id)
            );
            let failed = backend.error.is_some();
            resources.push(ShutdownResourceV0 {
                resource_id: resource_id.clone(),
                service: service_id("speech-backend")?,
                kind: ShutdownResourceKind::Backend,
                state: if failed {
                    ShutdownResourceState::Failed
                } else {
                    ShutdownResourceState::Stopped
                },
                // The host observed the backend shutdown call return, but the
                // generic backend trait exposes no private worker identities.
                // Do not turn one awaited future into invented worker facts.
                expected_workers: 0,
                joined_workers: 0,
            });
            if let Some(error) = &backend.error {
                failures.push(shutdown_failure(
                    &format!("speech.backend.{index}.shutdown"),
                    &resource_id,
                    "speech-backend",
                    error,
                )?);
            }
        }
        let expected_workers = resources
            .iter()
            .map(|resource| resource.expected_workers)
            .sum();
        let joined_workers = resources
            .iter()
            .map(|resource| resource.joined_workers)
            .sum();
        let summary = ClosedSummaryV0 {
            schema: CLOSED_SUMMARY_SCHEMA_V0.to_owned(),
            phase: SupervisorPhase::Closed,
            active_operations,
            retained_tasks: facts.tasks.active,
            expected_workers,
            joined_workers,
            resources,
            failures,
        };
        summary
            .validate()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        Ok(summary)
    }

    /// Retain the exact host final-relay IDs alongside the canonical summary.
    pub fn w1_closed_facts(&self) -> Result<SpeechW1ClosedFacts, SpeechHostError> {
        let summary = self.w1_closed_summary()?;
        let state = self
            .lifecycle
            .state
            .lock()
            .map_err(|_| SpeechHostError::StateUnavailable)?;
        let tasks = &state
            .shutdown_facts
            .as_ref()
            .ok_or(SpeechHostError::StateUnavailable)?
            .tasks;
        if tasks.expected_worker_ids.len() != summary.expected_workers
            || tasks.joined_worker_ids.len() != summary.joined_workers
        {
            return Err(SpeechHostError::StateUnavailable);
        }
        Ok(SpeechW1ClosedFacts {
            summary,
            host_expected_worker_ids: tasks.expected_worker_ids.clone(),
            host_joined_worker_ids: tasks.joined_worker_ids.clone(),
        })
    }
}

fn readiness_entry(
    backend: &speech_native_types::SpeechBackendDescriptor,
    observed_at_unix_ms: u64,
) -> CapabilityEntryV0 {
    let (readiness, remediation) = readiness(&backend.readiness);
    CapabilityEntryV0 {
        operation: "backend_readiness".to_owned(),
        backend_or_resource_id: backend.id.clone(),
        readiness,
        limits: BTreeMap::new(),
        network: TriState::Unknown,
        privacy_eligible: TriState::Unknown,
        evidence_source: "speech backend descriptor".to_owned(),
        evidence_outcome: format!("{readiness:?}").to_ascii_lowercase(),
        observed_at_unix_ms: Some(observed_at_unix_ms),
        remediation,
    }
}

fn capability_entry(
    backend: &speech_native_types::SpeechBackendDescriptor,
    capability: &SpeechCapability,
    observed_at_unix_ms: u64,
) -> CapabilityEntryV0 {
    let (readiness, remediation) = capability_readiness(backend, capability);
    CapabilityEntryV0 {
        operation: match &capability.operation {
            SpeechOperationCapability::Transcription(_) => "transcription",
            SpeechOperationCapability::Synthesis(_) => "synthesis",
        }
        .to_owned(),
        backend_or_resource_id: capability.id.clone(),
        readiness,
        limits: limits(&capability.limits),
        network: tri_state(capability.network),
        privacy_eligible: match capability.network {
            NetworkBehavior::Never => TriState::Yes,
            NetworkBehavior::Unknown => TriState::Unknown,
            NetworkBehavior::Optional | NetworkBehavior::Required => TriState::No,
        },
        evidence_source: capability.evidence.first().map_or_else(
            || "speech capability descriptor".to_owned(),
            |item| item.source_id.clone(),
        ),
        evidence_outcome: format!("{:?}", capability.availability).to_ascii_lowercase(),
        observed_at_unix_ms: Some(observed_at_unix_ms),
        remediation,
    }
}

fn readiness(readiness: &SpeechBackendReadiness) -> (Readiness, Option<String>) {
    match readiness {
        SpeechBackendReadiness::Ready => (Readiness::Ready, None),
        SpeechBackendReadiness::Unknown { reason } => (Readiness::Unknown, Some(reason.clone())),
        SpeechBackendReadiness::AssetInstallRequired { .. } => (
            Readiness::Unavailable,
            Some("install the declared speech assets".to_owned()),
        ),
        SpeechBackendReadiness::PermissionRequired { .. } => (
            Readiness::Unavailable,
            Some("grant the declared operating-system permission".to_owned()),
        ),
        SpeechBackendReadiness::NotConfigured { reason }
        | SpeechBackendReadiness::Unavailable { reason } => {
            (Readiness::Unavailable, Some(reason.clone()))
        }
    }
}

fn capability_readiness(
    backend: &speech_native_types::SpeechBackendDescriptor,
    capability: &SpeechCapability,
) -> (Readiness, Option<String>) {
    if !backend.readiness.is_ready() {
        return readiness(&backend.readiness);
    }
    match capability.availability {
        CapabilityAvailability::Available => (Readiness::Ready, None),
        CapabilityAvailability::Unknown => (
            Readiness::Unknown,
            Some("repeat the runtime capability probe".to_owned()),
        ),
        CapabilityAvailability::AssetInstallRequired => (
            Readiness::Unavailable,
            Some("install the capability assets".to_owned()),
        ),
        CapabilityAvailability::PermissionRequired => (
            Readiness::Unavailable,
            Some("grant the capability permission".to_owned()),
        ),
        CapabilityAvailability::Unavailable => (
            Readiness::Unavailable,
            Some("select another speech capability".to_owned()),
        ),
    }
}

fn limits(source: &SpeechCapabilityLimits) -> BTreeMap<String, u64> {
    let mut limits = BTreeMap::new();
    for (name, value) in [
        ("max_audio_ms", source.max_audio_ms),
        ("max_input_characters", source.max_input_characters),
        (
            "max_concurrent_requests",
            source.max_concurrent_requests.map(u64::from),
        ),
        ("max_speakers", source.max_speakers.map(u64::from)),
    ] {
        if let Some(value) = value {
            limits.insert(name.to_owned(), value);
        }
    }
    limits
}

const fn tri_state(network: NetworkBehavior) -> TriState {
    match network {
        NetworkBehavior::Never => TriState::No,
        NetworkBehavior::Optional | NetworkBehavior::Required => TriState::Yes,
        NetworkBehavior::Unknown => TriState::Unknown,
    }
}

fn shutdown_failure(
    failure_id: &str,
    resource_id: &str,
    service: &str,
    error: &SpeechError,
) -> Result<ShutdownFailureV0, SpeechHostError> {
    let service = service_id(service)?;
    Ok(ShutdownFailureV0 {
        failure_id: failure_id.to_owned(),
        resource_id: resource_id.to_owned(),
        service: service.clone(),
        error: ServiceErrorV0 {
            schema: SERVICE_ERROR_SCHEMA_V0.to_owned(),
            code: format!("speech.{}", error.code),
            class: error_class(error.class),
            retry: if error.retryable {
                RetryAdvice::AfterRestart
            } else {
                RetryAdvice::Never
            },
            operation_id: None,
            service,
            safe_detail: error.safe_detail.clone(),
        },
    })
}

const fn error_class(class: SpeechErrorClass) -> ErrorClass {
    match class {
        SpeechErrorClass::InvalidRequest => ErrorClass::InvalidRequest,
        SpeechErrorClass::Capability => ErrorClass::Unsupported,
        SpeechErrorClass::Privacy => ErrorClass::Privacy,
        SpeechErrorClass::Permission => ErrorClass::Permission,
        SpeechErrorClass::AssetMissing | SpeechErrorClass::Unavailable => ErrorClass::Unavailable,
        SpeechErrorClass::Timeout => ErrorClass::Timeout,
        SpeechErrorClass::Cancelled => ErrorClass::Cancelled,
        SpeechErrorClass::Internal => ErrorClass::Internal,
    }
}

fn service_id(value: &str) -> Result<ServiceId, SpeechHostError> {
    ServiceId::new(value).map_err(|_| SpeechHostError::StateUnavailable)
}

fn opaque_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect()
}
