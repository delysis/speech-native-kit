//! Conservative platform speech discovery.
//!
//! This crate describes adapters worth probing on a target platform and
//! aggregates evidence from native bridges. A compile target is only a
//! candidate: it never proves that an API is installed, permitted, on-device,
//! or safe for a local-only route.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use speech_native_types::{
    ApplicationIdentity, CapabilitySourceReport, EvidenceKind, PlatformAdapterCandidate,
    PlatformCapabilitySnapshot, PlatformFamily, PlatformTarget, ProbeSourceStatus,
    SPEECH_CAPABILITY_SCHEMA, SpeechBackendDescriptor, SpeechOperationKind,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::timeout;

#[cfg(target_os = "macos")]
pub mod apple;
#[cfg(target_os = "macos")]
pub mod apple_backend;

const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(3);
pub const SPEECH_CAPABILITY_REPORT_SCHEMA: &str = "fte.speech.capability_report.v1";

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum PlatformProbeError {
    #[error("capability source id is invalid: {0}")]
    SourceIdInvalid(String),
    #[error("capability source id is already registered: {0}")]
    SourceIdDuplicate(String),
    #[error("platform capability probe failed: {0}")]
    SourceFailed(String),
    #[error("capability report schema is unsupported: {0}")]
    SchemaUnsupported(String),
    #[error("capability report JSON is invalid: {0}")]
    JsonInvalid(String),
}

#[async_trait]
pub trait PlatformCapabilitySource: Send + Sync {
    fn source_id(&self) -> &str;

    async fn probe(
        &self,
        target: &PlatformTarget,
    ) -> Result<Vec<SpeechBackendDescriptor>, PlatformProbeError>;
}

/// A source used by native Swift, Kotlin, Windows, Linux, or embedded-model
/// bridges after they have performed their runtime checks.
#[derive(Debug, Clone)]
pub struct ReportedCapabilitySource {
    source_id: String,
    backends: Vec<SpeechBackendDescriptor>,
}

/// Versioned interchange payload for platform-native bridges. It is suitable
/// for a Tauri mobile plugin boundary or a narrow Swift/WinRT/Kotlin FFI shim.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityReportPayload {
    pub schema: String,
    pub source_id: String,
    #[serde(default)]
    pub backends: Vec<SpeechBackendDescriptor>,
}

impl CapabilityReportPayload {
    pub fn into_source(self) -> Result<ReportedCapabilitySource, PlatformProbeError> {
        if self.schema != SPEECH_CAPABILITY_REPORT_SCHEMA {
            return Err(PlatformProbeError::SchemaUnsupported(self.schema));
        }
        ReportedCapabilitySource::new(self.source_id, self.backends)
    }
}

impl ReportedCapabilitySource {
    pub fn new(
        source_id: impl Into<String>,
        backends: Vec<SpeechBackendDescriptor>,
    ) -> Result<Self, PlatformProbeError> {
        let source_id = source_id.into();
        validate_source_id(&source_id)?;
        Ok(Self {
            source_id,
            backends,
        })
    }

    pub fn from_json(json: &str) -> Result<Self, PlatformProbeError> {
        serde_json::from_str::<CapabilityReportPayload>(json)
            .map_err(|error| PlatformProbeError::JsonInvalid(error.to_string()))?
            .into_source()
    }
}

#[async_trait]
impl PlatformCapabilitySource for ReportedCapabilitySource {
    fn source_id(&self) -> &str {
        &self.source_id
    }

    async fn probe(
        &self,
        _target: &PlatformTarget,
    ) -> Result<Vec<SpeechBackendDescriptor>, PlatformProbeError> {
        Ok(self.backends.clone())
    }
}

pub struct PlatformCapabilityProbe {
    target: PlatformTarget,
    source_timeout: Duration,
    sources: Vec<Arc<dyn PlatformCapabilitySource>>,
    source_ids: HashSet<String>,
}

impl PlatformCapabilityProbe {
    #[must_use]
    pub fn new(target: PlatformTarget) -> Self {
        Self {
            target,
            source_timeout: DEFAULT_PROBE_TIMEOUT,
            sources: Vec::new(),
            source_ids: HashSet::new(),
        }
    }

    #[must_use]
    pub fn current() -> Self {
        Self::new(PlatformTarget::current())
    }

    #[must_use]
    pub fn with_source_timeout(mut self, source_timeout: Duration) -> Self {
        self.source_timeout = source_timeout;
        self
    }

    pub fn register(
        &mut self,
        source: Arc<dyn PlatformCapabilitySource>,
    ) -> Result<(), PlatformProbeError> {
        let source_id = source.source_id().to_string();
        validate_source_id(&source_id)?;
        if !self.source_ids.insert(source_id.clone()) {
            return Err(PlatformProbeError::SourceIdDuplicate(source_id));
        }
        self.sources.push(source);
        Ok(())
    }

    pub async fn probe(&self) -> PlatformCapabilitySnapshot {
        let mut tasks = JoinSet::new();
        for source in &self.sources {
            let source = Arc::clone(source);
            let source_id = source.source_id().to_string();
            let target = self.target.clone();
            let source_timeout = self.source_timeout;
            tasks.spawn(async move {
                let outcome = timeout(source_timeout, source.probe(&target)).await;
                source_report(source_id, outcome)
            });
        }

        let mut source_reports = Vec::with_capacity(self.sources.len());
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(report) => source_reports.push(report),
                Err(error) => source_reports.push(CapabilitySourceReport {
                    source_id: "probe-task".to_string(),
                    status: ProbeSourceStatus::Failed,
                    detail: Some(format!("capability probe task failed: {error}")),
                    backends: Vec::new(),
                }),
            }
        }
        source_reports.sort_by(|left, right| left.source_id.cmp(&right.source_id));

        PlatformCapabilitySnapshot {
            schema: SPEECH_CAPABILITY_SCHEMA.to_string(),
            captured_at_unix_ms: unix_time_ms(),
            target: self.target.clone(),
            adapter_candidates: adapter_candidates(&self.target),
            source_reports,
        }
    }
}

fn source_report(
    source_id: String,
    outcome: Result<
        Result<Vec<SpeechBackendDescriptor>, PlatformProbeError>,
        tokio::time::error::Elapsed,
    >,
) -> CapabilitySourceReport {
    match outcome {
        Ok(Ok(mut backends)) => match validate_report(&source_id, &backends) {
            Ok(()) => {
                sort_backends(&mut backends);
                CapabilitySourceReport {
                    source_id,
                    status: ProbeSourceStatus::Succeeded,
                    detail: None,
                    backends,
                }
            }
            Err(detail) => CapabilitySourceReport {
                source_id,
                status: ProbeSourceStatus::Failed,
                detail: Some(detail),
                backends: Vec::new(),
            },
        },
        Ok(Err(error)) => CapabilitySourceReport {
            source_id,
            status: ProbeSourceStatus::Failed,
            detail: Some(error.to_string()),
            backends: Vec::new(),
        },
        Err(_) => CapabilitySourceReport {
            source_id,
            status: ProbeSourceStatus::TimedOut,
            detail: Some("capability source exceeded its probe deadline".to_string()),
            backends: Vec::new(),
        },
    }
}

fn validate_report(source_id: &str, backends: &[SpeechBackendDescriptor]) -> Result<(), String> {
    let mut backend_ids = HashSet::new();
    let mut capability_ids = HashSet::new();
    for backend in backends {
        backend
            .validate()
            .map_err(|error| format!("invalid capability report: {error}"))?;
        if !backend_ids.insert(backend.id.as_str()) {
            return Err(format!(
                "duplicate backend id in capability report: {}",
                backend.id
            ));
        }
        for capability in &backend.capabilities {
            if !capability_ids.insert(capability.id.as_str()) {
                return Err(format!(
                    "duplicate capability id in capability report: {}",
                    capability.id
                ));
            }
            for evidence in &capability.evidence {
                if matches!(
                    evidence.kind,
                    EvidenceKind::RuntimeApi
                        | EvidenceKind::RealSmoke
                        | EvidenceKind::SystemInventory
                        | EvidenceKind::UserConfiguration
                ) && evidence.source_id != source_id
                {
                    return Err(format!(
                        "runtime evidence for {} must be owned by source {source_id}",
                        capability.id
                    ));
                }
            }
        }
    }
    Ok(())
}

fn sort_backends(backends: &mut [SpeechBackendDescriptor]) {
    for backend in backends.iter_mut() {
        backend
            .capabilities
            .sort_by(|left, right| left.id.cmp(&right.id));
        backend.models.sort_by(|left, right| left.id.cmp(&right.id));
        backend.voices.sort_by(|left, right| left.id.cmp(&right.id));
    }
    backends.sort_by(|left, right| left.id.cmp(&right.id));
}

#[must_use]
pub fn adapter_candidates(target: &PlatformTarget) -> Vec<PlatformAdapterCandidate> {
    let mut candidates = match target.family {
        PlatformFamily::MacOs | PlatformFamily::Ios => apple_candidates(),
        PlatformFamily::Windows => windows_candidates(target.application_identity),
        PlatformFamily::Android => android_candidates(),
        PlatformFamily::Linux => linux_candidates(),
        PlatformFamily::Other(_) => Vec::new(),
    };
    candidates.extend(embedded_candidates());
    candidates.sort_by(|left, right| left.id.cmp(&right.id));
    candidates
}

fn apple_candidates() -> Vec<PlatformAdapterCandidate> {
    vec![
        candidate(
            "apple.av-speech",
            "Apple system voices",
            &[SpeechOperationKind::Synthesis],
            "Each installed voice must be inventoried at runtime; voice quality and network behavior are not inferred from the OS name.",
        ),
        candidate(
            "apple.sf-speech",
            "Apple speech recognition",
            &[SpeechOperationKind::Transcription],
            "Eligible for local-only routing only when the recognizer reports on-device support and the request requires it.",
        ),
        candidate(
            "apple.speech-analyzer",
            "Apple SpeechAnalyzer",
            &[SpeechOperationKind::Transcription],
            "Eligible only after the required system asset and on-device runtime path are confirmed.",
        ),
    ]
}

fn windows_candidates(identity: ApplicationIdentity) -> Vec<PlatformAdapterCandidate> {
    let recognition_note = match identity {
        ApplicationIdentity::Packaged => {
            "Windows free-form dictation may use an online service; package identity alone never qualifies it as local-only."
        }
        ApplicationIdentity::Unpackaged | ApplicationIdentity::Unknown => {
            "Free-form Windows dictation may require package identity and an online service; use embedded recognition for private defaults."
        }
    };
    vec![
        candidate(
            "windows.speech-recognition",
            "Windows speech recognition",
            &[SpeechOperationKind::Transcription],
            recognition_note,
        ),
        candidate(
            "windows.speech-synthesis",
            "Windows installed voices",
            &[SpeechOperationKind::Synthesis],
            "Installed voices and returned-audio support must be confirmed through the runtime API.",
        ),
    ]
}

fn android_candidates() -> Vec<PlatformAdapterCandidate> {
    vec![
        candidate(
            "android.on-device-recognizer",
            "Android on-device speech recognizer",
            &[SpeechOperationKind::Transcription],
            "Only the explicit on-device recognizer with runtime availability evidence qualifies as local-only.",
        ),
        candidate(
            "android.tts",
            "Android installed voices",
            &[SpeechOperationKind::Synthesis],
            "Voices that report a required network connection are excluded from local-only routing.",
        ),
    ]
}

fn linux_candidates() -> Vec<PlatformAdapterCandidate> {
    vec![candidate(
        "linux.spiel",
        "Linux Spiel speech service",
        &[SpeechOperationKind::Synthesis],
        "Provider availability, installed voices, returned audio, and network behavior must be discovered at runtime.",
    )]
}

fn embedded_candidates() -> Vec<PlatformAdapterCandidate> {
    vec![
        candidate(
            "embedded.parakeet-asr",
            "Bundled Parakeet transcription",
            &[SpeechOperationKind::Transcription],
            "Private fallback when model assets are installed and an in-process real smoke succeeds.",
        ),
        candidate(
            "embedded.kokoro-tts",
            "Bundled Kokoro synthesis",
            &[SpeechOperationKind::Synthesis],
            "Private fallback when model assets are installed and an in-process real smoke succeeds.",
        ),
        candidate(
            "resident.gemma4-audio",
            "Resident Gemma 4 audio transcription",
            &[SpeechOperationKind::Transcription],
            "Fallback for complete audio transcription or understanding only; streaming, timestamps, and diarization require separate proof.",
        ),
    ]
}

fn candidate(
    id: &str,
    display_name: &str,
    operations: &[SpeechOperationKind],
    privacy_note: &str,
) -> PlatformAdapterCandidate {
    PlatformAdapterCandidate {
        id: id.to_string(),
        display_name: display_name.to_string(),
        operations: operations.to_vec(),
        requires_runtime_probe: true,
        privacy_note: privacy_note.to_string(),
    }
}

fn validate_source_id(source_id: &str) -> Result<(), PlatformProbeError> {
    let valid = !source_id.is_empty()
        && source_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-' | b'_')
        });
    if valid {
        Ok(())
    } else {
        Err(PlatformProbeError::SourceIdInvalid(source_id.to_string()))
    }
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
    use speech_native_types::{
        CapabilityAvailability, CapabilityEvidence, EvidenceOutcome, NetworkBehavior,
        SpeechBackendKind, SpeechBackendReadiness, SpeechCapability, SpeechCapabilityLimits,
        SpeechOperationCapability, TranscriptionCapabilities,
    };

    fn target(family: PlatformFamily) -> PlatformTarget {
        PlatformTarget {
            family,
            os_version: Some("fixture".to_string()),
            architecture: "fixture-arch".to_string(),
            application_identity: ApplicationIdentity::Unknown,
        }
    }

    fn backend(
        source_id: &str,
        backend_id: &str,
        network: NetworkBehavior,
        readiness: SpeechBackendReadiness,
    ) -> SpeechBackendDescriptor {
        SpeechBackendDescriptor {
            id: backend_id.to_string(),
            display_name: backend_id.to_string(),
            kind: SpeechBackendKind::PlatformOnDevice,
            readiness,
            capabilities: vec![SpeechCapability {
                id: format!("{backend_id}.transcription"),
                backend_id: backend_id.to_string(),
                model_id: None,
                operation: SpeechOperationCapability::Transcription(
                    TranscriptionCapabilities::default(),
                ),
                availability: CapabilityAvailability::Available,
                network,
                languages: vec!["en-US".to_string()],
                limits: SpeechCapabilityLimits::default(),
                evidence: vec![CapabilityEvidence {
                    source_id: source_id.to_string(),
                    source_version: Some("fixture".to_string()),
                    kind: EvidenceKind::RuntimeApi,
                    outcome: EvidenceOutcome::Confirmed,
                    observed_at_unix_ms: 1,
                    detail: "runtime fixture".to_string(),
                }],
            }],
            models: Vec::new(),
            voices: Vec::new(),
        }
    }

    #[tokio::test]
    async fn platform_candidates_never_claim_local_runtime_availability() {
        let snapshot = PlatformCapabilityProbe::new(target(PlatformFamily::MacOs))
            .probe()
            .await;
        assert!(!snapshot.adapter_candidates.is_empty());
        assert!(snapshot.local_only_capabilities().is_empty());
    }

    #[tokio::test]
    async fn confirmed_ready_on_device_runtime_is_locally_eligible() {
        let source = ReportedCapabilitySource::new(
            "apple-runtime",
            vec![backend(
                "apple-runtime",
                "apple.speech-analyzer",
                NetworkBehavior::Never,
                SpeechBackendReadiness::Ready,
            )],
        )
        .expect("valid source");
        let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::MacOs));
        probe.register(Arc::new(source)).expect("register source");
        let snapshot = probe.probe().await;
        assert_eq!(snapshot.local_only_capabilities().len(), 1);
    }

    #[tokio::test]
    async fn online_or_unknown_platform_services_never_enter_local_only_routes() {
        for network in [NetworkBehavior::Required, NetworkBehavior::Unknown] {
            let source = ReportedCapabilitySource::new(
                "platform-runtime",
                vec![backend(
                    "platform-runtime",
                    "platform.recognizer",
                    network,
                    SpeechBackendReadiness::Ready,
                )],
            )
            .expect("valid source");
            let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::Windows));
            probe.register(Arc::new(source)).expect("register source");
            assert!(probe.probe().await.local_only_capabilities().is_empty());
        }
    }

    #[tokio::test]
    async fn permission_or_asset_requirements_are_not_ready_local_routes() {
        for readiness in [
            SpeechBackendReadiness::PermissionRequired {
                permissions: Vec::new(),
            },
            SpeechBackendReadiness::AssetInstallRequired { assets: Vec::new() },
        ] {
            let source = ReportedCapabilitySource::new(
                "native-runtime",
                vec![backend(
                    "native-runtime",
                    "native.recognizer",
                    NetworkBehavior::Never,
                    readiness,
                )],
            )
            .expect("valid source");
            let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::Android));
            probe.register(Arc::new(source)).expect("register source");
            assert!(probe.probe().await.local_only_capabilities().is_empty());
        }
    }

    struct NeverReturns;

    #[async_trait]
    impl PlatformCapabilitySource for NeverReturns {
        fn source_id(&self) -> &str {
            "never-returns"
        }

        async fn probe(
            &self,
            _target: &PlatformTarget,
        ) -> Result<Vec<SpeechBackendDescriptor>, PlatformProbeError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn timed_out_source_does_not_erase_successful_sources() {
        let source = ReportedCapabilitySource::new(
            "working-runtime",
            vec![backend(
                "working-runtime",
                "working.recognizer",
                NetworkBehavior::Never,
                SpeechBackendReadiness::Ready,
            )],
        )
        .expect("valid source");
        let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::Linux))
            .with_source_timeout(Duration::from_millis(10));
        probe
            .register(Arc::new(NeverReturns))
            .expect("register source");
        probe.register(Arc::new(source)).expect("register source");
        let snapshot = probe.probe().await;
        assert_eq!(snapshot.local_only_capabilities().len(), 1);
        assert!(snapshot.source_reports.iter().any(|report| {
            report.source_id == "never-returns" && report.status == ProbeSourceStatus::TimedOut
        }));
    }

    #[tokio::test]
    async fn malformed_descriptor_fails_closed() {
        let source = ReportedCapabilitySource::new(
            "native-runtime",
            vec![backend(
                "different-source",
                "native.recognizer",
                NetworkBehavior::Never,
                SpeechBackendReadiness::Ready,
            )],
        )
        .expect("valid source");
        let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::Android));
        probe.register(Arc::new(source)).expect("register source");
        let snapshot = probe.probe().await;
        assert!(snapshot.local_only_capabilities().is_empty());
        assert_eq!(snapshot.source_reports[0].status, ProbeSourceStatus::Failed);
        assert!(snapshot.source_reports[0].backends.is_empty());
    }

    #[test]
    fn duplicate_source_ids_are_rejected() {
        let first =
            ReportedCapabilitySource::new("same-source", Vec::new()).expect("valid first source");
        let second =
            ReportedCapabilitySource::new("same-source", Vec::new()).expect("valid second source");
        let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::Linux));
        probe.register(Arc::new(first)).expect("register first");
        let error = probe
            .register(Arc::new(second))
            .expect_err("duplicate must fail");
        assert_eq!(
            error,
            PlatformProbeError::SourceIdDuplicate("same-source".to_string())
        );
    }

    #[tokio::test]
    async fn snapshot_is_serializable_and_deterministically_ordered() {
        let mut probe = PlatformCapabilityProbe::new(target(PlatformFamily::Linux));
        for source_id in ["z-source", "a-source"] {
            let source =
                ReportedCapabilitySource::new(source_id, Vec::new()).expect("valid source");
            probe.register(Arc::new(source)).expect("register source");
        }
        let snapshot = probe.probe().await;
        assert_eq!(snapshot.source_reports[0].source_id, "a-source");
        assert_eq!(snapshot.source_reports[1].source_id, "z-source");
        serde_json::to_string(&snapshot).expect("serialize capability snapshot");
    }

    #[test]
    fn native_bridge_payload_is_versioned_and_validated() {
        let payload = CapabilityReportPayload {
            schema: SPEECH_CAPABILITY_REPORT_SCHEMA.to_string(),
            source_id: "apple-runtime".to_string(),
            backends: vec![backend(
                "apple-runtime",
                "apple.speech-analyzer",
                NetworkBehavior::Never,
                SpeechBackendReadiness::Ready,
            )],
        };
        let json = serde_json::to_string(&payload).expect("serialize report payload");
        let source = ReportedCapabilitySource::from_json(&json).expect("parse report payload");
        assert_eq!(source.source_id(), "apple-runtime");

        let unsupported = serde_json::json!({
            "schema": "fte.speech.capability_report.v999",
            "source_id": "apple-runtime",
            "backends": []
        })
        .to_string();
        assert!(matches!(
            ReportedCapabilitySource::from_json(&unsupported),
            Err(PlatformProbeError::SchemaUnsupported(_))
        ));
    }
}
