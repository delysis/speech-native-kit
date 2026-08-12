//! Generation-safe ownership for public speech operations and backend attempts.
//!
//! This is the host's production operation registry. It owns public identity,
//! transition authority, cancellation, terminal linearization, bounded progress,
//! attempt identity, and executor release. Backend routing attachments remain in
//! the host because they are capabilities, not lifecycle state.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationIdentity {
    pub operation_id: String,
    pub attempt_id: String,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPhase {
    Reserved,
    Queued,
    Running,
    Terminal,
    Released,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalClass {
    Completed,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationSnapshot {
    pub identity: OperationIdentity,
    pub phase: OperationPhase,
    pub cancellation_requested: bool,
    pub terminal: Option<TerminalClass>,
    pub progress: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RegistryError {
    #[error("operation id is already active")]
    Duplicate,
    #[error("operation identity sequence is exhausted")]
    Exhausted,
    #[error("operation lease is stale")]
    Stale,
    #[error("operation transition is invalid")]
    InvalidTransition,
    #[error("operation registry state is unavailable")]
    StateUnavailable,
}

#[derive(Clone)]
pub struct OperationRegistry {
    inner: Arc<Mutex<RegistryState>>,
    faulted: Arc<AtomicBool>,
    progress_capacity: usize,
}

struct RegistryState {
    next_sequence: u64,
    operations: BTreeMap<String, OperationRecord>,
}

struct OperationRecord {
    identity: OperationIdentity,
    phase: OperationPhase,
    cancellation_requested: bool,
    terminal: Option<TerminalClass>,
    progress: VecDeque<u64>,
    progress_capacity: usize,
    next_attempt_sequence: u64,
    attempts: BTreeMap<u64, OperationIdentity>,
    released: Arc<Mutex<Option<OperationSnapshot>>>,
}

#[derive(Clone)]
pub struct OperationLease {
    registry: OperationRegistry,
    identity: OperationIdentity,
    released: Arc<Mutex<Option<OperationSnapshot>>>,
}

pub struct ConsumerGuard {
    lease: OperationLease,
    cancel_on_drop: bool,
}

#[derive(Clone)]
pub struct AttemptLease {
    registry: OperationRegistry,
    operation: OperationIdentity,
    identity: OperationIdentity,
}

impl OperationRegistry {
    #[must_use]
    pub fn new(next_sequence: u64, progress_capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState {
                next_sequence,
                operations: BTreeMap::new(),
            })),
            faulted: Arc::new(AtomicBool::new(false)),
            progress_capacity,
        }
    }

    pub fn reserve(
        &self,
        operation_id: &str,
    ) -> Result<(ConsumerGuard, OperationLease), RegistryError> {
        self.reserve_with_capacity(operation_id, self.progress_capacity)
    }

    pub fn reserve_with_capacity(
        &self,
        operation_id: &str,
        progress_capacity: usize,
    ) -> Result<(ConsumerGuard, OperationLease), RegistryError> {
        let mut state = self.lock()?;
        if state.operations.contains_key(operation_id) {
            return Err(RegistryError::Duplicate);
        }
        let sequence = state.next_sequence;
        state.next_sequence = sequence.checked_add(1).ok_or(RegistryError::Exhausted)?;
        let identity = OperationIdentity {
            operation_id: operation_id.to_owned(),
            attempt_id: format!("speech-operation-{sequence}"),
            sequence,
        };
        let released = Arc::new(Mutex::new(None));
        state.operations.insert(
            operation_id.to_owned(),
            OperationRecord {
                identity: identity.clone(),
                phase: OperationPhase::Reserved,
                cancellation_requested: false,
                terminal: None,
                progress: VecDeque::with_capacity(progress_capacity),
                progress_capacity,
                next_attempt_sequence: 1,
                attempts: BTreeMap::new(),
                released: Arc::clone(&released),
            },
        );
        let lease = OperationLease {
            registry: self.clone(),
            identity,
            released,
        };
        Ok((
            ConsumerGuard {
                lease: lease.clone(),
                cancel_on_drop: true,
            },
            lease,
        ))
    }

    pub fn active_count(&self) -> Result<usize, RegistryError> {
        Ok(self.lock()?.operations.len())
    }

    pub fn current(&self, operation_id: &str) -> Result<Option<OperationSnapshot>, RegistryError> {
        Ok(self.lock()?.operations.get(operation_id).map(snapshot))
    }

    pub fn current_lease(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationLease>, RegistryError> {
        let state = self.lock()?;
        let Some(record) = state.operations.get(operation_id) else {
            return Ok(None);
        };
        let identity = record.identity.clone();
        let released = Arc::clone(&record.released);
        drop(state);
        Ok(Some(OperationLease {
            registry: self.clone(),
            identity,
            released,
        }))
    }

    pub fn request_cancel_all(&self) -> Result<Vec<String>, RegistryError> {
        let mut state = self.lock()?;
        let ids = state.operations.keys().cloned().collect::<Vec<_>>();
        for record in state.operations.values_mut() {
            record.cancellation_requested = true;
        }
        Ok(ids)
    }

    #[must_use]
    pub fn diagnostic_faulted(&self) -> bool {
        self.faulted.load(Ordering::Acquire)
    }

    #[must_use]
    pub const fn progress_capacity(&self) -> usize {
        self.progress_capacity
    }

    #[cfg(test)]
    pub fn set_next_sequence_for_test(&self, next_sequence: u64) -> Result<(), RegistryError> {
        self.lock()?.next_sequence = next_sequence;
        Ok(())
    }

    fn record_mut<'a>(
        state: &'a mut RegistryState,
        identity: &OperationIdentity,
    ) -> Result<&'a mut OperationRecord, RegistryError> {
        let record = state
            .operations
            .get_mut(&identity.operation_id)
            .ok_or(RegistryError::Stale)?;
        if record.identity != *identity {
            return Err(RegistryError::Stale);
        }
        Ok(record)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RegistryState>, RegistryError> {
        self.inner.lock().map_err(|_| {
            self.faulted.store(true, Ordering::Release);
            RegistryError::StateUnavailable
        })
    }

    fn released_lock<'a>(
        &self,
        released: &'a Mutex<Option<OperationSnapshot>>,
    ) -> Result<std::sync::MutexGuard<'a, Option<OperationSnapshot>>, RegistryError> {
        released.lock().map_err(|_| {
            self.faulted.store(true, Ordering::Release);
            RegistryError::StateUnavailable
        })
    }
}

impl Default for OperationRegistry {
    fn default() -> Self {
        Self::new(1, 32)
    }
}

impl OperationLease {
    #[must_use]
    pub fn identity(&self) -> OperationIdentity {
        self.identity.clone()
    }

    pub fn snapshot(&self) -> Result<Option<OperationSnapshot>, RegistryError> {
        let state = self.registry.lock()?;
        if let Some(record) = state.operations.get(&self.identity.operation_id) {
            return Ok((record.identity == self.identity).then(|| snapshot(record)));
        }
        drop(state);
        Ok(self.registry.released_lock(&self.released)?.clone())
    }

    #[cfg(test)]
    pub fn poison_released_slot_for_test(&self) {
        let released = Arc::clone(&self.released);
        let _ = std::panic::catch_unwind(move || {
            let _slot = released.lock().expect("lock released snapshot slot");
            panic!("poison released snapshot slot fixture");
        });
    }

    pub fn is_active(&self) -> Result<bool, RegistryError> {
        Ok(self
            .registry
            .current(&self.identity.operation_id)?
            .is_some_and(|snapshot| snapshot.identity == self.identity))
    }

    pub fn queue(&self) -> Result<(), RegistryError> {
        self.transition(OperationPhase::Reserved, OperationPhase::Queued)
    }

    pub fn start(&self) -> Result<(), RegistryError> {
        self.transition(OperationPhase::Queued, OperationPhase::Running)
    }

    pub fn terminal(&self, terminal: TerminalClass) -> Result<(), RegistryError> {
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.phase != OperationPhase::Running || record.terminal.is_some() {
            return Err(RegistryError::InvalidTransition);
        }
        record.terminal = Some(terminal);
        record.phase = OperationPhase::Terminal;
        Ok(())
    }

    pub fn release(&self) -> Result<(), RegistryError> {
        let mut released_slot = self.registry.released_lock(&self.released)?;
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.phase != OperationPhase::Terminal || !record.attempts.is_empty() {
            return Err(RegistryError::InvalidTransition);
        }
        record.phase = OperationPhase::Released;
        let released = snapshot(record);
        state.operations.remove(&self.identity.operation_id);
        *released_slot = Some(released);
        Ok(())
    }

    pub fn finish_attempt_and_release(
        &self,
        attempt: &AttemptLease,
        terminal: TerminalClass,
    ) -> Result<OperationSnapshot, RegistryError> {
        if attempt.operation != self.identity {
            return Err(RegistryError::Stale);
        }
        let mut released_slot = self.registry.released_lock(&self.released)?;
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.phase != OperationPhase::Running
            || record.terminal.is_some()
            || record.attempts.len() != 1
            || record.attempts.get(&attempt.identity.sequence) != Some(&attempt.identity)
        {
            return Err(RegistryError::InvalidTransition);
        }
        record.attempts.remove(&attempt.identity.sequence);
        record.terminal = Some(terminal);
        record.phase = OperationPhase::Released;
        let released = snapshot(record);
        state.operations.remove(&self.identity.operation_id);
        *released_slot = Some(released.clone());
        Ok(released)
    }

    /// Fail and release an operation whose setup did not reach an executor.
    /// This is one checked rollback transaction; it never reports a successful
    /// terminal and never leaves a route admitted after setup failure.
    pub fn fail_setup_and_release(&self) -> Result<OperationSnapshot, RegistryError> {
        let mut released_slot = self.registry.released_lock(&self.released)?;
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.terminal.is_some() {
            return Err(RegistryError::InvalidTransition);
        }
        record.attempts.clear();
        record.terminal = Some(TerminalClass::Failed);
        record.phase = OperationPhase::Released;
        let released = snapshot(record);
        state.operations.remove(&self.identity.operation_id);
        *released_slot = Some(released.clone());
        Ok(released)
    }

    pub fn request_cancel(&self) -> Result<(), RegistryError> {
        let mut state = self.registry.lock()?;
        OperationRegistry::record_mut(&mut state, &self.identity)?.cancellation_requested = true;
        Ok(())
    }

    pub fn publish_progress(&self, sequence: u64) -> Result<(), RegistryError> {
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.phase != OperationPhase::Running || record.terminal.is_some() {
            return Err(RegistryError::InvalidTransition);
        }
        if record.progress_capacity != 0 {
            if record.progress.len() == record.progress_capacity {
                record.progress.pop_front();
            }
            record.progress.push_back(sequence);
        }
        Ok(())
    }

    pub fn start_attempt(&self) -> Result<AttemptLease, RegistryError> {
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.phase != OperationPhase::Running {
            return Err(RegistryError::InvalidTransition);
        }
        let sequence = record.next_attempt_sequence;
        record.next_attempt_sequence = sequence.checked_add(1).ok_or(RegistryError::Exhausted)?;
        let identity = OperationIdentity {
            operation_id: self.identity.operation_id.clone(),
            attempt_id: format!("{}-attempt-{sequence}", self.identity.attempt_id),
            sequence,
        };
        record.attempts.insert(sequence, identity.clone());
        Ok(AttemptLease {
            registry: self.registry.clone(),
            operation: self.identity.clone(),
            identity,
        })
    }

    pub fn active_attempts(&self) -> Result<Vec<OperationIdentity>, RegistryError> {
        let mut state = self.registry.lock()?;
        Ok(OperationRegistry::record_mut(&mut state, &self.identity)?
            .attempts
            .values()
            .cloned()
            .collect())
    }

    fn transition(
        &self,
        expected: OperationPhase,
        next: OperationPhase,
    ) -> Result<(), RegistryError> {
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.identity)?;
        if record.phase != expected {
            return Err(RegistryError::InvalidTransition);
        }
        record.phase = next;
        Ok(())
    }
}

impl ConsumerGuard {
    #[must_use]
    pub fn identity(&self) -> OperationIdentity {
        self.lease.identity()
    }

    pub fn cancel(&self) -> Result<(), RegistryError> {
        self.lease.request_cancel()
    }

    pub fn disarm(mut self) {
        self.cancel_on_drop = false;
    }
}

impl Drop for ConsumerGuard {
    fn drop(&mut self) {
        if self.cancel_on_drop
            && matches!(
                self.lease.request_cancel(),
                Err(RegistryError::StateUnavailable)
            )
        {
            self.lease.registry.faulted.store(true, Ordering::Release);
        }
    }
}

impl AttemptLease {
    #[must_use]
    pub fn identity(&self) -> OperationIdentity {
        self.identity.clone()
    }

    pub fn cancellation_requested(&self) -> Result<bool, RegistryError> {
        let mut state = self.registry.lock()?;
        Ok(OperationRegistry::record_mut(&mut state, &self.operation)?.cancellation_requested)
    }

    pub fn finish(self) -> Result<(), RegistryError> {
        let mut state = self.registry.lock()?;
        let record = OperationRegistry::record_mut(&mut state, &self.operation)?;
        record
            .attempts
            .remove(&self.identity.sequence)
            .filter(|current| *current == self.identity)
            .map(|_| ())
            .ok_or(RegistryError::Stale)
    }
}

fn snapshot(record: &OperationRecord) -> OperationSnapshot {
    OperationSnapshot {
        identity: record.identity.clone(),
        phase: record.phase,
        cancellation_requested: record.cancellation_requested,
        terminal: record.terminal,
        progress: record.progress.iter().copied().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_rollback_is_failed_released_and_empty() {
        let registry = OperationRegistry::default();
        let (_consumer, lease) = registry.reserve("setup").expect("reserve");
        lease.queue().expect("queue");
        let snapshot = lease.fail_setup_and_release().expect("rollback setup");

        assert_eq!(snapshot.phase, OperationPhase::Released);
        assert_eq!(snapshot.terminal, Some(TerminalClass::Failed));
        assert_eq!(registry.active_count(), Ok(0));
    }

    #[test]
    fn executor_finalization_commits_attempt_terminal_and_release_together() {
        let registry = OperationRegistry::default();
        let (_consumer, lease) = registry.reserve("finalize").expect("reserve");
        lease.queue().expect("queue");
        lease.start().expect("start");
        let attempt = lease.start_attempt().expect("attempt");
        let snapshot = lease
            .finish_attempt_and_release(&attempt, TerminalClass::Completed)
            .expect("atomic finish");

        assert_eq!(snapshot.phase, OperationPhase::Released);
        assert_eq!(snapshot.terminal, Some(TerminalClass::Completed));
        assert_eq!(registry.active_count(), Ok(0));
        assert_eq!(attempt.finish(), Err(RegistryError::Stale));
    }

    #[test]
    fn poisoned_registry_never_reports_empty_success() {
        let registry = OperationRegistry::default();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = registry.inner.lock().expect("lock registry state");
            panic!("poison registry fixture");
        }));

        assert_eq!(
            registry.active_count(),
            Err(RegistryError::StateUnavailable)
        );
        assert!(registry.diagnostic_faulted());
    }
}
