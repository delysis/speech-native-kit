use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskSupervisorPhase {
    Running,
    Quiescing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisedTaskFailureKind {
    Error,
    Panic,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedTaskFailure {
    pub label: String,
    pub kind: SupervisedTaskFailureKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisedTaskFailureSummary {
    pub first: SupervisedTaskFailure,
    pub additional_failures: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSupervisorSnapshot {
    pub active: usize,
    pub retained_failures: usize,
    pub total_failures: usize,
    pub admitted_tasks: usize,
    pub completed_tasks: usize,
    pub expected_worker_ids: Vec<String>,
    pub joined_worker_ids: Vec<String>,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum TaskSupervisorError {
    #[error("task supervisor admission is closed")]
    AdmissionClosed,
    #[error("task supervisor state is unavailable")]
    StateUnavailable,
    #[error("no Tokio runtime is available for the supervised task")]
    RuntimeUnavailable,
}

struct TaskSupervisorState {
    phase: TaskSupervisorPhase,
    active: usize,
    first_failure: Option<SupervisedTaskFailure>,
    additional_failures: usize,
    admitted_tasks: usize,
    completed_tasks: usize,
    next_worker_sequence: u64,
    expected_worker_ids: BTreeSet<String>,
    joined_worker_ids: Vec<String>,
    // Each entry is a retained reaper task. The reaper owns and awaits the
    // actual worker JoinHandle, then (and only then) records the worker ID as
    // joined and removes this retained outer handle.
    join_handles: BTreeMap<String, JoinHandle<()>>,
}

pub struct TaskSupervisor {
    scope: String,
    state: Mutex<TaskSupervisorState>,
    faulted: AtomicBool,
    idle: Notify,
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self {
            scope: "task-supervisor".to_owned(),
            state: Mutex::new(TaskSupervisorState {
                phase: TaskSupervisorPhase::Running,
                active: 0,
                first_failure: None,
                additional_failures: 0,
                admitted_tasks: 0,
                completed_tasks: 0,
                next_worker_sequence: 1,
                expected_worker_ids: BTreeSet::new(),
                joined_worker_ids: Vec::new(),
                join_handles: BTreeMap::new(),
            }),
            faulted: AtomicBool::new(false),
            idle: Notify::new(),
        }
    }
}

impl TaskSupervisor {
    #[must_use]
    pub fn with_scope(scope: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            ..Self::default()
        }
    }

    pub fn spawn<F>(
        self: &Arc<Self>,
        label: impl Into<String>,
        task: F,
    ) -> Result<(), TaskSupervisorError>
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| TaskSupervisorError::RuntimeUnavailable)?;
        let label = label.into();
        let worker_id = self.admit(&label)?;
        let worker = runtime.spawn(task);
        let (retained, retained_ready) = oneshot::channel();
        let supervisor = Arc::clone(self);
        let joined_worker_id = worker_id.clone();
        let join_handle = runtime.spawn(async move {
            let _retained = retained_ready.await;
            let joined = worker.await;
            let outcome = match joined {
                Ok(Ok(())) => Ok(()),
                Ok(Err(detail)) => Err(SupervisedTaskFailure {
                    label,
                    kind: SupervisedTaskFailureKind::Error,
                    detail,
                }),
                Err(error) => Err(join_failure(label, error)),
            };
            supervisor.finish(joined_worker_id, outcome);
        });
        self.retain_join_handle(worker_id, join_handle)?;
        let _already_joining = retained.send(()).is_err();
        Ok(())
    }

    pub fn spawn_blocking<F>(
        self: &Arc<Self>,
        label: impl Into<String>,
        task: F,
    ) -> Result<(), TaskSupervisorError>
    where
        F: FnOnce() -> Result<(), String> + Send + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| TaskSupervisorError::RuntimeUnavailable)?;
        let label = label.into();
        let worker_id = self.admit(&label)?;
        let worker = runtime.spawn_blocking(task);
        let (retained, retained_ready) = oneshot::channel();
        let supervisor = Arc::clone(self);
        let joined_worker_id = worker_id.clone();
        let join_handle = runtime.spawn(async move {
            let _retained = retained_ready.await;
            let outcome = match worker.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(detail)) => Err(SupervisedTaskFailure {
                    label,
                    kind: SupervisedTaskFailureKind::Error,
                    detail,
                }),
                Err(error) => Err(join_failure(label, error)),
            };
            supervisor.finish(joined_worker_id, outcome);
        });
        self.retain_join_handle(worker_id, join_handle)?;
        let _already_joining = retained.send(()).is_err();
        Ok(())
    }

    pub fn begin_shutdown(&self) -> Result<(), TaskSupervisorError> {
        if self.faulted.load(Ordering::Acquire) {
            return Err(TaskSupervisorError::StateUnavailable);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| TaskSupervisorError::StateUnavailable)?;
        state.phase = TaskSupervisorPhase::Quiescing;
        Ok(())
    }

    pub async fn wait_for_idle(&self) -> Result<(), TaskSupervisorError> {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.faulted.load(Ordering::Acquire) {
                return Err(TaskSupervisorError::StateUnavailable);
            }
            if self
                .state
                .lock()
                .map_err(|_| TaskSupervisorError::StateUnavailable)?
                .active
                == 0
            {
                return Ok(());
            }
            idle.await;
        }
    }

    #[must_use]
    pub fn diagnostic_faulted(&self) -> bool {
        self.faulted.load(Ordering::Acquire)
    }

    pub fn failure_summary(
        &self,
    ) -> Result<Option<SupervisedTaskFailureSummary>, TaskSupervisorError> {
        if self.faulted.load(Ordering::Acquire) {
            return Err(TaskSupervisorError::StateUnavailable);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| TaskSupervisorError::StateUnavailable)?;
        Ok(state
            .first_failure
            .clone()
            .map(|first| SupervisedTaskFailureSummary {
                first,
                additional_failures: state.additional_failures,
            }))
    }

    pub fn snapshot(&self) -> Result<TaskSupervisorSnapshot, TaskSupervisorError> {
        if self.faulted.load(Ordering::Acquire) {
            return Err(TaskSupervisorError::StateUnavailable);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| TaskSupervisorError::StateUnavailable)?;
        if state.active == 0 && !state.join_handles.is_empty() {
            self.faulted.store(true, Ordering::Release);
            self.idle.notify_waiters();
            return Err(TaskSupervisorError::StateUnavailable);
        }
        let retained_failures = usize::from(state.first_failure.is_some());
        let total_failures = retained_failures
            .checked_add(state.additional_failures)
            .ok_or(TaskSupervisorError::StateUnavailable)?;
        Ok(TaskSupervisorSnapshot {
            active: state.active,
            retained_failures,
            total_failures,
            admitted_tasks: state.admitted_tasks,
            completed_tasks: state.completed_tasks,
            expected_worker_ids: state.expected_worker_ids.iter().cloned().collect(),
            joined_worker_ids: state.joined_worker_ids.clone(),
        })
    }

    fn admit(&self, label: &str) -> Result<String, TaskSupervisorError> {
        if self.faulted.load(Ordering::Acquire) {
            return Err(TaskSupervisorError::StateUnavailable);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| TaskSupervisorError::StateUnavailable)?;
        if state.phase != TaskSupervisorPhase::Running {
            return Err(TaskSupervisorError::AdmissionClosed);
        }
        let active = state
            .active
            .checked_add(1)
            .ok_or(TaskSupervisorError::StateUnavailable)?;
        let admitted_tasks = state
            .admitted_tasks
            .checked_add(1)
            .ok_or(TaskSupervisorError::StateUnavailable)?;
        let sequence = state.next_worker_sequence;
        let next_worker_sequence = sequence
            .checked_add(1)
            .ok_or(TaskSupervisorError::StateUnavailable)?;
        if state.active == 0 {
            // Begin a new bounded work epoch. Exact identities remain
            // available for the most recently exercised concurrent epoch,
            // while completed process-lifetime history is represented only
            // by the checked admitted/completed counters.
            state.expected_worker_ids.clear();
            state.joined_worker_ids.clear();
        }
        let worker_id = format!("{}:task-{sequence}:{label}", self.scope);
        if !state.expected_worker_ids.insert(worker_id.clone()) {
            return Err(TaskSupervisorError::StateUnavailable);
        }
        state.active = active;
        state.admitted_tasks = admitted_tasks;
        state.next_worker_sequence = next_worker_sequence;
        Ok(worker_id)
    }

    fn retain_join_handle(
        &self,
        worker_id: String,
        join_handle: JoinHandle<()>,
    ) -> Result<(), TaskSupervisorError> {
        let mut state = self.state.lock().map_err(|_| {
            self.faulted.store(true, Ordering::Release);
            self.idle.notify_waiters();
            TaskSupervisorError::StateUnavailable
        })?;
        if state.join_handles.insert(worker_id, join_handle).is_some() {
            self.faulted.store(true, Ordering::Release);
            self.idle.notify_waiters();
            return Err(TaskSupervisorError::StateUnavailable);
        }
        Ok(())
    }

    fn finish(&self, worker_id: String, outcome: Result<(), SupervisedTaskFailure>) {
        let Ok(mut state) = self.state.lock() else {
            self.faulted.store(true, Ordering::Release);
            self.idle.notify_waiters();
            return;
        };
        let Some(completed_tasks) = state.completed_tasks.checked_add(1) else {
            self.faulted.store(true, Ordering::Release);
            self.idle.notify_waiters();
            return;
        };
        if state.active == 0
            || !state.expected_worker_ids.contains(&worker_id)
            || state.joined_worker_ids.contains(&worker_id)
            || !state.join_handles.contains_key(&worker_id)
        {
            self.faulted.store(true, Ordering::Release);
            self.idle.notify_waiters();
            return;
        }
        if let Err(failure) = outcome {
            if state.first_failure.is_none() {
                state.first_failure = Some(failure);
            } else {
                let Some(additional_failures) = state.additional_failures.checked_add(1) else {
                    self.faulted.store(true, Ordering::Release);
                    self.idle.notify_waiters();
                    return;
                };
                state.additional_failures = additional_failures;
            }
        }
        state.active -= 1;
        state.completed_tasks = completed_tasks;
        state.joined_worker_ids.push(worker_id.clone());
        state.join_handles.remove(&worker_id);
        if state.active == 0 {
            self.idle.notify_waiters();
        }
    }
}

fn join_failure(label: String, error: tokio::task::JoinError) -> SupervisedTaskFailure {
    let kind = if error.is_panic() {
        SupervisedTaskFailureKind::Panic
    } else {
        SupervisedTaskFailureKind::Cancelled
    };
    SupervisedTaskFailure {
        label,
        kind,
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn task_failures_are_bounded_and_counted() {
        let supervisor = Arc::new(TaskSupervisor::default());
        supervisor
            .spawn("first", async { Err("first error".to_string()) })
            .expect("spawn first failure");
        supervisor
            .spawn_blocking("second", || Err("second error".to_string()))
            .expect("spawn second failure");
        supervisor.wait_for_idle().await.expect("wait for tasks");

        let summary = supervisor
            .failure_summary()
            .expect("read failure summary")
            .expect("failure is retained");
        assert!(matches!(summary.first.label.as_str(), "first" | "second"));
        assert_eq!(summary.additional_failures, 1);
        let snapshot = supervisor.snapshot().expect("read task state");
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.retained_failures, 1);
        assert_eq!(snapshot.total_failures, 2);
        assert_eq!(snapshot.admitted_tasks, 2);
        assert_eq!(snapshot.completed_tasks, 2);
    }

    #[tokio::test]
    async fn panics_are_captured_before_task_state_is_reaped() {
        let supervisor = Arc::new(TaskSupervisor::default());
        supervisor
            .spawn("panic", async { panic!("supervised panic") })
            .expect("spawn panic fixture");
        supervisor.wait_for_idle().await.expect("wait for panic");

        let failure = supervisor
            .failure_summary()
            .expect("read failure summary")
            .expect("panic is retained")
            .first;
        assert_eq!(failure.kind, SupervisedTaskFailureKind::Panic);
        assert!(failure.detail.contains("supervised panic"));
    }

    #[tokio::test]
    async fn quiescing_rejects_new_tasks_and_waits_existing_tasks() {
        let supervisor = Arc::new(TaskSupervisor::default());
        let (release, released) = tokio::sync::oneshot::channel();
        supervisor
            .spawn("held", async move {
                released.await.map_err(|error| error.to_string())?;
                Ok(())
            })
            .expect("spawn held task");
        let active = supervisor.snapshot().expect("read retained task state");
        assert_eq!(active.active, 1);
        assert_eq!(active.joined_worker_ids, Vec::<String>::new());
        assert_eq!(
            supervisor
                .state
                .lock()
                .expect("read retained join handles")
                .join_handles
                .len(),
            1
        );
        supervisor.begin_shutdown().expect("begin shutdown");
        assert_eq!(
            supervisor.spawn("late", async { Ok(()) }),
            Err(TaskSupervisorError::AdmissionClosed)
        );
        release.send(()).expect("release held task");
        supervisor.wait_for_idle().await.expect("wait for idle");
        assert_eq!(supervisor.snapshot().expect("read task state").active, 0);
        let snapshot = supervisor.snapshot().expect("read completed task state");
        assert_eq!(snapshot.admitted_tasks, 1);
        assert_eq!(snapshot.completed_tasks, 1);
        assert_eq!(snapshot.joined_worker_ids, snapshot.expected_worker_ids);
        assert!(
            supervisor
                .state
                .lock()
                .expect("read reaped join handles")
                .join_handles
                .is_empty()
        );
    }

    #[tokio::test]
    async fn completed_running_history_is_bounded_before_shutdown_epoch() {
        let supervisor = Arc::new(TaskSupervisor::with_scope("bounded-history"));
        for sequence in 0..10_000 {
            supervisor
                .spawn(format!("short-{sequence}"), async { Ok(()) })
                .expect("spawn short task");
            supervisor.wait_for_idle().await.expect("join short task");
        }

        let snapshot = supervisor.snapshot().expect("bounded snapshot");
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.admitted_tasks, 10_000);
        assert_eq!(snapshot.completed_tasks, 10_000);
        assert_eq!(snapshot.expected_worker_ids.len(), 1);
        assert_eq!(snapshot.joined_worker_ids, snapshot.expected_worker_ids);
        assert!(
            supervisor
                .state
                .lock()
                .expect("bounded supervisor state")
                .join_handles
                .is_empty()
        );
    }

    #[tokio::test]
    async fn duplicate_completion_faults_without_manufacturing_join_evidence() {
        let supervisor = TaskSupervisor::default();
        let worker_id = supervisor.admit("manual").expect("admit manual task");
        supervisor.finish(worker_id.clone(), Ok(()));
        supervisor.finish(worker_id, Ok(()));

        assert_eq!(
            supervisor.snapshot(),
            Err(TaskSupervisorError::StateUnavailable)
        );
        assert_eq!(
            supervisor.wait_for_idle().await,
            Err(TaskSupervisorError::StateUnavailable)
        );
    }

    #[test]
    fn poisoned_state_fails_closed() {
        let supervisor = TaskSupervisor::default();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = supervisor.state.lock().expect("lock task state");
            panic!("poison task supervisor fixture");
        }));

        assert_eq!(
            supervisor.snapshot(),
            Err(TaskSupervisorError::StateUnavailable)
        );
    }
}
