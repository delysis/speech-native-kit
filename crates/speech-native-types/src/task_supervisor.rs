use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::Notify;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskSupervisorSnapshot {
    pub active: usize,
    pub retained_failures: usize,
    pub total_failures: usize,
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
}

pub struct TaskSupervisor {
    state: Mutex<TaskSupervisorState>,
    idle: Notify,
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self {
            state: Mutex::new(TaskSupervisorState {
                phase: TaskSupervisorPhase::Running,
                active: 0,
                first_failure: None,
                additional_failures: 0,
            }),
            idle: Notify::new(),
        }
    }
}

impl TaskSupervisor {
    pub fn spawn<F>(
        self: &Arc<Self>,
        label: impl Into<String>,
        task: F,
    ) -> Result<(), TaskSupervisorError>
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| TaskSupervisorError::RuntimeUnavailable)?;
        self.admit()?;
        let supervisor = Arc::clone(self);
        let label = label.into();
        runtime.spawn(async move {
            let joined = tokio::spawn(task).await;
            let outcome = match joined {
                Ok(Ok(())) => Ok(()),
                Ok(Err(detail)) => Err(SupervisedTaskFailure {
                    label,
                    kind: SupervisedTaskFailureKind::Error,
                    detail,
                }),
                Err(error) => Err(join_failure(label, error)),
            };
            supervisor.finish(outcome);
        });
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
        self.admit()?;
        let supervisor = Arc::clone(self);
        let label = label.into();
        runtime.spawn_blocking(move || {
            let outcome = match catch_unwind(AssertUnwindSafe(task)) {
                Ok(Ok(())) => Ok(()),
                Ok(Err(detail)) => Err(SupervisedTaskFailure {
                    label,
                    kind: SupervisedTaskFailureKind::Error,
                    detail,
                }),
                Err(payload) => Err(SupervisedTaskFailure {
                    label,
                    kind: SupervisedTaskFailureKind::Panic,
                    detail: panic_detail(payload.as_ref()),
                }),
            };
            supervisor.finish(outcome);
        });
        Ok(())
    }

    pub fn begin_shutdown(&self) -> Result<(), TaskSupervisorError> {
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

    pub fn failure_summary(
        &self,
    ) -> Result<Option<SupervisedTaskFailureSummary>, TaskSupervisorError> {
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
        let state = self
            .state
            .lock()
            .map_err(|_| TaskSupervisorError::StateUnavailable)?;
        let retained_failures = usize::from(state.first_failure.is_some());
        Ok(TaskSupervisorSnapshot {
            active: state.active,
            retained_failures,
            total_failures: retained_failures.saturating_add(state.additional_failures),
        })
    }

    fn admit(&self) -> Result<(), TaskSupervisorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TaskSupervisorError::StateUnavailable)?;
        if state.phase != TaskSupervisorPhase::Running {
            return Err(TaskSupervisorError::AdmissionClosed);
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or(TaskSupervisorError::StateUnavailable)?;
        Ok(())
    }

    fn finish(&self, outcome: Result<(), SupervisedTaskFailure>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(failure) = outcome {
            if state.first_failure.is_none() {
                state.first_failure = Some(failure);
            } else {
                state.additional_failures = state.additional_failures.saturating_add(1);
            }
        }
        state.active = state.active.saturating_sub(1);
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

fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "task panicked with a non-string payload".to_string()
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
        supervisor.begin_shutdown().expect("begin shutdown");
        assert_eq!(
            supervisor.spawn("late", async { Ok(()) }),
            Err(TaskSupervisorError::AdmissionClosed)
        );
        release.send(()).expect("release held task");
        supervisor.wait_for_idle().await.expect("wait for idle");
        assert_eq!(supervisor.snapshot().expect("read task state").active, 0);
    }
}
