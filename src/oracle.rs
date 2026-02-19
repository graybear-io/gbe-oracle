use async_trait::async_trait;
use gbe_jobs_domain::{JobDefinition, JobId, TaskId};

use crate::error::OracleError;

/// Outcome reported by an operative after executing a task.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    Completed {
        output: Vec<String>,
        result_ref: Option<String>,
    },
    Failed {
        exit_code: i32,
        error: String,
    },
}

/// The Oracle walks job DAGs and dispatches tasks as dependencies resolve.
/// It does not execute tasks — that is the Operative's concern.
///
/// The oracle only reads `depends_on` from task definitions. All other
/// fields (params, timeout, retries) pass through opaquely to the operative.
#[async_trait]
pub trait Oracle: Send + Sync {
    /// Submit a job definition. Returns a job ID.
    async fn submit(&self, def: JobDefinition) -> Result<JobId, OracleError>;

    /// Drive all active jobs forward. Called in a loop or on event.
    async fn tick(&self) -> Result<(), OracleError>;

    /// Handle a task completion/failure report from an operative.
    async fn task_reported(
        &self,
        task_id: TaskId,
        outcome: TaskOutcome,
    ) -> Result<(), OracleError>;
}
