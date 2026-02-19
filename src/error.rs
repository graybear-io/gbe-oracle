use gbe_jobs_domain::JobsDomainError;

#[derive(Debug, thiserror::Error)]
pub enum OracleError {
    #[error("job domain error: {0}")]
    Domain(#[from] JobsDomainError),

    #[error("job not found: {0}")]
    JobNotFound(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("state store error: {0}")]
    StateStore(String),
}
