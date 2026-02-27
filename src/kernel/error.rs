use thiserror::Error;

#[derive(Debug, Error)]
pub enum KernelError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("runtime timeout: {0}")]
    RuntimeTimeout(String),
    #[error("internal error: {0}")]
    Internal(String),
}
