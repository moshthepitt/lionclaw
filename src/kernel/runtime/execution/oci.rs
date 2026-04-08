use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{
    backend::{ExecutionBackend, ExecutionOutput, ExecutionRequest},
    plan::ConfinementBackend,
};

/// Rootless OCI execution backend placeholder.
///
/// This backend contract exists before the concrete launch path is wired in so
/// adapter-owned process spawning can move here later.
#[derive(Debug, Default, Clone, Copy)]
pub struct OciExecutionBackend;

#[async_trait]
impl ExecutionBackend for OciExecutionBackend {
    fn kind(&self) -> ConfinementBackend {
        ConfinementBackend::Oci
    }

    async fn execute(&self, _request: ExecutionRequest) -> Result<ExecutionOutput> {
        Err(anyhow!("OCI execution backend is not implemented"))
    }
}
