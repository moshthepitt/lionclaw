use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::{
    backend::{ExecutionBackend, ExecutionOutput, ExecutionRequest},
    plan::ConfinementBackend,
};

/// Rootless OCI execution backend placeholder.
///
/// Phase 0 introduces the backend contract without changing any existing launch
/// behavior. Later phases will replace adapter-owned process spawning with a
/// concrete Podman-backed implementation here.
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
