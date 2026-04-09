use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

use super::oci::OciExecutionBackend;
use super::plan::{ConfinementBackend, EffectiveExecutionPlan, RuntimeProgramSpec};

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub plan: EffectiveExecutionPlan,
    pub program: RuntimeProgramSpec,
}

pub type ExecutionOutput = super::process::ProcessOutput;

pub type ExecutionStdoutSender = mpsc::UnboundedSender<String>;

#[async_trait]
pub trait ExecutionBackend: Send + Sync {
    fn kind(&self) -> ConfinementBackend;

    async fn execute_streaming(
        &self,
        request: ExecutionRequest,
        stdout: ExecutionStdoutSender,
    ) -> Result<ExecutionOutput>;
}

pub async fn execute_streaming(
    request: ExecutionRequest,
    stdout: ExecutionStdoutSender,
) -> Result<ExecutionOutput> {
    match request.plan.confinement.backend() {
        ConfinementBackend::Oci => OciExecutionBackend.execute_streaming(request, stdout).await,
    }
}
