use anyhow::Result;
use async_trait::async_trait;

use super::plan::{ConfinementBackend, EffectiveExecutionPlan, RuntimeProgramSpec};

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub plan: EffectiveExecutionPlan,
    pub program: RuntimeProgramSpec,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
}

impl ExecutionOutput {
    pub fn success(&self) -> bool {
        self.exit_code == Some(0)
    }
}

#[async_trait]
pub trait ExecutionBackend: Send + Sync {
    fn kind(&self) -> ConfinementBackend;

    async fn execute(&self, request: ExecutionRequest) -> Result<ExecutionOutput>;
}
