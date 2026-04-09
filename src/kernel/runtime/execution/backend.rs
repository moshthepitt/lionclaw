use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

use super::oci::OciExecutionBackend;
use super::plan::{ConfinementBackend, EffectiveExecutionPlan, RuntimeProgramSpec};

#[derive(Clone)]
pub struct ExecutionRequest {
    pub plan: EffectiveExecutionPlan,
    pub program: RuntimeProgramSpec,
}

impl fmt::Debug for ExecutionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionRequest")
            .field("plan", &self.plan)
            .field("program", &self.program)
            .finish()
    }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ExecutionRequest;
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, NetworkMode,
        OciConfinementConfig, RuntimeProgramSpec, WorkspaceAccess,
    };

    #[test]
    fn execution_request_debug_redacts_nested_secret_values() {
        let debug = format!(
            "{:?}",
            ExecutionRequest {
                plan: EffectiveExecutionPlan {
                    runtime_id: "codex".to_string(),
                    preset_name: "everyday".to_string(),
                    confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
                    workspace_access: WorkspaceAccess::ReadWrite,
                    network_mode: NetworkMode::On,
                    working_dir: None,
                    environment: vec![("GITHUB_TOKEN".to_string(), "ghp_secret".to_string())],
                    idle_timeout: Duration::from_secs(30),
                    hard_timeout: Duration::from_secs(90),
                    mounts: Vec::new(),
                    secret_env: vec!["GITHUB_TOKEN".to_string()],
                    escape_classes: Default::default(),
                    limits: ExecutionLimits::default(),
                },
                program: RuntimeProgramSpec {
                    executable: "codex".to_string(),
                    args: vec!["exec".to_string()],
                    environment: vec![("OPENAI_API_KEY".to_string(), "sk-secret".to_string())],
                    stdin: "hello".to_string(),
                },
            }
        );

        assert!(!debug.contains("ghp_secret"));
        assert!(!debug.contains("sk-secret"));
        assert!(!debug.contains("hello"));
    }
}
