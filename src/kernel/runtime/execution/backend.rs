use std::{fmt, path::PathBuf};

use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::oci::OciExecutionBackend;
use super::plan::{ConfinementBackend, EffectiveExecutionPlan, RuntimeProgramSpec};

pub const RUNTIME_SECRETS_NAME_PREFIX: &str = "lionclaw-runtime-secrets-";

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeSecretsMount {
    pub source: PathBuf,
}

impl RuntimeSecretsMount {
    pub fn mounted_name(&self) -> String {
        let digest = Sha256::digest(self.source.to_string_lossy().as_bytes());
        format!(
            "{}{}",
            RUNTIME_SECRETS_NAME_PREFIX,
            &hex::encode(digest)[..12]
        )
    }
}

impl fmt::Debug for RuntimeSecretsMount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeSecretsMount")
            .field("source", &self.source)
            .field("mounted_name", &self.mounted_name())
            .finish()
    }
}

#[derive(Clone)]
pub struct ExecutionRequest {
    pub plan: EffectiveExecutionPlan,
    pub program: RuntimeProgramSpec,
    pub runtime_secrets_mount: Option<RuntimeSecretsMount>,
}

impl fmt::Debug for ExecutionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionRequest")
            .field("plan", &self.plan)
            .field("program", &self.program)
            .field("runtime_secrets_mount", &self.runtime_secrets_mount)
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

    use super::{ExecutionRequest, RuntimeSecretsMount, RUNTIME_SECRETS_NAME_PREFIX};
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
                    mount_runtime_secrets: true,
                    escape_classes: Default::default(),
                    limits: ExecutionLimits::default(),
                },
                program: RuntimeProgramSpec {
                    executable: "codex".to_string(),
                    args: vec!["exec".to_string()],
                    environment: vec![("OPENAI_API_KEY".to_string(), "sk-secret".to_string())],
                    stdin: "hello".to_string(),
                },
                runtime_secrets_mount: Some(super::RuntimeSecretsMount {
                    source: "/tmp/runtime-secrets.env".into(),
                }),
            }
        );

        assert!(!debug.contains("ghp_secret"));
        assert!(!debug.contains("sk-secret"));
        assert!(!debug.contains("hello"));
    }

    #[test]
    fn runtime_secrets_mount_name_is_stable_and_prefixed() {
        let mount = RuntimeSecretsMount {
            source: "/tmp/home-a/config/runtime-secrets.env".into(),
        };
        let same_mount = RuntimeSecretsMount {
            source: "/tmp/home-a/config/runtime-secrets.env".into(),
        };
        let other_mount = RuntimeSecretsMount {
            source: "/tmp/home-b/config/runtime-secrets.env".into(),
        };

        assert_eq!(mount.mounted_name(), same_mount.mounted_name());
        assert_ne!(mount.mounted_name(), other_mount.mounted_name());
        assert!(mount.mounted_name().starts_with(RUNTIME_SECRETS_NAME_PREFIX));
    }
}
