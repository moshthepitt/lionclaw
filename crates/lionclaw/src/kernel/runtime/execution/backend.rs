use std::{fmt, path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use lionclaw_runtime_api::{RuntimeAuthContext, RuntimeAuthProvider, RuntimeProgramSession};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use uuid::Uuid;

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

    pub fn fresh_mounted_name(&self) -> String {
        let stable_name = self.mounted_name();
        let nonce = Uuid::new_v4().simple().to_string();
        format!("{}-{}", stable_name, &nonce[..12])
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
    pub runtime_auth_provider: Option<Arc<dyn RuntimeAuthProvider>>,
    pub runtime_auth_context: RuntimeAuthContext,
}

impl fmt::Debug for ExecutionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionRequest")
            .field("plan", &self.plan)
            .field("program", &self.program)
            .field("runtime_secrets_mount", &self.runtime_secrets_mount)
            .field(
                "runtime_auth_provider",
                &self
                    .runtime_auth_provider
                    .as_ref()
                    .map(|provider| provider.kind()),
            )
            .field("runtime_auth_context", &self.runtime_auth_context)
            .finish()
    }
}

pub type ExecutionOutput = super::process::ProcessOutput;

pub type ExecutionStdoutSender = mpsc::UnboundedSender<String>;

pub enum ExecutionSession {
    Oci(super::oci::OciExecutionSession),
}

impl ExecutionSession {
    pub async fn write_line(&mut self, line: &str) -> Result<()> {
        match self {
            Self::Oci(session) => session.write_line(line).await,
        }
    }

    pub async fn read_line(&mut self) -> Result<Option<String>> {
        match self {
            Self::Oci(session) => session.read_line().await,
        }
    }

    pub async fn shutdown(self) -> Result<ExecutionOutput> {
        match self {
            Self::Oci(session) => session.shutdown().await,
        }
    }
}

pub struct RuntimeExecutionSession {
    session: ExecutionSession,
}

impl RuntimeExecutionSession {
    pub fn new(session: ExecutionSession) -> Self {
        Self { session }
    }
}

#[async_trait]
impl RuntimeProgramSession for RuntimeExecutionSession {
    async fn write_line(&mut self, line: &str) -> Result<()> {
        self.session.write_line(line).await
    }

    async fn read_line(&mut self) -> Result<Option<String>> {
        self.session.read_line().await
    }

    async fn shutdown(self: Box<Self>) -> Result<ExecutionOutput> {
        self.session.shutdown().await
    }
}

#[async_trait]
pub trait ExecutionBackend: Send + Sync {
    fn kind(&self) -> ConfinementBackend;

    async fn execute_streaming(
        &self,
        request: ExecutionRequest,
        stdout: ExecutionStdoutSender,
    ) -> Result<ExecutionOutput>;

    async fn execute_captured(&self, request: ExecutionRequest) -> Result<ExecutionOutput>;

    async fn spawn_interactive(&self, request: ExecutionRequest) -> Result<ExecutionSession>;

    async fn execute_attached(&self, request: ExecutionRequest) -> Result<ExecutionOutput>;
}

pub async fn execute_streaming(
    request: ExecutionRequest,
    stdout: ExecutionStdoutSender,
) -> Result<ExecutionOutput> {
    match request.plan.confinement.backend() {
        ConfinementBackend::Oci => OciExecutionBackend.execute_streaming(request, stdout).await,
    }
}

pub async fn execute_captured(request: ExecutionRequest) -> Result<ExecutionOutput> {
    match request.plan.confinement.backend() {
        ConfinementBackend::Oci => OciExecutionBackend.execute_captured(request).await,
    }
}

pub async fn spawn_interactive(request: ExecutionRequest) -> Result<ExecutionSession> {
    match request.plan.confinement.backend() {
        ConfinementBackend::Oci => OciExecutionBackend.spawn_interactive(request).await,
    }
}

pub async fn execute_attached(request: ExecutionRequest) -> Result<ExecutionOutput> {
    match request.plan.confinement.backend() {
        ConfinementBackend::Oci => OciExecutionBackend.execute_attached(request).await,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ExecutionRequest, RuntimeSecretsMount, RUNTIME_SECRETS_NAME_PREFIX};
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, NetworkMode,
        OciConfinementConfig, RuntimeAuthContext, RuntimeProgramSpec, WorkspaceAccess,
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
                    skill_projection: None,
                    workspace_access: WorkspaceAccess::ReadWrite,
                    network_mode: NetworkMode::On,
                    working_dir: None,
                    environment: vec![("GITHUB_TOKEN".to_string(), "ghp_secret".to_string())],
                    mcp_servers: Vec::new(),
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
                    auth: None,
                },
                runtime_secrets_mount: Some(super::RuntimeSecretsMount {
                    source: "/tmp/runtime-secrets.env".into(),
                }),
                runtime_auth_provider: None,
                runtime_auth_context: RuntimeAuthContext::default(),
            }
        );

        assert!(!debug.contains("ghp_secret"));
        assert!(!debug.contains("sk-secret"));
        assert!(!debug.contains("hello"));
        assert!(debug.contains("runtime_auth_context"));
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
        assert!(mount
            .mounted_name()
            .starts_with(RUNTIME_SECRETS_NAME_PREFIX));
    }

    #[test]
    fn runtime_secrets_fresh_mount_name_preserves_prefix_and_avoids_reuse() {
        let mount = RuntimeSecretsMount {
            source: "/tmp/home-a/config/runtime-secrets.env".into(),
        };

        let first = mount.fresh_mounted_name();
        let second = mount.fresh_mounted_name();

        assert_ne!(first, second);
        assert!(first.starts_with(RUNTIME_SECRETS_NAME_PREFIX));
        assert!(second.starts_with(RUNTIME_SECRETS_NAME_PREFIX));
        assert!(first.starts_with(&mount.mounted_name()));
        assert!(second.starts_with(&mount.mounted_name()));
    }
}
