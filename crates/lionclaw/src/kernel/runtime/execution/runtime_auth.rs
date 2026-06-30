use std::path::Path;

use anyhow::anyhow;
use anyhow::Result;
use lionclaw_runtime_api::RuntimeAuthPreparation;

use super::{
    backend::ExecutionRequest,
    plan::{runtime_native_home_mount_source, MountSpec},
};

pub async fn prepare_runtime_auth(request: &ExecutionRequest) -> Result<Vec<(String, String)>> {
    match &request.program.auth {
        None => Ok(Vec::new()),
        Some(auth) => {
            let provider = request.runtime_auth_provider.as_ref().ok_or_else(|| {
                anyhow!(
                    "runtime '{}' requested unsupported runtime auth kind '{}'",
                    request.plan.runtime_id,
                    auth.as_str()
                )
            })?;
            if provider.kind() != auth.as_str() {
                return Err(anyhow!(
                    "runtime '{}' requested auth kind '{}' but received provider '{}'",
                    request.plan.runtime_id,
                    auth.as_str(),
                    provider.kind()
                ));
            }
            provider
                .prepare(RuntimeAuthPreparation {
                    runtime_id: &request.plan.runtime_id,
                    network_mode: request.plan.network_mode,
                    runtime_home_root: runtime_home_root(&request.plan.mounts),
                    host_context: &request.runtime_auth_context,
                })
                .await
        }
    }
}

fn runtime_home_root(mounts: &[MountSpec]) -> Option<&Path> {
    runtime_native_home_mount_source(mounts)
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use async_trait::async_trait;
    use lionclaw_runtime_api::{RuntimeAuthContext, RuntimeAuthProvider};

    use super::*;
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, MountAccess, MountSpec,
        NetworkMode, OciConfinementConfig, RuntimeAuthKind, RuntimeProgramSpec, WorkspaceAccess,
    };

    const TEST_AUTH_KIND: &str = "test-auth";

    #[derive(Debug)]
    struct TestRuntimeAuthProvider;

    #[async_trait]
    impl RuntimeAuthProvider for TestRuntimeAuthProvider {
        fn kind(&self) -> &'static str {
            TEST_AUTH_KIND
        }

        async fn validate(&self, _context: &RuntimeAuthContext) -> Result<()> {
            Ok(())
        }

        async fn prepare(
            &self,
            input: RuntimeAuthPreparation<'_>,
        ) -> Result<Vec<(String, String)>> {
            if input.network_mode != NetworkMode::On {
                bail!(
                    "runtime '{}' requires network-mode 'on' when test runtime auth is enabled",
                    input.runtime_id
                );
            }

            Ok(vec![("TEST_AUTH".to_string(), "enabled".to_string())])
        }
    }

    fn sample_request(network_mode: NetworkMode) -> ExecutionRequest {
        ExecutionRequest {
            plan: EffectiveExecutionPlan {
                runtime_id: "test-runtime".to_string(),
                preset_name: "everyday".to_string(),
                confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
                skill_projection: None,
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode,
                working_dir: None,
                environment: Vec::new(),
                mcp_servers: Vec::new(),
                idle_timeout: std::time::Duration::from_secs(30),
                hard_timeout: std::time::Duration::from_secs(90),
                mounts: vec![
                    MountSpec {
                        source: "/tmp/lionclaw-runtime-auth-test".into(),
                        target: "/runtime".to_string(),
                        access: MountAccess::ReadWrite,
                    },
                    MountSpec {
                        source: "/tmp/lionclaw-runtime-auth-test-home".into(),
                        target: "/runtime/home".to_string(),
                        access: MountAccess::ReadWrite,
                    },
                ],
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
                limits: ExecutionLimits::default(),
            },
            program: RuntimeProgramSpec {
                executable: "test-runtime".to_string(),
                args: vec!["exec".to_string()],
                environment: Vec::new(),
                stdin: "hello".to_string(),
                auth: Some(RuntimeAuthKind::from_static(TEST_AUTH_KIND)),
            },
            runtime_secrets_mount: None,
            runtime_auth_provider: Some(std::sync::Arc::new(TestRuntimeAuthProvider)),
            runtime_auth_context: Default::default(),
        }
    }

    #[test]
    fn runtime_home_root_prefers_runtime_home_mount() {
        let mounts = [
            MountSpec {
                source: "/tmp/workspace".into(),
                target: "/workspace".to_string(),
                access: MountAccess::ReadWrite,
            },
            MountSpec {
                source: "/tmp/runtime".into(),
                target: "/runtime".to_string(),
                access: MountAccess::ReadWrite,
            },
            MountSpec {
                source: "/tmp/runtime-home".into(),
                target: "/runtime/home".to_string(),
                access: MountAccess::ReadWrite,
            },
        ];
        let runtime_root = runtime_home_root(&mounts).expect("runtime home root");

        assert_eq!(runtime_root, Path::new("/tmp/runtime-home"));
    }

    #[tokio::test]
    async fn runtime_auth_provider_receives_execution_context() {
        let err = prepare_runtime_auth(&sample_request(NetworkMode::None))
            .await
            .expect_err("network-off auth preparation should fail");

        assert!(err.to_string().contains("requires network-mode 'on'"));
    }
}
