use std::path::Path;

use anyhow::{anyhow, bail, Result};

use super::{
    backend::ExecutionRequest,
    plan::{NetworkMode, RuntimeAuthKind},
};
use crate::kernel::runtime::sync_codex_home_into_runtime;

const CONTAINER_CODEX_HOME: &str = "/runtime/home/.codex";

pub async fn prepare_runtime_auth(request: &ExecutionRequest) -> Result<Vec<(String, String)>> {
    match request.program.auth {
        None => Ok(Vec::new()),
        Some(RuntimeAuthKind::Codex) => prepare_codex_runtime_auth(request).await,
    }
}

async fn prepare_codex_runtime_auth(request: &ExecutionRequest) -> Result<Vec<(String, String)>> {
    if request.plan.network_mode != NetworkMode::On {
        bail!(
            "runtime '{}' requires network-mode 'on' when Codex runtime auth is enabled",
            request.plan.runtime_id
        );
    }

    let runtime_state_root = runtime_state_root(&request.plan.mounts).ok_or_else(|| {
        anyhow!(
            "runtime '{}' requires a /runtime mount when Codex runtime auth is enabled",
            request.plan.runtime_id
        )
    })?;
    sync_codex_home_into_runtime(runtime_state_root, request.codex_home_override.as_deref())
        .await?;

    Ok(vec![(
        "CODEX_HOME".to_string(),
        CONTAINER_CODEX_HOME.to_string(),
    )])
}

fn runtime_state_root(mounts: &[super::plan::MountSpec]) -> Option<&Path> {
    mounts
        .iter()
        .find(|mount| mount.target == "/runtime")
        .map(|mount| mount.source.as_path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::runtime::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, MountAccess, MountSpec,
        OciConfinementConfig, RuntimeProgramSpec, WorkspaceAccess,
    };

    fn sample_request(network_mode: NetworkMode) -> ExecutionRequest {
        ExecutionRequest {
            plan: EffectiveExecutionPlan {
                runtime_id: "codex".to_string(),
                preset_name: "everyday".to_string(),
                confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode,
                working_dir: None,
                environment: Vec::new(),
                idle_timeout: std::time::Duration::from_secs(30),
                hard_timeout: std::time::Duration::from_secs(90),
                mounts: vec![MountSpec {
                    source: "/tmp/lionclaw-runtime-auth-test".into(),
                    target: "/runtime".to_string(),
                    access: MountAccess::ReadWrite,
                }],
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
                limits: ExecutionLimits::default(),
            },
            program: RuntimeProgramSpec {
                executable: "codex".to_string(),
                args: vec!["exec".to_string()],
                environment: Vec::new(),
                stdin: "hello".to_string(),
                auth: Some(RuntimeAuthKind::Codex),
            },
            runtime_secrets_mount: None,
            codex_home_override: None,
        }
    }

    #[test]
    fn runtime_state_root_prefers_runtime_mount() {
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
        ];
        let runtime_root = runtime_state_root(&mounts).expect("runtime root");

        assert_eq!(runtime_root, Path::new("/tmp/runtime"));
    }

    #[tokio::test]
    async fn codex_runtime_auth_requires_network() {
        let err = prepare_runtime_auth(&sample_request(NetworkMode::None))
            .await
            .expect_err("network-off Codex launch should fail");

        assert!(err.to_string().contains("requires network-mode 'on'"));
    }
}
