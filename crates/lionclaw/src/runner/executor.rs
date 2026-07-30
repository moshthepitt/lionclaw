//! The mission's `RuntimeProgramExecutor`: binds a moat-compiled plan to the
//! confinement backend. Faithful duplicate of the kernel's executor plumbing
//! (`core.rs` `KernelRuntimeProgramExecutor` + `runtime_execution_context`),
//! minus kernel-only concerns (secrets mounts, channel sockets, MCP).

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;
use lionclaw_confinement::{
    execute_captured, execute_streaming, map_host_path_into_runtime_mount, spawn_interactive,
    EffectiveExecutionPlan, ExecutionRequest, RuntimeExecutionSession, RUNTIME_HOME_MOUNT_TARGET,
    RUNTIME_MOUNT_TARGET,
};
use lionclaw_runtime_api::{
    ExecutionOutput, RuntimeAuthMaterialization, RuntimeExecutionContext, RuntimePathProjection,
    RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec, RuntimeProgramStdoutSender,
    RuntimeStateDir, TypedFailure,
};

pub struct MissionProgramExecutor {
    plan: EffectiveExecutionPlan,
    runtime_auth: Option<RuntimeAuthMaterialization>,
    resource_name: String,
    auth_staging_root: Option<PathBuf>,
}

impl MissionProgramExecutor {
    pub fn new(
        plan: EffectiveExecutionPlan,
        runtime_auth: Option<RuntimeAuthMaterialization>,
        effect_id: &crate::model::EffectId,
        auth_staging_root: Option<PathBuf>,
    ) -> Self {
        Self {
            plan,
            runtime_auth,
            resource_name: effect_id.resource_name(),
            auth_staging_root,
        }
    }

    fn request(&self, program: RuntimeProgramSpec) -> ExecutionRequest {
        ExecutionRequest {
            plan: self.plan.clone(),
            resource_name: Some(self.resource_name.clone()),
            runtime_auth: self.runtime_auth.clone(),
            program,
            runtime_secrets_mount: None,
            auth_staging_root: self.auth_staging_root.clone(),
        }
    }
}

fn launch_refusal(error: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(TypedFailure::permanent(
        "kernel.launch",
        format!("runtime program launch refused: {error:#}"),
    ))
}

#[async_trait]
impl RuntimeProgramExecutor for MissionProgramExecutor {
    async fn execute_streaming(
        &mut self,
        program: RuntimeProgramSpec,
        stdout: RuntimeProgramStdoutSender,
    ) -> Result<ExecutionOutput> {
        execute_streaming(self.request(program), stdout).await
    }

    async fn execute_captured(&mut self, program: RuntimeProgramSpec) -> Result<ExecutionOutput> {
        execute_captured(self.request(program)).await
    }

    async fn spawn(
        &mut self,
        program: RuntimeProgramSpec,
    ) -> Result<Box<dyn RuntimeProgramSession>> {
        let session = spawn_interactive(self.request(program))
            .await
            .map_err(|error| {
                // This is the last boundary before an adapter has a live native
                // transport. Preserve a refusal to cross it as kernel launch
                // evidence so adapter-specific runtime projection cannot flatten
                // it into an ordinary turn failure.
                launch_refusal(error)
            })?;
        Ok(Box::new(RuntimeExecutionSession::new(session)))
    }
}

/// The runtime-visible execution context for a compiled plan.
pub fn mission_execution_context(
    plan: &EffectiveExecutionPlan,
    runtime_state: Option<RuntimeStateDir>,
) -> Result<RuntimeExecutionContext> {
    let projections = plan
        .mounts
        .iter()
        .filter(|mount| {
            matches!(
                mount.target.as_str(),
                RUNTIME_MOUNT_TARGET | RUNTIME_HOME_MOUNT_TARGET
            )
        })
        .map(|mount| RuntimePathProjection::directory(mount.target.clone(), mount.source.clone()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RuntimeExecutionContext {
        network: plan.network.clone(),
        working_dir: plan
            .working_dir
            .as_deref()
            .map(|dir| map_host_path_into_runtime_mount(dir, &plan.mounts, "working directory"))
            .transpose()?,
        environment: plan.environment.clone(),
        runtime_state,
        runtime_path_projections: projections,
        mcp_servers: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interactive_launch_refusal_is_typed_before_adapter_projection() {
        let error = launch_refusal(anyhow::anyhow!("OCI setup refused the executable"));
        let failure = error
            .downcast_ref::<TypedFailure>()
            .expect("launch refusal must cross the adapter boundary as typed evidence");

        assert_eq!(failure.evidence().code.as_deref(), Some("kernel.launch"));
        assert!(failure
            .evidence()
            .detail
            .contains("OCI setup refused the executable"));
        assert_eq!(
            failure.evidence().configuration,
            lionclaw_runtime_api::AppliedRuntimeConfiguration::default()
        );
        assert!(failure.evidence().final_response.is_empty());
    }
}
