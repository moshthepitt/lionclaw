//! The mission's `RuntimeProgramExecutor`: binds a moat-compiled plan to the
//! confinement backend. Faithful duplicate of the kernel's executor plumbing
//! (`core.rs` `KernelRuntimeProgramExecutor` + `runtime_execution_context`),
//! minus kernel-only concerns (secrets mounts, channel sockets, MCP).

use anyhow::Result;
use async_trait::async_trait;
use lionclaw_confinement::{
    execute_captured, execute_streaming, map_host_path_into_runtime_mount,
    runtime_state_mount_source, spawn_interactive, EffectiveExecutionPlan, ExecutionRequest,
    RuntimeExecutionSession, RUNTIME_HOME_MOUNT_TARGET, RUNTIME_MOUNT_TARGET,
};
use lionclaw_runtime_api::{
    ExecutionOutput, RuntimeAuthContext, RuntimeAuthRegistry, RuntimeExecutionContext,
    RuntimePathProjection, RuntimeProgramExecutor, RuntimeProgramSession, RuntimeProgramSpec,
    RuntimeProgramStdoutSender,
};

pub struct MissionProgramExecutor {
    plan: EffectiveExecutionPlan,
    auth_registry: RuntimeAuthRegistry,
    auth_context: RuntimeAuthContext,
}

impl MissionProgramExecutor {
    pub fn new(plan: EffectiveExecutionPlan, auth_registry: RuntimeAuthRegistry) -> Self {
        Self {
            plan,
            auth_registry,
            auth_context: RuntimeAuthContext::default(),
        }
    }

    fn request(&self, program: RuntimeProgramSpec) -> ExecutionRequest {
        ExecutionRequest {
            plan: self.plan.clone(),
            runtime_auth_provider: program
                .auth
                .as_ref()
                .and_then(|auth| self.auth_registry.get(auth)),
            program,
            runtime_secrets_mount: None,
            runtime_auth_context: self.auth_context.clone(),
        }
    }
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
        let session = spawn_interactive(self.request(program)).await?;
        Ok(Box::new(RuntimeExecutionSession::new(session)))
    }
}

/// The runtime-visible execution context for a compiled plan.
pub fn mission_execution_context(plan: &EffectiveExecutionPlan) -> Result<RuntimeExecutionContext> {
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
        network_mode: plan.network_mode,
        working_dir: plan
            .working_dir
            .as_deref()
            .map(|dir| map_host_path_into_runtime_mount(dir, &plan.mounts, "working directory"))
            .transpose()?,
        environment: plan.environment.clone(),
        runtime_state_root: runtime_state_mount_source(&plan.mounts).map(Into::into),
        runtime_path_projections: projections,
        mcp_servers: Vec::new(),
    })
}
