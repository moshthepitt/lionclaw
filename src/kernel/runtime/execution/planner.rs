use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use crate::kernel::runtime_policy::{RuntimeExecutionPolicy, RuntimeExecutionRequest};

use super::plan::{
    ConfinementConfig, EffectiveExecutionPlan, ExecutionPreset, MountAccess, MountSpec,
    OciConfinementConfig, WorkspaceAccess,
};

pub const BUILTIN_PRESET_EVERYDAY: &str = "everyday";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeExecutionProfile {
    pub confinement: ConfinementConfig,
}

impl Default for RuntimeExecutionProfile {
    fn default() -> Self {
        Self {
            confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionPlannerConfig {
    pub policy: RuntimeExecutionPolicy,
    pub default_preset_name: Option<String>,
    pub presets: BTreeMap<String, ExecutionPreset>,
    pub runtimes: BTreeMap<String, RuntimeExecutionProfile>,
    pub workspace_root: Option<PathBuf>,
    pub project_workspace_root: Option<PathBuf>,
    pub runtime_root: Option<PathBuf>,
    pub workspace_name: Option<String>,
    pub default_idle_timeout: Duration,
    pub default_hard_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlanRequest {
    pub runtime_id: String,
    pub preset_name: Option<String>,
    pub working_dir: Option<String>,
    pub env_passthrough_keys: Vec<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlanner {
    policy: RuntimeExecutionPolicy,
    default_preset_name: Option<String>,
    presets: BTreeMap<String, ExecutionPreset>,
    runtimes: BTreeMap<String, RuntimeExecutionProfile>,
    workspace_root: Option<PathBuf>,
    project_workspace_root: Option<PathBuf>,
    runtime_root: Option<PathBuf>,
    workspace_name: Option<String>,
    default_idle_timeout: Duration,
    default_hard_timeout: Duration,
}

impl ExecutionPlanner {
    pub fn new(config: ExecutionPlannerConfig) -> Self {
        Self {
            policy: config.policy,
            default_preset_name: config.default_preset_name,
            presets: config.presets,
            runtimes: config.runtimes,
            workspace_root: config.workspace_root,
            project_workspace_root: config.project_workspace_root,
            runtime_root: config.runtime_root,
            workspace_name: config.workspace_name,
            default_idle_timeout: config.default_idle_timeout,
            default_hard_timeout: config.default_hard_timeout,
        }
    }

    pub fn plan(&self, request: ExecutionPlanRequest) -> Result<EffectiveExecutionPlan, String> {
        let execution_context = self.policy.evaluate(
            &request.runtime_id,
            RuntimeExecutionRequest::new(
                request.working_dir.clone(),
                request.env_passthrough_keys.clone(),
                request.timeout_ms,
            ),
            self.default_idle_timeout,
            self.default_hard_timeout,
        )?;
        let runtime_profile = self
            .runtimes
            .get(&request.runtime_id)
            .cloned()
            .unwrap_or_default();
        let (preset_name, preset) = self.resolve_preset(request.preset_name.as_deref())?;
        let mounts = self.build_mounts(&request.runtime_id, &preset, &runtime_profile);
        let limits = match &runtime_profile.confinement {
            ConfinementConfig::Oci(config) => config.limits.clone(),
        };

        Ok(EffectiveExecutionPlan {
            runtime_id: request.runtime_id,
            preset_name,
            confinement: runtime_profile.confinement,
            workspace_access: preset.workspace_access,
            network_mode: preset.network_mode,
            working_dir: execution_context.working_dir,
            environment: execution_context.environment,
            idle_timeout: execution_context.idle_timeout,
            hard_timeout: execution_context.hard_timeout,
            mounts,
            secret_bindings: preset.secret_bindings,
            escape_classes: preset.escape_classes,
            limits,
        })
    }

    fn resolve_preset(
        &self,
        requested_name: Option<&str>,
    ) -> Result<(String, ExecutionPreset), String> {
        if let Some(name) = requested_name
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let preset = self
                .presets
                .get(name)
                .cloned()
                .ok_or_else(|| format!("preset '{}' is not configured", name))?;
            return Ok((name.to_string(), preset));
        }

        if let Some(default_name) = self
            .default_preset_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            let preset =
                self.presets.get(default_name).cloned().ok_or_else(|| {
                    format!("default preset '{}' is not configured", default_name)
                })?;
            return Ok((default_name.to_string(), preset));
        }

        Ok((
            BUILTIN_PRESET_EVERYDAY.to_string(),
            ExecutionPreset::default(),
        ))
    }

    fn build_mounts(
        &self,
        runtime_id: &str,
        preset: &ExecutionPreset,
        runtime_profile: &RuntimeExecutionProfile,
    ) -> Vec<MountSpec> {
        let mut mounts = Vec::new();

        if let Some(workspace_source) = self
            .project_workspace_root
            .clone()
            .or_else(|| self.workspace_root.clone())
        {
            mounts.push(MountSpec {
                source: workspace_source,
                target: "/workspace".to_string(),
                access: workspace_access_to_mount_access(preset.workspace_access),
            });
        }

        if let (Some(runtime_root), Some(workspace_name)) =
            (self.runtime_root.as_ref(), self.workspace_name.as_ref())
        {
            let runtime_workspace = runtime_root.join(runtime_id).join(workspace_name);
            mounts.push(MountSpec {
                source: runtime_workspace.clone(),
                target: "/runtime".to_string(),
                access: MountAccess::ReadWrite,
            });
            mounts.push(MountSpec {
                source: runtime_workspace.join("drafts"),
                target: "/drafts".to_string(),
                access: MountAccess::ReadWrite,
            });
        }

        match &runtime_profile.confinement {
            ConfinementConfig::Oci(config) => mounts.extend(config.additional_mounts.clone()),
        }

        mounts
    }
}

fn workspace_access_to_mount_access(access: WorkspaceAccess) -> MountAccess {
    match access {
        WorkspaceAccess::ReadOnly => MountAccess::ReadOnly,
        WorkspaceAccess::ReadWrite => MountAccess::ReadWrite,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use tempfile::tempdir;

    use super::{
        ExecutionPlanRequest, ExecutionPlanner, ExecutionPlannerConfig, RuntimeExecutionProfile,
        BUILTIN_PRESET_EVERYDAY,
    };
    use crate::kernel::runtime::{
        ConfinementConfig, ExecutionPreset, MountAccess, MountSpec, NetworkMode,
        OciConfinementConfig, WorkspaceAccess,
    };
    use crate::kernel::runtime_policy::{RuntimeExecutionPolicy, RuntimeExecutionRule};

    #[test]
    fn planner_uses_builtin_everyday_preset_when_none_configured() {
        let planner = ExecutionPlanner::new(ExecutionPlannerConfig {
            policy: RuntimeExecutionPolicy::default(),
            default_preset_name: None,
            presets: BTreeMap::new(),
            runtimes: BTreeMap::new(),
            workspace_root: None,
            project_workspace_root: None,
            runtime_root: None,
            workspace_name: None,
            default_idle_timeout: Duration::from_secs(30),
            default_hard_timeout: Duration::from_secs(90),
        });

        let plan = planner
            .plan(ExecutionPlanRequest {
                runtime_id: "codex".to_string(),
                preset_name: None,
                working_dir: None,
                env_passthrough_keys: Vec::new(),
                timeout_ms: None,
            })
            .expect("plan");

        assert_eq!(plan.preset_name, BUILTIN_PRESET_EVERYDAY);
        assert_eq!(plan.workspace_access, WorkspaceAccess::ReadWrite);
        assert_eq!(plan.network_mode, NetworkMode::On);
        assert_eq!(plan.idle_timeout, Duration::from_secs(30));
        assert_eq!(plan.hard_timeout, Duration::from_secs(90));
    }

    #[test]
    fn planner_builds_workspace_runtime_and_draft_mounts() {
        let sandbox = tempdir().expect("temp dir");
        let workspace_root = sandbox.path().join("project");
        let runtime_root = sandbox.path().join("runtime");
        let extra_mount = MountSpec {
            source: sandbox.path().join("refs"),
            target: "/refs".to_string(),
            access: MountAccess::ReadOnly,
        };
        let runtimes = [(
            "codex".to_string(),
            RuntimeExecutionProfile {
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    additional_mounts: vec![extra_mount.clone()],
                    ..OciConfinementConfig::default()
                }),
            },
        )]
        .into_iter()
        .collect();
        let planner = ExecutionPlanner::new(ExecutionPlannerConfig {
            policy: RuntimeExecutionPolicy::default(),
            default_preset_name: None,
            presets: BTreeMap::new(),
            runtimes,
            workspace_root: Some(workspace_root.clone()),
            project_workspace_root: None,
            runtime_root: Some(runtime_root.clone()),
            workspace_name: Some("main".to_string()),
            default_idle_timeout: Duration::from_secs(30),
            default_hard_timeout: Duration::from_secs(90),
        });

        let plan = planner
            .plan(ExecutionPlanRequest {
                runtime_id: "codex".to_string(),
                preset_name: None,
                working_dir: None,
                env_passthrough_keys: Vec::new(),
                timeout_ms: None,
            })
            .expect("plan");

        assert_eq!(plan.mounts[0].source, workspace_root);
        assert_eq!(plan.mounts[0].target, "/workspace");
        assert_eq!(plan.mounts[0].access, MountAccess::ReadWrite);
        assert_eq!(
            plan.mounts[1].source,
            runtime_root.join("codex").join("main")
        );
        assert_eq!(plan.mounts[1].target, "/runtime");
        assert_eq!(
            plan.mounts[2].source,
            runtime_root.join("codex").join("main").join("drafts")
        );
        assert_eq!(plan.mounts[2].target, "/drafts");
        assert_eq!(plan.mounts[3], extra_mount);
    }

    #[test]
    fn planner_rejects_unknown_default_preset() {
        let planner = ExecutionPlanner::new(ExecutionPlannerConfig {
            policy: RuntimeExecutionPolicy::default(),
            default_preset_name: Some("missing".to_string()),
            presets: BTreeMap::new(),
            runtimes: BTreeMap::new(),
            workspace_root: None,
            project_workspace_root: None,
            runtime_root: None,
            workspace_name: None,
            default_idle_timeout: Duration::from_secs(30),
            default_hard_timeout: Duration::from_secs(90),
        });

        let err = planner
            .plan(ExecutionPlanRequest {
                runtime_id: "codex".to_string(),
                preset_name: None,
                working_dir: None,
                env_passthrough_keys: Vec::new(),
                timeout_ms: None,
            })
            .expect_err("plan should fail");

        assert!(err.contains("default preset 'missing' is not configured"));
    }

    #[test]
    fn planner_applies_configured_preset_and_policy() {
        let sandbox = tempdir().expect("temp dir");
        let child = sandbox.path().join("child");
        std::fs::create_dir(&child).expect("create child");
        let policy = RuntimeExecutionPolicy::default().with_rule(
            "codex",
            RuntimeExecutionRule {
                working_dir_roots: vec![sandbox.path().to_path_buf()],
                ..RuntimeExecutionRule::default()
            },
        );
        let presets = [(
            "readonly".to_string(),
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadOnly,
                ..ExecutionPreset::default()
            },
        )]
        .into_iter()
        .collect();
        let planner = ExecutionPlanner::new(ExecutionPlannerConfig {
            policy,
            default_preset_name: Some("readonly".to_string()),
            presets,
            runtimes: BTreeMap::new(),
            workspace_root: Some(sandbox.path().join("workspace")),
            project_workspace_root: None,
            runtime_root: None,
            workspace_name: None,
            default_idle_timeout: Duration::from_secs(30),
            default_hard_timeout: Duration::from_secs(90),
        });

        let plan = planner
            .plan(ExecutionPlanRequest {
                runtime_id: "codex".to_string(),
                preset_name: None,
                working_dir: Some(child.to_string_lossy().to_string()),
                env_passthrough_keys: Vec::new(),
                timeout_ms: Some(45_000),
            })
            .expect("plan");

        assert_eq!(plan.preset_name, "readonly");
        assert_eq!(plan.workspace_access, WorkspaceAccess::ReadOnly);
        assert_eq!(
            plan.working_dir.as_deref(),
            Some(child.to_string_lossy().as_ref())
        );
        assert_eq!(plan.idle_timeout, Duration::from_secs(45));
        assert_eq!(plan.hard_timeout, Duration::from_secs(90));
    }
}
