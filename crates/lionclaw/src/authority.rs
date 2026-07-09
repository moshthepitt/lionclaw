//! Authority compilation and the moat predicate.
//!
//! Authority is engine-internal: the author declares only output semantics
//! (plus the `network`/`secrets` flags); everything enforceable is compiled
//! here as `effective = role_request ∩ ceiling ∩ semantics_floor`, then
//! container-enforced. **The moat**: a verdict-emitting node must be
//! read-only on an enforcing rung, with no read-write mount overlapping the
//! judged set, no escape class that could feed back, and no secrets. Any
//! violation refuses to compile — the mission never starts.
//!
//! [`CompiledRolePlan`] has one constructor, [`compile_role_plan`], and the
//! runner accepts nothing else: an un-vetted plan is unrepresentable.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lionclaw_confinement::{
    ConfinementConfig, EffectiveExecutionPlan, EscapeClass, ExecutionPreset, InstallPolicy,
    MountAccess, MountSpec, NetworkMode, WorkspaceAccess, RUNTIME_HOME_MOUNT_TARGET,
    RUNTIME_MOUNT_TARGET, WORKSPACE_MOUNT_TARGET,
};

use crate::mission_type::RoleDefinition;
use crate::model::OutputSemantics;

/// Mission targets no mount may shadow.
const RESERVED_TARGETS: &[&str] = &[
    WORKSPACE_MOUNT_TARGET,
    RUNTIME_MOUNT_TARGET,
    RUNTIME_HOME_MOUNT_TARGET,
    "/mission",
    "/scratch",
];

/// The operator/mission bound authority can never exceed. Intersected,
/// never unioned.
#[derive(Debug, Clone, Default)]
pub struct AuthorityCeiling {
    pub allow_secrets: bool,
    pub allowed_escapes: BTreeSet<EscapeClass>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MoatViolation {
    #[error("confinement backend is not an enforcing rung")]
    NonEnforcingBackend,
    #[error("verdict role '{role}' would have a writable workspace")]
    WritableJudge { role: String },
    #[error("verdict role '{role}' would hold escape class '{escape}'")]
    JudgeEscape { role: String, escape: String },
    #[error(
        "read-write mount '{mount_source}' overlaps judged root '{root}' for verdict role '{role}'"
    )]
    RwMountOverlapsJudgedSet {
        role: String,
        mount_source: PathBuf,
        root: PathBuf,
    },
    #[error("verdict role '{role}' may not mount runtime secrets")]
    SecretsForJudge { role: String },
    #[error("mount target '{target}' shadows a reserved mission target")]
    ReservedTargetShadowed { target: String },
}

/// The engine-compiled authority of one role: preset + the output axis it
/// was derived from. Fields are private — the only way to obtain one is
/// through the compilers in this module.
#[derive(Debug, Clone)]
pub struct CompiledAuthority {
    role_name: String,
    output: OutputSemantics,
    preset: ExecutionPreset,
}

impl CompiledAuthority {
    pub fn output(&self) -> OutputSemantics {
        self.output
    }

    pub fn preset(&self) -> &ExecutionPreset {
        &self.preset
    }

    pub fn role_name(&self) -> &str {
        &self.role_name
    }

    #[cfg(test)]
    pub(crate) fn for_tests(
        role_name: &str,
        output: OutputSemantics,
        preset: ExecutionPreset,
    ) -> Self {
        Self {
            role_name: role_name.to_string(),
            output,
            preset,
        }
    }
}

/// Compile a role's authority. Workspace access is implied solely by output
/// semantics — there is no author knob to make a judge writable.
pub fn compile_authority(
    role: &RoleDefinition,
    ceiling: &AuthorityCeiling,
) -> Result<CompiledAuthority, MoatViolation> {
    let workspace_access = match role.output {
        OutputSemantics::ProducesArtifact => WorkspaceAccess::ReadWrite,
        OutputSemantics::Plans | OutputSemantics::EmitsVerdict => WorkspaceAccess::ReadOnly,
    };
    if role.secrets && role.output == OutputSemantics::EmitsVerdict {
        // Fail closed rather than silently clamp: a judge asking for secrets
        // is a mission-type bug the author must see.
        return Err(MoatViolation::SecretsForJudge {
            role: role.name.to_string(),
        });
    }
    let preset = ExecutionPreset {
        workspace_access,
        // Enforced from the role's `network` flag (default on — agent roles
        // reach the model API; `network: false` air-gaps the container).
        // Oracles take the separate network-off path (`oracle_authority`).
        network_mode: if role.network {
            NetworkMode::On
        } else {
            NetworkMode::None
        },
        install_policy: match role.output {
            OutputSemantics::ProducesArtifact => InstallPolicy::User,
            _ => InstallPolicy::None,
        },
        mount_runtime_secrets: role.secrets && ceiling.allow_secrets,
        // Roles have no escape-request knob yet: request = ∅, so the
        // intersection with the ceiling is always ∅.
        escape_classes: BTreeSet::new(),
    };
    Ok(CompiledAuthority {
        role_name: role.name.to_string(),
        output: role.output,
        preset,
    })
}

/// The authority an engine-run oracle executes under: a verdict node with
/// no agent — read-only, network off, nothing else.
pub fn oracle_authority(oracle_name: &str) -> CompiledAuthority {
    CompiledAuthority {
        role_name: format!("oracle:{oracle_name}"),
        output: OutputSemantics::EmitsVerdict,
        preset: ExecutionPreset {
            workspace_access: WorkspaceAccess::ReadOnly,
            network_mode: NetworkMode::None,
            install_policy: InstallPolicy::None,
            mount_runtime_secrets: false,
            escape_classes: BTreeSet::new(),
        },
    }
}

/// The mount set the engine builds for one dispatch.
#[derive(Debug, Clone)]
pub struct MissionMounts {
    /// The judged/edited tree → `/workspace`; access is set here and must
    /// agree with the compiled authority.
    pub workspace: MountSpec,
    /// Handoff, scratch, runtime-state mounts — sources must live outside
    /// any judged root.
    pub extras: Vec<MountSpec>,
}

/// Everything needed to compile one dispatch into an enforceable plan.
pub struct RolePlanRequest<'a> {
    pub authority: &'a CompiledAuthority,
    pub runtime_id: String,
    pub confinement: ConfinementConfig,
    pub mounts: MissionMounts,
    /// Canonical roots of the tree(s) any verdict from this node is about.
    pub judged_roots: &'a [PathBuf],
    pub environment: Vec<(String, String)>,
    pub idle_timeout: Duration,
    pub hard_timeout: Duration,
}

/// A moat-vetted execution plan. Private field, no other constructor: the
/// runner cannot receive a plan that did not pass the predicate.
#[derive(Debug, Clone)]
pub struct CompiledRolePlan(EffectiveExecutionPlan);

impl CompiledRolePlan {
    pub fn plan(&self) -> &EffectiveExecutionPlan {
        &self.0
    }
}

/// The single seam where authority becomes an enforceable plan — the moat
/// predicate runs here, fail-closed, for roles and oracles alike.
pub fn compile_role_plan(request: RolePlanRequest<'_>) -> Result<CompiledRolePlan, MoatViolation> {
    let authority = request.authority;
    let role = authority.role_name.clone();

    // (1) Enforcing rung. Exhaustive: a future non-enforcing backend must
    // be classified here before anything compiles under it.
    let enforcing = match request.confinement {
        ConfinementConfig::Oci(_) => true,
    };
    if !enforcing {
        return Err(MoatViolation::NonEnforcingBackend);
    }

    // (2) Reserved targets may not be shadowed by config-supplied mounts —
    // OR by a config tmpfs. A tmpfs is a writable in-memory filesystem the
    // backend applies verbatim, so a tmpfs at (or under) /workspace, /mission,
    // /scratch, /runtime would layer a writable region over the judged tree
    // exactly as a rw bind mount would. `/tmp` is not reserved, so the default
    // scratch tmpfs is unaffected.
    let oci = request.confinement.oci();
    for mount in &oci.additional_mounts {
        if RESERVED_TARGETS
            .iter()
            .any(|reserved| target_shadows(&mount.target, reserved))
        {
            return Err(MoatViolation::ReservedTargetShadowed {
                target: mount.target.clone(),
            });
        }
    }
    for entry in &oci.tmpfs {
        // A tmpfs entry is `<target>[:<options>]`.
        let target = entry.split(':').next().unwrap_or(entry);
        if RESERVED_TARGETS
            .iter()
            .any(|reserved| target_shadows(target, reserved))
        {
            return Err(MoatViolation::ReservedTargetShadowed {
                target: target.to_string(),
            });
        }
    }

    // (3) The verdict floor.
    if authority.output == OutputSemantics::EmitsVerdict {
        if authority.preset.workspace_access != WorkspaceAccess::ReadOnly
            || request.mounts.workspace.access != MountAccess::ReadOnly
        {
            return Err(MoatViolation::WritableJudge { role });
        }
        if authority.preset.mount_runtime_secrets {
            return Err(MoatViolation::SecretsForJudge { role });
        }
        for escape in [EscapeClass::ChannelSend, EscapeClass::ArtifactPublish] {
            if authority.preset.escape_classes.contains(&escape) {
                return Err(MoatViolation::JudgeEscape {
                    role,
                    escape: escape.as_str().to_string(),
                });
            }
        }
        let rw_mounts = request
            .mounts
            .extras
            .iter()
            .chain(request.confinement.oci().additional_mounts.iter())
            .filter(|m| m.access == MountAccess::ReadWrite);
        for mount in rw_mounts {
            let source = canonical_or_lexical(&mount.source);
            for judged in request.judged_roots {
                let judged = canonical_or_lexical(judged);
                if paths_overlap(&source, &judged) {
                    return Err(MoatViolation::RwMountOverlapsJudgedSet {
                        role,
                        mount_source: mount.source.clone(),
                        root: judged,
                    });
                }
            }
        }
    }

    // Compile. The workspace mount's access is forced from the authority —
    // the mount builder cannot widen it.
    let mut workspace = request.mounts.workspace;
    workspace.access = match authority.preset.workspace_access {
        WorkspaceAccess::ReadOnly => MountAccess::ReadOnly,
        WorkspaceAccess::ReadWrite => MountAccess::ReadWrite,
    };
    let working_dir = workspace.source.to_string_lossy().into_owned();
    let mut mounts = vec![workspace];
    mounts.extend(request.mounts.extras);
    let limits = request.confinement.oci().limits.clone();

    Ok(CompiledRolePlan(EffectiveExecutionPlan {
        runtime_id: request.runtime_id,
        preset_name: format!("mission-{}", kind_slug(authority.output)),
        confinement: request.confinement,
        skill_projection: None,
        workspace_access: authority.preset.workspace_access,
        network_mode: authority.preset.network_mode,
        install_policy: authority.preset.install_policy,
        root_in_userns: false,
        working_dir: Some(working_dir),
        environment: request.environment,
        mcp_servers: Vec::new(),
        idle_timeout: request.idle_timeout,
        hard_timeout: request.hard_timeout,
        mounts,
        mount_runtime_secrets: authority.preset.mount_runtime_secrets,
        escape_classes: authority.preset.escape_classes.clone(),
        limits,
    }))
}

fn kind_slug(output: OutputSemantics) -> &'static str {
    match output {
        OutputSemantics::Plans => "plans",
        OutputSemantics::ProducesArtifact => "produces-artifact",
        OutputSemantics::EmitsVerdict => "emits-verdict",
    }
}

/// Resolve symlinks where possible; fall back to the lexical path for a
/// not-yet-existing source (still prefix-checked). Shared by the moat's
/// overlap check and the runner/oracle judged-root construction.
pub(crate) fn canonical_or_lexical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn paths_overlap(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

fn target_shadows(target: &str, reserved: &str) -> bool {
    target == reserved || Path::new(target).starts_with(reserved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RoleName;

    fn role(output: OutputSemantics, secrets: bool) -> RoleDefinition {
        RoleDefinition {
            name: RoleName::new("probe").expect("role name"),
            output,
            runtime: None,
            network: true,
            secrets,
            skills: Vec::new(),
            prompt_body: "p".to_string(),
        }
    }

    fn oci() -> ConfinementConfig {
        ConfinementConfig::Oci(Default::default())
    }

    fn mounts(workspace_access: MountAccess, extras: Vec<MountSpec>) -> MissionMounts {
        MissionMounts {
            workspace: MountSpec {
                source: "/repo".into(),
                target: WORKSPACE_MOUNT_TARGET.to_string(),
                access: workspace_access,
            },
            extras,
        }
    }

    fn request<'a>(
        authority: &'a CompiledAuthority,
        m: MissionMounts,
        judged: &'a [PathBuf],
    ) -> RolePlanRequest<'a> {
        RolePlanRequest {
            authority,
            runtime_id: "codex".to_string(),
            confinement: oci(),
            mounts: m,
            judged_roots: judged,
            environment: Vec::new(),
            idle_timeout: Duration::from_secs(60),
            hard_timeout: Duration::from_secs(120),
        }
    }

    #[test]
    fn output_semantics_imply_workspace_access() {
        let ceiling = AuthorityCeiling::default();
        let worker = compile_authority(&role(OutputSemantics::ProducesArtifact, false), &ceiling)
            .expect("worker");
        assert_eq!(worker.preset().workspace_access, WorkspaceAccess::ReadWrite);
        for output in [OutputSemantics::Plans, OutputSemantics::EmitsVerdict] {
            let authority = compile_authority(&role(output, false), &ceiling).expect("read-only");
            assert_eq!(
                authority.preset().workspace_access,
                WorkspaceAccess::ReadOnly,
                "{output:?} must be read-only"
            );
        }
    }

    #[test]
    fn judge_requesting_secrets_refuses_to_compile() {
        let err = compile_authority(
            &role(OutputSemantics::EmitsVerdict, true),
            &AuthorityCeiling {
                allow_secrets: true,
                ..Default::default()
            },
        )
        .expect_err("must refuse");
        assert!(matches!(err, MoatViolation::SecretsForJudge { .. }));
    }

    #[test]
    fn worker_secrets_are_ceiling_clamped() {
        let ceiling = AuthorityCeiling::default(); // allow_secrets: false
        let authority = compile_authority(&role(OutputSemantics::ProducesArtifact, true), &ceiling)
            .expect("worker");
        assert!(!authority.preset().mount_runtime_secrets);
    }

    #[test]
    fn worker_secrets_are_granted_when_ceiling_allows() {
        // Pins the AND: ceiling permits + role requests ⇒ actually mounted.
        let ceiling = AuthorityCeiling {
            allow_secrets: true,
            ..Default::default()
        };
        let authority = compile_authority(&role(OutputSemantics::ProducesArtifact, true), &ceiling)
            .expect("worker");
        assert!(authority.preset().mount_runtime_secrets);
    }

    #[test]
    fn writable_judge_plan_refuses_to_compile() {
        let ceiling = AuthorityCeiling::default();
        let judge = compile_authority(&role(OutputSemantics::EmitsVerdict, false), &ceiling)
            .expect("judge");
        // A mount builder bug hands the judge a writable workspace: refused.
        let err = compile_role_plan(request(
            &judge,
            mounts(MountAccess::ReadWrite, Vec::new()),
            &[],
        ))
        .expect_err("must refuse");
        assert!(matches!(err, MoatViolation::WritableJudge { .. }));
    }

    #[test]
    fn forged_writable_judge_authority_refuses_to_compile() {
        // Even an authority forged with a writable preset (test-only
        // constructor; production fields are private) hits the moat.
        let forged = CompiledAuthority::for_tests(
            "forged",
            OutputSemantics::EmitsVerdict,
            ExecutionPreset {
                workspace_access: WorkspaceAccess::ReadWrite,
                ..Default::default()
            },
        );
        let err = compile_role_plan(request(
            &forged,
            mounts(MountAccess::ReadOnly, Vec::new()),
            &[],
        ))
        .expect_err("must refuse");
        assert!(matches!(err, MoatViolation::WritableJudge { .. }));
    }

    #[test]
    fn judge_escape_refuses_to_compile() {
        let mut preset = ExecutionPreset {
            workspace_access: WorkspaceAccess::ReadOnly,
            mount_runtime_secrets: false,
            ..Default::default()
        };
        preset.escape_classes.insert(EscapeClass::ChannelSend);
        let forged = CompiledAuthority::for_tests("forged", OutputSemantics::EmitsVerdict, preset);
        let err = compile_role_plan(request(
            &forged,
            mounts(MountAccess::ReadOnly, Vec::new()),
            &[],
        ))
        .expect_err("must refuse");
        assert!(matches!(err, MoatViolation::JudgeEscape { .. }));
    }

    #[test]
    fn rw_mount_overlapping_judged_root_refuses_to_compile() {
        let ceiling = AuthorityCeiling::default();
        let judge = compile_authority(&role(OutputSemantics::EmitsVerdict, false), &ceiling)
            .expect("judge");
        let judged = vec![PathBuf::from("/repo")];
        let overlapping = MountSpec {
            source: "/repo/.lionclaw/handoff".into(),
            target: "/mission/handoff".to_string(),
            access: MountAccess::ReadWrite,
        };
        let err = compile_role_plan(request(
            &judge,
            mounts(MountAccess::ReadOnly, vec![overlapping]),
            &judged,
        ))
        .expect_err("must refuse");
        assert!(matches!(
            err,
            MoatViolation::RwMountOverlapsJudgedSet { .. }
        ));
    }

    #[test]
    fn rw_mount_overlap_detected_through_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        let judged_root = dir.path().join("repo");
        std::fs::create_dir_all(judged_root.join("sub")).expect("mkdir");
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&judged_root, &alias).expect("symlink");

        let ceiling = AuthorityCeiling::default();
        let judge = compile_authority(&role(OutputSemantics::EmitsVerdict, false), &ceiling)
            .expect("judge");
        let judged = vec![judged_root];
        // The rw mount source hides behind a symlink outside the judged
        // root lexically — canonicalization must still catch it.
        let sneaky = MountSpec {
            source: alias.join("sub"),
            target: "/mission/handoff".to_string(),
            access: MountAccess::ReadWrite,
        };
        let err = compile_role_plan(request(
            &judge,
            mounts(MountAccess::ReadOnly, vec![sneaky]),
            &judged,
        ))
        .expect_err("must refuse");
        assert!(matches!(
            err,
            MoatViolation::RwMountOverlapsJudgedSet { .. }
        ));
    }

    #[test]
    fn read_only_mount_overlap_is_allowed_for_judges() {
        let ceiling = AuthorityCeiling::default();
        let judge = compile_authority(&role(OutputSemantics::EmitsVerdict, false), &ceiling)
            .expect("judge");
        let judged = vec![PathBuf::from("/repo")];
        let ro_extra = MountSpec {
            source: "/repo/docs".into(),
            target: "/mission/context".to_string(),
            access: MountAccess::ReadOnly,
        };
        compile_role_plan(request(
            &judge,
            mounts(MountAccess::ReadOnly, vec![ro_extra]),
            &judged,
        ))
        .expect("read-only overlap is fine");
    }

    #[test]
    fn tmpfs_over_judged_workspace_refuses_to_compile() {
        // Regression (review): a writable tmpfs layered over the judged tree
        // bypasses the rw-mount check unless the moat inspects tmpfs too.
        let ceiling = AuthorityCeiling::default();
        let judge = compile_authority(&role(OutputSemantics::EmitsVerdict, false), &ceiling)
            .expect("judge");
        for target in [
            "/workspace",
            "/workspace/sub:rw",
            "/mission/oracle",
            "/scratch",
        ] {
            let mut confinement = oci();
            confinement.oci_mut().tmpfs.push(target.to_string());
            let err = compile_role_plan(RolePlanRequest {
                confinement,
                ..request(&judge, mounts(MountAccess::ReadOnly, Vec::new()), &[])
            })
            .expect_err("tmpfs over the judged tree must refuse");
            assert!(
                matches!(err, MoatViolation::ReservedTargetShadowed { .. }),
                "target {target}: {err:?}"
            );
        }
    }

    #[test]
    fn default_tmp_tmpfs_is_allowed() {
        // The engine's own scratch tmpfs at /tmp is not a reserved target.
        let ceiling = AuthorityCeiling::default();
        let worker = compile_authority(&role(OutputSemantics::ProducesArtifact, false), &ceiling)
            .expect("worker");
        let mut confinement = oci();
        confinement
            .oci_mut()
            .tmpfs
            .push("/tmp:rw,size=512m".to_string());
        compile_role_plan(RolePlanRequest {
            confinement,
            ..request(&worker, mounts(MountAccess::ReadWrite, Vec::new()), &[])
        })
        .expect("/tmp tmpfs compiles");
    }

    #[test]
    fn reserved_target_shadowing_refuses_to_compile() {
        let ceiling = AuthorityCeiling::default();
        let worker = compile_authority(&role(OutputSemantics::ProducesArtifact, false), &ceiling)
            .expect("worker");
        let mut confinement = oci();
        confinement.oci_mut().additional_mounts.push(MountSpec {
            source: "/evil".into(),
            target: "/workspace/vendor".to_string(),
            access: MountAccess::ReadWrite,
        });
        let err = compile_role_plan(RolePlanRequest {
            confinement,
            ..request(&worker, mounts(MountAccess::ReadWrite, Vec::new()), &[])
        })
        .expect_err("must refuse");
        assert!(matches!(err, MoatViolation::ReservedTargetShadowed { .. }));
    }

    #[test]
    fn worker_plan_compiles_read_write_and_forces_workspace_access() {
        let ceiling = AuthorityCeiling::default();
        let worker = compile_authority(&role(OutputSemantics::ProducesArtifact, false), &ceiling)
            .expect("worker");
        // Mount builder handed a read-only workspace; authority forces rw.
        let compiled = compile_role_plan(request(
            &worker,
            mounts(MountAccess::ReadOnly, Vec::new()),
            &[],
        ))
        .expect("compiles");
        assert_eq!(compiled.plan().workspace_access, WorkspaceAccess::ReadWrite);
        assert_eq!(compiled.plan().mounts[0].access, MountAccess::ReadWrite);
        assert_eq!(compiled.plan().network_mode, NetworkMode::On);
    }

    #[test]
    fn oracle_authority_is_network_off_read_only() {
        let authority = oracle_authority("cargo-test");
        assert_eq!(authority.output(), OutputSemantics::EmitsVerdict);
        assert_eq!(
            authority.preset().workspace_access,
            WorkspaceAccess::ReadOnly
        );
        assert_eq!(authority.preset().network_mode, NetworkMode::None);
        assert_eq!(authority.preset().install_policy, InstallPolicy::None);
        let compiled = compile_role_plan(request(
            &authority,
            mounts(MountAccess::ReadOnly, Vec::new()),
            &[PathBuf::from("/repo")],
        ))
        .expect("oracle plan compiles");
        assert_eq!(compiled.plan().network_mode, NetworkMode::None);
    }

    #[test]
    fn network_flag_is_enforced() {
        let ceiling = AuthorityCeiling::default();
        // Default (true) → network on.
        let on = compile_authority(&role(OutputSemantics::ProducesArtifact, false), &ceiling)
            .expect("worker");
        assert_eq!(on.preset().network_mode, NetworkMode::On);
        // Opt out → network off, honored (not silently ignored).
        let mut air_gapped = role(OutputSemantics::ProducesArtifact, false);
        air_gapped.network = false;
        let off = compile_authority(&air_gapped, &ceiling).expect("worker");
        assert_eq!(off.preset().network_mode, NetworkMode::None);
    }
}
