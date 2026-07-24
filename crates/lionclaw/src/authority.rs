//! Authority compilation and the moat predicate.
//!
//! Authority is engine-internal: each team role requests grants across the
//! secrets, network, install, writes, devices, and inputs axes. Everything
//! enforceable is compiled here as
//! `effective = role_request ∩ ceiling ∩ semantics_floor`, then
//! container-enforced. **The moat**: every non-artifact role is read-only on an
//! enforcing rung. Proof roles additionally receive no secrets, no escape
//! class that could feed back, and no read-write mount overlapping the judged
//! set. Any violation refuses to compile; the mission never starts.
//!
//! [`CompiledRolePlan`] has one constructor, [`compile_role_plan`], and the
//! runner accepts nothing else: an un-vetted plan is unrepresentable.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use lionclaw_confinement::{
    parse_runtime_tmpfs_entry, ConfinementConfig, EffectiveExecutionPlan, ExecutionPreset,
    InstallPolicy, MountAccess, MountSpec, NetworkMode, WorkspaceAccess, RUNTIME_HOME_MOUNT_TARGET,
    RUNTIME_MOUNT_TARGET, WORKSPACE_MOUNT_TARGET,
};

use crate::model::{ConfinementResources, ConfinementTmpfsResource, OutputSemantics, RoleInstance};

/// Mission targets no mount may shadow.
const RESERVED_TARGETS: &[&str] = &[
    WORKSPACE_MOUNT_TARGET,
    RUNTIME_MOUNT_TARGET,
    RUNTIME_HOME_MOUNT_TARGET,
    "/mission",
    "/scratch",
    "/inputs",
    "/output",
    "/lionclaw",
];

/// The operator/mission bound authority can never exceed. Intersected,
/// never unioned. (`allowed_escapes` returns here alongside a real per-role
/// escape-request knob; until then roles request no escapes, so the ceiling's
/// escape set would always be the empty intersection.)
#[derive(Debug, Clone, Default)]
pub struct AuthorityCeiling {
    pub allow_secrets: bool,
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
    #[error("invalid tmpfs entry '{entry}': {detail}")]
    InvalidTmpfs { entry: String, detail: String },
    #[error("resource override for '{role}' is invalid: {detail}")]
    InvalidResourceOverride { role: String, detail: String },
    #[error("resource override for '{role}' exceeds mission ceiling: {detail}")]
    ResourceExceedsCeiling { role: String, detail: String },
    #[error("resource override for '{role}' would shrink profile default: {detail}")]
    ResourceShrinksDefault { role: String, detail: String },
}

/// The engine-compiled authority of one role: preset + the output axis it
/// was derived from. Fields are private — the only way to obtain one is
/// through the compilers in this module.
#[derive(Debug, Clone)]
pub struct CompiledAuthority {
    role_name: String,
    output: OutputSemantics,
    preset: ExecutionPreset,
    devices: BTreeSet<String>,
}

impl CompiledAuthority {
    pub fn output(&self) -> OutputSemantics {
        self.output
    }

    pub fn preset(&self) -> &ExecutionPreset {
        &self.preset
    }
}

/// Compile a role's authority. Workspace access is implied solely by output
/// semantics — there is no author knob to make a judge writable.
pub fn compile_authority(
    role: &RoleInstance,
    ceiling: &AuthorityCeiling,
) -> Result<CompiledAuthority, MoatViolation> {
    validate_role_authority_request(role)?;
    let workspace_access = match role.output {
        OutputSemantics::ProducesArtifact => WorkspaceAccess::ReadWrite,
        OutputSemantics::ProducesReport
        | OutputSemantics::EmitsVerdict
        | OutputSemantics::EmitsGapVerdict
        | OutputSemantics::ProposesPlan => WorkspaceAccess::ReadOnly,
    };
    let preset = ExecutionPreset {
        workspace_access,
        // Enforced from the role's `network` flag (default on — agent roles
        // reach the model API; `network: false` air-gaps the container).
        // Oracles take the separate network-off path (`oracle_authority`).
        network_mode: if role.grants.network {
            NetworkMode::On
        } else {
            NetworkMode::None
        },
        install_policy: if role.grants.install {
            InstallPolicy::User
        } else {
            InstallPolicy::None
        },
        mount_runtime_secrets: role.grants.secrets && ceiling.allow_secrets,
        // Roles have no escape-request knob yet: request = ∅, so the
        // intersection with the ceiling is always ∅.
        escape_classes: BTreeSet::new(),
    };
    Ok(CompiledAuthority {
        role_name: role.id.to_string(),
        output: role.output,
        preset,
        devices: role.grants.devices.clone(),
    })
}

/// Validate role-authored authority before any operator ceiling is applied.
/// A ceiling may remove grants; it can never legalize a request that breaks
/// the semantic moat.
pub(crate) fn validate_role_authority_request(role: &RoleInstance) -> Result<(), MoatViolation> {
    let proof_role = matches!(
        role.output,
        OutputSemantics::EmitsVerdict | OutputSemantics::EmitsGapVerdict
    );
    if proof_role && role.grants.secrets {
        return Err(MoatViolation::SecretsForJudge {
            role: role.id.to_string(),
        });
    }
    if proof_role && role.grants.writes {
        return Err(MoatViolation::WritableJudge {
            role: role.id.to_string(),
        });
    }
    if role.output == OutputSemantics::ProducesArtifact && !role.grants.writes {
        return Err(MoatViolation::WritableJudge {
            role: role.id.to_string(),
        });
    }
    Ok(())
}

/// The authority an engine-run oracle executes under: a verdict node with
/// no agent — read-only, network off, nothing else.
pub fn oracle_authority(oracle_name: &str) -> CompiledAuthority {
    oracle_authority_with_devices(oracle_name, BTreeSet::new())
}

pub fn oracle_authority_with_devices(
    oracle_name: &str,
    devices: BTreeSet<String>,
) -> CompiledAuthority {
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
        devices,
    }
}

/// Authority for producing one declared, immutable mission input. The source
/// workspace is read-only and secret-free; only the declaration controls
/// network access, and the output mount lives outside the judged tree.
pub fn prepared_input_authority(input_name: &str, network: bool) -> CompiledAuthority {
    CompiledAuthority {
        role_name: format!("input:{input_name}"),
        output: OutputSemantics::EmitsVerdict,
        preset: ExecutionPreset {
            workspace_access: WorkspaceAccess::ReadOnly,
            network_mode: if network {
                NetworkMode::On
            } else {
                NetworkMode::None
            },
            install_policy: InstallPolicy::None,
            mount_runtime_secrets: false,
            escape_classes: BTreeSet::new(),
        },
        devices: BTreeSet::new(),
    }
}

/// The mount set the engine builds for one dispatch.
#[derive(Debug, Clone)]
pub struct MissionMounts {
    /// The judged/edited tree. The compiler creates its `/workspace` mount and
    /// derives access solely from the compiled authority.
    pub workspace: PathBuf,
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
    pub resources: ConfinementResources,
    pub resource_ceilings: &'a ConfinementResources,
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
    let mut confinement = request.confinement;

    apply_resource_overrides(
        &mut confinement,
        &request.resources,
        request.resource_ceilings,
        &role,
    )?;

    // (1) Enforcing rung. Exhaustive: a future non-enforcing backend must
    // be classified here before anything compiles under it.
    let enforcing = match &confinement {
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
    let oci = confinement.oci();
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
        let parsed =
            parse_runtime_tmpfs_entry(entry).map_err(|detail| MoatViolation::InvalidTmpfs {
                entry: entry.clone(),
                detail,
            })?;
        let target = parsed.target();
        if RESERVED_TARGETS
            .iter()
            .any(|reserved| target_shadows(target, reserved))
        {
            return Err(MoatViolation::ReservedTargetShadowed {
                target: target.to_string(),
            });
        }
    }

    // (3) Every non-artifact role is read-only. Proof roles additionally stay
    // secret-free and escape-free; planning/report roles may hold bounded
    // credentials without gaining write authority over the judged tree.
    if authority.output != OutputSemantics::ProducesArtifact {
        if authority.preset.workspace_access != WorkspaceAccess::ReadOnly {
            return Err(MoatViolation::WritableJudge { role });
        }
        let proof_role = matches!(
            authority.output,
            OutputSemantics::EmitsVerdict | OutputSemantics::EmitsGapVerdict
        );
        if proof_role && authority.preset.mount_runtime_secrets {
            return Err(MoatViolation::SecretsForJudge { role });
        }
        // Escape-free means *no* escape class, not just the two we thought of:
        // any non-empty set (NetEgress, SecretRequest, SchedulerRun, …) is a
        // channel a judge/planner could use to influence what it reads. Refuse
        // the whole set so a newly-added variant can never silently slip through.
        if proof_role {
            if let Some(escape) = authority.preset.escape_classes.iter().next() {
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
            .chain(confinement.oci().additional_mounts.iter())
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

    // Compile. Callers provide only a source path; workspace access exists in
    // one place and cannot be widened by a mount builder.
    let workspace = MountSpec {
        source: request.mounts.workspace,
        target: WORKSPACE_MOUNT_TARGET.to_string(),
        access: match authority.preset.workspace_access {
            WorkspaceAccess::ReadOnly => MountAccess::ReadOnly,
            WorkspaceAccess::ReadWrite => MountAccess::ReadWrite,
        },
    };
    let working_dir = workspace.source.to_string_lossy().into_owned();
    let mut mounts = vec![workspace];
    mounts.extend(request.mounts.extras);
    mounts.extend(confinement.oci().additional_mounts.clone());
    let limits = confinement.oci().limits.clone();

    Ok(CompiledRolePlan(EffectiveExecutionPlan {
        runtime_id: request.runtime_id,
        preset_name: format!("mission-{}", authority.output.slug()),
        confinement,
        workspace_access: authority.preset.workspace_access,
        network_mode: authority.preset.network_mode,
        install_policy: authority.preset.install_policy,
        root_in_userns: false,
        working_dir: Some(working_dir),
        environment: request.environment,
        mcp_servers: Vec::new(),
        mounts,
        mount_runtime_secrets: authority.preset.mount_runtime_secrets,
        devices: authority.devices.clone(),
        escape_classes: authority.preset.escape_classes.clone(),
        limits,
    }))
}

fn apply_resource_overrides(
    confinement: &mut ConfinementConfig,
    overrides: &ConfinementResources,
    ceilings: &ConfinementResources,
    role: &str,
) -> Result<(), MoatViolation> {
    if overrides.is_empty() {
        return Ok(());
    }
    overrides
        .validate()
        .map_err(|detail| MoatViolation::InvalidResourceOverride {
            role: role.to_string(),
            detail,
        })?;
    overrides
        .within(ceilings)
        .map_err(|detail| MoatViolation::ResourceExceedsCeiling {
            role: role.to_string(),
            detail,
        })?;

    let oci = confinement.oci_mut();
    let defaults =
        tmpfs_by_target(&oci.tmpfs).map_err(|detail| MoatViolation::InvalidResourceOverride {
            role: role.to_string(),
            detail: format!("profile tmpfs is invalid: {detail}"),
        })?;
    let mut effective = defaults
        .iter()
        .map(|(target, entry)| (target.clone(), entry.runtime_argument()))
        .collect::<std::collections::BTreeMap<_, _>>();
    for override_entry in &overrides.tmpfs {
        let parsed = ConfinementTmpfsResource::parse(override_entry).map_err(|detail| {
            MoatViolation::InvalidResourceOverride {
                role: role.to_string(),
                detail: format!("tmpfs entry '{override_entry}': {detail}"),
            }
        })?;
        if let Some(default) = defaults.get(parsed.target()) {
            if parsed.size_bytes() < default.size_bytes() {
                return Err(MoatViolation::ResourceShrinksDefault {
                    role: role.to_string(),
                    detail: format!(
                        "tmpfs target '{}' requests {}, below profile default {}",
                        parsed.target(),
                        parsed.runtime_argument(),
                        default.runtime_argument()
                    ),
                });
            }
        }
        effective.insert(parsed.target().to_string(), parsed.runtime_argument());
    }
    oci.tmpfs = effective.into_values().collect();
    Ok(())
}

fn tmpfs_by_target(
    entries: &[String],
) -> Result<std::collections::BTreeMap<String, ConfinementTmpfsResource>, String> {
    let mut parsed = std::collections::BTreeMap::new();
    for entry in entries {
        let tmpfs = parse_runtime_tmpfs_entry(entry)?;
        let resource = ConfinementTmpfsResource::parse(tmpfs.argument())?;
        if parsed
            .insert(resource.target().to_string(), resource)
            .is_some()
        {
            return Err(format!("tmpfs target declared more than once in '{entry}'"));
        }
    }
    Ok(parsed)
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
mod team_authority_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::model::{AuthorityGrants, RoleInstanceId};

    fn role(output: OutputSemantics, grants: AuthorityGrants) -> RoleInstance {
        RoleInstance {
            id: RoleInstanceId::new("specialist").unwrap(),
            purpose: "specialist".into(),
            output,
            runtime: "codex".into(),
            instructions: "Perform the assigned work.".into(),
            skills: Vec::new(),
            environment: BTreeMap::new(),
            grants,
            resources: Default::default(),
            deadline_secs: None,
        }
    }

    #[test]
    fn device_grants_reach_the_compiled_plan_without_hardware() {
        let grants = AuthorityGrants {
            writes: true,
            devices: BTreeSet::from(["/dev/dri".to_string()]),
            ..Default::default()
        };
        let authority = compile_authority(
            &role(OutputSemantics::ProducesArtifact, grants),
            &Default::default(),
        )
        .unwrap();
        let compiled = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement: ConfinementConfig::Oci(Default::default()),
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources::default(),
            resource_ceilings: &ConfinementResources::default(),
        })
        .unwrap();
        assert_eq!(
            compiled.plan().devices,
            BTreeSet::from(["/dev/dri".to_string()])
        );
    }

    #[test]
    fn oracle_device_declarations_reach_the_compiled_plan_without_hardware() {
        let authority = oracle_authority_with_devices(
            "metric-scalar",
            BTreeSet::from(["/dev/dri".to_string()]),
        );
        let compiled = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement: ConfinementConfig::Oci(Default::default()),
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources::default(),
            resource_ceilings: &ConfinementResources::default(),
        })
        .unwrap();
        assert_eq!(
            compiled.plan().devices,
            BTreeSet::from(["/dev/dri".to_string()])
        );
    }

    #[test]
    fn proof_floor_rejects_secrets_while_planning_can_receive_them() {
        let grants = AuthorityGrants {
            secrets: true,
            ..Default::default()
        };
        assert!(matches!(
            compile_authority(
                &role(OutputSemantics::EmitsVerdict, grants.clone()),
                &AuthorityCeiling {
                    allow_secrets: true
                }
            ),
            Err(MoatViolation::SecretsForJudge { .. })
        ));
        assert!(compile_authority(
            &role(OutputSemantics::ProposesPlan, grants),
            &AuthorityCeiling {
                allow_secrets: true
            }
        )
        .is_ok());
    }

    #[test]
    fn l19_role_resource_override_replaces_profile_tmpfs_within_ceiling() {
        let mut confinement = ConfinementConfig::Oci(Default::default());
        confinement.oci_mut().tmpfs = vec!["/tmp:rw,size=512m".to_string()];
        let authority = compile_authority(
            &role(
                OutputSemantics::ProducesArtifact,
                AuthorityGrants {
                    writes: true,
                    ..Default::default()
                },
            ),
            &Default::default(),
        )
        .unwrap();
        let compiled = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement,
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=1536m".to_string()],
            },
            resource_ceilings: &ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=2g".to_string()],
            },
        })
        .unwrap();

        assert_eq!(
            compiled.plan().confinement.oci().tmpfs,
            vec!["/tmp:rw,size=1536m".to_string()]
        );
    }

    #[test]
    fn l19_oracle_without_resource_override_keeps_profile_tmpfs_default() {
        let mut confinement = ConfinementConfig::Oci(Default::default());
        confinement.oci_mut().tmpfs = vec!["/tmp:rw,size=512m".to_string()];
        let authority = oracle_authority("cargo-test");
        let compiled = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement,
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources::default(),
            resource_ceilings: &ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=2g".to_string()],
            },
        })
        .unwrap();

        assert_eq!(
            compiled.plan().confinement.oci().tmpfs,
            vec!["/tmp:rw,size=512m".to_string()]
        );
    }

    #[test]
    fn l19_oracle_resource_override_above_ceiling_refuses_at_compile_role_plan() {
        let mut confinement = ConfinementConfig::Oci(Default::default());
        confinement.oci_mut().tmpfs = vec!["/tmp:rw,size=512m".to_string()];
        let authority = oracle_authority("cargo-test");
        let err = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement,
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=3g".to_string()],
            },
            resource_ceilings: &ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=2g".to_string()],
            },
        })
        .expect_err("over-ceiling resource override must refuse before runtime");

        assert!(matches!(err, MoatViolation::ResourceExceedsCeiling { .. }));
    }

    #[test]
    fn l19_resource_override_below_profile_default_is_refused_at_compile_role_plan() {
        let mut confinement = ConfinementConfig::Oci(Default::default());
        confinement.oci_mut().tmpfs = vec!["/tmp:rw,size=512m".to_string()];
        let authority = oracle_authority("cargo-test");
        let err = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement,
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=256m".to_string()],
            },
            resource_ceilings: &ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=2g".to_string()],
            },
        })
        .expect_err("resource override must not shrink the profile tmpfs default");

        assert!(matches!(err, MoatViolation::ResourceShrinksDefault { .. }));
    }

    #[test]
    fn resource_override_cannot_smuggle_tmpfs_authority_flags() {
        let mut confinement = ConfinementConfig::Oci(Default::default());
        confinement.oci_mut().tmpfs = vec!["/tmp:rw,size=512m".to_string()];
        let authority = oracle_authority("cargo-test");
        let err = compile_role_plan(RolePlanRequest {
            authority: &authority,
            runtime_id: "codex".into(),
            confinement,
            mounts: MissionMounts {
                workspace: "/tmp/work".into(),
                extras: Vec::new(),
            },
            judged_roots: &["/tmp/work".into()],
            environment: Vec::new(),
            resources: ConfinementResources {
                tmpfs: vec!["/tmp:rw,exec,size=1536m".to_string()],
            },
            resource_ceilings: &ConfinementResources {
                tmpfs: vec!["/tmp:rw,size=2g".to_string()],
            },
        })
        .expect_err("resource override must not pass through tmpfs authority flags");

        assert!(matches!(err, MoatViolation::InvalidResourceOverride { .. }));
    }
}
