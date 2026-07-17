//! Fail-closed mission-type loading. A mission type is a directory of prose; every role
//! compiles through the moat at load time, every oracle is a real executable,
//! or the mission type does not load and the mission never starts.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::authority::{compile_authority, AuthorityCeiling, MoatViolation};
use crate::model::{
    InputName, OracleName, PlanningDag, PlanningTask, RoleName, StopBar, TerminalReviewConfig,
};

use super::bounded_tree::{BoundedTree, ControlTextBudget};
use super::digest::ContentDigest;
use super::frontmatter::{parse_role_file, RoleFrontmatter};
use super::manifest::{is_path_safe_name, ManifestFile, ManifestInput, ManifestPlanningDag};
use super::prepared_input::{validate_prepared_inputs, PreparedInputContractError};
use super::skills::load_skills;
use super::{
    has_shebang, is_executable, MissionType, MissionTypeDefinition, PreparedInput, RoleDefinition,
    SkillPackage,
};

#[derive(Debug, thiserror::Error)]
pub enum MissionTypeError {
    #[error("io error at '{path}': {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid mission.toml: {0}")]
    Manifest(String),
    #[error("role '{role}' is invalid: {detail}")]
    Role { role: String, detail: String },
    #[error("role '{role}' does not satisfy the moat: {violation}")]
    Moat {
        role: String,
        #[source]
        violation: MoatViolation,
    },
    #[error("oracle '{oracle}' is invalid: {detail}")]
    Oracle { oracle: String, detail: String },
    #[error("prepared input '{input}' is invalid: {detail}")]
    Input { input: String, detail: String },
    #[error("skill '{skill}' is invalid: {detail}")]
    Skill { skill: String, detail: String },
    #[error("mission type at '{0}' has no roles")]
    NoRoles(PathBuf),
}

pub fn load_mission_type(
    source: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType, MissionTypeError> {
    let owned = tempfile::Builder::new()
        .prefix("lionclaw-mission-type-")
        .tempdir()
        .map_err(|source_error| MissionTypeError::Io {
            path: source.to_path_buf(),
            source: source_error,
        })?;
    let root = owned.path().join("bundle");
    let source_tree =
        BoundedTree::open(source).map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    source_tree
        .copy_to(&root)
        .map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    load_materialized_mission_type(&root, ceiling)
        .map(|mission_type| mission_type.with_source_owner(std::sync::Arc::new(owned)))
}

/// Load a LionClaw-owned closed tree. Arbitrary source directories must cross
/// `load_mission_type` or `materialize_mission_type` first so semantic path
/// reads and later runtime mounts cannot observe a different tree than the one
/// admitted through `BoundedTree`.
pub(crate) fn load_materialized_mission_type(
    root: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType, MissionTypeError> {
    let tree =
        BoundedTree::open(root).map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    let mut text_budget = ControlTextBudget::default();
    let manifest_path = root.join("mission.toml");
    let manifest_text = read(&tree, Path::new("mission.toml"), &mut text_budget)?;
    let manifest: ManifestFile = toml::from_str(&manifest_text)
        .map_err(|e| MissionTypeError::Manifest(format!("{manifest_path:?}: {e}")))?;
    if !is_path_safe_name(&manifest.mission_type.name) {
        return Err(MissionTypeError::Manifest(format!(
            "mission type name '{}' is not path-safe",
            manifest.mission_type.name
        )));
    }
    let stop = match manifest.mission_type.stop.as_str() {
        "verified" => StopBar::Verified,
        "reviewed" => StopBar::Reviewed,
        other => {
            return Err(MissionTypeError::Manifest(format!(
                "stop must be 'verified' or 'reviewed', got '{other}'"
            )))
        }
    };
    let skills = load_skills(root, &tree, &mut text_budget)?;
    let inputs = load_inputs(root, manifest.inputs)?;
    let oracles = load_oracles(&root.join("oracles"))?;
    let roles = load_roles(
        root,
        &tree,
        &root.join("roles"),
        ceiling,
        &skills,
        &mut text_budget,
    )?;
    if roles.is_empty() {
        return Err(MissionTypeError::NoRoles(root.to_path_buf()));
    }
    let planning = resolve_planning_dag(manifest.planning, &roles)?;

    let terminal_review = match &manifest.terminal_review {
        None => None,
        Some(declared) => {
            let role_name = RoleName::new(&declared.role)
                .map_err(|e| MissionTypeError::Manifest(format!("[terminal-review] role: {e}")))?;
            Some(TerminalReviewConfig { role: role_name })
        }
    };

    let playbook = read(&tree, Path::new("playbook.md"), &mut text_budget)?;
    if playbook.trim().is_empty() {
        return Err(MissionTypeError::Manifest(
            "playbook.md must not be empty".to_string(),
        ));
    }
    let digest = compute_digest(&tree)?;

    let definition = MissionTypeDefinition {
        name: manifest.mission_type.name,
        stop,
        image: manifest.mission_type.image,
        planning,
        recovery: manifest.recovery,
        execution: manifest.execution,
        terminal_review,
        playbook: Some(playbook),
        roles,
        skills,
        inputs,
        oracles,
    };
    let mission_type = MissionType::from_loaded(definition, digest);
    mission_type
        .validate_at(0)
        .map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    Ok(mission_type)
}

fn resolve_planning_dag(
    dag: ManifestPlanningDag,
    roles: &BTreeMap<RoleName, RoleDefinition>,
) -> Result<PlanningDag, MissionTypeError> {
    let tasks = dag
        .tasks
        .into_iter()
        .map(|task| {
            let output = roles
                .get(&task.role)
                .map(|role| role.output)
                .ok_or_else(|| {
                    MissionTypeError::Manifest(format!(
                    "[planning] task '{}' names role '{}' which the mission type does not provide",
                    task.id, task.role
                ))
                })?;
            Ok(PlanningTask {
                id: task.id,
                role: task.role,
                output,
                body: task.body,
                depends_on: task.depends_on,
            })
        })
        .collect::<Result<Vec<_>, MissionTypeError>>()?;
    Ok(PlanningDag { tasks })
}

fn load_inputs(
    root: &Path,
    declared: Vec<ManifestInput>,
) -> Result<BTreeMap<InputName, PreparedInput>, MissionTypeError> {
    let mut inputs = BTreeMap::new();
    for input in declared {
        let name = InputName::new(&input.name).map_err(|error| MissionTypeError::Input {
            input: input.name.clone(),
            detail: error.to_string(),
        })?;
        let program = root.join("inputs").join(name.as_str());
        if inputs
            .insert(
                name.clone(),
                PreparedInput {
                    name,
                    program,
                    network: input.network,
                    key_files: input.key_files,
                    environment: input.environment,
                },
            )
            .is_some()
        {
            return Err(MissionTypeError::Input {
                input: input.name,
                detail: "name is declared more than once".to_string(),
            });
        }
    }
    validate_prepared_inputs(&inputs).map_err(map_prepared_input_error)?;
    Ok(inputs)
}

fn map_prepared_input_error(error: PreparedInputContractError) -> MissionTypeError {
    match error {
        PreparedInputContractError::Invalid { input, detail } => {
            MissionTypeError::Input { input, detail }
        }
        PreparedInputContractError::Io { path, source } => MissionTypeError::Io { path, source },
    }
}

fn load_roles(
    root: &Path,
    tree: &BoundedTree,
    dir: &Path,
    ceiling: &AuthorityCeiling,
    packages: &BTreeMap<String, SkillPackage>,
    text_budget: &mut ControlTextBudget,
) -> Result<BTreeMap<RoleName, RoleDefinition>, MissionTypeError> {
    let mut roles = BTreeMap::new();
    if !dir.exists() {
        return Ok(roles);
    }
    for entry in read_dir(dir)? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        let stem =
            path.file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| MissionTypeError::Role {
                    role: path.display().to_string(),
                    detail: "non-utf8 filename".to_string(),
                })?;
        let name = RoleName::new(stem).map_err(|e| MissionTypeError::Role {
            role: stem.to_string(),
            detail: e.to_string(),
        })?;
        let relative = path
            .strip_prefix(root)
            .map_err(|_| MissionTypeError::Role {
                role: stem.to_string(),
                detail: "role path escaped the mission root".to_string(),
            })?;
        let text = read(tree, relative, text_budget)?;
        let RoleFrontmatter {
            output,
            network,
            secrets,
            runtime,
            timeout_secs,
            skills,
            prompt_body,
        } = parse_role_file(&text).map_err(|e| MissionTypeError::Role {
            role: stem.to_string(),
            detail: e,
        })?;
        let mut seen_skills = std::collections::BTreeSet::new();
        for skill in &skills {
            lionclaw_confinement::validate_skill_alias(skill).map_err(|err| {
                MissionTypeError::Role {
                    role: stem.to_string(),
                    detail: err.to_string(),
                }
            })?;
            if !seen_skills.insert(skill) {
                return Err(MissionTypeError::Role {
                    role: stem.to_string(),
                    detail: format!("declares skill '{skill}' more than once"),
                });
            }
            if !packages.contains_key(skill) {
                return Err(MissionTypeError::Role {
                    role: stem.to_string(),
                    detail: format!("references missing skill package '{skill}'"),
                });
            }
        }
        let role = RoleDefinition {
            name: name.clone(),
            output,
            runtime,
            timeout_secs,
            network,
            secrets,
            skills,
            prompt_body,
        };
        // Fail-closed moat check at load time: an authority that cannot
        // compile (e.g. a judge requesting secrets) rejects the mission type.
        compile_authority(&role, ceiling).map_err(|violation| MissionTypeError::Moat {
            role: stem.to_string(),
            violation,
        })?;
        roles.insert(name, role);
    }
    Ok(roles)
}

fn load_oracles(dir: &Path) -> Result<BTreeMap<OracleName, PathBuf>, MissionTypeError> {
    let mut oracles = BTreeMap::new();
    if !dir.exists() {
        return Ok(oracles);
    }
    for entry in read_dir(dir)? {
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| MissionTypeError::Io {
            path: path.clone(),
            source: e,
        })?;
        // No symlinks out of the mission type: an oracle must be a regular file.
        if !file_type.is_file() {
            return Err(MissionTypeError::Oracle {
                oracle: path.display().to_string(),
                detail: "must be a regular file (no symlinks)".to_string(),
            });
        }
        let stem =
            path.file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| MissionTypeError::Oracle {
                    oracle: path.display().to_string(),
                    detail: "non-utf8 filename".to_string(),
                })?;
        let name = OracleName::new(stem).map_err(|e| MissionTypeError::Oracle {
            oracle: stem.to_string(),
            detail: e.to_string(),
        })?;
        if !is_executable(&path) {
            return Err(MissionTypeError::Oracle {
                oracle: stem.to_string(),
                detail: "must be executable".to_string(),
            });
        }
        if !has_shebang(&path) {
            return Err(MissionTypeError::Oracle {
                oracle: stem.to_string(),
                detail: "must start with a #! shebang".to_string(),
            });
        }
        oracles.insert(name, path);
    }
    Ok(oracles)
}

fn read(
    tree: &BoundedTree,
    relative: &Path,
    text_budget: &mut ControlTextBudget,
) -> Result<String, MissionTypeError> {
    text_budget
        .read(tree, relative)
        .map_err(|error| MissionTypeError::Manifest(error.to_string()))
}

/// A content digest over everything the loader consumes: manifest, optional
/// lock/playbook, roles, oracles, and each resolved skill package recursively.
/// Entries contribute logical path, bytes, and executable bit. Verified on
/// every engine open, so mutated mission behavior is caught.
fn compute_digest(tree: &BoundedTree) -> Result<String, MissionTypeError> {
    let mut digest = ContentDigest::new();
    tree.feed_digest(&mut digest, "")
        .map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    Ok(digest.finish())
}

fn read_dir(dir: &Path) -> Result<Vec<std::fs::DirEntry>, MissionTypeError> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|source| MissionTypeError::Io {
            path: dir.to_path_buf(),
            source,
        })?
        .collect::<Result<_, _>>()
        .map_err(|source| MissionTypeError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
    // Deterministic order.
    entries.sort_by_key(|e| e.file_name());
    Ok(entries)
}
