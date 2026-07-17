//! Fail-closed mission-type loading. A mission type is a directory of prose; every role
//! compiles through the moat at load time, every oracle is a real executable,
//! or the mission type does not load and the mission never starts.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::authority::{compile_authority, AuthorityCeiling, MoatViolation};
use crate::model::{
    InputName, OracleName, OutputSemantics, PlanningDag, PlanningTask, RoleName, StopBar,
    TerminalReviewConfig,
};

use super::digest::ContentDigest;
use super::frontmatter::{parse_role_file, RoleFrontmatter};
use super::install::validate_closed_tree;
use super::manifest::{
    is_path_safe_name, ManifestFile, ManifestInput, ManifestPlanningDag, MISSION_LOCK_FILE,
};
use super::skills::{load_skills, package_files};
use super::{MissionType, PreparedInput, RoleDefinition, SkillPackage};

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
    root: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType, MissionTypeError> {
    let root_metadata = std::fs::symlink_metadata(root).map_err(|source| MissionTypeError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(MissionTypeError::Manifest(format!(
            "mission type root '{}' must be a directory, not a symlink",
            root.display()
        )));
    }
    validate_closed_tree(root).map_err(|err| MissionTypeError::Manifest(err.to_string()))?;
    let manifest_path = root.join("mission.toml");
    let manifest_text = read(&manifest_path)?;
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
    if manifest.recovery.max_attempts == 0 {
        return Err(MissionTypeError::Manifest(
            "[recovery] max-attempts must be at least 1".to_string(),
        ));
    }
    manifest
        .execution
        .validate()
        .map_err(|error| MissionTypeError::Manifest(format!("[execution] {error}")))?;

    let skills = load_skills(root)?;
    let inputs = load_inputs(root, manifest.inputs)?;
    let oracles = load_oracles(&root.join("oracles"))?;
    let roles = load_roles(&root.join("roles"), ceiling, &skills)?;
    if roles.is_empty() {
        return Err(MissionTypeError::NoRoles(root.to_path_buf()));
    }
    let planning = resolve_planning_dag(manifest.planning, &roles)?;

    // The closing review, fail-closed like the planning DAG: the named role
    // must exist and be a judge. An agent-graded bar without an independent
    // closing review would be the workers' own validators agreeing with the
    // workers — so `reviewed` requires the declaration.
    let terminal_review = match &manifest.terminal_review {
        None if stop == StopBar::Reviewed => {
            return Err(MissionTypeError::Manifest(
                "stop = \"reviewed\" requires [terminal-review]: the reviewed bar is \
                 defined by an independent terminal review"
                    .to_string(),
            ));
        }
        None => None,
        Some(declared) => {
            let role_name = RoleName::new(&declared.role)
                .map_err(|e| MissionTypeError::Manifest(format!("[terminal-review] role: {e}")))?;
            let Some(role) = roles.get(&role_name) else {
                return Err(MissionTypeError::Manifest(format!(
                    "[terminal-review] role '{}' is not provided by this mission type",
                    declared.role
                )));
            };
            if role.output != OutputSemantics::EmitsGapVerdict {
                return Err(MissionTypeError::Manifest(format!(
                    "[terminal-review] role '{}' must be emits-gap-verdict, got {}",
                    declared.role,
                    role.output.slug()
                )));
            }
            Some(TerminalReviewConfig { role: role_name })
        }
    };

    let playbook = read(&root.join("playbook.md"))?;
    if playbook.trim().is_empty() {
        return Err(MissionTypeError::Manifest(
            "playbook.md must not be empty".to_string(),
        ));
    }
    let digest = compute_digest(root, &skills)?;

    let mission_type = MissionType {
        name: manifest.mission_type.name,
        digest,
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
    // Validate the planning DAG fail-closed at load against this type's own
    // inventory (roles must be planning roles; the author is the unique sink).
    let errors =
        crate::model::validate_planning_dag(&mission_type.planning, &mission_type.inventory());
    if !errors.is_empty() {
        return Err(MissionTypeError::Manifest(format!(
            "[planning] is invalid:\n{}",
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        )));
    }
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
    let mut environment_owners = BTreeMap::<String, InputName>::new();
    for input in declared {
        let name = InputName::new(&input.name).map_err(|error| MissionTypeError::Input {
            input: input.name.clone(),
            detail: error.to_string(),
        })?;
        if input.key_files.is_empty() {
            return Err(MissionTypeError::Input {
                input: input.name,
                detail: "key-files must contain at least one workspace path".to_string(),
            });
        }
        let mut seen = std::collections::BTreeSet::new();
        for path in &input.key_files {
            if !safe_relative_path(path) {
                return Err(MissionTypeError::Input {
                    input: input.name.clone(),
                    detail: format!(
                        "key-file '{}' must be a non-empty relative path without traversal",
                        path.display()
                    ),
                });
            }
            if !seen.insert(path) {
                return Err(MissionTypeError::Input {
                    input: input.name.clone(),
                    detail: format!("key-file '{}' is declared more than once", path.display()),
                });
            }
        }
        for variable in input.environment.keys() {
            if !valid_environment_name(variable) {
                return Err(MissionTypeError::Input {
                    input: input.name.clone(),
                    detail: format!("environment key '{variable}' is invalid"),
                });
            }
            if let Some(owner) = environment_owners.insert(variable.clone(), name.clone()) {
                return Err(MissionTypeError::Input {
                    input: input.name.clone(),
                    detail: format!(
                        "environment key '{variable}' is already provided by input '{owner}'"
                    ),
                });
            }
        }
        let program = root.join("inputs").join(name.as_str());
        let metadata =
            std::fs::symlink_metadata(&program).map_err(|source| MissionTypeError::Io {
                path: program.clone(),
                source,
            })?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(MissionTypeError::Input {
                input: input.name,
                detail: "program must be a regular file (no symlinks)".to_string(),
            });
        }
        if !is_executable(&program) || !has_shebang(&program) {
            return Err(MissionTypeError::Input {
                input: name.to_string(),
                detail: "program must be executable and start with a #! shebang".to_string(),
            });
        }
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
    Ok(inputs)
}

fn safe_relative_path(path: &Path) -> bool {
    use std::path::Component;
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn load_roles(
    dir: &Path,
    ceiling: &AuthorityCeiling,
    packages: &BTreeMap<String, SkillPackage>,
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
        let text = read(&path)?;
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

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn has_shebang(path: &Path) -> bool {
    use std::io::Read;
    let mut buf = [0u8; 2];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut buf))
        .map(|_| &buf == b"#!")
        .unwrap_or(false)
}

fn read(path: &Path) -> Result<String, MissionTypeError> {
    std::fs::read_to_string(path).map_err(|source| MissionTypeError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_bytes(path: &Path) -> Result<Vec<u8>, MissionTypeError> {
    std::fs::read(path).map_err(|source| MissionTypeError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// A content digest over everything the loader consumes: manifest, optional
/// lock/playbook, roles, oracles, and each resolved skill package recursively.
/// Entries contribute logical path, bytes, and executable bit. Verified on
/// every engine open, so mutated mission behavior is caught.
fn compute_digest(
    root: &Path,
    skills: &BTreeMap<String, SkillPackage>,
) -> Result<String, MissionTypeError> {
    let mut digest = ContentDigest::new();
    digest.feed(
        "mission.toml",
        &read_bytes(&root.join("mission.toml"))?,
        false,
    );
    if let Ok(lock) = std::fs::read(root.join(MISSION_LOCK_FILE)) {
        digest.feed(MISSION_LOCK_FILE, &lock, false);
    }
    if let Ok(playbook) = std::fs::read(root.join("playbook.md")) {
        digest.feed("playbook.md", &playbook, false);
    }
    for (subdir, hash_exec) in [("roles", false), ("inputs", true), ("oracles", true)] {
        let dir = root.join(subdir);
        if !dir.exists() {
            continue;
        }
        for entry in read_dir(&dir)? {
            let path = entry.path();
            let rel = format!("{subdir}/{}", entry.file_name().to_string_lossy());
            digest.feed(&rel, &read_bytes(&path)?, hash_exec && is_executable(&path));
        }
    }
    for (name, package) in skills {
        for path in package_files(name, &package.root)? {
            let relative =
                path.strip_prefix(&package.root)
                    .map_err(|_| MissionTypeError::Skill {
                        skill: name.clone(),
                        detail: format!("package entry '{}' escaped its root", path.display()),
                    })?;
            let logical = format!("skills/{name}/{}", relative.to_string_lossy());
            digest.feed(&logical, &read_bytes(&path)?, is_executable(&path));
        }
    }
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
