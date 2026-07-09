//! Fail-closed mission-type loading. A mission type is a directory of prose; every role
//! compiles through the moat at load time, every oracle is a real executable,
//! or the mission type does not load and the mission never starts.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::authority::{compile_authority, AuthorityCeiling, MoatViolation};
use crate::model::{OracleName, RoleName, StopBar};

use super::frontmatter::{parse_role_file, RoleFrontmatter};
use super::{MissionType, RoleDefinition};

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
    #[error("mission type at '{0}' has no roles")]
    NoRoles(PathBuf),
}

#[derive(serde::Deserialize)]
struct ManifestFile {
    #[serde(rename = "mission-type")]
    mission_type: ManifestMissionType,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestMissionType {
    name: String,
    stop: String,
    /// The confinement image every role and oracle runs in; carries this
    /// domain's toolchain. Required — the engine ships no default image.
    image: String,
}

pub fn load_mission_type(
    root: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<MissionType, MissionTypeError> {
    let manifest_path = root.join("mission.toml");
    let manifest_text = read(&manifest_path)?;
    let manifest: ManifestFile = toml::from_str(&manifest_text)
        .map_err(|e| MissionTypeError::Manifest(format!("{manifest_path:?}: {e}")))?;
    if !is_path_safe(&manifest.mission_type.name) {
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

    let oracles = load_oracles(&root.join("oracles"))?;
    let roles = load_roles(&root.join("roles"), ceiling)?;
    if roles.is_empty() {
        return Err(MissionTypeError::NoRoles(root.to_path_buf()));
    }

    let playbook = read(&root.join("playbook.md")).ok();
    let digest = compute_digest(root)?;

    Ok(MissionType {
        name: manifest.mission_type.name,
        digest,
        stop,
        image: manifest.mission_type.image,
        root: root.to_path_buf(),
        playbook,
        roles,
        oracles,
    })
}

fn load_roles(
    dir: &Path,
    ceiling: &AuthorityCeiling,
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
            skills,
            prompt_body,
        } = parse_role_file(&text).map_err(|e| MissionTypeError::Role {
            role: stem.to_string(),
            detail: e,
        })?;
        // Skill projection is not wired yet; fail closed so an author can't
        // declare a silently-ignored capability.
        if !skills.is_empty() {
            return Err(MissionTypeError::Role {
                role: stem.to_string(),
                detail: "skills projection is not supported yet".to_string(),
            });
        }
        let role = RoleDefinition {
            name: name.clone(),
            output,
            runtime,
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

fn is_path_safe(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
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

/// A content digest over everything the loader consumes: `mission.toml`,
/// `playbook.md` (if present), and every `roles/*` and `oracles/*` file, in a
/// deterministic order, each contributing its relative path, bytes, and — for
/// oracles — its executable bit. Verified on every engine open, so a mutated
/// role or oracle (the fake-green vector) is caught. Umask-insensitive: only an
/// oracle's exec bit is hashed, not raw file modes.
fn compute_digest(root: &Path) -> Result<String, MissionTypeError> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut feed = |rel: &str, bytes: &[u8], exec: bool| {
        hasher.update((rel.len() as u64).to_le_bytes());
        hasher.update(rel.as_bytes());
        hasher.update([exec as u8]);
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    };
    feed(
        "mission.toml",
        &read_bytes(&root.join("mission.toml"))?,
        false,
    );
    if let Ok(playbook) = std::fs::read(root.join("playbook.md")) {
        feed("playbook.md", &playbook, false);
    }
    for (subdir, hash_exec) in [("roles", false), ("oracles", true)] {
        let dir = root.join(subdir);
        if !dir.exists() {
            continue;
        }
        for entry in read_dir(&dir)? {
            let path = entry.path();
            let rel = format!("{subdir}/{}", entry.file_name().to_string_lossy());
            feed(&rel, &read_bytes(&path)?, hash_exec && is_executable(&path));
        }
    }
    Ok(hex::encode(hasher.finalize()))
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
