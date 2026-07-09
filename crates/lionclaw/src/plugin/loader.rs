//! Fail-closed plugin loading. A plugin is a directory of prose; every role
//! compiles through the moat at load time, every oracle is a real executable,
//! or the plugin does not load and the mission never starts.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::authority::{compile_authority, AuthorityCeiling, MoatViolation};
use crate::model::{OracleName, RoleName, StopBar};

use super::frontmatter::{parse_role_file, RoleFrontmatter};
use super::{LoadedPlugin, RoleDefinition};

#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("plugin io error at '{path}': {source}")]
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
    #[error("plugin at '{0}' has no roles")]
    NoRoles(PathBuf),
}

#[derive(serde::Deserialize)]
struct ManifestFile {
    plugin: ManifestPlugin,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestPlugin {
    name: String,
    stop: String,
}

pub fn load_plugin(root: &Path, ceiling: &AuthorityCeiling) -> Result<LoadedPlugin, PluginError> {
    let manifest_path = root.join("mission.toml");
    let manifest_text = read(&manifest_path)?;
    let manifest: ManifestFile = toml::from_str(&manifest_text)
        .map_err(|e| PluginError::Manifest(format!("{manifest_path:?}: {e}")))?;
    if !is_path_safe(&manifest.plugin.name) {
        return Err(PluginError::Manifest(format!(
            "plugin name '{}' is not path-safe",
            manifest.plugin.name
        )));
    }
    let stop = match manifest.plugin.stop.as_str() {
        "verified" => StopBar::Verified,
        "reviewed" => StopBar::Reviewed,
        other => {
            return Err(PluginError::Manifest(format!(
                "stop must be 'verified' or 'reviewed', got '{other}'"
            )))
        }
    };

    let oracles = load_oracles(&root.join("oracles"))?;
    let roles = load_roles(&root.join("roles"), ceiling)?;
    if roles.is_empty() {
        return Err(PluginError::NoRoles(root.to_path_buf()));
    }

    let playbook = read(&root.join("playbook.md")).ok();

    Ok(LoadedPlugin {
        name: manifest.plugin.name,
        stop,
        root: root.to_path_buf(),
        playbook,
        roles,
        oracles,
    })
}

fn load_roles(
    dir: &Path,
    ceiling: &AuthorityCeiling,
) -> Result<BTreeMap<RoleName, RoleDefinition>, PluginError> {
    let mut roles = BTreeMap::new();
    if !dir.exists() {
        return Ok(roles);
    }
    for entry in read_dir(dir)? {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("md") {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| PluginError::Role {
                role: path.display().to_string(),
                detail: "non-utf8 filename".to_string(),
            })?;
        let name = RoleName::new(stem).map_err(|e| PluginError::Role {
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
        } = parse_role_file(&text).map_err(|e| PluginError::Role {
            role: stem.to_string(),
            detail: e,
        })?;
        // Skill projection is not wired yet; fail closed so an author can't
        // declare a silently-ignored capability.
        if !skills.is_empty() {
            return Err(PluginError::Role {
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
        // compile (e.g. a judge requesting secrets) rejects the plugin.
        compile_authority(&role, ceiling).map_err(|violation| PluginError::Moat {
            role: stem.to_string(),
            violation,
        })?;
        roles.insert(name, role);
    }
    Ok(roles)
}

fn load_oracles(dir: &Path) -> Result<BTreeMap<OracleName, PathBuf>, PluginError> {
    let mut oracles = BTreeMap::new();
    if !dir.exists() {
        return Ok(oracles);
    }
    for entry in read_dir(dir)? {
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| PluginError::Io {
            path: path.clone(),
            source: e,
        })?;
        // No symlinks out of the plugin: an oracle must be a regular file.
        if !file_type.is_file() {
            return Err(PluginError::Oracle {
                oracle: path.display().to_string(),
                detail: "must be a regular file (no symlinks)".to_string(),
            });
        }
        let stem =
            path.file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| PluginError::Oracle {
                    oracle: path.display().to_string(),
                    detail: "non-utf8 filename".to_string(),
                })?;
        let name = OracleName::new(stem).map_err(|e| PluginError::Oracle {
            oracle: stem.to_string(),
            detail: e.to_string(),
        })?;
        if !is_executable(&path) {
            return Err(PluginError::Oracle {
                oracle: stem.to_string(),
                detail: "must be executable".to_string(),
            });
        }
        if !has_shebang(&path) {
            return Err(PluginError::Oracle {
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

fn read(path: &Path) -> Result<String, PluginError> {
    std::fs::read_to_string(path).map_err(|source| PluginError::Io {
        path: path.to_path_buf(),
        source,
    })
}

fn read_dir(dir: &Path) -> Result<Vec<std::fs::DirEntry>, PluginError> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|source| PluginError::Io {
            path: dir.to_path_buf(),
            source,
        })?
        .collect::<Result<_, _>>()
        .map_err(|source| PluginError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
    // Deterministic order.
    entries.sort_by_key(|e| e.file_name());
    Ok(entries)
}
