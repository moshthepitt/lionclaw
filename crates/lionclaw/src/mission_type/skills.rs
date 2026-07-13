use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::digest::ContentDigest;
use super::loader::MissionTypeError;
use super::manifest::{MissionLockFile, MISSION_LOCK_FILE};
use super::SkillPackage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedSkillPackage {
    pub name: String,
    pub description: String,
    pub digest: String,
}

pub(crate) fn load_skills(
    mission_root: &Path,
) -> Result<BTreeMap<String, SkillPackage>, MissionTypeError> {
    let lock = load_lock(mission_root)?;
    let skills_root = mission_root.join("skills");
    let mut packages = BTreeMap::new();

    match std::fs::symlink_metadata(&skills_root) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(MissionTypeError::Io {
                path: skills_root,
                source,
            })
        }
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(MissionTypeError::Skill {
                skill: "skills".to_string(),
                detail: "skills root must be a directory, not a symlink".to_string(),
            })
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(MissionTypeError::Skill {
                skill: "skills".to_string(),
                detail: "skills root must be a directory".to_string(),
            })
        }
        Ok(_) => {
            for entry in read_dir(&skills_root)? {
                let path = entry.path();
                let name =
                    entry
                        .file_name()
                        .into_string()
                        .map_err(|_| MissionTypeError::Skill {
                            skill: path.display().to_string(),
                            detail: "package directory name must be UTF-8".to_string(),
                        })?;
                let file_type = entry.file_type().map_err(|source| MissionTypeError::Io {
                    path: path.clone(),
                    source,
                })?;
                if file_type.is_symlink() {
                    return Err(MissionTypeError::Skill {
                        skill: name,
                        detail: "skill package must be a directory, not a symlink".to_string(),
                    });
                }
                if !file_type.is_dir() {
                    return Err(MissionTypeError::Skill {
                        skill: name,
                        detail: "every entry under skills/ must be a package directory".to_string(),
                    });
                }
                let validated = validate_skill_package(&path)?;
                if validated.name != name {
                    return Err(MissionTypeError::Skill {
                        skill: name.clone(),
                        detail: format!(
                            "SKILL.md name '{}' must match package directory '{name}'",
                            validated.name
                        ),
                    });
                }
                packages.insert(
                    name.clone(),
                    SkillPackage {
                        name,
                        root: path,
                        description: validated.description,
                    },
                );
            }
        }
    }

    validate_lock(&lock, &packages)?;
    Ok(packages)
}

pub(crate) fn validate_skill_package(
    root: &Path,
) -> Result<ValidatedSkillPackage, MissionTypeError> {
    let metadata = std::fs::symlink_metadata(root).map_err(|source| MissionTypeError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(MissionTypeError::Skill {
            skill: root.display().to_string(),
            detail: "package root must be a directory, not a symlink".to_string(),
        });
    }

    let skill_md = root.join("SKILL.md");
    let metadata = std::fs::symlink_metadata(&skill_md).map_err(|source| MissionTypeError::Io {
        path: skill_md.clone(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MissionTypeError::Skill {
            skill: root.display().to_string(),
            detail: "SKILL.md must be a regular file".to_string(),
        });
    }

    let text = std::fs::read_to_string(&skill_md).map_err(|source| MissionTypeError::Io {
        path: skill_md.clone(),
        source,
    })?;
    let (yaml, body) =
        split_skill_frontmatter(&text).map_err(|detail| MissionTypeError::Skill {
            skill: root.display().to_string(),
            detail,
        })?;
    let metadata: SkillFrontmatter =
        serde_saphyr::from_str(yaml).map_err(|err| MissionTypeError::Skill {
            skill: root.display().to_string(),
            detail: format!("invalid SKILL.md YAML frontmatter: {err}"),
        })?;
    validate_standard_skill_name(&metadata.name).map_err(|detail| MissionTypeError::Skill {
        skill: metadata.name.clone(),
        detail,
    })?;
    lionclaw_confinement::validate_skill_alias(&metadata.name).map_err(|err| {
        MissionTypeError::Skill {
            skill: metadata.name.clone(),
            detail: err.to_string(),
        }
    })?;
    let description = metadata.description.trim();
    if description.is_empty() || description.len() > 1024 {
        return Err(MissionTypeError::Skill {
            skill: metadata.name,
            detail: "SKILL.md description must contain 1 to 1024 bytes".to_string(),
        });
    }
    if body.trim().is_empty() {
        return Err(MissionTypeError::Skill {
            skill: metadata.name,
            detail: "SKILL.md instruction body must not be empty".to_string(),
        });
    }

    let digest = package_digest(&metadata.name, root)?;
    Ok(ValidatedSkillPackage {
        name: metadata.name,
        description: description.to_string(),
        digest,
    })
}

pub(crate) fn package_files(
    name: &str,
    package_root: &Path,
) -> Result<Vec<PathBuf>, MissionTypeError> {
    walk_skill_files(name, package_root, package_root)
}

pub(crate) fn load_lock(root: &Path) -> Result<MissionLockFile, MissionTypeError> {
    let path = root.join(MISSION_LOCK_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MissionLockFile {
                version: 1,
                skills: BTreeMap::new(),
            })
        }
        Err(source) => return Err(MissionTypeError::Io { path, source }),
    };
    let lock: MissionLockFile = toml::from_str(&text)
        .map_err(|err| MissionTypeError::Manifest(format!("{}: {err}", path.display())))?;
    if lock.version != 1 {
        return Err(MissionTypeError::Manifest(format!(
            "unsupported {MISSION_LOCK_FILE} version {}",
            lock.version
        )));
    }
    Ok(lock)
}

fn validate_lock(
    lock: &MissionLockFile,
    packages: &BTreeMap<String, SkillPackage>,
) -> Result<(), MissionTypeError> {
    for (name, locked) in &lock.skills {
        let package = packages.get(name).ok_or_else(|| {
            MissionTypeError::Manifest(format!(
                "{MISSION_LOCK_FILE} references missing skill '{name}'"
            ))
        })?;
        let actual = package_digest(name, &package.root)?;
        if actual != locked.digest {
            return Err(MissionTypeError::Manifest(format!(
                "{MISSION_LOCK_FILE} digest for skill '{name}' does not match its package"
            )));
        }
    }
    Ok(())
}

fn package_digest(name: &str, root: &Path) -> Result<String, MissionTypeError> {
    let mut digest = ContentDigest::new();
    for path in package_files(name, root)? {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| MissionTypeError::Skill {
                skill: name.to_string(),
                detail: format!("package entry '{}' escaped its root", path.display()),
            })?;
        let bytes = std::fs::read(&path).map_err(|source| MissionTypeError::Io {
            path: path.clone(),
            source,
        })?;
        digest.feed(&relative.to_string_lossy(), &bytes, is_executable(&path));
    }
    Ok(digest.finish())
}

#[derive(serde::Deserialize)]
struct SkillFrontmatter {
    name: String,
    description: String,
}

fn split_skill_frontmatter(text: &str) -> Result<(&str, &str), String> {
    let rest = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
        .ok_or("SKILL.md must start with a '---' YAML frontmatter fence")?;
    let mut offset = 0usize;
    for line in rest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            return Ok((&rest[..offset], &rest[offset + line.len()..]));
        }
        offset += line.len();
    }
    Err("SKILL.md has no closing '---' YAML frontmatter fence".to_string())
}

fn validate_standard_skill_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("SKILL.md name must contain 1 to 64 characters".to_string());
    }
    if name.starts_with('-')
        || name.ends_with('-')
        || name.contains("--")
        || !name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return Err(
            "SKILL.md name must use lowercase ASCII letters, numbers, and single interior hyphens"
                .to_string(),
        );
    }
    Ok(())
}

fn walk_skill_files(
    name: &str,
    package_root: &Path,
    directory: &Path,
) -> Result<Vec<PathBuf>, MissionTypeError> {
    let mut files = Vec::new();
    for entry in read_dir(directory)? {
        if entry.file_name() == ".git" {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type().map_err(|source| MissionTypeError::Io {
            path: path.clone(),
            source,
        })?;
        if file_type.is_symlink() {
            return Err(MissionTypeError::Skill {
                skill: name.to_string(),
                detail: format!(
                    "package entry '{}' must not be a symlink",
                    path.strip_prefix(package_root).unwrap_or(&path).display()
                ),
            });
        }
        if file_type.is_dir() {
            files.extend(walk_skill_files(name, package_root, &path)?);
        } else if file_type.is_file() {
            files.push(path);
        } else {
            return Err(MissionTypeError::Skill {
                skill: name.to_string(),
                detail: format!(
                    "package entry '{}' must be a regular file or directory",
                    path.strip_prefix(package_root).unwrap_or(&path).display()
                ),
            });
        }
    }
    Ok(files)
}

fn read_dir(directory: &Path) -> Result<Vec<std::fs::DirEntry>, MissionTypeError> {
    let mut entries = std::fs::read_dir(directory)
        .map_err(|source| MissionTypeError::Io {
            path: directory.to_path_buf(),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| MissionTypeError::Io {
            path: directory.to_path_buf(),
            source,
        })?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    Ok(entries)
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}
