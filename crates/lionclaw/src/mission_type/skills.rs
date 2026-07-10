use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use super::loader::MissionTypeError;
use super::manifest::{
    LockedSkillSource, ManifestSkill, ManifestSkillSource, MissionLockFile, MISSION_LOCK_FILE,
};
use super::SkillPackage;

pub(crate) fn load_skills(
    mission_root: &Path,
    declarations: &BTreeMap<String, ManifestSkill>,
) -> Result<BTreeMap<String, SkillPackage>, MissionTypeError> {
    let lock = load_lock(mission_root)?;
    if let Some(lock) = &lock {
        validate_lock(declarations, lock)?;
    }
    let mut packages = BTreeMap::new();
    for (name, declaration) in declarations {
        lionclaw_confinement::validate_skill_alias(name).map_err(|err| {
            MissionTypeError::Skill {
                skill: name.clone(),
                detail: err.to_string(),
            }
        })?;
        let root = if let Some(locked) = lock.as_ref().and_then(|lock| lock.skills.get(name)) {
            resolve_package_path(mission_root, &locked.path).map_err(|detail| {
                MissionTypeError::Skill {
                    skill: name.clone(),
                    detail,
                }
            })?
        } else {
            source_package_path(mission_root, name, declaration)?
        };
        let description = validate_skill_tree(name, mission_root, &root)?;
        packages.insert(
            name.clone(),
            SkillPackage {
                name: name.clone(),
                root,
                description,
            },
        );
    }
    Ok(packages)
}

fn validate_package_containment(
    name: &str,
    mission_root: &Path,
    package_root: &Path,
) -> Result<(), MissionTypeError> {
    let canonical_mission = mission_root
        .canonicalize()
        .map_err(|source| MissionTypeError::Io {
            path: mission_root.to_path_buf(),
            source,
        })?;
    let canonical_package = package_root
        .canonicalize()
        .map_err(|source| MissionTypeError::Io {
            path: package_root.to_path_buf(),
            source,
        })?;
    if !canonical_package.starts_with(&canonical_mission) {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!(
                "package root '{}' resolves outside mission type '{}'",
                package_root.display(),
                mission_root.display()
            ),
        });
    }
    Ok(())
}

pub(crate) fn package_files(
    name: &str,
    package_root: &Path,
) -> Result<Vec<PathBuf>, MissionTypeError> {
    walk_skill_files(name, package_root, package_root)
}

fn load_lock(root: &Path) -> Result<Option<MissionLockFile>, MissionTypeError> {
    let path = root.join(MISSION_LOCK_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(MissionTypeError::Io { path, source }),
    };
    toml::from_str(&text)
        .map(Some)
        .map_err(|err| MissionTypeError::Manifest(format!("{}: {err}", path.display())))
}

fn validate_lock(
    declarations: &BTreeMap<String, ManifestSkill>,
    lock: &MissionLockFile,
) -> Result<(), MissionTypeError> {
    if lock.version != 1 {
        return Err(MissionTypeError::Manifest(format!(
            "unsupported {MISSION_LOCK_FILE} version {}",
            lock.version
        )));
    }
    let declared = declarations
        .keys()
        .collect::<std::collections::BTreeSet<_>>();
    let locked = lock
        .skills
        .keys()
        .collect::<std::collections::BTreeSet<_>>();
    if declared != locked {
        return Err(MissionTypeError::Manifest(format!(
            "{MISSION_LOCK_FILE} skills do not match mission.toml"
        )));
    }
    for (name, declaration) in declarations {
        let locked = &lock.skills[name];
        let expected_path = PathBuf::from("skills").join(name);
        if locked.path != expected_path {
            return Err(MissionTypeError::Manifest(format!(
                "{MISSION_LOCK_FILE} path for skill '{name}' must be '{}'",
                expected_path.display()
            )));
        }
        let source_matches = match (&declaration.source, &locked.source) {
            (ManifestSkillSource::Path(declared), LockedSkillSource::Path { path }) => {
                declared.path == *path
            }
            (ManifestSkillSource::Git(declared), LockedSkillSource::Git { git, rev, subdir }) => {
                declared.git == *git && declared.rev == *rev && declared.subdir == *subdir
            }
            _ => false,
        };
        if !source_matches {
            return Err(MissionTypeError::Manifest(format!(
                "{MISSION_LOCK_FILE} source for skill '{name}' does not match mission.toml"
            )));
        }
    }
    Ok(())
}

fn source_package_path(
    mission_root: &Path,
    name: &str,
    declaration: &ManifestSkill,
) -> Result<PathBuf, MissionTypeError> {
    match &declaration.source {
        ManifestSkillSource::Path(source) => {
            resolve_package_path(mission_root, &source.path).map_err(|detail| {
                MissionTypeError::Skill {
                    skill: name.to_string(),
                    detail,
                }
            })
        }
        ManifestSkillSource::Git(source) => Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!(
                "git source '{}@{}' (subdir '{}') is not materialized; install the mission type first",
                source.git,
                source.rev,
                source.subdir.display()
            ),
        }),
    }
}

pub(crate) fn resolve_package_path(
    mission_root: &Path,
    relative: &Path,
) -> Result<PathBuf, String> {
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err(format!(
            "source path '{}' must be a non-empty relative path",
            relative.display()
        ));
    }
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "source path '{}' must stay inside the mission type",
            relative.display()
        ));
    }
    Ok(mission_root.join(relative))
}

fn validate_skill_tree(
    name: &str,
    mission_root: &Path,
    root: &Path,
) -> Result<String, MissionTypeError> {
    let metadata = std::fs::symlink_metadata(root).map_err(|source| MissionTypeError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_symlink() {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!("package root '{}' must not be a symlink", root.display()),
        });
    }
    if !metadata.is_dir() {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!("package root '{}' must be a directory", root.display()),
        });
    }
    validate_package_containment(name, mission_root, root)?;
    let skill_md = root.join("SKILL.md");
    let metadata = std::fs::symlink_metadata(&skill_md).map_err(|source| MissionTypeError::Io {
        path: skill_md.clone(),
        source,
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!("'{}' must be a regular file", skill_md.display()),
        });
    }
    let description = validate_skill_md(name, &skill_md)?;
    package_files(name, root).map(|_| description)
}

#[derive(serde::Deserialize)]
struct SkillFrontmatter {
    name: String,
    description: String,
}

fn validate_skill_md(name: &str, path: &Path) -> Result<String, MissionTypeError> {
    let text = std::fs::read_to_string(path).map_err(|source| MissionTypeError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let (yaml, body) =
        split_skill_frontmatter(&text).map_err(|detail| MissionTypeError::Skill {
            skill: name.to_string(),
            detail,
        })?;
    let metadata: SkillFrontmatter =
        serde_saphyr::from_str(yaml).map_err(|err| MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!("invalid SKILL.md YAML frontmatter: {err}"),
        })?;
    validate_standard_skill_name(&metadata.name).map_err(|detail| MissionTypeError::Skill {
        skill: name.to_string(),
        detail,
    })?;
    if metadata.name != name {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: format!(
                "SKILL.md name '{}' must match declared skill name '{name}'",
                metadata.name
            ),
        });
    }
    let description = metadata.description.trim();
    if description.is_empty() || description.len() > 1024 {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: "SKILL.md description must contain 1 to 1024 characters".to_string(),
        });
    }
    if body.trim().is_empty() {
        return Err(MissionTypeError::Skill {
            skill: name.to_string(),
            detail: "SKILL.md instruction body must not be empty".to_string(),
        });
    }
    Ok(description.to_string())
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
