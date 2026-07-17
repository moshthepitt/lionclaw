use std::collections::BTreeMap;
use std::path::Path;

use super::bounded_tree::{BoundedTree, ControlTextBudget, TreeEntryKind};
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
    mission_tree: &BoundedTree,
    text_budget: &mut ControlTextBudget,
) -> Result<BTreeMap<String, SkillPackage>, MissionTypeError> {
    let lock = load_lock_with_budget(mission_tree, text_budget)?;
    let mut packages = BTreeMap::new();
    let mut package_digests = BTreeMap::new();

    if mission_tree.contains(Path::new("skills"), TreeEntryKind::File) {
        return Err(MissionTypeError::Skill {
            skill: "skills".to_string(),
            detail: "skills root must be a directory".to_string(),
        });
    }
    if mission_tree.contains(Path::new("skills"), TreeEntryKind::Directory) {
        for entry in mission_tree
            .entries()
            .iter()
            .filter(|entry| entry.relative.parent() == Some(Path::new("skills")))
        {
            let path = mission_root.join(&entry.relative);
            let name = entry
                .relative
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| MissionTypeError::Skill {
                    skill: entry.relative.display().to_string(),
                    detail: "non-UTF-8 package directory name is not allowed".to_string(),
                })?
                .to_string();
            if entry.kind != TreeEntryKind::Directory {
                return Err(MissionTypeError::Skill {
                    skill: name,
                    detail: "every entry under skills/ must be a package directory".to_string(),
                });
            }
            let validated = validate_skill_package_with_budget(&path, text_budget)?;
            if validated.name != name {
                return Err(MissionTypeError::Skill {
                    skill: name.clone(),
                    detail: format!(
                        "SKILL.md name '{}' must match package directory '{name}'",
                        validated.name
                    ),
                });
            }
            package_digests.insert(name.clone(), validated.digest);
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

    validate_lock(&lock, &package_digests)?;
    Ok(packages)
}

pub(crate) fn validate_skill_package(
    root: &Path,
) -> Result<ValidatedSkillPackage, MissionTypeError> {
    validate_skill_package_with_budget(root, &mut ControlTextBudget::default())
}

fn validate_skill_package_with_budget(
    root: &Path,
    text_budget: &mut ControlTextBudget,
) -> Result<ValidatedSkillPackage, MissionTypeError> {
    let tree = BoundedTree::open(root).map_err(|error| MissionTypeError::Skill {
        skill: root.display().to_string(),
        detail: error.to_string(),
    })?;
    if !tree.contains(Path::new("SKILL.md"), TreeEntryKind::File) {
        return Err(MissionTypeError::Skill {
            skill: root.display().to_string(),
            detail: "SKILL.md must be a regular file".to_string(),
        });
    }
    let text = text_budget
        .read(&tree, Path::new("SKILL.md"))
        .map_err(|error| MissionTypeError::Skill {
            skill: root.display().to_string(),
            detail: error.to_string(),
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

    let digest = package_digest(&metadata.name, &tree)?;
    Ok(ValidatedSkillPackage {
        name: metadata.name,
        description: description.to_string(),
        digest,
    })
}

pub(crate) fn load_lock(root: &Path) -> Result<MissionLockFile, MissionTypeError> {
    let tree =
        BoundedTree::open(root).map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    load_lock_with_budget(&tree, &mut ControlTextBudget::default())
}

fn load_lock_with_budget(
    tree: &BoundedTree,
    text_budget: &mut ControlTextBudget,
) -> Result<MissionLockFile, MissionTypeError> {
    let path = Path::new(MISSION_LOCK_FILE);
    if tree.contains(path, TreeEntryKind::Directory) {
        return Err(MissionTypeError::Manifest(format!(
            "{MISSION_LOCK_FILE} must be a regular file"
        )));
    }
    if !tree.contains(path, TreeEntryKind::File) {
        return Ok(MissionLockFile {
            version: 1,
            skills: BTreeMap::new(),
        });
    }
    let text = text_budget
        .read(tree, path)
        .map_err(|error| MissionTypeError::Manifest(error.to_string()))?;
    let lock: MissionLockFile = toml::from_str(&text)
        .map_err(|err| MissionTypeError::Manifest(format!("{MISSION_LOCK_FILE}: {err}")))?;
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
    package_digests: &BTreeMap<String, String>,
) -> Result<(), MissionTypeError> {
    for (name, locked) in &lock.skills {
        let actual = package_digests.get(name).ok_or_else(|| {
            MissionTypeError::Manifest(format!(
                "{MISSION_LOCK_FILE} references missing skill '{name}'"
            ))
        })?;
        if actual != &locked.digest {
            return Err(MissionTypeError::Manifest(format!(
                "{MISSION_LOCK_FILE} digest for skill '{name}' does not match its package"
            )));
        }
    }
    Ok(())
}

fn package_digest(name: &str, tree: &BoundedTree) -> Result<String, MissionTypeError> {
    let mut digest = ContentDigest::new();
    tree.feed_digest(&mut digest, "")
        .map_err(|error| MissionTypeError::Skill {
            skill: name.to_string(),
            detail: error.to_string(),
        })?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_skill_resources_are_hashed_by_the_streaming_package_path() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("large-resource");
        std::fs::create_dir_all(root.join("references")).unwrap();
        std::fs::write(
            root.join("SKILL.md"),
            "---\nname: large-resource\ndescription: Streams resources.\n---\n\n# Instructions\n",
        )
        .unwrap();
        let resource = root.join("references/data.bin");
        std::fs::File::create(&resource)
            .unwrap()
            .set_len(65 * 1024 * 1024)
            .unwrap();

        let validated = validate_skill_package(&root).expect("large package validates");
        assert_eq!(validated.digest.len(), 64);
        let tree = BoundedTree::open(&root).unwrap();
        assert_eq!(
            validated.digest,
            package_digest("large-resource", &tree).unwrap()
        );
    }
}
