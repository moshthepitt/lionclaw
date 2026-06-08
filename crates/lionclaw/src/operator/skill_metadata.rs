use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

pub const SKILL_METADATA_FILE: &str = "lionclaw.toml";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateContextProjectorMetadata {
    pub command: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillEntrypointSymlinkPolicy {
    AllowParentSymlinks,
    RejectParentSymlinks,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateContextMetadataFile {
    version: u32,
    #[allow(
        dead_code,
        reason = "private context metadata parsing allows channel-owned sections in the shared skill metadata file"
    )]
    #[serde(default)]
    channel: Option<toml::Value>,
    #[allow(
        dead_code,
        reason = "private context metadata parsing allows channel contact metadata in the shared skill metadata file"
    )]
    #[serde(default)]
    contact: Option<toml::Value>,
    #[serde(default)]
    private_context_projector: Option<PrivateContextProjectorMetadataSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateContextProjectorMetadataSection {
    command: String,
}

pub fn load_private_context_projector_metadata(
    skill_dir: &Path,
) -> Result<Option<PrivateContextProjectorMetadata>> {
    let skill_dir = canonical_skill_dir(skill_dir)?;
    let path = skill_dir.join(SKILL_METADATA_FILE);
    let Some(content) = read_optional_skill_metadata(&path)? else {
        return Ok(None);
    };
    let parsed: PrivateContextMetadataFile = toml::from_str(&content)
        .map_err(|err| anyhow!("failed to parse {}: {err}", path.display()))?;
    validate_metadata_version(parsed.version, &path)?;
    let Some(section) = parsed.private_context_projector else {
        return Ok(None);
    };
    let command = section.command.trim().to_string();
    validate_skill_entrypoint_path(&command, "private context projector command")?;
    Ok(Some(PrivateContextProjectorMetadata { command }))
}

pub fn skill_metadata_declares_channel(skill_dir: &Path) -> Result<bool> {
    let path = skill_dir.join(SKILL_METADATA_FILE);
    let Some(content) = read_optional_skill_metadata(&path)? else {
        return Ok(false);
    };
    let parsed: toml::Value = toml::from_str(&content)
        .map_err(|err| anyhow!("failed to parse {}: {err}", path.display()))?;
    let Some(table) = parsed.as_table() else {
        bail!("skill metadata {} must be a TOML table", path.display());
    };
    Ok(table.contains_key("channel"))
}

pub fn canonical_skill_dir(path: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to stat skill directory {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("skill directory {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("skill directory {} is not a directory", path.display());
    }
    fs::canonicalize(path).with_context(|| format!("failed to resolve {}", path.display()))
}

pub fn resolve_skill_entrypoint(
    skill_dir: &Path,
    relative_path: &str,
    label: &str,
    symlink_policy: SkillEntrypointSymlinkPolicy,
) -> Result<PathBuf> {
    validate_skill_entrypoint_path(relative_path, label)?;
    let skill_dir = canonical_skill_dir(skill_dir)?;
    if symlink_policy == SkillEntrypointSymlinkPolicy::RejectParentSymlinks {
        validate_no_symlink_parent_components(&skill_dir, relative_path, label)?;
    }

    let candidate = skill_dir.join(relative_path);
    let metadata = match fs::symlink_metadata(&candidate) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "{label} entrypoint is missing under '{}'; expected '{}'",
                skill_dir.display(),
                relative_path
            );
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to stat {label} {}", candidate.display()));
        }
    };
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", candidate.display());
    }
    if !metadata.is_file() {
        bail!("{label} {} is not a file", candidate.display());
    }
    if !is_executable_file(&metadata) {
        bail!("{label} {} is not executable", candidate.display());
    }
    let canonical = fs::canonicalize(&candidate)
        .with_context(|| format!("failed to resolve {}", candidate.display()))?;
    if !canonical.starts_with(&skill_dir) {
        bail!(
            "{label} {} escapes skill directory {}",
            canonical.display(),
            skill_dir.display()
        );
    }
    Ok(canonical)
}

pub fn validate_skill_entrypoint_path(relative_path: &str, label: &str) -> Result<()> {
    if relative_path.is_empty() {
        bail!("{label} path is required");
    }
    let path = Path::new(relative_path);
    if path.is_absolute() {
        bail!("{label} path '{relative_path}' must be relative to the skill directory");
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("{label} path '{relative_path}' must stay inside the skill directory");
    }
    Ok(())
}

fn validate_no_symlink_parent_components(
    skill_dir: &Path,
    relative_path: &str,
    label: &str,
) -> Result<()> {
    let mut current = skill_dir.to_path_buf();
    let path = Path::new(relative_path);
    let component_count = path.components().count();
    for (index, component) in path.components().enumerate() {
        let Component::Normal(name) = component else {
            bail!("{label} path '{relative_path}' must stay inside the skill directory");
        };
        if index + 1 == component_count {
            break;
        }
        current.push(name);
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("failed to stat {label} parent {}", current.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("{label} parent {} must not be a symlink", current.display());
        }
        if !metadata.is_dir() {
            bail!("{label} parent {} is not a directory", current.display());
        }
    }
    Ok(())
}

fn read_optional_skill_metadata(path: &Path) -> Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() {
        bail!("skill metadata {} must not be a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("skill metadata {} is not a file", path.display());
    }

    fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))
        .map(Some)
}

fn validate_metadata_version(version: u32, path: &Path) -> Result<()> {
    if version != 1 {
        bail!(
            "unsupported skill metadata version {} in {}; expected version = 1",
            version,
            path.display()
        );
    }
    Ok(())
}

fn is_executable_file(metadata: &fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        metadata.permissions().mode() & 0o111 != 0
    }

    #[cfg(not(unix))]
    {
        let _ = metadata;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        load_private_context_projector_metadata, resolve_skill_entrypoint,
        skill_metadata_declares_channel, SkillEntrypointSymlinkPolicy,
    };

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }

    #[cfg(unix)]
    fn write_skill(root: &std::path::Path) -> std::path::PathBuf {
        let skill = root.join("private-context-core");
        fs::create_dir_all(skill.join("scripts")).expect("scripts");
        fs::write(
            skill.join("SKILL.md"),
            "---\nname: private-context-core\ndescription: private context\n---\n",
        )
        .expect("skill");
        fs::write(skill.join("scripts/projector"), "#!/usr/bin/env bash\n").expect("projector");
        make_executable(&skill.join("scripts/projector"));
        skill
    }

    fn write_private_context_projector_metadata(skill: &std::path::Path, command: &str) {
        fs::write(
            skill.join("lionclaw.toml"),
            format!("version = 1\n\n[private_context_projector]\ncommand = \"{command}\"\n"),
        )
        .expect("metadata");
    }

    #[cfg(unix)]
    #[test]
    fn parses_private_context_projector_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        write_private_context_projector_metadata(&skill, "scripts/projector");

        let metadata = load_private_context_projector_metadata(&skill)
            .expect("metadata")
            .expect("private context metadata");

        assert_eq!(metadata.command, "scripts/projector");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_legacy_memory_projector_metadata_section() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[memory_projector]\ncommand = \"scripts/projector\"\n",
        )
        .expect("metadata");

        let err = load_private_context_projector_metadata(&skill)
            .expect_err("legacy memory projector metadata should be rejected");

        assert!(err.to_string().contains("memory_projector"));
    }

    #[cfg(unix)]
    #[test]
    fn private_context_only_metadata_does_not_declare_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        write_private_context_projector_metadata(&skill, "scripts/projector");

        assert!(!skill_metadata_declares_channel(&skill).expect("declares channel"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_private_context_projector_absolute_command_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        write_private_context_projector_metadata(&skill, "/bin/echo");

        let err = load_private_context_projector_metadata(&skill)
            .expect_err("absolute private context projector command should fail");

        assert!(err.to_string().contains("must be relative"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_private_context_projector_parent_traversal_command_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        write_private_context_projector_metadata(&skill, "../projector");

        let err = load_private_context_projector_metadata(&skill)
            .expect_err("parent traversal private context projector command should fail");

        assert!(err.to_string().contains("must stay inside"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_missing_private_context_projector_command() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());

        let err = resolve_skill_entrypoint(
            &skill,
            "scripts/missing",
            "private context projector command",
            SkillEntrypointSymlinkPolicy::RejectParentSymlinks,
        )
        .expect_err("missing projector command should fail");

        assert!(err.to_string().contains("is missing"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_non_executable_private_context_projector_command() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        let command = skill.join("scripts/not-executable");
        fs::write(&command, "#!/usr/bin/env bash\n").expect("projector");

        let err = resolve_skill_entrypoint(
            &skill,
            "scripts/not-executable",
            "private context projector command",
            SkillEntrypointSymlinkPolicy::RejectParentSymlinks,
        )
        .expect_err("non-executable projector command should fail");

        assert!(err.to_string().contains("not executable"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_private_context_projector_command() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        let target = skill.join("scripts/real-projector");
        fs::write(&target, "#!/usr/bin/env bash\n").expect("real projector");
        make_executable(&target);
        fs::remove_file(skill.join("scripts/projector")).expect("remove projector");
        symlink(&target, skill.join("scripts/projector")).expect("projector symlink");

        let err = resolve_skill_entrypoint(
            &skill,
            "scripts/projector",
            "private context projector command",
            SkillEntrypointSymlinkPolicy::RejectParentSymlinks,
        )
        .expect_err("symlinked projector command should fail");

        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_private_context_projector_parent_symlink() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_skill(temp_dir.path());
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(&outside).expect("outside");
        fs::write(outside.join("projector"), "#!/usr/bin/env bash\n").expect("outside projector");
        make_executable(&outside.join("projector"));
        fs::remove_dir_all(skill.join("scripts")).expect("remove scripts");
        symlink(&outside, skill.join("scripts")).expect("scripts symlink");

        let err = resolve_skill_entrypoint(
            &skill,
            "scripts/projector",
            "private context projector command",
            SkillEntrypointSymlinkPolicy::RejectParentSymlinks,
        )
        .expect_err("parent symlink should fail");

        assert!(err.to_string().contains("parent"));
        assert!(err.to_string().contains("symlink"));
    }
}
