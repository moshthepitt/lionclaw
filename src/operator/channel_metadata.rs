use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::{
    home::LionClawHome, kernel::skills::validate_skill_alias, operator::config::ChannelLaunchMode,
};

pub const CHANNEL_METADATA_FILE: &str = "lionclaw.toml";
pub const DEFAULT_CHANNEL_WORKER: &str = "scripts/worker";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelMetadata {
    pub id: String,
    pub launch: ChannelLaunchMode,
    pub worker: String,
    pub env: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelSkillSource {
    Path,
    Installed { alias: String },
    Bundled,
}

#[derive(Debug, Clone)]
pub struct DiscoveredChannelSkill {
    pub source: ChannelSkillSource,
    pub skill_dir: PathBuf,
    pub metadata: ChannelMetadata,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelMetadataFile {
    version: u32,
    channel: ChannelMetadataSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelMetadataSection {
    id: String,
    launch: String,
    worker: String,
    #[serde(default)]
    env: Vec<String>,
}

pub fn discover_channel_skill(home: &LionClawHome, raw: &str) -> Result<DiscoveredChannelSkill> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("channel or path is required");
    }

    if looks_like_path(raw) {
        let skill_dir = canonical_skill_dir(Path::new(raw))?;
        let metadata = load_channel_metadata(&skill_dir)?;
        return Ok(DiscoveredChannelSkill {
            source: ChannelSkillSource::Path,
            skill_dir,
            metadata,
        });
    }

    validate_channel_id(raw)?;
    if let Some(installed) = discover_installed_channel_skill(home, raw)? {
        return Ok(installed);
    }

    let bundled = bundled_channel_skill_dir(raw);
    if bundled.exists() {
        let skill_dir = canonical_skill_dir(&bundled)?;
        let metadata = load_channel_metadata(&skill_dir)?;
        if metadata.id != raw {
            bail!(
                "bundled channel skill '{}' declares channel id '{}'",
                bundled.display(),
                metadata.id
            );
        }
        return Ok(DiscoveredChannelSkill {
            source: ChannelSkillSource::Bundled,
            skill_dir,
            metadata,
        });
    }

    bail!(
        "channel '{raw}' was not found; pass an explicit skill path or install a skill with matching channel metadata"
    )
}

pub fn load_channel_metadata(skill_dir: &Path) -> Result<ChannelMetadata> {
    let skill_dir = canonical_skill_dir(skill_dir)?;
    let path = skill_dir.join(CHANNEL_METADATA_FILE);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("failed to stat channel metadata {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("channel metadata {} must not be a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("channel metadata {} is not a file", path.display());
    }

    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed: ChannelMetadataFile = toml::from_str(&content)
        .map_err(|err| anyhow!("failed to parse {}: {err}", path.display()))?;
    if parsed.version != 1 {
        bail!(
            "unsupported channel metadata version {} in {}; expected version = 1",
            parsed.version,
            path.display()
        );
    }

    let id = parsed.channel.id.trim().to_string();
    validate_channel_id(&id)?;
    let launch = parsed
        .channel
        .launch
        .parse::<ChannelLaunchMode>()
        .map_err(anyhow::Error::msg)?;
    let worker = parsed.channel.worker.trim().to_string();
    validate_channel_worker(&skill_dir, &worker)?;
    let mut env = parsed
        .channel
        .env
        .into_iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    for key in &env {
        validate_channel_env_name(key)?;
    }
    env.sort();
    env.dedup();

    Ok(ChannelMetadata {
        id,
        launch,
        worker,
        env,
    })
}

pub fn resolve_channel_worker_entrypoint(
    skill_dir: &Path,
    worker: Option<&str>,
) -> Result<PathBuf> {
    let worker = worker.unwrap_or(DEFAULT_CHANNEL_WORKER);
    validate_channel_worker(skill_dir, worker)
}

pub fn validate_channel_id(id: &str) -> Result<()> {
    let trimmed = id.trim();
    if id != trimmed {
        bail!("channel id '{id}' has surrounding whitespace");
    }
    if trimmed.is_empty() {
        bail!("channel id is required");
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        bail!("channel id is required");
    };
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        bail!("channel id '{id}' must start with a lowercase ASCII letter or digit");
    }
    if trimmed.chars().any(|ch| {
        !(ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_' | '.'))
    }) {
        bail!(
            "channel id '{id}' may only contain lowercase ASCII letters, numbers, '.', '_' and '-'"
        );
    }
    Ok(())
}

pub fn validate_channel_env_name(name: &str) -> Result<()> {
    let trimmed = name.trim();
    if name != trimmed {
        bail!("environment variable name '{name}' has surrounding whitespace");
    }
    if trimmed.is_empty() {
        bail!("environment variable name is required");
    }
    let mut chars = trimmed.chars();
    let first = chars
        .next()
        .ok_or_else(|| anyhow!("environment variable name is required"))?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        bail!("environment variable name '{name}' must start with an ASCII letter or '_'");
    }
    if chars.any(|ch| !(ch.is_ascii_alphanumeric() || ch == '_')) {
        bail!(
            "environment variable name '{name}' may only contain ASCII letters, numbers, and '_'"
        );
    }
    Ok(())
}

fn discover_installed_channel_skill(
    home: &LionClawHome,
    channel_id: &str,
) -> Result<Option<DiscoveredChannelSkill>> {
    let skills_root = home.skills_dir();
    let metadata = match fs::symlink_metadata(&skills_root) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", skills_root.display()))
        }
    };
    if metadata.file_type().is_symlink() {
        bail!(
            "skills root {} must not be a symlink",
            skills_root.display()
        );
    }
    if !metadata.is_dir() {
        bail!("skills root {} is not a directory", skills_root.display());
    }

    let mut matches = Vec::new();
    let mut entries = fs::read_dir(&skills_root)
        .with_context(|| format!("failed to read {}", skills_root.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to iterate {}", skills_root.display()))?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let alias = entry
            .file_name()
            .to_str()
            .ok_or_else(|| {
                anyhow!(
                    "invalid installed skill name under {}",
                    skills_root.display()
                )
            })?
            .to_string();
        if alias.starts_with('.') {
            continue;
        }
        validate_skill_alias(&alias)?;
        let skill_dir = entry.path();
        let metadata = fs::symlink_metadata(&skill_dir)
            .with_context(|| format!("failed to stat {}", skill_dir.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "installed skill {} must not be a symlink",
                skill_dir.display()
            );
        }
        if !metadata.is_dir() {
            continue;
        }
        if !skill_dir.join(CHANNEL_METADATA_FILE).exists() {
            continue;
        }
        let skill_dir = fs::canonicalize(&skill_dir)
            .with_context(|| format!("failed to resolve {}", skill_dir.display()))?;
        let metadata = load_channel_metadata(&skill_dir)
            .with_context(|| format!("installed skill '{alias}' has invalid channel metadata"))?;
        if metadata.id == channel_id {
            matches.push(DiscoveredChannelSkill {
                source: ChannelSkillSource::Installed { alias },
                skill_dir,
                metadata,
            });
        }
    }

    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => bail!(
            "multiple installed skills declare channel id '{channel_id}'; pass an explicit path or remove the duplicate channel skills"
        ),
    }
}

fn canonical_skill_dir(path: &Path) -> Result<PathBuf> {
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

fn validate_channel_worker(skill_dir: &Path, worker: &str) -> Result<PathBuf> {
    if worker.is_empty() {
        bail!("channel worker path is required");
    }
    let worker_path = Path::new(worker);
    if worker_path.is_absolute() {
        bail!("channel worker path '{worker}' must be relative to the skill directory");
    }
    if worker_path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        bail!("channel worker path '{worker}' must stay inside the skill directory");
    }

    let skill_dir = canonical_skill_dir(skill_dir)?;
    let candidate = skill_dir.join(worker_path);
    let metadata = match fs::symlink_metadata(&candidate) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            bail!(
                "worker entrypoint is missing under '{}'; expected '{}'",
                skill_dir.display(),
                worker
            );
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to stat channel worker {}", candidate.display()));
        }
    };
    if metadata.file_type().is_symlink() {
        bail!(
            "channel worker {} must not be a symlink",
            candidate.display()
        );
    }
    if !metadata.is_file() {
        bail!("channel worker {} is not a file", candidate.display());
    }
    if !is_executable_file(&metadata) {
        bail!("channel worker {} is not executable", candidate.display());
    }
    let canonical = fs::canonicalize(&candidate)
        .with_context(|| format!("failed to resolve {}", candidate.display()))?;
    if !canonical.starts_with(&skill_dir) {
        bail!(
            "channel worker {} escapes skill directory {}",
            canonical.display(),
            skill_dir.display()
        );
    }
    Ok(canonical)
}

fn looks_like_path(raw: &str) -> bool {
    let path = Path::new(raw);
    path.is_absolute()
        || raw.starts_with('.')
        || raw.contains(std::path::MAIN_SEPARATOR)
        || raw.contains('/')
}

fn bundled_channel_skill_dir(channel_id: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("skills")
        .join(format!("channel-{channel_id}"))
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

    use super::{discover_channel_skill, load_channel_metadata, validate_channel_env_name};
    use crate::{home::LionClawHome, operator::snapshot::copy_snapshot_tree};

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }

    #[cfg(unix)]
    fn write_channel_skill(root: &std::path::Path, name: &str, id: &str) -> std::path::PathBuf {
        let skill = root.join(name);
        fs::create_dir_all(skill.join("scripts")).expect("scripts");
        fs::write(
            skill.join("SKILL.md"),
            "---\nname: test\ndescription: test\n---\n",
        )
        .expect("skill");
        fs::write(skill.join("scripts/worker"), "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&skill.join("scripts/worker"));
        fs::write(
            skill.join("lionclaw.toml"),
            format!(
                "version = 1\n\n[channel]\nid = \"{id}\"\nlaunch = \"service\"\nworker = \"scripts/worker\"\nenv = [\"TELEGRAM_BOT_TOKEN\"]\n"
            ),
        )
        .expect("metadata");
        skill
    }

    #[cfg(unix)]
    #[test]
    fn parses_valid_channel_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-telegram", "telegram");

        let metadata = load_channel_metadata(&skill).expect("metadata");

        assert_eq!(metadata.id, "telegram");
        assert_eq!(metadata.worker, "scripts/worker");
        assert_eq!(metadata.env, vec!["TELEGRAM_BOT_TOKEN"]);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unknown_metadata_fields() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-telegram", "telegram");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\nextra = true\n\n[channel]\nid = \"telegram\"\nlaunch = \"service\"\nworker = \"scripts/worker\"\n",
        )
        .expect("metadata");

        let err = load_channel_metadata(&skill).expect_err("unknown field should fail");

        assert!(err.to_string().contains("unknown field"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_invalid_channel_metadata_values() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-telegram", "Telegram");

        let err = load_channel_metadata(&skill).expect_err("uppercase id should fail");

        assert!(err.to_string().contains("channel id"));
        validate_channel_env_name("BAD-NAME").expect_err("invalid env name");
    }

    #[cfg(unix)]
    #[test]
    fn discovery_rejects_ambiguous_installed_channel_ids() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        fs::create_dir_all(home.skills_dir()).expect("skills");
        let first = write_channel_skill(temp_dir.path(), "first", "telegram");
        let second = write_channel_skill(temp_dir.path(), "second", "telegram");
        copy_snapshot_tree(&first, &home.skills_dir().join("first")).expect("first");
        copy_snapshot_tree(&second, &home.skills_dir().join("second")).expect("second");

        let err = discover_channel_skill(&home, "telegram").expect_err("ambiguous");

        assert!(err.to_string().contains("multiple installed skills"));
    }
}
