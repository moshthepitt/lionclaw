use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;

use crate::{
    home::LionClawHome,
    kernel::skills::validate_skill_alias,
    operator::{
        config::ChannelLaunchMode,
        skill_metadata::{
            canonical_skill_dir, resolve_skill_entrypoint, skill_metadata_declares_channel,
            SkillEntrypointSymlinkPolicy, SKILL_METADATA_FILE,
        },
        snapshot::installed_snapshot_matches_source,
    },
};

pub const CHANNEL_METADATA_FILE: &str = SKILL_METADATA_FILE;
pub const DEFAULT_CHANNEL_WORKER: &str = "scripts/worker";
const RESERVED_CHANNEL_ENV_PREFIX: &str = "LIONCLAW_";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelMetadata {
    pub id: String,
    pub launch: ChannelLaunchMode,
    pub worker: String,
    pub setup: Option<ChannelSetupMetadata>,
    pub env: Vec<String>,
    pub optional_env: Vec<String>,
    pub contact: Option<ChannelContactMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelSetupMetadata {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelContactMetadata {
    pub conversation_ref_template: String,
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
    #[serde(default)]
    contact: Option<ChannelContactMetadataSection>,
    #[allow(
        dead_code,
        reason = "channel metadata parsing allows memory projector metadata in the shared skill metadata file"
    )]
    #[serde(default)]
    memory_projector: Option<toml::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelMetadataSection {
    id: String,
    launch: String,
    worker: String,
    #[serde(default)]
    setup: Option<ChannelSetupMetadataSection>,
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    optional_env: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelSetupMetadataSection {
    command: String,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelContactMetadataSection {
    conversation_ref_template: String,
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
    let installed = discover_installed_channel_skill(home, raw)?;
    let bundled = discover_bundled_channel_skill(raw)?;
    match (installed, bundled) {
        (Some(installed), Some(bundled)) => {
            if installed_channel_is_refreshable_bundled_snapshot(raw, &installed, &bundled)? {
                Ok(bundled)
            } else {
                Ok(installed)
            }
        }
        (Some(installed), _) => Ok(installed),
        (None, Some(bundled)) => Ok(bundled),
        (None, None) => bail!(
            "channel '{raw}' was not found; pass an explicit skill path or install a skill with matching channel metadata"
        ),
    }
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
    let setup = parsed
        .channel
        .setup
        .map(normalize_channel_setup_metadata)
        .transpose()?;
    let (env, optional_env) =
        normalize_channel_env_metadata(parsed.channel.env, parsed.channel.optional_env)?;
    let contact = parsed
        .contact
        .map(|contact| -> Result<ChannelContactMetadata> {
            let template = contact.conversation_ref_template.trim().to_string();
            validate_contact_template(&template)?;
            Ok(ChannelContactMetadata {
                conversation_ref_template: template,
            })
        })
        .transpose()?;

    Ok(ChannelMetadata {
        id,
        launch,
        worker,
        setup,
        env,
        optional_env,
        contact,
    })
}

fn normalize_channel_env_metadata(
    required: Vec<String>,
    optional: Vec<String>,
) -> Result<(Vec<String>, Vec<String>)> {
    let required = normalize_env_names(required)?;
    let optional = normalize_env_names(optional)?;
    let required_set = required.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let duplicate_optional = optional
        .iter()
        .find(|key| required_set.contains(key.as_str()));
    if let Some(key) = duplicate_optional {
        bail!("channel optional_env duplicates required env key '{key}'");
    }

    Ok((required, optional))
}

fn normalize_env_names(raw: Vec<String>) -> Result<Vec<String>> {
    let mut names = raw
        .into_iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    for key in &names {
        validate_channel_env_name(key)?;
    }
    names.sort();
    names.dedup();
    Ok(names)
}

pub fn render_contact_template(template: &str, instance_name: &str) -> Result<String> {
    validate_contact_template(template)?;
    let rendered = template.replace("{instance}", instance_name);
    let rendered = rendered.trim().to_string();
    if rendered.is_empty() {
        bail!("contact conversation_ref template rendered an empty value");
    }
    Ok(rendered)
}

pub fn resolve_channel_worker_entrypoint(
    skill_dir: &Path,
    worker: Option<&str>,
) -> Result<PathBuf> {
    let worker = worker.unwrap_or(DEFAULT_CHANNEL_WORKER);
    validate_channel_worker(skill_dir, worker)
}

pub fn resolve_channel_setup_command_entrypoint(
    skill_dir: &Path,
    setup: &ChannelSetupMetadata,
) -> Result<PathBuf> {
    resolve_channel_entrypoint(skill_dir, &setup.command, "channel setup command")
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
    if trimmed.starts_with(RESERVED_CHANNEL_ENV_PREFIX) {
        bail!(
            "environment variable name '{name}' uses the reserved LionClaw namespace '{RESERVED_CHANNEL_ENV_PREFIX}'"
        );
    }
    Ok(())
}

fn validate_contact_template(template: &str) -> Result<()> {
    if template.is_empty() {
        bail!("contact conversation_ref_template is required");
    }
    let mut saw_instance = false;
    let mut chars = template.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '}' => bail!("contact conversation_ref_template contains an unmatched '}}'"),
            '{' => {
                let mut variable = String::new();
                let mut closed = false;
                for inner in chars.by_ref() {
                    if inner == '}' {
                        closed = true;
                        break;
                    }
                    variable.push(inner);
                }
                if !closed {
                    bail!("contact conversation_ref_template contains an unmatched '{{'");
                }
                if variable != "instance" {
                    bail!(
                        "contact conversation_ref_template uses unsupported variable '{{{variable}}}'; supported variable: '{{instance}}'"
                    );
                }
                saw_instance = true;
            }
            _ => {}
        }
    }
    if !saw_instance {
        bail!("contact conversation_ref_template must include '{{instance}}'");
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
        if !skill_metadata_declares_channel(&skill_dir)? {
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

fn discover_bundled_channel_skill(channel_id: &str) -> Result<Option<DiscoveredChannelSkill>> {
    let bundled = bundled_channel_skill_dir(channel_id);
    if !bundled.exists() {
        return Ok(None);
    }
    let skill_dir = canonical_skill_dir(&bundled)?;
    let metadata = load_channel_metadata(&skill_dir)?;
    if metadata.id != channel_id {
        bail!(
            "bundled channel skill '{}' declares channel id '{}'",
            bundled.display(),
            metadata.id
        );
    }
    Ok(Some(DiscoveredChannelSkill {
        source: ChannelSkillSource::Bundled,
        skill_dir,
        metadata,
    }))
}

fn installed_channel_is_refreshable_bundled_snapshot(
    channel_id: &str,
    installed: &DiscoveredChannelSkill,
    bundled: &DiscoveredChannelSkill,
) -> Result<bool> {
    let ChannelSkillSource::Installed { alias } = &installed.source else {
        return Ok(false);
    };

    installed_channel_snapshot_is_from_bundled_source(
        alias,
        channel_id,
        &installed.skill_dir,
        &bundled.skill_dir,
    )
    .with_context(|| format!("failed to verify installed channel skill '{alias}' source metadata"))
}

pub(crate) fn installed_channel_snapshot_is_from_bundled_source(
    alias: &str,
    channel_id: &str,
    snapshot_dir: &Path,
    bundled_skill_dir: &Path,
) -> Result<bool> {
    if alias != channel_id {
        return Ok(false);
    }
    installed_snapshot_matches_source(snapshot_dir, bundled_skill_dir)
}

fn validate_channel_worker(skill_dir: &Path, worker: &str) -> Result<PathBuf> {
    resolve_channel_entrypoint(skill_dir, worker, "channel worker")
}

fn normalize_channel_setup_metadata(
    setup: ChannelSetupMetadataSection,
) -> Result<ChannelSetupMetadata> {
    let command = setup.command.trim().to_string();
    validate_channel_entrypoint_path(&command, "channel setup command")?;
    let args = setup
        .args
        .into_iter()
        .map(normalize_setup_arg)
        .collect::<Result<Vec<_>>>()?;
    Ok(ChannelSetupMetadata { command, args })
}

fn normalize_setup_arg(arg: String) -> Result<String> {
    if arg.is_empty() {
        bail!("channel setup args must not include empty values");
    }
    if arg.chars().any(char::is_control) {
        bail!("channel setup args must not include control characters");
    }
    Ok(arg)
}

fn resolve_channel_entrypoint(
    skill_dir: &Path,
    relative_path: &str,
    label: &str,
) -> Result<PathBuf> {
    let normalized = normalize_channel_entrypoint_path(relative_path, label)?;
    let normalized = normalized.to_string_lossy();
    resolve_skill_entrypoint(
        skill_dir,
        normalized.as_ref(),
        label,
        SkillEntrypointSymlinkPolicy::AllowParentSymlinks,
    )
}

fn validate_channel_entrypoint_path(relative_path: &str, label: &str) -> Result<()> {
    normalize_channel_entrypoint_path(relative_path, label).map(|_| ())
}

fn normalize_channel_entrypoint_path(relative_path: &str, label: &str) -> Result<PathBuf> {
    if relative_path.is_empty() {
        bail!("{label} path is required");
    }
    let path = Path::new(relative_path);
    if path.is_absolute() {
        bail!("{label} path '{relative_path}' must be relative to the skill directory");
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("{label} path '{relative_path}' must stay inside the skill directory");
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        bail!("{label} path is required");
    }
    Ok(normalized)
}

fn looks_like_path(raw: &str) -> bool {
    let path = Path::new(raw);
    path.is_absolute()
        || raw.starts_with('.')
        || raw.contains(std::path::MAIN_SEPARATOR)
        || raw.contains('/')
}

pub(crate) fn bundled_channel_skill_dir(channel_id: &str) -> PathBuf {
    bundled_channel_skill_dir_from_exe(channel_id, std::env::current_exe().ok().as_deref())
}

fn bundled_channel_skill_dir_from_exe(channel_id: &str, current_exe: Option<&Path>) -> PathBuf {
    let skill_dir_name = bundled_channel_skill_name(channel_id);
    if let Some(exe_dir) = current_exe.and_then(Path::parent) {
        let installed = exe_dir.join("skills").join(&skill_dir_name);
        if installed.exists() {
            return installed;
        }
    }
    source_bundled_channel_skill_dir(&skill_dir_name)
}

fn source_bundled_channel_skill_dir(skill_dir_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("skills")
        .join(skill_dir_name)
}

fn bundled_channel_skill_name(channel_id: &str) -> String {
    format!("channel-{channel_id}")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        bundled_channel_skill_dir, bundled_channel_skill_dir_from_exe, discover_channel_skill,
        load_channel_metadata, render_contact_template, resolve_channel_setup_command_entrypoint,
        validate_channel_env_name, ChannelSkillSource,
    };
    use crate::{
        home::LionClawHome,
        operator::snapshot::{copy_snapshot_tree, install_snapshot, SKILL_INSTALL_METADATA_FILE},
    };

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
                "version = 1\n\n[channel]\nid = \"{id}\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\nenv = [\"TELEGRAM_BOT_TOKEN\"]\n"
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
        assert!(metadata.optional_env.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn parses_channel_worker_paths_with_current_directory_components() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-telegram", "telegram");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"telegram\"\nlaunch = \"background\"\nworker = \"./scripts/./worker\"\n",
        )
        .expect("metadata");

        let metadata = load_channel_metadata(&skill).expect("metadata");

        assert_eq!(metadata.worker, "./scripts/./worker");
    }

    #[cfg(unix)]
    #[test]
    fn parses_optional_channel_env_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-email", "email");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"email\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\nenv = [\"EMAIL_ADDRESS\"]\noptional_env = [\"EMAIL_ADMIN_DIGEST_TO\", \"EMAIL_MAX_MESSAGE_BYTES\"]\n",
        )
        .expect("metadata");

        let metadata = load_channel_metadata(&skill).expect("metadata");

        assert_eq!(metadata.env, vec!["EMAIL_ADDRESS"]);
        assert_eq!(
            metadata.optional_env,
            vec!["EMAIL_ADMIN_DIGEST_TO", "EMAIL_MAX_MESSAGE_BYTES"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn parses_channel_metadata_with_memory_projector_section() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-memory", "memory");
        fs::write(skill.join("scripts/projector"), "#!/usr/bin/env bash\n").expect("projector");
        make_executable(&skill.join("scripts/projector"));
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"memory\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[memory_projector]\ncommand = \"scripts/projector\"\n",
        )
        .expect("metadata");

        let metadata = load_channel_metadata(&skill).expect("metadata");

        assert_eq!(metadata.id, "memory");
        assert_eq!(metadata.worker, "scripts/worker");
    }

    #[test]
    fn bundled_email_auth_mode_is_optional_for_basic_compatibility() {
        let metadata =
            load_channel_metadata(&bundled_channel_skill_dir("email")).expect("email metadata");

        assert!(!metadata.env.iter().any(|key| key == "EMAIL_AUTH_MODE"));
        assert!(metadata
            .optional_env
            .iter()
            .any(|key| key == "EMAIL_AUTH_MODE"));
    }

    #[cfg(unix)]
    #[test]
    fn parses_setup_metadata_without_requiring_source_command_file() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-email", "email");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"email\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\nenv = [\"EMAIL_ADDRESS\"]\n\n[channel.setup]\ncommand = \"bin/lionclaw-channel-email\"\nargs = [\"setup\"]\n",
        )
        .expect("metadata");

        let metadata = load_channel_metadata(&skill).expect("metadata");
        let setup = metadata.setup.expect("setup metadata");

        assert_eq!(setup.command, "bin/lionclaw-channel-email");
        assert_eq!(setup.args, vec!["setup"]);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_setup_metadata_that_escapes_skill_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-email", "email");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"email\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[channel.setup]\ncommand = \"../helper\"\n",
        )
        .expect("metadata");

        let err = load_channel_metadata(&skill).expect_err("escaping setup command should fail");

        assert!(err.to_string().contains("must stay inside"));
    }

    #[cfg(unix)]
    #[test]
    fn resolves_installed_setup_command_entrypoint() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-email", "email");
        fs::create_dir_all(skill.join("bin")).expect("bin dir");
        fs::write(skill.join("bin/setup"), "#!/usr/bin/env bash\n").expect("setup");
        make_executable(&skill.join("bin/setup"));
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"email\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[channel.setup]\ncommand = \"bin/setup\"\n",
        )
        .expect("metadata");

        let setup = load_channel_metadata(&skill)
            .expect("metadata")
            .setup
            .expect("setup");

        assert_eq!(
            resolve_channel_setup_command_entrypoint(&skill, &setup).expect("entrypoint"),
            fs::canonicalize(skill.join("bin/setup")).expect("canonical setup")
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolves_setup_command_paths_with_current_directory_components() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-email", "email");
        fs::create_dir_all(skill.join("bin")).expect("bin dir");
        fs::write(skill.join("bin/setup"), "#!/usr/bin/env bash\n").expect("setup");
        make_executable(&skill.join("bin/setup"));
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"email\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[channel.setup]\ncommand = \"./bin/./setup\"\n",
        )
        .expect("metadata");

        let setup = load_channel_metadata(&skill)
            .expect("metadata")
            .setup
            .expect("setup");

        assert_eq!(setup.command, "./bin/./setup");
        assert_eq!(
            resolve_channel_setup_command_entrypoint(&skill, &setup).expect("entrypoint"),
            fs::canonicalize(skill.join("bin/setup")).expect("canonical setup")
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_optional_env_that_duplicates_required_env() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-email", "email");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"email\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\nenv = [\"EMAIL_ADDRESS\"]\noptional_env = [\"EMAIL_ADDRESS\"]\n",
        )
        .expect("metadata");

        let err = load_channel_metadata(&skill).expect_err("duplicate env should fail");

        assert!(err.to_string().contains("optional_env duplicates"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unknown_metadata_fields() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-telegram", "telegram");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\nextra = true\n\n[channel]\nid = \"telegram\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n",
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
        validate_channel_env_name("LIONCLAW_HOME").expect_err("reserved env name");
    }

    #[cfg(unix)]
    #[test]
    fn rejects_reserved_channel_env_metadata_values() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-telegram", "telegram");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"telegram\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\nenv = [\"LIONCLAW_HOME\"]\n",
        )
        .expect("metadata");

        let err = load_channel_metadata(&skill).expect_err("reserved env should fail");

        assert!(err.to_string().contains("reserved LionClaw namespace"));
    }

    #[cfg(unix)]
    #[test]
    fn parses_and_renders_contact_template_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-team-local", "team-local");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"team-local\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[contact]\nconversation_ref_template = \"member:{instance}\"\n",
        )
        .expect("metadata");

        let metadata = load_channel_metadata(&skill).expect("metadata");
        let template = metadata
            .contact
            .expect("contact metadata")
            .conversation_ref_template;

        assert_eq!(
            render_contact_template(&template, "reviewer").expect("render template"),
            "member:reviewer"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unknown_contact_template_variables() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-team-local", "team-local");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"team-local\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[contact]\nconversation_ref_template = \"member:{handle}\"\n",
        )
        .expect("metadata");

        let err = load_channel_metadata(&skill).expect_err("unknown variable should fail");

        assert!(err.to_string().contains("unsupported variable"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_static_contact_template_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill = write_channel_skill(temp_dir.path(), "channel-team-local", "team-local");
        fs::write(
            skill.join("lionclaw.toml"),
            "version = 1\n\n[channel]\nid = \"team-local\"\nlaunch = \"background\"\nworker = \"scripts/worker\"\n\n[contact]\nconversation_ref_template = \"member:reviewer\"\n",
        )
        .expect("metadata");

        let err = load_channel_metadata(&skill).expect_err("static template should fail");

        assert!(err.to_string().contains("must include '{instance}'"));
    }

    #[test]
    fn bundled_skill_resolution_prefers_release_layout_next_to_executable() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let install_dir = temp_dir.path().join("install");
        let skill_dir = install_dir.join("skills/channel-team-local");
        fs::create_dir_all(&skill_dir).expect("skill dir");
        fs::write(install_dir.join("lionclaw"), "").expect("exe");

        let resolved =
            bundled_channel_skill_dir_from_exe("team-local", Some(&install_dir.join("lionclaw")));

        assert_eq!(resolved, skill_dir);
    }

    #[test]
    fn bundled_skill_resolution_falls_back_to_source_layout() {
        let temp_dir = tempfile::tempdir().expect("temp dir");

        let resolved = bundled_channel_skill_dir_from_exe(
            "team-local",
            Some(&temp_dir.path().join("lionclaw")),
        );

        assert_eq!(
            resolved,
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../skills/channel-team-local")
        );
    }

    #[cfg(unix)]
    #[test]
    fn discovery_rejects_ambiguous_installed_channel_ids() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        fs::create_dir_all(home.skills_dir()).expect("skills");
        let first = write_channel_skill(temp_dir.path(), "first", "matrix");
        let second = write_channel_skill(temp_dir.path(), "second", "matrix");
        copy_snapshot_tree(&first, &home.skills_dir().join("first")).expect("first");
        copy_snapshot_tree(&second, &home.skills_dir().join("second")).expect("second");

        let err = discover_channel_skill(&home, "matrix").expect_err("ambiguous");

        assert!(err.to_string().contains("multiple installed skills"));
    }

    #[cfg(unix)]
    #[test]
    fn discovery_refreshes_prior_bundled_channel_snapshot() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        fs::create_dir_all(home.skills_dir()).expect("skills");
        let bundled = bundled_channel_skill_dir("telegram");
        install_snapshot(
            &home,
            "telegram",
            bundled.to_string_lossy().as_ref(),
            "local",
        )
        .expect("install bundled snapshot");
        fs::write(
            home.skills_dir().join("telegram/SKILL.md"),
            "---\nname: channel-telegram\ndescription: old\n---\nold snapshot\n",
        )
        .expect("stale skill");
        fs::remove_file(home.skills_dir().join("telegram/lionclaw.toml"))
            .expect("remove stale metadata");

        let discovered = discover_channel_skill(&home, "telegram").expect("discover telegram");

        assert_eq!(discovered.source, ChannelSkillSource::Bundled);
        assert_eq!(discovered.metadata.id, "telegram");
        assert!(discovered.skill_dir.ends_with("skills/channel-telegram"));
    }

    #[cfg(unix)]
    #[test]
    fn discovery_preserves_external_channel_snapshot_over_bundled_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        fs::create_dir_all(home.skills_dir()).expect("skills");
        let external = write_channel_skill(temp_dir.path(), "external-telegram", "telegram");
        copy_snapshot_tree(&external, &home.skills_dir().join("telegram")).expect("install");

        let discovered = discover_channel_skill(&home, "telegram").expect("discover telegram");

        assert_eq!(
            discovered.source,
            ChannelSkillSource::Installed {
                alias: "telegram".to_string()
            }
        );
        assert_eq!(discovered.metadata.id, "telegram");
        assert_eq!(
            discovered.skill_dir,
            fs::canonicalize(home.skills_dir().join("telegram")).expect("installed path")
        );
    }

    #[cfg(unix)]
    #[test]
    fn discovery_skips_memory_only_installed_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        fs::create_dir_all(home.skills_dir()).expect("skills");
        let memory = temp_dir.path().join("memory-core");
        fs::create_dir_all(memory.join("scripts")).expect("scripts");
        fs::write(
            memory.join("SKILL.md"),
            "---\nname: memory-core\ndescription: memory\n---\n",
        )
        .expect("skill");
        fs::write(memory.join("scripts/projector"), "#!/usr/bin/env bash\n").expect("projector");
        make_executable(&memory.join("scripts/projector"));
        fs::write(
            memory.join("lionclaw.toml"),
            "version = 1\n\n[memory_projector]\ncommand = \"scripts/projector\"\n",
        )
        .expect("metadata");
        copy_snapshot_tree(&memory, &home.skills_dir().join("memory-core")).expect("install");

        let err = discover_channel_skill(&home, "memory-core")
            .expect_err("memory-only skill should not be discovered as a channel");

        assert!(err.to_string().contains("was not found"));
    }

    #[cfg(unix)]
    #[test]
    fn discovery_preserves_external_channel_snapshot_with_malformed_install_metadata() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        fs::create_dir_all(home.skills_dir()).expect("skills");
        let external = write_channel_skill(temp_dir.path(), "external-telegram", "telegram");
        let snapshot_dir = home.skills_dir().join("telegram");
        copy_snapshot_tree(&external, &snapshot_dir).expect("install");
        fs::write(
            snapshot_dir.join(SKILL_INSTALL_METADATA_FILE),
            "source = [\n",
        )
        .expect("malformed metadata");

        let discovered = discover_channel_skill(&home, "telegram").expect("discover telegram");

        assert_eq!(
            discovered.source,
            ChannelSkillSource::Installed {
                alias: "telegram".to_string()
            }
        );
        assert_eq!(discovered.metadata.id, "telegram");
        assert_eq!(
            discovered.skill_dir,
            fs::canonicalize(snapshot_dir).expect("installed path")
        );
    }
}
