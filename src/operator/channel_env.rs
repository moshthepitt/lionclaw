use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use anyhow::{anyhow, bail, Context, Result};

use crate::{home::LionClawHome, operator::channel_metadata::validate_channel_env_name};

pub type ChannelEnv = BTreeMap<String, String>;

pub fn load_channel_env(home: &LionClawHome, channel_id: &str) -> Result<ChannelEnv> {
    let path = home.channel_env_path(channel_id);
    let Some(content) = read_private_env_file(&path, "channel env")? else {
        return Ok(ChannelEnv::new());
    };
    parse_env_content(&content)
}

pub fn save_channel_env(home: &LionClawHome, channel_id: &str, values: &ChannelEnv) -> Result<()> {
    let dir = home.channel_env_dir();
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    ensure_private_dir(&dir, "channel env directory")?;
    let path = home.channel_env_path(channel_id);
    ensure_file_write_target_not_symlink(&path, "channel env file")?;

    let mut content = String::new();
    for (key, value) in values {
        validate_channel_env_name(key)?;
        content.push_str(key);
        content.push('=');
        content.push_str(&escape_env_value(value));
        content.push('\n');
    }
    fs::write(&path, content).with_context(|| format!("failed to write {}", path.display()))?;
    set_private_file_permissions(&path)?;
    Ok(())
}

pub fn merge_channel_env(
    home: &LionClawHome,
    channel_id: &str,
    updates: &ChannelEnv,
) -> Result<ChannelEnv> {
    let mut existing = load_channel_env(home, channel_id)?;
    for (key, value) in updates {
        validate_channel_env_name(key)?;
        existing.insert(key.clone(), value.clone());
    }
    save_channel_env(home, channel_id, &existing)?;
    Ok(existing)
}

pub fn load_required_channel_env(
    home: &LionClawHome,
    channel_id: &str,
    required_env: &[String],
) -> Result<Vec<(String, String)>> {
    let stored = load_channel_env(home, channel_id)?;
    let mut values = Vec::with_capacity(required_env.len());
    for key in required_env {
        validate_channel_env_name(key)?;
        let value = stored.get(key).ok_or_else(|| {
            anyhow!(
                "required environment value '{key}' is not configured for channel '{channel_id}'"
            )
        })?;
        values.push((key.clone(), value.clone()));
    }
    Ok(values)
}

pub fn missing_required_env(stored: &ChannelEnv, required_env: &[String]) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    for key in required_env {
        validate_channel_env_name(key)?;
        if !stored.contains_key(key) {
            missing.push(key.clone());
        }
    }
    Ok(missing)
}

pub fn parse_env_file(path: &Path) -> Result<ChannelEnv> {
    let content = read_private_or_regular_env_file(path, "env file")?
        .ok_or_else(|| anyhow!("env file {} does not exist", path.display()))?;
    parse_env_content(&content)
}

pub fn collect_from_process_env(keys: &[String]) -> Result<ChannelEnv> {
    let mut values = ChannelEnv::new();
    let mut seen = BTreeSet::new();
    for key in keys {
        validate_channel_env_name(key)?;
        if !seen.insert(key.clone()) {
            continue;
        }
        let value = std::env::var(key)
            .with_context(|| format!("environment variable '{key}' is not set"))?;
        values.insert(key.clone(), value);
    }
    Ok(values)
}

pub fn render_missing_env_repair(channel_id: &str, missing: &[String]) -> String {
    let names = missing.join(", ");
    let from_env = missing
        .iter()
        .map(|key| format!(" --from-env {key}"))
        .collect::<String>();
    format!(
        "missing required environment values for channel '{channel_id}': {names}\nRun:\n  lionclaw connect {channel_id} --env-file ./{}.env\n  lionclaw connect {channel_id}{from_env}",
        channel_id
    )
}

fn parse_env_content(content: &str) -> Result<ChannelEnv> {
    let mut values = ChannelEnv::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            bail!("invalid env line {}; expected KEY=VALUE", index + 1);
        };
        let key = key.trim().to_string();
        validate_channel_env_name(&key)?;
        values.insert(key, unquote_env_value(value.trim())?);
    }
    Ok(values)
}

fn escape_env_value(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '@'))
    {
        return value.to_string();
    }
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{escaped}\"")
}

fn unquote_env_value(value: &str) -> Result<String> {
    let Some(body) = value
        .strip_prefix('"')
        .and_then(|body| body.strip_suffix('"'))
    else {
        return Ok(value.to_string());
    };
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => bail!("invalid trailing escape in env value"),
        }
    }
    Ok(out)
}

fn read_private_env_file(path: &Path, label: &str) -> Result<Option<String>> {
    let content = read_private_or_regular_env_file(path, label)?;
    if content.is_some() {
        harden_private_env_file(path, label)?;
    }
    Ok(content)
}

fn read_private_or_regular_env_file(path: &Path, label: &str) -> Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("{label} {} is not a file", path.display());
    }
    fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))
        .map(Some)
}

fn harden_private_env_file(path: &Path, label: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow!(
            "{label} {} does not have a parent directory",
            path.display()
        )
    })?;
    ensure_private_dir(parent, &format!("{label} directory"))?;
    set_private_file_permissions(path)
}

fn ensure_private_dir(path: &Path, label: &str) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("failed to stat {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("{label} {} is not a directory", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .with_context(|| format!("failed to chmod {}", path.display()))?;
        }
    }
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    Ok(())
}

fn ensure_file_write_target_not_symlink(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("{label} {} must not be a symlink", path.display());
            }
            if metadata.is_dir() {
                bail!("{label} {} is not a file", path.display());
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        load_channel_env, missing_required_env, parse_env_file, save_channel_env, ChannelEnv,
    };
    use crate::home::LionClawHome;

    #[test]
    fn channel_env_round_trips_without_printing_values() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut values = ChannelEnv::new();
        values.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret token".to_string());

        save_channel_env(&home, "telegram", &values).expect("save env");
        let loaded = load_channel_env(&home, "telegram").expect("load env");

        assert_eq!(
            loaded.get("TELEGRAM_BOT_TOKEN").map(String::as_str),
            Some("secret token")
        );
    }

    #[cfg(unix)]
    #[test]
    fn channel_env_file_is_private_and_rejects_symlink() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut values = ChannelEnv::new();
        values.insert("TOKEN".to_string(), "secret".to_string());

        save_channel_env(&home, "telegram", &values).expect("save env");
        let mode = fs::metadata(home.channel_env_path("telegram"))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);

        fs::remove_file(home.channel_env_path("telegram")).expect("remove");
        let outside = temp_dir.path().join("outside.env");
        symlink(&outside, home.channel_env_path("telegram")).expect("symlink");
        let err = save_channel_env(&home, "telegram", &values).expect_err("symlink should fail");
        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside.exists());
    }

    #[test]
    fn env_file_parser_reports_missing_required_names() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=\"secret\"\n").expect("write env");

        let values = parse_env_file(&env_file).expect("parse env");
        assert!(
            missing_required_env(&values, &["TELEGRAM_BOT_TOKEN".to_string()])
                .expect("missing")
                .is_empty()
        );
        assert_eq!(
            missing_required_env(&values, &["OTHER_TOKEN".to_string()]).expect("missing"),
            vec!["OTHER_TOKEN".to_string()]
        );
    }
}
