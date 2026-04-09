use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use tokio::process::Command;

use crate::home::LionClawHome;

pub const DAEMON_UNIT_NAME: &str = "lionclawd.service";

#[derive(Debug, Clone)]
pub struct ManagedServiceUnit {
    pub name: String,
    pub unit_path: PathBuf,
    pub unit_content: String,
    pub env_path: PathBuf,
    pub env_content: String,
}

#[derive(Debug, Clone)]
pub struct ChannelServiceSpec {
    pub channel_id: String,
    pub worker_path: PathBuf,
    pub env: Vec<(String, String)>,
}

pub fn channel_unit_name(channel_id: &str) -> String {
    format!("lionclaw-channel-{}.service", channel_id)
}

pub fn unit_status_is_active(status: &str) -> bool {
    status.split('/').nth(1) == Some("active")
}

pub fn render_daemon_unit(
    home: &LionClawHome,
    daemon_bin: &Path,
    bind_addr: &str,
    runtime_id: &str,
    workspace: &str,
) -> ManagedServiceUnit {
    let env_path = home.services_env_dir().join("lionclawd.env");
    let unit_path = home.services_systemd_dir().join(DAEMON_UNIT_NAME);

    let (host, port) = parse_bind_addr(bind_addr);
    let env_lines = [
        (
            "LIONCLAW_HOME".to_string(),
            home.root().display().to_string(),
        ),
        ("LIONCLAW_BIND_ADDR".to_string(), bind_addr.to_string()),
        ("LIONCLAW_HOST".to_string(), host),
        ("LIONCLAW_PORT".to_string(), port),
        (
            "LIONCLAW_DEFAULT_RUNTIME_ID".to_string(),
            runtime_id.to_string(),
        ),
        ("LIONCLAW_WORKSPACE".to_string(), workspace.to_string()),
    ];
    let env_content = env_lines
        .iter()
        .map(|(key, value)| format!("{key}={}\n", escape_env_value(value)))
        .collect::<String>();
    let unit_content = format!(
        "[Unit]\nDescription=LionClaw daemon\nAfter=default.target\n\n[Service]\nType=simple\nEnvironmentFile={env}\nExecStart={exec}\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        env = env_path.display(),
        exec = daemon_bin.display(),
    );

    ManagedServiceUnit {
        name: DAEMON_UNIT_NAME.to_string(),
        unit_path,
        unit_content,
        env_path,
        env_content,
    }
}

pub fn render_channel_unit(home: &LionClawHome, spec: &ChannelServiceSpec) -> ManagedServiceUnit {
    let name = channel_unit_name(&spec.channel_id);
    let env_path = home
        .services_env_dir()
        .join(format!("{}.env", spec.channel_id));
    let unit_path = home.services_systemd_dir().join(&name);
    let env_content = spec
        .env
        .iter()
        .map(|(key, value)| format!("{key}={}\n", escape_env_value(value)))
        .collect::<String>();
    let unit_content = format!(
        "[Unit]\nDescription=LionClaw channel worker ({channel})\nAfter=lionclawd.service\nRequires=lionclawd.service\n\n[Service]\nType=simple\nEnvironmentFile={env}\nExecStart={exec}\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        channel = spec.channel_id,
        env = env_path.display(),
        exec = spec.worker_path.display(),
    );

    ManagedServiceUnit {
        name,
        unit_path,
        unit_content,
        env_path,
        env_content,
    }
}

#[async_trait]
pub trait ServiceManager: Send + Sync {
    async fn apply_units(
        &self,
        home: &LionClawHome,
        units: &[ManagedServiceUnit],
    ) -> Result<Vec<String>>;
    async fn up_units(&self, units: &[String]) -> Result<()>;
    async fn restart_units(&self, units: &[String]) -> Result<()>;
    async fn down_units(&self, units: &[String]) -> Result<()>;
    async fn unit_status(&self, unit: &str) -> Result<String>;
    async fn logs(&self, units: &[String], lines: usize) -> Result<String>;
}

pub struct SystemdUserServiceManager;

#[async_trait]
impl ServiceManager for SystemdUserServiceManager {
    async fn apply_units(
        &self,
        home: &LionClawHome,
        units: &[ManagedServiceUnit],
    ) -> Result<Vec<String>> {
        let user_unit_dir = systemd_user_unit_dir()?;
        tokio::fs::create_dir_all(&user_unit_dir)
            .await
            .with_context(|| format!("failed to create {}", user_unit_dir.display()))?;
        prune_stale_generated_files(home, &user_unit_dir, units)?;

        let mut changed_units = Vec::new();
        for unit in units {
            if let Some(parent) = unit.unit_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            if let Some(parent) = unit.env_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }

            let unit_changed = file_content_differs(&unit.unit_path, &unit.unit_content)
                .with_context(|| format!("failed to compare {}", unit.unit_path.display()))?;
            let env_changed = file_content_differs(&unit.env_path, &unit.env_content)
                .with_context(|| format!("failed to compare {}", unit.env_path.display()))?;
            tokio::fs::write(&unit.unit_path, &unit.unit_content)
                .await
                .with_context(|| format!("failed to write {}", unit.unit_path.display()))?;
            tokio::fs::write(&unit.env_path, &unit.env_content)
                .await
                .with_context(|| format!("failed to write {}", unit.env_path.display()))?;

            let permissions = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&unit.env_path, permissions)
                .with_context(|| format!("failed to chmod {}", unit.env_path.display()))?;

            let user_link = user_unit_dir.join(&unit.name);
            if path_entry_exists(&user_link)? {
                let metadata = std::fs::symlink_metadata(&user_link)
                    .with_context(|| format!("failed to inspect {}", user_link.display()))?;
                if metadata.file_type().is_symlink() || metadata.is_file() {
                    std::fs::remove_file(&user_link)
                        .with_context(|| format!("failed to remove {}", user_link.display()))?;
                }
            }
            std::os::unix::fs::symlink(&unit.unit_path, &user_link).with_context(|| {
                format!(
                    "failed to link '{}' to '{}'",
                    unit.unit_path.display(),
                    user_link.display()
                )
            })?;

            if unit_changed || env_changed {
                changed_units.push(unit.name.clone());
            }
        }
        run_systemctl(["--user", "daemon-reload"]).await?;
        changed_units.sort();
        changed_units.dedup();
        Ok(changed_units)
    }

    async fn up_units(&self, units: &[String]) -> Result<()> {
        for unit in units {
            run_systemctl(["--user", "enable", "--now", unit]).await?;
        }
        Ok(())
    }

    async fn restart_units(&self, units: &[String]) -> Result<()> {
        for unit in units {
            run_systemctl(["--user", "restart", unit]).await?;
        }
        Ok(())
    }

    async fn down_units(&self, units: &[String]) -> Result<()> {
        for unit in units {
            let _ = run_systemctl(["--user", "disable", "--now", unit]).await;
        }
        Ok(())
    }

    async fn unit_status(&self, unit: &str) -> Result<String> {
        match run_command(
            "systemctl",
            [
                "--user",
                "show",
                unit,
                "--property=LoadState,ActiveState,SubState",
                "--value",
            ],
        )
        .await
        {
            Ok(output) => Ok(output.trim().replace('\n', "/")),
            Err(err) if is_missing_unit_error(&err) => Ok("not-installed".to_string()),
            Err(err) => Err(err),
        }
    }

    async fn logs(&self, units: &[String], lines: usize) -> Result<String> {
        let line_count = lines.to_string();
        let mut command = Command::new("journalctl");
        command
            .arg("--user")
            .arg("--no-pager")
            .arg("-n")
            .arg(&line_count);
        for unit in units {
            command.arg("-u").arg(unit);
        }

        let output = command.output().await.context("failed to run journalctl")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.contains("No journal files were found") || stderr.contains("No entries") {
                return Ok(String::new());
            }
            return Err(anyhow!("journalctl failed: {}", stderr));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

#[derive(Default)]
pub struct FakeServiceManager {
    states: Mutex<HashMap<String, String>>,
    units: Mutex<HashMap<String, ManagedServiceUnit>>,
    restarted: Mutex<Vec<String>>,
}

impl FakeServiceManager {
    pub fn set_unit_status(&self, unit: impl Into<String>, status: impl Into<String>) {
        self.states
            .lock()
            .expect("states lock")
            .insert(unit.into(), status.into());
    }

    pub fn was_restarted(&self, unit: &str) -> bool {
        self.restarted
            .lock()
            .expect("restarted lock")
            .iter()
            .any(|value| value == unit)
    }
}

#[async_trait]
impl ServiceManager for FakeServiceManager {
    async fn apply_units(
        &self,
        _home: &LionClawHome,
        units: &[ManagedServiceUnit],
    ) -> Result<Vec<String>> {
        let mut stored = self.units.lock().expect("units lock");
        let mut changed = Vec::new();
        for unit in units {
            let was_changed = stored
                .get(&unit.name)
                .map(|existing| {
                    existing.unit_content != unit.unit_content
                        || existing.env_content != unit.env_content
                })
                .unwrap_or(true);
            stored.insert(unit.name.clone(), unit.clone());
            if was_changed {
                changed.push(unit.name.clone());
            }
        }
        Ok(changed)
    }

    async fn up_units(&self, units: &[String]) -> Result<()> {
        let mut states = self.states.lock().expect("states lock");
        for unit in units {
            states.insert(unit.clone(), "loaded/active/running".to_string());
        }
        Ok(())
    }

    async fn restart_units(&self, units: &[String]) -> Result<()> {
        let mut states = self.states.lock().expect("states lock");
        let mut restarted = self.restarted.lock().expect("restarted lock");
        for unit in units {
            states.insert(unit.clone(), "loaded/active/running".to_string());
            restarted.push(unit.clone());
        }
        Ok(())
    }

    async fn down_units(&self, units: &[String]) -> Result<()> {
        let mut states = self.states.lock().expect("states lock");
        for unit in units {
            states.insert(unit.clone(), "loaded/inactive/dead".to_string());
        }
        Ok(())
    }

    async fn unit_status(&self, unit: &str) -> Result<String> {
        let states = self.states.lock().expect("states lock");
        Ok(states
            .get(unit)
            .cloned()
            .unwrap_or_else(|| "not-found".to_string()))
    }

    async fn logs(&self, units: &[String], _lines: usize) -> Result<String> {
        Ok(units.join("\n"))
    }
}

fn systemd_user_unit_dir() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".config/systemd/user"))
}

fn parse_bind_addr(bind_addr: &str) -> (String, String) {
    if let Ok(addr) = bind_addr.parse::<std::net::SocketAddr>() {
        return (addr.ip().to_string(), addr.port().to_string());
    }

    bind_addr
        .rsplit_once(':')
        .map(|(host, port)| (host.to_string(), port.to_string()))
        .unwrap_or_else(|| ("127.0.0.1".to_string(), "8979".to_string()))
}

fn escape_env_value(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{}\"", escaped)
}

fn prune_stale_generated_files(
    home: &LionClawHome,
    user_unit_dir: &Path,
    units: &[ManagedServiceUnit],
) -> Result<()> {
    let desired_names = units
        .iter()
        .map(|unit| unit.name.as_str())
        .collect::<Vec<_>>();
    prune_unit_dir(&home.services_systemd_dir(), &desired_names)?;
    prune_env_dir(&home.services_env_dir(), units)?;
    prune_user_unit_dir(home, user_unit_dir, &desired_names)?;
    Ok(())
}

fn prune_unit_dir(dir: &Path, desired_names: &[&str]) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to iterate {}", dir.display()))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.starts_with("lionclaw") || !file_name.ends_with(".service") {
            continue;
        }
        if desired_names.iter().any(|name| *name == file_name) {
            continue;
        }
        remove_path_if_exists(&path)?;
    }

    Ok(())
}

fn prune_env_dir(dir: &Path, units: &[ManagedServiceUnit]) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    let desired = units
        .iter()
        .filter_map(|unit| unit.env_path.file_name().map(|value| value.to_os_string()))
        .collect::<Vec<_>>();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to iterate {}", dir.display()))?;
        let file_name = entry.file_name();
        if !desired.iter().any(|wanted| wanted == &file_name) {
            remove_path_if_exists(&entry.path())?;
        }
    }

    Ok(())
}

fn prune_user_unit_dir(home: &LionClawHome, dir: &Path, desired_names: &[&str]) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    let managed_dir = home.services_systemd_dir();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to iterate {}", dir.display()))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.starts_with("lionclaw") || !file_name.ends_with(".service") {
            continue;
        }
        if desired_names.iter().any(|name| *name == file_name) {
            continue;
        }

        let metadata = std::fs::symlink_metadata(&path)
            .with_context(|| format!("failed to inspect {}", path.display()))?;
        if !metadata.file_type().is_symlink() {
            continue;
        }

        let target = std::fs::read_link(&path)
            .with_context(|| format!("failed to read link {}", path.display()))?;
        let resolved = if target.is_absolute() {
            target
        } else {
            path.parent()
                .unwrap_or(dir)
                .join(target)
                .components()
                .collect::<PathBuf>()
        };
        if resolved.starts_with(&managed_dir) {
            remove_path_if_exists(&path)?;
        }
    }

    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    if !path_entry_exists(path)? {
        return Ok(());
    }

    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if metadata.file_type().is_dir() {
        std::fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    } else {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

fn path_entry_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn file_content_differs(path: &Path, expected: &str) -> Result<bool> {
    match std::fs::read_to_string(path) {
        Ok(existing) => Ok(existing != expected),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn is_missing_unit_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.contains("not-found") || message.contains("could not be found")
}

async fn run_systemctl<const N: usize>(args: [&str; N]) -> Result<()> {
    let _ = run_command("systemctl", args).await?;
    Ok(())
}

async fn run_command<const N: usize>(program: &str, args: [&str; N]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to run {}", program))?;

    if !output.status.success() {
        return Err(anyhow!(
            "{} failed: {}",
            program,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use std::{fs, os::unix::fs::symlink, path::Path};

    use super::{
        channel_unit_name, path_entry_exists, prune_user_unit_dir, render_channel_unit,
        render_daemon_unit, ChannelServiceSpec,
    };

    #[test]
    fn renders_expected_unit_names() {
        assert_eq!(
            channel_unit_name("telegram"),
            "lionclaw-channel-telegram.service"
        );
    }

    #[test]
    fn renders_units_with_expected_execs() {
        let home = crate::home::LionClawHome::new("/tmp/lionclaw-home".into());
        let daemon = render_daemon_unit(
            &home,
            Path::new("/tmp/bin/lionclawd"),
            "127.0.0.1:8979",
            "codex",
            "main",
        );
        assert!(daemon.unit_content.contains("ExecStart=/tmp/bin/lionclawd"));
        assert!(daemon
            .env_content
            .contains("LIONCLAW_BIND_ADDR=\"127.0.0.1:8979\""));
        assert!(daemon.env_content.contains("LIONCLAW_WORKSPACE=\"main\""));

        let channel = render_channel_unit(
            &home,
            &ChannelServiceSpec {
                channel_id: "telegram".to_string(),
                worker_path: "/tmp/skills/telegram/scripts/worker".into(),
                env: vec![("TELEGRAM_BOT_TOKEN".to_string(), "secret".to_string())],
            },
        );
        assert!(channel.unit_content.contains("lionclawd.service"));
    }

    #[test]
    fn prunes_only_user_symlinks_that_point_to_managed_units() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        std::fs::create_dir_all(home.services_systemd_dir()).expect("managed dir");
        let user_dir = temp_dir.path().join("systemd-user");
        std::fs::create_dir_all(&user_dir).expect("user dir");

        let managed_unit = home
            .services_systemd_dir()
            .join("lionclaw-channel-old.service");
        fs::write(&managed_unit, "[Unit]\n").expect("managed unit");
        let managed_link = user_dir.join("lionclaw-channel-old.service");
        symlink(&managed_unit, &managed_link).expect("managed link");

        let unrelated_target = temp_dir.path().join("custom.service");
        fs::write(&unrelated_target, "[Unit]\n").expect("custom unit");
        let unrelated_link = user_dir.join("lionclaw-custom.service");
        symlink(&unrelated_target, &unrelated_link).expect("custom link");

        prune_user_unit_dir(&home, &user_dir, &[]).expect("prune");

        assert!(!managed_link.exists(), "managed symlink should be removed");
        assert!(
            unrelated_link.exists(),
            "unrelated symlink should be preserved"
        );
    }

    #[test]
    fn detects_broken_symlink_as_existing_path_entry() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let link_path = temp_dir.path().join("broken.service");
        symlink(temp_dir.path().join("missing.service"), &link_path).expect("broken symlink");

        assert!(
            path_entry_exists(&link_path).expect("inspect path"),
            "broken symlink should still count as an existing entry"
        );
    }
}
