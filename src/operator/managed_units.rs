use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tokio::process::Command;
use tracing::warn;
use uuid::Uuid;

use crate::{
    home::LionClawHome,
    operator::private_paths::{
        create_private_dir_all, ensure_private_file_readable, ensure_private_file_write_target,
    },
};

pub const DAEMON_UNIT_NAME: &str = "lionclawd.service";

#[derive(Debug, Clone)]
pub struct ManagedUnit {
    pub name: String,
    pub unit_path: PathBuf,
    pub unit_content: String,
    pub env_path: PathBuf,
    pub env_content: String,
    pub extra_env_files: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitIdentity {
    pub unit_group_id: String,
    pub home_id: String,
    pub home_root: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct DaemonUnitSpec<'a> {
    pub bind_addr: &'a str,
    pub runtime_id: &'a str,
    pub workspace: &'a str,
    pub project_workspace_root: &'a Path,
    pub daemon_fingerprint: &'a str,
    pub codex_home_override: Option<&'a Path>,
}

#[derive(Debug, Clone)]
pub struct ChannelUnitSpec {
    pub channel_id: String,
    pub worker_path: PathBuf,
    pub env: Vec<(String, String)>,
    pub channel_env_path: Option<PathBuf>,
}

pub fn daemon_unit_name(identity: &UnitIdentity) -> String {
    format!("lionclaw-{}.service", identity.unit_group_id)
}

pub fn daemon_env_path(home: &LionClawHome, identity: &UnitIdentity) -> PathBuf {
    unit_env_path(home, &daemon_unit_name(identity))
}

pub fn channel_unit_name(identity: &UnitIdentity, channel_id: &str) -> String {
    format!(
        "lionclaw-channel-{}-{channel_id}.service",
        identity.unit_group_id
    )
}

pub fn unit_status_is_active(status: &str) -> bool {
    status.split('/').nth(1) == Some("active")
}

pub fn ensure_unit_identity(home: &LionClawHome) -> Result<UnitIdentity> {
    let identity = read_or_create_unit_identity(home)?;
    ensure_identity_not_colliding(home, identity)
}

pub fn existing_unit_identity(home: &LionClawHome) -> Result<Option<UnitIdentity>> {
    let Some(unit_group_id) = read_unit_group_id(home)? else {
        return Ok(None);
    };
    let home_id = match futures_home_id(home)? {
        Some(home_id) => home_id,
        None => return Ok(None),
    };
    let home_root = canonical_home_root(home)?;
    Ok(Some(UnitIdentity {
        unit_group_id,
        home_id,
        home_root,
    }))
}

fn read_or_create_unit_identity(home: &LionClawHome) -> Result<UnitIdentity> {
    let unit_group_id = match read_unit_group_id(home)? {
        Some(unit_group_id) => unit_group_id,
        None => {
            let unit_group_id = Uuid::new_v4().to_string();
            write_unit_group_id(home, &unit_group_id)?;
            unit_group_id
        }
    };
    let home_id = futures_home_id(home)?.ok_or_else(|| {
        anyhow!(
            "home id is not configured for {}; create a LionClaw project instance first",
            home.root().display()
        )
    })?;
    let home_root = canonical_home_root(home)?;
    Ok(UnitIdentity {
        unit_group_id,
        home_id,
        home_root,
    })
}

fn ensure_identity_not_colliding(
    home: &LionClawHome,
    identity: UnitIdentity,
) -> Result<UnitIdentity> {
    ensure_identity_not_colliding_in_dir(home, identity, &systemd_user_unit_dir()?)
}

fn ensure_identity_not_colliding_in_dir(
    home: &LionClawHome,
    identity: UnitIdentity,
    systemd_dir: &Path,
) -> Result<UnitIdentity> {
    if find_existing_unit_group_id_collision(&identity, systemd_dir)?.is_none() {
        return Ok(identity);
    }

    let unit_group_id = Uuid::new_v4().to_string();
    write_unit_group_id(home, &unit_group_id)?;
    Ok(UnitIdentity {
        unit_group_id,
        home_id: identity.home_id,
        home_root: identity.home_root,
    })
}

fn find_existing_unit_group_id_collision(
    identity: &UnitIdentity,
    systemd_dir: &Path,
) -> Result<Option<PathBuf>> {
    if !systemd_dir.exists() {
        return Ok(None);
    }

    for entry in std::fs::read_dir(systemd_dir)
        .with_context(|| format!("failed to read directory {}", systemd_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to iterate {}", systemd_dir.display()))?;
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path)
            .with_context(|| format!("failed to inspect {}", path.display()))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            continue;
        }

        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if unit_metadata_value(&content, "X-LionClaw-UnitGroupId").as_deref()
            != Some(identity.unit_group_id.as_str())
        {
            continue;
        }
        let Some(recorded_home) =
            unit_metadata_value(&content, "X-LionClaw-HomeRoot").map(PathBuf::from)
        else {
            continue;
        };
        if recorded_home != identity.home_root && recorded_home.exists() {
            return Ok(Some(recorded_home));
        }
    }

    Ok(None)
}

fn read_unit_group_id(home: &LionClawHome) -> Result<Option<String>> {
    let path = home.unit_group_id_path();
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    if metadata.file_type().is_symlink() {
        bail!(
            "unit group id file {} must not be a symlink",
            path.display()
        );
    }
    if !metadata.is_file() {
        bail!("unit group id path {} is not a file", path.display());
    }
    let value = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    Uuid::parse_str(value)
        .with_context(|| format!("invalid unit group id in {}", path.display()))?;
    Ok(Some(value.to_string()))
}

fn write_unit_group_id(home: &LionClawHome, unit_group_id: &str) -> Result<()> {
    Uuid::parse_str(unit_group_id).context("unit group id must be a UUID")?;
    let path = home.unit_group_id_path();
    ensure_private_file_write_target(home, &path, "unit group id file")?;
    std::fs::write(&path, format!("{unit_group_id}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    #[cfg(unix)]
    {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    Ok(())
}

fn futures_home_id(home: &LionClawHome) -> Result<Option<String>> {
    let path = home.home_id_path();
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to stat {}", path.display())),
    };
    if metadata.file_type().is_symlink() {
        bail!("home id file {} must not be a symlink", path.display());
    }
    if !metadata.is_file() {
        bail!("home id path {} is not a file", path.display());
    }
    let value = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(Some(value.trim().to_string()).filter(|value| !value.is_empty()))
}

fn canonical_home_root(home: &LionClawHome) -> Result<PathBuf> {
    std::fs::canonicalize(home.root())
        .with_context(|| format!("failed to resolve {}", home.root().display()))
}

pub fn unit_recorded_home_root(path: &Path) -> Result<Option<PathBuf>> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", path.display())),
    };
    Ok(unit_metadata_value(&content, "X-LionClaw-HomeRoot").map(PathBuf::from))
}

pub fn unit_channel_id(path: &Path) -> Result<Option<String>> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", path.display())),
    };
    Ok(unit_metadata_value(&content, "X-LionClaw-Channel"))
}

pub fn unit_belongs_to_identity(path: &Path, identity: &UnitIdentity) -> Result<bool> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", path.display()))
        }
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let unit_group_id = unit_metadata_value(&content, "X-LionClaw-UnitGroupId");
    let home_root = unit_metadata_value(&content, "X-LionClaw-HomeRoot");
    Ok(
        unit_group_id.as_deref() == Some(identity.unit_group_id.as_str())
            && home_root.as_deref() == Some(identity.home_root.to_string_lossy().as_ref()),
    )
}

fn unit_metadata_value(content: &str, key: &str) -> Option<String> {
    content.lines().find_map(|line| {
        let (left, right) = line.split_once('=')?;
        (left.trim() == key)
            .then(|| right.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

pub fn render_daemon_unit(
    home: &LionClawHome,
    identity: &UnitIdentity,
    daemon_bin: &Path,
    spec: DaemonUnitSpec<'_>,
) -> ManagedUnit {
    let name = daemon_unit_name(identity);
    let env_path = daemon_env_path(home, identity);
    let unit_path = home.units_systemd_dir().join(&name);

    let (host, port) = parse_bind_addr(spec.bind_addr);
    let mut env_lines = vec![
        (
            "LIONCLAW_HOME".to_string(),
            home.root().display().to_string(),
        ),
        ("LIONCLAW_BIND_ADDR".to_string(), spec.bind_addr.to_string()),
        ("LIONCLAW_HOST".to_string(), host),
        ("LIONCLAW_PORT".to_string(), port),
        (
            "LIONCLAW_DEFAULT_RUNTIME_ID".to_string(),
            spec.runtime_id.to_string(),
        ),
        ("LIONCLAW_WORKSPACE".to_string(), spec.workspace.to_string()),
        (
            "LIONCLAW_WORKSPACE_ROOT".to_string(),
            spec.project_workspace_root.display().to_string(),
        ),
        (
            "LIONCLAW_DAEMON_FINGERPRINT".to_string(),
            spec.daemon_fingerprint.to_string(),
        ),
    ];
    if let Some(codex_home_override) = spec.codex_home_override {
        env_lines.push((
            "CODEX_HOME".to_string(),
            codex_home_override.display().to_string(),
        ));
    }
    let env_content = env_lines
        .iter()
        .map(|(key, value)| format!("{key}={}\n", escape_env_value(value)))
        .collect::<String>();
    let unit_content = format!(
        "[Unit]\nDescription=LionClaw daemon\nAfter=default.target\nX-LionClaw-UnitGroupId={unit_group_id}\nX-LionClaw-HomeId={home_id}\nX-LionClaw-HomeRoot={home_root}\n\n[Service]\nType=simple\nEnvironmentFile={env}\nExecStart={exec}\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        unit_group_id = identity.unit_group_id,
        home_id = identity.home_id,
        home_root = identity.home_root.display(),
        env = env_path.display(),
        exec = daemon_bin.display(),
    );

    ManagedUnit {
        name,
        unit_path,
        unit_content,
        env_path,
        env_content,
        extra_env_files: Vec::new(),
    }
}

pub fn render_channel_unit(
    home: &LionClawHome,
    identity: &UnitIdentity,
    spec: &ChannelUnitSpec,
) -> ManagedUnit {
    let daemon_name = daemon_unit_name(identity);
    let name = channel_unit_name(identity, &spec.channel_id);
    let env_path = unit_env_path(home, &name);
    let unit_path = home.units_systemd_dir().join(&name);
    let env_content = spec
        .env
        .iter()
        .map(|(key, value)| format!("{key}={}\n", escape_env_value(value)))
        .collect::<String>();
    let mut environment_files = format!("EnvironmentFile={env}\n", env = env_path.display());
    if let Some(channel_env_path) = &spec.channel_env_path {
        environment_files.push_str(&format!("EnvironmentFile={}\n", channel_env_path.display()));
    }
    let unit_content = format!(
        "[Unit]\nDescription=LionClaw channel worker ({channel})\nAfter={daemon}\nRequires={daemon}\nPartOf={daemon}\nX-LionClaw-UnitGroupId={unit_group_id}\nX-LionClaw-HomeId={home_id}\nX-LionClaw-HomeRoot={home_root}\nX-LionClaw-Channel={channel}\n\n[Service]\nType=simple\n{environment_files}ExecStart={exec}\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
        channel = spec.channel_id,
        daemon = daemon_name,
        unit_group_id = identity.unit_group_id,
        home_id = identity.home_id,
        home_root = identity.home_root.display(),
        environment_files = environment_files,
        exec = spec.worker_path.display(),
    );

    ManagedUnit {
        name,
        unit_path,
        unit_content,
        env_path,
        env_content,
        extra_env_files: spec.channel_env_path.iter().cloned().collect(),
    }
}

fn unit_env_path(home: &LionClawHome, unit_name: &str) -> PathBuf {
    home.units_env_dir()
        .join(format!("{}.env", unit_name.trim_end_matches(".service")))
}

#[async_trait]
pub trait UnitManager: Send + Sync {
    async fn apply_units(&self, home: &LionClawHome, units: &[ManagedUnit]) -> Result<Vec<String>>;
    async fn up_units(&self, units: &[String]) -> Result<()>;
    async fn restart_units(&self, units: &[String]) -> Result<()>;
    async fn down_units(&self, units: &[String]) -> Result<()>;
    async fn unit_status(&self, unit: &str) -> Result<String>;
    async fn logs(&self, units: &[String], lines: usize) -> Result<String>;
}

pub struct SystemdUserUnitManager;

#[async_trait]
impl UnitManager for SystemdUserUnitManager {
    async fn apply_units(&self, home: &LionClawHome, units: &[ManagedUnit]) -> Result<Vec<String>> {
        let user_unit_dir = systemd_user_unit_dir()?;
        tokio::fs::create_dir_all(&user_unit_dir)
            .await
            .with_context(|| format!("failed to create {}", user_unit_dir.display()))?;
        ensure_private_unit_paths(home, units)?;
        prune_stale_generated_files(home, &user_unit_dir, units)?;

        let mut changed_units = Vec::new();
        for unit in units {
            let unit_path = user_unit_dir.join(&unit.name);
            ensure_owned_or_absent(home, &unit_path)?;
            let unit_changed = file_content_differs(&unit_path, &unit.unit_content)
                .with_context(|| format!("failed to compare {}", unit_path.display()))?;
            let env_changed = file_content_differs(&unit.env_path, &unit.env_content)
                .with_context(|| format!("failed to compare {}", unit.env_path.display()))?;
            tokio::fs::write(&unit_path, &unit.unit_content)
                .await
                .with_context(|| format!("failed to write {}", unit_path.display()))?;
            tokio::fs::write(&unit.env_path, &unit.env_content)
                .await
                .with_context(|| format!("failed to write {}", unit.env_path.display()))?;

            let permissions = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&unit.env_path, permissions)
                .with_context(|| format!("failed to chmod {}", unit.env_path.display()))?;

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
            if let Err(err) = run_systemctl(["--user", "disable", "--now", unit]).await {
                warn!(?err, unit, "failed to stop systemd user unit");
            }
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
            return Err(anyhow!("journalctl failed: {stderr}"));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

fn ensure_private_unit_paths(home: &LionClawHome, units: &[ManagedUnit]) -> Result<()> {
    create_private_dir_all(home, &home.units_systemd_dir(), "managed unit directory")?;
    create_private_dir_all(home, &home.units_env_dir(), "unit env directory")?;
    for unit in units {
        ensure_private_file_write_target(home, &unit.env_path, "unit env file")?;
        for extra_env_file in &unit.extra_env_files {
            ensure_private_file_readable(home, extra_env_file, "channel env file")?;
        }
    }
    Ok(())
}

#[derive(Default)]
pub struct FakeUnitManager {
    states: Mutex<HashMap<String, String>>,
    units: Mutex<HashMap<String, ManagedUnit>>,
    restarted: Mutex<Vec<String>>,
    log_output: Mutex<Option<String>>,
    log_error: Mutex<Option<String>>,
    fail_up_after_started: Mutex<Option<usize>>,
}

impl FakeUnitManager {
    pub fn set_unit_status(
        &self,
        unit: impl Into<String>,
        status: impl Into<String>,
    ) -> Result<()> {
        self.states
            .lock()
            .map_err(|_| anyhow!("states lock poisoned"))?
            .insert(unit.into(), status.into());
        Ok(())
    }

    pub fn was_restarted(&self, unit: &str) -> Result<bool> {
        Ok(self
            .restarted
            .lock()
            .map_err(|_| anyhow!("restarted lock poisoned"))?
            .iter()
            .any(|value| value == unit))
    }

    pub fn managed_unit(&self, unit: &str) -> Result<Option<ManagedUnit>> {
        Ok(self
            .units
            .lock()
            .map_err(|_| anyhow!("units lock poisoned"))?
            .get(unit)
            .cloned())
    }

    pub fn set_logs(&self, output: impl Into<String>) -> Result<()> {
        *self
            .log_output
            .lock()
            .map_err(|_| anyhow!("log output lock poisoned"))? = Some(output.into());
        Ok(())
    }

    pub fn fail_logs(&self, error: impl Into<String>) -> Result<()> {
        *self
            .log_error
            .lock()
            .map_err(|_| anyhow!("log error lock poisoned"))? = Some(error.into());
        Ok(())
    }

    pub fn fail_up_after_started(&self, started_units: usize) -> Result<()> {
        *self
            .fail_up_after_started
            .lock()
            .map_err(|_| anyhow!("fail up lock poisoned"))? = Some(started_units);
        Ok(())
    }
}

#[async_trait]
impl UnitManager for FakeUnitManager {
    async fn apply_units(
        &self,
        _home: &LionClawHome,
        units: &[ManagedUnit],
    ) -> Result<Vec<String>> {
        let changed = {
            let mut stored = self
                .units
                .lock()
                .map_err(|_| anyhow!("units lock poisoned"))?;
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
            drop(stored);
            changed
        };
        Ok(changed)
    }

    async fn up_units(&self, units: &[String]) -> Result<()> {
        let fail_after_started = *self
            .fail_up_after_started
            .lock()
            .map_err(|_| anyhow!("fail up lock poisoned"))?;
        {
            let mut states = self
                .states
                .lock()
                .map_err(|_| anyhow!("states lock poisoned"))?;
            for (started, unit) in units.iter().enumerate() {
                if fail_after_started == Some(started) {
                    return Err(anyhow!(
                        "configured unit start failure after {started} unit(s)"
                    ));
                }
                states.insert(unit.clone(), "loaded/active/running".to_string());
            }
        }
        Ok(())
    }

    async fn restart_units(&self, units: &[String]) -> Result<()> {
        {
            let mut states = self
                .states
                .lock()
                .map_err(|_| anyhow!("states lock poisoned"))?;
            let mut restarted = self
                .restarted
                .lock()
                .map_err(|_| anyhow!("restarted lock poisoned"))?;
            for unit in units {
                states.insert(unit.clone(), "loaded/active/running".to_string());
                restarted.push(unit.clone());
            }
            drop(restarted);
            drop(states);
        }
        Ok(())
    }

    async fn down_units(&self, units: &[String]) -> Result<()> {
        {
            let mut states = self
                .states
                .lock()
                .map_err(|_| anyhow!("states lock poisoned"))?;
            for unit in units {
                states.insert(unit.clone(), "loaded/inactive/dead".to_string());
            }
        }
        Ok(())
    }

    async fn unit_status(&self, unit: &str) -> Result<String> {
        let status = {
            let states = self
                .states
                .lock()
                .map_err(|_| anyhow!("states lock poisoned"))?;
            states
                .get(unit)
                .cloned()
                .unwrap_or_else(|| "not-found".to_string())
        };
        Ok(status)
    }

    async fn logs(&self, units: &[String], _lines: usize) -> Result<String> {
        let log_error = self
            .log_error
            .lock()
            .map_err(|_| anyhow!("log error lock poisoned"))?
            .clone();
        if let Some(error) = log_error {
            return Err(anyhow!(error));
        }
        let log_output = self
            .log_output
            .lock()
            .map_err(|_| anyhow!("log output lock poisoned"))?
            .clone();
        if let Some(output) = log_output {
            return Ok(output);
        }
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
    format!("\"{escaped}\"")
}

fn prune_stale_generated_files(
    home: &LionClawHome,
    user_unit_dir: &Path,
    units: &[ManagedUnit],
) -> Result<()> {
    let desired_names = units
        .iter()
        .map(|unit| unit.name.as_str())
        .collect::<Vec<_>>();
    prune_unit_dir(&home.units_systemd_dir(), &desired_names)?;
    prune_env_dir(&home.units_env_dir(), units)?;
    prune_user_unit_dir(home, user_unit_dir, &desired_names)?;
    Ok(())
}

fn ensure_owned_or_absent(home: &LionClawHome, path: &Path) -> Result<()> {
    if !path_entry_exists(path)? {
        return Ok(());
    }
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "managed unit {} is a symlink; refusing to overwrite it",
            path.display()
        );
    }
    let recorded_home = unit_recorded_home_root(path)?;
    let home_root = canonical_home_root(home)?;
    match recorded_home {
        Some(recorded) if recorded == home_root || !recorded.exists() => Ok(()),
        Some(recorded) => bail!(
            "managed unit {} belongs to {}; refusing to overwrite it for {}",
            path.display(),
            recorded.display(),
            home_root.display()
        ),
        None => bail!(
            "managed unit {} exists without LionClaw ownership metadata; refusing to overwrite it",
            path.display()
        ),
    }
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

fn prune_env_dir(dir: &Path, units: &[ManagedUnit]) -> Result<()> {
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

    let home_root = canonical_home_root(home)?;
    let managed_dir = normalize_lexical_path(&home.units_systemd_dir());
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
        if metadata.file_type().is_symlink() {
            if symlink_points_inside(&path, dir, &managed_dir)? {
                remove_path_if_exists(&path)?;
            }
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        if unit_recorded_home_root(&path)?.as_deref() == Some(home_root.as_path()) {
            remove_path_if_exists(&path)?;
        }
    }

    Ok(())
}

fn symlink_points_inside(path: &Path, dir: &Path, managed_dir: &Path) -> Result<bool> {
    let target = std::fs::read_link(path)
        .with_context(|| format!("failed to read link {}", path.display()))?;
    let resolved = if target.is_absolute() {
        target
    } else {
        path.parent().unwrap_or(dir).join(target)
    };
    Ok(normalize_lexical_path(&resolved).starts_with(managed_dir))
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(_) | Component::RootDir | Component::Prefix(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
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
    run_command("systemctl", args).await?;
    Ok(())
}

async fn run_command<const N: usize>(program: &str, args: [&str; N]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to run {program}"))?;

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
        channel_unit_name, daemon_unit_name, ensure_identity_not_colliding_in_dir,
        ensure_private_unit_paths, path_entry_exists, prune_user_unit_dir,
        read_or_create_unit_identity, render_channel_unit, render_daemon_unit,
        unit_belongs_to_identity, write_unit_group_id, ChannelUnitSpec, DaemonUnitSpec,
        ManagedUnit, UnitIdentity,
    };

    fn seed_home(root: &Path) -> crate::home::LionClawHome {
        let home = crate::home::LionClawHome::new(root.to_path_buf());
        fs::create_dir_all(home.config_dir()).expect("home config dir");
        fs::write(home.home_id_path(), format!("{}\n", uuid::Uuid::new_v4())).expect("home id");
        home
    }

    fn write_unit(path: &Path, unit_group_id: &str, home_root: &Path, channel_id: Option<&str>) {
        let channel = channel_id
            .map(|value| format!("X-LionClaw-Channel={value}\n"))
            .unwrap_or_default();
        fs::write(
            path,
            format!(
                "[Unit]\nX-LionClaw-UnitGroupId={unit_group_id}\nX-LionClaw-HomeRoot={}\n{channel}",
                home_root.display()
            ),
        )
        .expect("unit metadata");
    }

    #[test]
    fn renders_expected_unit_names() {
        let identity = UnitIdentity {
            unit_group_id: "11111111-1111-4111-8111-111111111111".to_string(),
            home_id: "home".to_string(),
            home_root: "/tmp/lionclaw-home".into(),
        };
        assert_eq!(
            daemon_unit_name(&identity),
            "lionclaw-11111111-1111-4111-8111-111111111111.service"
        );
        assert_eq!(
            channel_unit_name(&identity, "telegram"),
            "lionclaw-channel-11111111-1111-4111-8111-111111111111-telegram.service"
        );
    }

    #[test]
    fn renders_units_with_expected_execs() {
        let home = crate::home::LionClawHome::new("/tmp/lionclaw-home".into());
        let identity = UnitIdentity {
            unit_group_id: "11111111-1111-4111-8111-111111111111".to_string(),
            home_id: "home".to_string(),
            home_root: "/tmp/lionclaw-home".into(),
        };
        let daemon = render_daemon_unit(
            &home,
            &identity,
            Path::new("/tmp/bin/lionclawd"),
            DaemonUnitSpec {
                bind_addr: "127.0.0.1:8979",
                runtime_id: "codex",
                workspace: "main",
                project_workspace_root: Path::new("/tmp/project"),
                daemon_fingerprint: "daemon-state-test",
                codex_home_override: Some(Path::new("/tmp/custom-codex-home")),
            },
        );
        assert!(daemon.unit_content.contains("ExecStart=/tmp/bin/lionclawd"));
        assert!(daemon
            .env_content
            .contains("LIONCLAW_BIND_ADDR=\"127.0.0.1:8979\""));
        assert!(daemon.env_content.contains("LIONCLAW_WORKSPACE=\"main\""));
        assert!(daemon
            .env_content
            .contains("LIONCLAW_WORKSPACE_ROOT=\"/tmp/project\""));
        assert!(daemon
            .env_content
            .contains("LIONCLAW_DAEMON_FINGERPRINT=\"daemon-state-test\""));
        assert!(daemon
            .env_content
            .contains("CODEX_HOME=\"/tmp/custom-codex-home\""));

        let channel = render_channel_unit(
            &home,
            &identity,
            &ChannelUnitSpec {
                channel_id: "telegram".to_string(),
                worker_path: "/tmp/skills/telegram/scripts/worker".into(),
                env: vec![("TELEGRAM_BOT_TOKEN".to_string(), "secret".to_string())],
                channel_env_path: None,
            },
        );
        assert!(channel.unit_content.contains(&daemon.name));
        assert!(channel.unit_content.contains("PartOf="));
    }

    #[test]
    fn unit_identity_is_generated_once_and_preserved() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = seed_home(&temp_dir.path().join(".lionclaw"));

        let first = read_or_create_unit_identity(&home).expect("first unit group identity");
        let second = read_or_create_unit_identity(&home).expect("second unit group identity");

        assert_eq!(first, second);
        assert_eq!(
            fs::read_to_string(home.unit_group_id_path())
                .expect("unit group id")
                .trim(),
            first.unit_group_id
        );
    }

    #[test]
    fn copied_home_collision_rotates_selected_unit_group_id() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let selected = seed_home(&temp_dir.path().join("selected"));
        let original = seed_home(&temp_dir.path().join("original"));
        let systemd_dir = temp_dir.path().join("systemd-user");
        fs::create_dir_all(&systemd_dir).expect("systemd dir");

        let colliding_unit_group_id = uuid::Uuid::new_v4().to_string();
        write_unit_group_id(&selected, &colliding_unit_group_id).expect("selected unit group id");
        let selected_identity = read_or_create_unit_identity(&selected).expect("selected identity");
        write_unit(
            &systemd_dir.join(daemon_unit_name(&selected_identity)),
            &colliding_unit_group_id,
            &std::fs::canonicalize(original.root()).expect("original root"),
            None,
        );

        let rotated =
            ensure_identity_not_colliding_in_dir(&selected, selected_identity, &systemd_dir)
                .expect("rotate identity");

        assert_ne!(rotated.unit_group_id, colliding_unit_group_id);
        assert_eq!(
            fs::read_to_string(selected.unit_group_id_path())
                .expect("rotated unit group id")
                .trim(),
            rotated.unit_group_id
        );
    }

    #[test]
    fn moved_home_reclaims_missing_recorded_home_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let selected = seed_home(&temp_dir.path().join("selected"));
        let systemd_dir = temp_dir.path().join("systemd-user");
        fs::create_dir_all(&systemd_dir).expect("systemd dir");

        let unit_group_id = uuid::Uuid::new_v4().to_string();
        write_unit_group_id(&selected, &unit_group_id).expect("selected unit group id");
        let identity = read_or_create_unit_identity(&selected).expect("selected identity");
        write_unit(
            &systemd_dir.join(daemon_unit_name(&identity)),
            &unit_group_id,
            &temp_dir.path().join("missing-home"),
            None,
        );

        let reclaimed = ensure_identity_not_colliding_in_dir(&selected, identity, &systemd_dir)
            .expect("reclaim identity");

        assert_eq!(reclaimed.unit_group_id, unit_group_id);
    }

    #[test]
    fn unit_ownership_requires_matching_unit_group_id_and_home_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = seed_home(&temp_dir.path().join(".lionclaw"));
        let identity = read_or_create_unit_identity(&home).expect("identity");
        let unit_path = temp_dir
            .path()
            .join(channel_unit_name(&identity, "telegram"));
        write_unit(
            &unit_path,
            &identity.unit_group_id,
            &identity.home_root,
            Some("telegram"),
        );

        assert!(
            unit_belongs_to_identity(&unit_path, &identity).expect("owned unit"),
            "unit with matching unit group id and home root should be owned"
        );

        let other = UnitIdentity {
            unit_group_id: uuid::Uuid::new_v4().to_string(),
            home_id: identity.home_id.clone(),
            home_root: identity.home_root.clone(),
        };
        assert!(
            !unit_belongs_to_identity(&unit_path, &other).expect("foreign unit"),
            "different unit group id should not be owned"
        );
    }

    #[test]
    fn prunes_only_owned_user_unit_files() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        std::fs::create_dir_all(home.units_systemd_dir()).expect("managed dir");
        let user_dir = temp_dir.path().join("systemd-user");
        std::fs::create_dir_all(&user_dir).expect("user dir");
        let home_root = std::fs::canonicalize(home.root()).expect("home root");

        let managed_unit = user_dir.join("lionclaw-channel-old.service");
        fs::write(
            &managed_unit,
            format!(
                "[Unit]\nDescription=old\nX-LionClaw-HomeRoot={}\n",
                home_root.display()
            ),
        )
        .expect("managed unit");

        let managed_link_target = home.units_systemd_dir().join("lionclawd.service");
        fs::write(&managed_link_target, "[Unit]\n").expect("legacy managed target");
        let managed_link = user_dir.join("lionclawd.service");
        symlink(&managed_link_target, &managed_link).expect("legacy managed link");

        let unrelated_target = temp_dir.path().join("custom.service");
        fs::write(&unrelated_target, "[Unit]\n").expect("custom unit");
        let unrelated_link = user_dir.join("lionclaw-custom.service");
        symlink(&unrelated_target, &unrelated_link).expect("custom link");

        let escaping_target = home.units_systemd_dir().join("../outside.service");
        let escaping_link = user_dir.join("lionclaw-escape.service");
        symlink(&escaping_target, &escaping_link).expect("escaping link");

        prune_user_unit_dir(&home, &user_dir, &[]).expect("prune");

        assert!(
            !managed_unit.exists(),
            "managed unit file should be removed"
        );
        assert!(
            !path_entry_exists(&managed_link).expect("managed link exists"),
            "legacy managed symlink should be removed"
        );
        assert!(
            managed_link_target.exists(),
            "managed symlink target should be preserved"
        );
        assert!(
            unrelated_link.exists(),
            "unrelated symlink should be preserved"
        );
        assert!(
            path_entry_exists(&escaping_link).expect("escaping link exists"),
            "escaping symlink should be preserved"
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

    #[cfg(unix)]
    #[test]
    fn private_unit_paths_reject_symlinked_env_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = crate::home::LionClawHome::new(temp_dir.path().join(".lionclaw"));
        fs::create_dir_all(home.units_dir()).expect("units dir");
        let outside_env = temp_dir.path().join("outside-env");
        fs::create_dir_all(&outside_env).expect("outside env");
        symlink(&outside_env, home.units_env_dir()).expect("symlink env dir");
        let unit = ManagedUnit {
            name: "lionclaw-test.service".to_string(),
            unit_path: home.units_systemd_dir().join("lionclaw-test.service"),
            unit_content: "[Unit]\n".to_string(),
            env_path: home.units_env_dir().join("lionclaw-test.env"),
            env_content: "TOKEN=\"secret\"\n".to_string(),
            extra_env_files: Vec::new(),
        };

        let err = ensure_private_unit_paths(&home, &[unit])
            .expect_err("symlinked unit env directory should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside_env.join("lionclaw-test.env").exists());
    }
}
