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

pub fn render_daemon_unit(
    home: &LionClawHome,
    daemon_bin: &Path,
    bind_addr: &str,
    runtime_id: &str,
) -> ManagedServiceUnit {
    let env_path = home.services_env_dir().join("lionclawd.env");
    let unit_path = home.services_systemd_dir().join(DAEMON_UNIT_NAME);

    let (host, port) = bind_addr.split_once(':').unwrap_or(("127.0.0.1", "3000"));
    let env_content = format!(
        "LIONCLAW_HOME={home}\nLIONCLAW_HOST={host}\nLIONCLAW_PORT={port}\nLIONCLAW_DEFAULT_RUNTIME_ID={runtime_id}\n",
        home = home.root().display()
    );
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
        .map(|(key, value)| format!("{key}={value}\n"))
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
    async fn apply_units(&self, home: &LionClawHome, units: &[ManagedServiceUnit]) -> Result<()>;
    async fn up_units(&self, units: &[String]) -> Result<()>;
    async fn down_units(&self, units: &[String]) -> Result<()>;
    async fn unit_status(&self, unit: &str) -> Result<String>;
    async fn logs(&self, units: &[String], lines: usize) -> Result<String>;
}

pub struct SystemdUserServiceManager;

#[async_trait]
impl ServiceManager for SystemdUserServiceManager {
    async fn apply_units(&self, home: &LionClawHome, units: &[ManagedServiceUnit]) -> Result<()> {
        let user_unit_dir = systemd_user_unit_dir()?;
        tokio::fs::create_dir_all(&user_unit_dir)
            .await
            .with_context(|| format!("failed to create {}", user_unit_dir.display()))?;

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
            if user_link.exists() {
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
        }

        let _ = home;
        run_systemctl(["--user", "daemon-reload"]).await?;
        Ok(())
    }

    async fn up_units(&self, units: &[String]) -> Result<()> {
        for unit in units {
            run_systemctl(["--user", "enable", "--now", unit]).await?;
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
        let output = run_command(
            "systemctl",
            [
                "--user",
                "show",
                unit,
                "--property=LoadState,ActiveState,SubState",
                "--value",
            ],
        )
        .await?;
        Ok(output.trim().replace('\n', "/"))
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
            return Err(anyhow!(
                "journalctl failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

#[derive(Default)]
pub struct FakeServiceManager {
    states: Mutex<HashMap<String, String>>,
    units: Mutex<HashMap<String, ManagedServiceUnit>>,
}

#[async_trait]
impl ServiceManager for FakeServiceManager {
    async fn apply_units(&self, _home: &LionClawHome, units: &[ManagedServiceUnit]) -> Result<()> {
        let mut stored = self.units.lock().expect("units lock");
        for unit in units {
            stored.insert(unit.name.clone(), unit.clone());
        }
        Ok(())
    }

    async fn up_units(&self, units: &[String]) -> Result<()> {
        let mut states = self.states.lock().expect("states lock");
        for unit in units {
            states.insert(unit.clone(), "loaded/active/running".to_string());
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
    use std::path::Path;

    use super::{channel_unit_name, render_channel_unit, render_daemon_unit, ChannelServiceSpec};

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
            "127.0.0.1:3000",
            "codex",
        );
        assert!(daemon.unit_content.contains("ExecStart=/tmp/bin/lionclawd"));

        let channel = render_channel_unit(
            &home,
            &ChannelServiceSpec {
                channel_id: "telegram".to_string(),
                worker_path: "/tmp/skills/telegram/scripts/worker.sh".into(),
                env: vec![("TELEGRAM_BOT_TOKEN".to_string(), "secret".to_string())],
            },
        );
        assert!(channel.unit_content.contains("lionclawd.service"));
    }
}
