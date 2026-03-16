use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use tokio::{
    net::{lookup_host, TcpStream},
    process::Command,
    time::sleep,
};
use uuid::Uuid;

use crate::{
    home::LionClawHome,
    operator::{
        config::ChannelLaunchMode,
        reconcile::{apply, base_url_from_bind, resolve_worker_entrypoint, up, StackBinaryPaths},
        services::ServiceManager,
    },
};

#[derive(Debug, Clone)]
pub(crate) struct ChannelAttachSpec {
    pub worker_path: PathBuf,
    pub bind_addr: String,
    pub env: Vec<(String, String)>,
    pub started_services: bool,
}

pub async fn attach_channel<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: String,
    requested_peer_id: Option<String>,
    requested_runtime_id: Option<String>,
) -> Result<()> {
    let binaries = crate::operator::reconcile::resolve_stack_binaries()?;
    let spec = prepare_channel_attach(
        home,
        manager,
        channel_id,
        requested_peer_id,
        requested_runtime_id,
        &binaries,
    )
    .await?;

    if spec.started_services {
        wait_for_daemon(&spec.bind_addr, Duration::from_secs(5)).await?;
    }

    launch_channel_attach(spec).await
}

pub(crate) async fn prepare_channel_attach<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: String,
    requested_peer_id: Option<String>,
    requested_runtime_id: Option<String>,
    binaries: &StackBinaryPaths,
) -> Result<ChannelAttachSpec> {
    let initial_config = crate::operator::config::OperatorConfig::load(home).await?;
    let mut started_services = false;
    let applied = if daemon_is_reachable(&initial_config.daemon.bind).await {
        apply(home).await?
    } else {
        let runtime_id = initial_config.resolve_runtime_id(requested_runtime_id.as_deref())?;
        started_services = true;
        up(home, manager, &runtime_id, binaries).await?
    };

    let channel = applied
        .config
        .channels
        .iter()
        .find(|channel| channel.id == channel_id)
        .ok_or_else(|| anyhow!("channel '{}' is not configured", channel_id))?;
    if !channel.enabled {
        return Err(anyhow!("channel '{}' is disabled", channel_id));
    }
    if channel.launch_mode != ChannelLaunchMode::Interactive {
        return Err(anyhow!(
            "channel '{}' uses launch mode '{}'; use 'lionclaw service up' for service channels",
            channel_id,
            channel.launch_mode.as_str()
        ));
    }

    let locked_skill = applied.lockfile.find_skill(&channel.skill).ok_or_else(|| {
        anyhow!(
            "channel '{}' skill '{}' is not installed",
            channel_id,
            channel.skill
        )
    })?;
    let worker_path = resolve_worker_entrypoint(home, &locked_skill.snapshot_dir)?;
    let peer_id = requested_peer_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(default_local_peer_id);
    let attach_id = Uuid::new_v4();
    let mut env = BTreeMap::from([
        (
            "LIONCLAW_HOME".to_string(),
            home.root().display().to_string(),
        ),
        (
            "LIONCLAW_BASE_URL".to_string(),
            base_url_from_bind(&applied.config.daemon.bind),
        ),
        ("LIONCLAW_CHANNEL_ID".to_string(), channel_id.clone()),
        (
            "LIONCLAW_CHANNEL_RUNTIME_DIR".to_string(),
            home.runtime_channel_dir(&channel_id).display().to_string(),
        ),
        ("LIONCLAW_PEER_ID".to_string(), peer_id.clone()),
        (
            "LIONCLAW_CONSUMER_ID".to_string(),
            format!("interactive:{}:{}:{}", channel_id, peer_id, attach_id),
        ),
        ("LIONCLAW_STREAM_START_MODE".to_string(), "tail".to_string()),
    ]);
    if let Ok(path) = std::env::var("PATH") {
        if !path.trim().is_empty() {
            env.insert("PATH".to_string(), path);
        }
    }

    Ok(ChannelAttachSpec {
        worker_path,
        bind_addr: applied.config.daemon.bind,
        env: env.into_iter().collect(),
        started_services,
    })
}

async fn launch_channel_attach(spec: ChannelAttachSpec) -> Result<()> {
    let mut command = Command::new(&spec.worker_path);
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    if let Some(parent) = spec.worker_path.parent() {
        command.current_dir(parent);
    }

    let status = command
        .status()
        .await
        .with_context(|| format!("failed to launch '{}'", spec.worker_path.display()))?;
    if status.success() {
        return Ok(());
    }

    Err(anyhow!(
        "interactive channel worker '{}' exited with status {}",
        spec.worker_path.display(),
        status
    ))
}

async fn daemon_is_reachable(bind_addr: &str) -> bool {
    let addrs = match lookup_host(bind_addr).await {
        Ok(values) => values.collect::<Vec<_>>(),
        Err(_) => return false,
    };

    for addr in addrs {
        if matches!(
            tokio::time::timeout(Duration::from_millis(250), TcpStream::connect(addr)).await,
            Ok(Ok(_))
        ) {
            return true;
        }
    }

    false
}

async fn wait_for_daemon(bind_addr: &str, timeout_duration: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout_duration;
    while Instant::now() < deadline {
        if daemon_is_reachable(bind_addr).await {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow!(
        "lionclawd did not become reachable at '{}' within {}s",
        bind_addr,
        timeout_duration.as_secs()
    ))
}

fn default_local_peer_id() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local-user".to_string())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use super::prepare_channel_attach;
    use crate::{
        home::LionClawHome,
        operator::{
            config::{
                ChannelLaunchMode, ManagedChannelConfig, ManagedSkillConfig, OperatorConfig,
                RuntimeProfileConfig,
            },
            services::{FakeServiceManager, ServiceManager},
        },
    };

    fn binaries() -> crate::operator::reconcile::StackBinaryPaths {
        crate::operator::reconcile::StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        }
    }

    #[cfg(unix)]
    fn write_executable(path: &std::path::Path, content: &str) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, content).expect("write executable");
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }

    #[cfg(unix)]
    async fn seed_interactive_channel(
        launch_mode: ChannelLaunchMode,
    ) -> (tempfile::TempDir, LionClawHome, FakeServiceManager) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        crate::operator::reconcile::onboard(&home)
            .await
            .expect("onboard");

        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        write_executable(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n");

        let skill_source = temp_dir.path().join("channel-terminal");
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-terminal\ndescription: test\n---\n",
        )
        .expect("skill md");
        write_executable(
            &skill_source.join("scripts/worker"),
            "#!/usr/bin/env bash\nenv | sort > /tmp/channel-attach-env\n",
        );

        let config = OperatorConfig {
            daemon: crate::operator::config::DaemonConfig {
                bind: "127.0.0.1:38979".to_string(),
                workspace: "main".to_string(),
            },
            runtimes: [(
                "codex".to_string(),
                RuntimeProfileConfig::Codex {
                    executable: runtime_stub.to_string_lossy().to_string(),
                    model: None,
                    sandbox: "read-only".to_string(),
                    skip_git_repo_check: true,
                    ephemeral: true,
                },
            )]
            .into_iter()
            .collect(),
            skills: vec![ManagedSkillConfig {
                alias: "terminal".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
                enabled: true,
            }],
            channels: vec![ManagedChannelConfig {
                id: "terminal".to_string(),
                skill: "terminal".to_string(),
                enabled: true,
                launch_mode,
                required_env: Vec::new(),
            }],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");

        (temp_dir, home, FakeServiceManager::default())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_service_channels() {
        let (_temp_dir, home, manager) = seed_interactive_channel(ChannelLaunchMode::Service).await;

        let err = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("service launch mode should fail");
        assert!(err.to_string().contains("use 'lionclaw service up'"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_builds_ephemeral_tail_env_and_starts_services() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;

        let spec = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(
            spec.started_services,
            "daemon should be ensured when unreachable"
        );
        assert!(
            spec.worker_path.ends_with("scripts/worker"),
            "canonical worker entrypoint should be preferred"
        );

        let env = spec.env.into_iter().collect::<BTreeMap<_, _>>();
        assert_eq!(
            env.get("LIONCLAW_CHANNEL_ID").map(String::as_str),
            Some("terminal")
        );
        assert_eq!(
            env.get("LIONCLAW_PEER_ID").map(String::as_str),
            Some("mosh")
        );
        assert_eq!(
            env.get("LIONCLAW_STREAM_START_MODE").map(String::as_str),
            Some("tail")
        );
        assert!(env
            .get("LIONCLAW_CONSUMER_ID")
            .is_some_and(|value| value.starts_with("interactive:terminal:mosh:")));
        assert_eq!(
            manager
                .unit_status(crate::operator::services::DAEMON_UNIT_NAME)
                .await
                .expect("unit status"),
            "loaded/active/running"
        );
    }
}
