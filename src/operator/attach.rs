use std::{collections::BTreeMap, path::PathBuf, process::Stdio, time::Duration};

use anyhow::{anyhow, Context, Result};
use tokio::process::Command;
use uuid::Uuid;

use crate::{
    config::resolve_project_workspace_root,
    home::runtime_project_partition_key,
    home::LionClawHome,
    operator::{
        config::ChannelLaunchMode,
        daemon_probe::{classify_daemon, wait_for_same_home_daemon, DaemonClassification},
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
    let home_id = home.ensure_home_id().await?;
    let project_scope = current_project_scope()?;
    let spec = prepare_channel_attach(
        home,
        manager,
        channel_id,
        requested_peer_id,
        requested_runtime_id,
        &project_scope,
        &binaries,
    )
    .await?;

    if spec.started_services {
        match wait_for_same_home_daemon(
            &spec.bind_addr,
            &home_id,
            &project_scope,
            Duration::from_secs(5),
        )
        .await?
        {
            DaemonClassification::SameHome => {}
            DaemonClassification::Absent => {
                return Err(anyhow!(
                    "lionclawd did not become available at '{}' within 5s",
                    spec.bind_addr
                ));
            }
            DaemonClassification::SameHomeDifferentProject => {
                return Err(anyhow!(
                    "bind '{}' became owned by this LionClaw home for a different project while attaching",
                    spec.bind_addr
                ));
            }
            DaemonClassification::ForeignHome(info) => {
                return Err(anyhow!(
                    "bind '{}' became owned by a different LionClaw home at '{}' while attaching",
                    spec.bind_addr,
                    info.home_root
                ));
            }
            DaemonClassification::IncompatibleLionClaw => {
                return Err(anyhow!(
                    "bind '{}' became available with an older LionClaw daemon while attaching",
                    spec.bind_addr
                ));
            }
            DaemonClassification::UnknownListener => {
                return Err(anyhow!(
                    "bind '{}' became available with a non-LionClaw listener while attaching",
                    spec.bind_addr
                ));
            }
        }
    }

    launch_channel_attach(spec).await
}

pub(crate) async fn prepare_channel_attach<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: String,
    requested_peer_id: Option<String>,
    requested_runtime_id: Option<String>,
    expected_project_scope: &str,
    binaries: &StackBinaryPaths,
) -> Result<ChannelAttachSpec> {
    let initial_config = crate::operator::config::OperatorConfig::load(home).await?;
    let home_id = home.ensure_home_id().await?;
    let mut started_services = false;
    let applied = match classify_daemon(
        &initial_config.daemon.bind,
        &home_id,
        expected_project_scope,
    )
    .await?
    {
        DaemonClassification::Absent => {
            let runtime_id = initial_config.resolve_runtime_id(requested_runtime_id.as_deref())?;
            started_services = true;
            up(home, manager, &runtime_id, binaries).await?
        }
        DaemonClassification::SameHome => apply(home).await?,
        DaemonClassification::SameHomeDifferentProject => {
            return Err(anyhow!(
                "bind '{}' is already served by this LionClaw home for a different project; stop that daemon or attach from the matching project root",
                initial_config.daemon.bind
            ));
        }
        DaemonClassification::ForeignHome(info) => {
            return Err(anyhow!(
                "bind '{}' is already served by a different LionClaw home at '{}'; stop that daemon or choose a different bind",
                initial_config.daemon.bind,
                info.home_root
            ));
        }
        DaemonClassification::IncompatibleLionClaw => {
            return Err(anyhow!(
                "bind '{}' is already served by an older LionClaw daemon; restart that daemon before attaching",
                initial_config.daemon.bind
            ));
        }
        DaemonClassification::UnknownListener => {
            return Err(anyhow!(
                "bind '{}' is already in use by a non-LionClaw listener",
                initial_config.daemon.bind
            ));
        }
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
    let effective_runtime_id = match requested_runtime_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(runtime_id) => Some(applied.config.resolve_runtime_id(Some(runtime_id))?),
        None => None,
    };
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
    if let Some(runtime_id) = effective_runtime_id {
        env.insert("LIONCLAW_RUNTIME_ID".to_string(), runtime_id);
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

fn default_local_peer_id() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local-user".to_string())
}

fn current_project_scope() -> Result<String> {
    let project_root =
        resolve_project_workspace_root().context("failed to resolve project workspace root")?;
    Ok(runtime_project_partition_key(Some(project_root.as_path())))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use axum::{routing::get, Json, Router};
    use serde_json::json;

    use super::prepare_channel_attach;
    use crate::{
        config::resolve_project_workspace_root,
        contracts::DaemonInfoResponse,
        home::{runtime_project_partition_key, LionClawHome},
        kernel::runtime::{ConfinementConfig, OciConfinementConfig},
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

    fn current_project_scope() -> String {
        let project_root =
            resolve_project_workspace_root().expect("resolve project workspace root");
        runtime_project_partition_key(Some(project_root.as_path()))
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
        let bind = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
            let addr = listener.local_addr().expect("listener addr");
            format!("127.0.0.1:{}", addr.port())
        };
        crate::operator::reconcile::onboard(&home, None)
            .await
            .expect("onboard");

        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        write_executable(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n");
        let podman = temp_dir.path().join("podman");
        write_executable(&podman, "#!/usr/bin/env bash\nexit 0\n");

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
                bind,
                workspace: "main".to_string(),
            },
            runtimes: [(
                "codex".to_string(),
                RuntimeProfileConfig::Codex {
                    executable: "codex".to_string(),
                    model: None,
                    confinement: ConfinementConfig::Oci(OciConfinementConfig {
                        engine: podman.to_string_lossy().to_string(),
                        image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                        ..OciConfinementConfig::default()
                    }),
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

    async fn spawn_probe_server(app: Router, bind_addr: &str) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("bind probe server");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve probe app");
        })
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
            &current_project_scope(),
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
            &current_project_scope(),
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
            "canonical worker entrypoint should be required"
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

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_reuses_same_home_daemon() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_root = home.root().display().to_string();
                    let home_id = home_id.clone();
                    let project_scope = current_project_scope();
                    move || {
                        let response = DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            &bind_addr,
        )
        .await;

        let spec = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            None,
            Some("codex".to_string()),
            &current_project_scope(),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(!spec.started_services, "same-home daemon should be reused");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_exports_requested_runtime_when_reusing_same_home_daemon() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_root = home.root().display().to_string();
                    let home_id = home_id.clone();
                    let project_scope = current_project_scope();
                    move || {
                        let response = DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            &bind_addr,
        )
        .await;

        let spec = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            None,
            Some("codex".to_string()),
            &current_project_scope(),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        let env = spec.env.into_iter().collect::<BTreeMap<_, _>>();
        assert_eq!(
            env.get("LIONCLAW_RUNTIME_ID").map(String::as_str),
            Some("codex")
        );
        assert!(!spec.started_services, "same-home daemon should be reused");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_unknown_requested_runtime_when_reusing_same_home_daemon(
    ) {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_root = home.root().display().to_string();
                    let home_id = home_id.clone();
                    let project_scope = current_project_scope();
                    move || {
                        let response = DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            &bind_addr,
        )
        .await;

        let err = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            None,
            Some("missing".to_string()),
            &current_project_scope(),
            &binaries(),
        )
        .await
        .expect_err("unknown requested runtime should fail");

        assert!(err
            .to_string()
            .contains("runtime profile 'missing' is not configured"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_foreign_home_daemon() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let bind_addr = config.daemon.bind.clone();
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    move || async move {
                        Json(DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: "foreign-home".to_string(),
                            home_root: "/tmp/foreign-home".to_string(),
                            bind_addr: bind_addr.clone(),
                            project_scope: "foreign-project".to_string(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;

        let err = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            None,
            Some("codex".to_string()),
            &current_project_scope(),
            &binaries(),
        )
        .await
        .expect_err("foreign daemon should fail");

        assert!(err.to_string().contains("/tmp/foreign-home"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_incompatible_lionclaw_daemon() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let bind_addr = config.daemon.bind.clone();
        let _server = spawn_probe_server(
            Router::new().route(
                "/health",
                get(|| async { Json(json!({"service": "lionclawd", "status": "ok"})) }),
            ),
            &bind_addr,
        )
        .await;

        let err = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            None,
            Some("codex".to_string()),
            &current_project_scope(),
            &binaries(),
        )
        .await
        .expect_err("older daemon should fail");

        assert!(err.to_string().contains("older LionClaw daemon"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_same_home_different_project_daemon() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let home_root = home.root().display().to_string();
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_id = home_id.clone();
                    let home_root = home_root.clone();
                    move || async move {
                        Json(DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: "different-project".to_string(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;

        let err = prepare_channel_attach(
            &home,
            &manager,
            "terminal".to_string(),
            None,
            Some("codex".to_string()),
            &current_project_scope(),
            &binaries(),
        )
        .await
        .expect_err("same-home different-project daemon should fail");

        assert!(err.to_string().contains("different project"));
    }
}
