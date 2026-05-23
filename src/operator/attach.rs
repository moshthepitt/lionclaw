use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use tokio::process::Command;
use uuid::Uuid;

use crate::{
    applied::compute_daemon_fingerprint,
    home::runtime_project_partition_key,
    home::LionClawHome,
    operator::{
        config::ChannelLaunchMode,
        daemon_probe::{classify_daemon, wait_for_same_home_daemon, DaemonClassification},
        managed_units::UnitManager,
        reconcile::{
            base_url_from_bind, load_operator_state, resolve_applied_skill_worker_entrypoint,
            resolve_required_channel_env, up_for_work_root, StackBinaryPaths,
        },
        runtime::{
            resolve_runtime_execution_context, resolve_runtime_id,
            validate_runtime_launch_prerequisites_for_work_root,
        },
    },
};

#[derive(Debug, Clone)]
pub(crate) struct ChannelAttachSpec {
    pub worker_path: PathBuf,
    pub bind_addr: String,
    pub env: Vec<(String, String)>,
    pub started_managed_units: bool,
    pub expected_daemon_fingerprint: String,
}

#[derive(Clone, Copy)]
pub(crate) struct ChannelAttachContext<'a, M> {
    pub home: &'a LionClawHome,
    pub manager: &'a M,
    pub project_root: Option<&'a Path>,
    pub work_root: &'a Path,
}

pub async fn attach_channel<M: UnitManager>(
    home: &LionClawHome,
    manager: &M,
    project_root: Option<&Path>,
    work_root: &Path,
    channel_id: String,
    requested_peer_id: Option<String>,
    requested_runtime_id: Option<String>,
) -> Result<()> {
    let binaries = crate::operator::reconcile::resolve_stack_binaries()?;
    attach_channel_with_binaries(
        ChannelAttachContext {
            home,
            manager,
            project_root,
            work_root,
        },
        channel_id,
        requested_peer_id,
        requested_runtime_id,
        &binaries,
    )
    .await
}

pub(crate) async fn attach_channel_with_binaries<M: UnitManager>(
    context: ChannelAttachContext<'_, M>,
    channel_id: String,
    requested_peer_id: Option<String>,
    requested_runtime_id: Option<String>,
    binaries: &StackBinaryPaths,
) -> Result<()> {
    let ChannelAttachContext {
        home,
        manager: _,
        project_root: _,
        work_root,
    } = context;
    let home_id = home.ensure_home_id().await?;
    let project_scope = project_scope_for_work_root(work_root);
    let spec = prepare_channel_attach(
        context,
        channel_id,
        requested_peer_id,
        requested_runtime_id,
        binaries,
    )
    .await?;

    if spec.started_managed_units {
        match wait_for_same_home_daemon(
            &spec.bind_addr,
            &home_id,
            &project_scope,
            &spec.expected_daemon_fingerprint,
            Duration::from_secs(5),
        )
        .await?
        {
            DaemonClassification::SameHome => {}
            DaemonClassification::SameHomeDifferentConfig => {
                return Err(anyhow!(
                    "bind '{}' became available with stale daemon-compatible config while attaching",
                    spec.bind_addr
                ));
            }
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

pub(crate) async fn prepare_channel_attach<M: UnitManager>(
    context: ChannelAttachContext<'_, M>,
    channel_id: String,
    requested_peer_id: Option<String>,
    requested_runtime_id: Option<String>,
    binaries: &StackBinaryPaths,
) -> Result<ChannelAttachSpec> {
    let ChannelAttachContext {
        home,
        manager,
        project_root,
        work_root,
    } = context;
    let expected_project_scope = project_scope_for_work_root(work_root);
    let local_state = load_operator_state(home).await?;
    let initial_config = local_state.config.clone();
    let requested_runtime_id = requested_runtime_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let initial_runtime_id = resolve_runtime_id(&initial_config, requested_runtime_id.as_deref())?;
    validate_runtime_launch_prerequisites_for_work_root(
        home,
        &initial_config,
        &initial_runtime_id,
        project_root,
        Some(work_root),
    )
    .await?;
    let expected_runtime_config_fingerprint =
        resolve_runtime_execution_context(home, &initial_config, Some(&initial_runtime_id))
            .await?
            .daemon_config_fingerprint;
    let expected_daemon_fingerprint = compute_daemon_fingerprint(
        &expected_runtime_config_fingerprint,
        &local_state.applied_state,
    );
    let home_id = home.ensure_home_id().await?;
    let mut started_managed_units = false;
    let applied = match classify_daemon(
        &initial_config.daemon.bind,
        &home_id,
        &expected_project_scope,
        &expected_daemon_fingerprint,
    )
    .await?
    {
        DaemonClassification::Absent => {
            started_managed_units = true;
            up_for_work_root(
                home,
                manager,
                &initial_runtime_id,
                binaries,
                project_root,
                work_root,
            )
            .await?
        }
        DaemonClassification::SameHome => local_state,
        DaemonClassification::SameHomeDifferentConfig => {
            started_managed_units = true;
            up_for_work_root(
                home,
                manager,
                &initial_runtime_id,
                binaries,
                project_root,
                work_root,
            )
            .await?
        }
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
        .applied_state
        .channel(&channel_id)
        .ok_or_else(|| anyhow!("channel '{channel_id}' is not configured"))?;
    if channel.launch_mode != ChannelLaunchMode::Interactive {
        return Err(anyhow!(
            "channel '{}' uses launch mode '{}'; run 'lionclaw up' to start managed channels",
            channel_id,
            channel.launch_mode.as_str()
        ));
    }

    let worker_path = resolve_applied_skill_worker_entrypoint(
        &applied.applied_state,
        &channel.skill_alias,
        Some(&channel.worker),
    )
    .with_context(|| format!("channel '{}' worker resolution failed", channel.id))?;
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
            format!("interactive:{channel_id}:{peer_id}:{attach_id}"),
        ),
        ("LIONCLAW_STREAM_START_MODE".to_string(), "tail".to_string()),
    ]);
    if let Ok(path) = std::env::var("PATH") {
        if !path.trim().is_empty() {
            env.insert("PATH".to_string(), path);
        }
    }
    for (key, value) in resolve_required_channel_env(home, &channel.id, &channel.required_env)? {
        env.insert(key, value);
    }

    Ok(ChannelAttachSpec {
        worker_path,
        bind_addr: applied.config.daemon.bind,
        env: env.into_iter().collect(),
        started_managed_units,
        expected_daemon_fingerprint,
    })
}

async fn launch_channel_attach(spec: ChannelAttachSpec) -> Result<()> {
    let mut command = Command::new(&spec.worker_path);
    command
        .env_clear()
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

fn project_scope_for_work_root(work_root: &Path) -> String {
    runtime_project_partition_key(Some(work_root))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use axum::{routing::get, Json, Router};
    use serde_json::json;

    use super::{
        launch_channel_attach, prepare_channel_attach, ChannelAttachContext, ChannelAttachSpec,
    };
    use crate::{
        applied::{compute_daemon_fingerprint, AppliedState},
        config::resolve_project_workspace_root,
        contracts::DaemonInfoResponse,
        home::{runtime_project_partition_key, LionClawHome},
        kernel::runtime::{ConfinementConfig, OciConfinementConfig},
        operator::{
            channel_env::{merge_channel_env, ChannelEnv},
            config::{ChannelLaunchMode, OperatorConfig, RuntimeProfileConfig},
            managed_units::{daemon_unit_name, ensure_unit_identity, FakeUnitManager, UnitManager},
            reconcile::{add_channel, add_skill},
            runtime::resolve_runtime_execution_context,
        },
    };

    fn binaries() -> crate::operator::reconcile::StackBinaryPaths {
        crate::operator::reconcile::StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        }
    }

    fn current_work_root() -> std::path::PathBuf {
        resolve_project_workspace_root().expect("resolve project workspace root")
    }

    fn current_project_scope() -> String {
        let project_root = current_work_root();
        runtime_project_partition_key(Some(project_root.as_path()))
    }

    fn test_daemon_unit_name(home: &LionClawHome) -> String {
        let identity = ensure_unit_identity(home).expect("unit identity");
        daemon_unit_name(&identity)
    }

    async fn current_daemon_fingerprint(
        home: &LionClawHome,
        config: &OperatorConfig,
        runtime_id: Option<&str>,
    ) -> String {
        let runtime_config_fingerprint =
            resolve_runtime_execution_context(home, config, runtime_id)
                .await
                .expect("resolve runtime context")
                .daemon_config_fingerprint;
        let applied_state = AppliedState::load(home).await.expect("load applied state");
        compute_daemon_fingerprint(&runtime_config_fingerprint, &applied_state)
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
    ) -> (tempfile::TempDir, LionClawHome, FakeUnitManager) {
        let (temp_dir, home, manager, _listener) =
            seed_interactive_channel_with_bind(launch_mode, false).await;
        (temp_dir, home, manager)
    }

    #[cfg(unix)]
    async fn seed_interactive_channel_with_reserved_bind(
        launch_mode: ChannelLaunchMode,
    ) -> (
        tempfile::TempDir,
        LionClawHome,
        FakeUnitManager,
        std::net::TcpListener,
    ) {
        let (temp_dir, home, manager, listener) =
            seed_interactive_channel_with_bind(launch_mode, true).await;
        (
            temp_dir,
            home,
            manager,
            listener.expect("reserved daemon bind listener"),
        )
    }

    #[cfg(unix)]
    async fn seed_interactive_channel_with_bind(
        launch_mode: ChannelLaunchMode,
        reserve_bind: bool,
    ) -> (
        tempfile::TempDir,
        LionClawHome,
        FakeUnitManager,
        Option<std::net::TcpListener>,
    ) {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("listener addr");
        let bind = format!("127.0.0.1:{}", addr.port());
        let listener = reserve_bind.then_some(listener);
        home.ensure_base_dirs().await.expect("create base dirs");
        let codex_home = home.root().join(".codex");
        tokio::fs::create_dir_all(&codex_home)
            .await
            .expect("create codex home");
        tokio::fs::write(
            codex_home.join("auth.json"),
            r#"{
  "OPENAI_API_KEY": "sk-test"
}"#,
        )
        .await
        .expect("write runtime auth");

        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        write_executable(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n");
        let podman = temp_dir.path().join("podman");
        write_executable(
            &podman,
            "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nexit 0\n",
        );

        let skill_source = temp_dir.path().join("channel-fixture");
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-fixture\ndescription: test\n---\n",
        )
        .expect("skill md");
        write_executable(
            &skill_source.join("scripts/worker"),
            "#!/usr/bin/env bash\nenv | sort > /tmp/channel-attach-env\n",
        );

        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.daemon.bind = bind;
        config.runtimes = [(
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
        .collect();
        config.save(&home).await.expect("save config");
        add_skill(
            &home,
            "loopback".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install channel skill");
        add_channel(
            &home,
            "loopback".to_string(),
            "loopback".to_string(),
            launch_mode,
            Vec::new(),
        )
        .await
        .expect("add channel");

        (temp_dir, home, FakeUnitManager::default(), listener)
    }

    async fn spawn_probe_server(
        app: Router,
        listener: std::net::TcpListener,
    ) -> tokio::task::JoinHandle<()> {
        listener
            .set_nonblocking(true)
            .expect("set probe listener nonblocking");
        let listener = tokio::net::TcpListener::from_std(listener).expect("convert probe listener");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve probe app");
        })
    }

    #[cfg(unix)]
    struct RemovingInstalledSkillOnStartManager {
        inner: FakeUnitManager,
        home: LionClawHome,
        alias: String,
    }

    #[cfg(unix)]
    impl RemovingInstalledSkillOnStartManager {
        fn new(home: &LionClawHome, alias: &str) -> Self {
            Self {
                inner: FakeUnitManager::default(),
                home: home.clone(),
                alias: alias.to_string(),
            }
        }

        fn remove_installed_skill(&self) -> Result<()> {
            let skill_root = self.home.skills_dir().join(&self.alias);
            if skill_root.exists() {
                fs::remove_dir_all(&skill_root)
                    .with_context(|| format!("remove installed skill {}", skill_root.display()))?;
            }
            Ok(())
        }
    }

    #[cfg(unix)]
    #[async_trait]
    impl UnitManager for RemovingInstalledSkillOnStartManager {
        fn owned_units(
            &self,
            home: &LionClawHome,
        ) -> Result<crate::operator::managed_units::OwnedManagedUnits> {
            self.inner.owned_units(home)
        }

        async fn apply_units(
            &self,
            home: &LionClawHome,
            units: &[crate::operator::managed_units::ManagedUnit],
        ) -> Result<Vec<String>> {
            self.inner.apply_units(home, units).await
        }

        async fn up_units(&self, units: &[String]) -> Result<()> {
            self.inner.up_units(units).await?;
            self.remove_installed_skill()
        }

        async fn restart_units(&self, units: &[String]) -> Result<()> {
            self.inner.restart_units(units).await?;
            self.remove_installed_skill()
        }

        async fn down_units(&self, units: &[String]) -> Result<()> {
            self.inner.down_units(units).await
        }

        async fn unit_status(&self, unit: &str) -> Result<String> {
            self.inner.unit_status(unit).await
        }

        async fn logs(&self, units: &[String], lines: usize) -> Result<String> {
            self.inner.logs(units, lines).await
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_background_channels() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Background).await;

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("background launch mode should fail");
        assert!(err.to_string().contains("run 'lionclaw up'"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_missing_codex_runtime_auth() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let codex_home = home.root().join(".codex");
        tokio::fs::write(codex_home.join("auth.json"), "{}")
            .await
            .expect("rewrite runtime auth");

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("missing runtime auth should fail");

        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_builds_ephemeral_tail_env_and_starts_managed_units() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;

        let spec = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(
            spec.started_managed_units,
            "daemon should be ensured when unreachable"
        );
        assert!(
            spec.worker_path.ends_with("scripts/worker"),
            "canonical worker entrypoint should be required"
        );

        let env = spec.env.into_iter().collect::<BTreeMap<_, _>>();
        assert_eq!(
            env.get("LIONCLAW_CHANNEL_ID").map(String::as_str),
            Some("loopback")
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
            .is_some_and(|value| value.starts_with("interactive:loopback:mosh:")));
        assert_eq!(
            manager
                .unit_status(&test_daemon_unit_name(&home))
                .await
                .expect("unit status"),
            "loaded/active/running"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_requires_and_exports_required_env_for_interactive_channels() {
        let (_temp_dir, home, manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        add_channel(
            &home,
            "loopback".to_string(),
            "loopback".to_string(),
            ChannelLaunchMode::Interactive,
            vec!["PATH".to_string()],
        )
        .await
        .expect("update channel required env");
        let mut channel_env = ChannelEnv::new();
        channel_env.insert("PATH".to_string(), "/usr/bin:/bin".to_string());
        merge_channel_env(&home, "loopback", &channel_env).expect("persist channel env");

        let spec = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(spec
            .env
            .iter()
            .any(|(key, value)| key == "PATH" && value == "/usr/bin:/bin"));

        add_channel(
            &home,
            "loopback".to_string(),
            "loopback".to_string(),
            ChannelLaunchMode::Interactive,
            vec!["LIONCLAW_TEST_MISSING_REQUIRED_ENV".to_string()],
        )
        .await
        .expect("update channel with missing required env");

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("missing required env should fail");

        assert!(err
            .to_string()
            .contains("LIONCLAW_TEST_MISSING_REQUIRED_ENV"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn launch_channel_attach_clears_inherited_environment() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let worker = temp_dir.path().join("worker.sh");
        let output = temp_dir.path().join("env.txt");
        write_executable(
            &worker,
            &format!(
                "#!/bin/sh\n/usr/bin/env | /usr/bin/sort > '{}'\n",
                output.display()
            ),
        );

        launch_channel_attach(ChannelAttachSpec {
            worker_path: worker,
            bind_addr: "127.0.0.1:0".to_string(),
            env: vec![("LIONCLAW_CHANNEL_ID".to_string(), "loopback".to_string())],
            started_managed_units: false,
            expected_daemon_fingerprint: String::new(),
        })
        .await
        .expect("launch worker");

        let env_output = fs::read_to_string(output).expect("env output");
        assert!(env_output.contains("LIONCLAW_CHANNEL_ID=loopback\n"));
        if std::env::var_os("HOME").is_some() {
            assert!(
                !env_output.lines().any(|line| line.starts_with("HOME=")),
                "worker should not inherit operator HOME"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_uses_applied_worker_snapshot_after_startup() {
        let (_temp_dir, home, _manager) =
            seed_interactive_channel(ChannelLaunchMode::Interactive).await;
        let manager = RemovingInstalledSkillOnStartManager::new(&home, "loopback");

        let spec = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            Some("mosh".to_string()),
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(
            spec.started_managed_units,
            "daemon should be ensured when unreachable"
        );
        assert!(
            !home.skills_dir().join("loopback").exists(),
            "test manager should remove the live installed alias"
        );
        assert!(
            spec.worker_path
                .starts_with(home.skills_dir().join(".applied")),
            "interactive attach should resolve workers from the frozen applied snapshot"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_reuses_same_home_daemon() {
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config, Some("codex")).await;
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
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            daemon_fingerprint: daemon_fingerprint.clone(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            listener,
        )
        .await;

        let spec = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(
            !spec.started_managed_units,
            "same-home daemon should be reused"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_does_not_export_requested_runtime_to_worker() {
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config, Some("codex")).await;
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
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            daemon_fingerprint: daemon_fingerprint.clone(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            listener,
        )
        .await;

        let spec = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        let env = spec.env.into_iter().collect::<BTreeMap<_, _>>();
        assert!(!env.contains_key("LIONCLAW_RUNTIME_ID"));
        assert!(
            !spec.started_managed_units,
            "same-home daemon should be reused"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_restarts_same_home_daemon_when_config_is_stale() {
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
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
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            daemon_fingerprint: "daemon-stale-state".to_string(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            listener,
        )
        .await;
        let daemon_unit = test_daemon_unit_name(&home);
        manager
            .set_unit_status(&daemon_unit, "loaded/active/running")
            .expect("set unit status");

        let spec = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect("prepare attach");

        assert!(
            spec.started_managed_units,
            "stale daemon should be reconciled"
        );
        assert!(
            manager
                .was_restarted(&daemon_unit)
                .expect("read restart state"),
            "managed daemon should be restarted when config fingerprint changes"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_unknown_requested_runtime_when_reusing_same_home_daemon(
    ) {
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config, Some("codex")).await;
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
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            daemon_fingerprint: daemon_fingerprint.clone(),
                        };
                        async move { Json(response) }
                    }
                }),
            ),
            listener,
        )
        .await;

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("missing".to_string()),
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
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let bind_addr = config.daemon.bind.clone();
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config, Some("codex")).await;
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    move || async move {
                        Json(DaemonInfoResponse {
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: "foreign-home".to_string(),
                            home_root: "/tmp/foreign-home".to_string(),
                            bind_addr: bind_addr.clone(),
                            project_scope: "foreign-project".to_string(),
                            daemon_fingerprint: daemon_fingerprint.clone(),
                        })
                    }
                }),
            ),
            listener,
        )
        .await;

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("foreign daemon should fail");

        assert!(err.to_string().contains("/tmp/foreign-home"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_incompatible_lionclaw_daemon() {
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
        let _server = spawn_probe_server(
            Router::new().route(
                "/health",
                get(|| async { Json(json!({"daemon": "lionclawd", "status": "ok"})) }),
            ),
            listener,
        )
        .await;

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("older daemon should fail");

        assert!(err.to_string().contains("older LionClaw daemon"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepare_channel_attach_rejects_same_home_different_project_daemon() {
        let (_temp_dir, home, manager, listener) =
            seed_interactive_channel_with_reserved_bind(ChannelLaunchMode::Interactive).await;
        let config = OperatorConfig::load(&home).await.expect("load config");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let home_root = home.root().display().to_string();
        let daemon_fingerprint = current_daemon_fingerprint(&home, &config, Some("codex")).await;
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_id = home_id.clone();
                    let home_root = home_root.clone();
                    move || async move {
                        Json(DaemonInfoResponse {
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: "different-project".to_string(),
                            daemon_fingerprint: daemon_fingerprint.clone(),
                        })
                    }
                }),
            ),
            listener,
        )
        .await;

        let err = prepare_channel_attach(
            ChannelAttachContext {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: &current_work_root(),
            },
            "loopback".to_string(),
            None,
            Some("codex".to_string()),
            &binaries(),
        )
        .await
        .expect_err("same-home different-project daemon should fail");

        assert!(err.to_string().contains("different project"));
    }
}
