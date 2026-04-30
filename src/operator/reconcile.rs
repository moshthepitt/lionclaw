use anyhow::{anyhow, Context, Result};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{
    config::resolve_project_workspace_root,
    contracts::{ChannelBindRequest, ChannelPeerApproveRequest, ChannelPeerResponse, TrustTier},
    home::{runtime_project_partition_key, LionClawHome},
    kernel::{skills::validate_skill_alias, Kernel, KernelOptions, RuntimeExecutionPolicy},
    operator::{
        config::{
            normalize_local_source, ChannelLaunchMode, ManagedChannelConfig, ManagedSkillConfig,
            OperatorConfig,
        },
        daemon_probe::{classify_daemon, DaemonClassification},
        lockfile::{LockedChannel, LockedSkill, OperatorLockfile},
        runtime::{
            register_configured_runtimes, resolve_runtime_execution_context,
            validate_runtime_launch_prerequisites,
        },
        services::{
            channel_unit_name, render_channel_unit, render_daemon_unit, unit_status_is_active,
            ChannelServiceSpec, DaemonServiceSpec, ManagedServiceUnit, ServiceManager,
            DAEMON_UNIT_NAME,
        },
        snapshot::{install_snapshot, InstalledSnapshot},
    },
    runtime_timeouts::RuntimeTurnTimeouts,
    workspace::{bootstrap_workspace, read_workspace_sections, GENERATED_AGENTS_FILE},
};

#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub config: OperatorConfig,
    pub lockfile: OperatorLockfile,
}

#[derive(Debug, Clone)]
pub struct StackStatus {
    pub daemon_status: String,
    pub channels: Vec<ChannelStatus>,
}

#[derive(Debug, Clone)]
pub struct ChannelStatus {
    pub id: String,
    pub skill: String,
    pub skill_id: String,
    pub launch_mode: String,
    pub binding_enabled: bool,
    pub unit_status: String,
    pub pending_peers: u64,
    pub approved_peers: u64,
    pub blocked_peers: u64,
    pub latest_inbound_at: Option<String>,
    pub latest_outbound_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StackBinaryPaths {
    pub daemon_bin: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnboardBindSelection {
    Explicit(String),
    Auto,
}

pub async fn onboard(
    home: &LionClawHome,
    bind_selection: Option<OnboardBindSelection>,
) -> Result<OperatorConfig> {
    home.ensure_base_dirs().await?;
    let mut config = OperatorConfig::load(home).await?;
    config.daemon.bind = resolve_onboard_bind(&config.daemon.bind, bind_selection.as_ref())?;
    bootstrap_workspace(&config.workspace_root(home)).await?;
    config.save(home).await?;
    OperatorLockfile::load(home).await?.save(home).await?;
    Ok(config)
}

pub async fn add_skill(
    home: &LionClawHome,
    alias: String,
    source: String,
    reference: String,
) -> Result<()> {
    validate_skill_alias(&alias)?;
    let mut config = OperatorConfig::load(home).await?;
    let source = normalize_local_source(&source)?;
    config.upsert_skill(ManagedSkillConfig {
        alias,
        source,
        reference,
    });
    config.save(home).await
}

pub async fn remove_skill(home: &LionClawHome, alias: &str) -> Result<bool> {
    let mut config = OperatorConfig::load(home).await?;
    let removed = config.remove_skill(alias);
    config.save(home).await?;
    Ok(removed)
}

pub async fn add_channel(
    home: &LionClawHome,
    id: String,
    skill: String,
    launch_mode: ChannelLaunchMode,
    required_env: Vec<String>,
) -> Result<()> {
    let mut config = OperatorConfig::load(home).await?;
    config.upsert_channel(ManagedChannelConfig {
        id,
        skill,
        enabled: true,
        launch_mode,
        required_env,
    });
    config.save(home).await
}

pub async fn remove_channel(home: &LionClawHome, id: &str) -> Result<bool> {
    let mut config = OperatorConfig::load(home).await?;
    let removed = config.remove_channel(id);
    config.save(home).await?;
    Ok(removed)
}

pub async fn apply(home: &LionClawHome) -> Result<ApplyResult> {
    home.ensure_base_dirs().await?;
    let config = OperatorConfig::load(home).await?;
    bootstrap_workspace(&config.workspace_root(home)).await?;
    let previous_lock = OperatorLockfile::load(home).await?;

    let kernel = open_kernel(home, &config, None).await?;
    let mut next_lock = OperatorLockfile::default();
    let mut installed_skills = BTreeMap::new();
    let mut desired_skills = Vec::new();
    let mut desired_aliases = BTreeSet::new();
    let mut snapshot_aliases = BTreeMap::new();

    for skill in &config.skills {
        validate_skill_alias(&skill.alias)?;
        if !desired_aliases.insert(skill.alias.clone()) {
            return Err(anyhow!(
                "skill alias '{}' is configured more than once",
                skill.alias
            ));
        }
        let snapshot = install_snapshot(home, &skill.alias, &skill.source, &skill.reference)?;
        if let Some(existing_alias) =
            snapshot_aliases.insert(snapshot.skill_id.clone(), skill.alias.clone())
        {
            return Err(anyhow!(
                "skill snapshot is configured under both '{}' and '{}'; remove one alias",
                existing_alias,
                skill.alias
            ));
        }
        desired_skills.push((skill, snapshot));
    }

    for channel in &config.channels {
        if !desired_aliases.contains(channel.skill.as_str()) {
            return Err(anyhow!(
                "channel '{}' references missing skill alias '{}'",
                channel.id,
                channel.skill
            ));
        }
    }

    for (skill, snapshot) in desired_skills {
        let installed = kernel
            .install_skill_with_actor(
                crate::contracts::SkillInstallRequest {
                    source: snapshot.source_uri.clone(),
                    alias: skill.alias.clone(),
                    reference: Some(snapshot.reference.clone()),
                    hash: Some(snapshot.hash.clone()),
                    skill_md: Some(snapshot.skill_md.clone()),
                    snapshot_path: Some(snapshot.snapshot_abs_dir.to_string_lossy().to_string()),
                },
                "operator",
            )
            .await
            .map_err(to_anyhow)?;

        installed_skills.insert(
            skill.alias.clone(),
            (snapshot.clone(), installed.skill_id.clone()),
        );
        next_lock
            .skills
            .push(to_locked_skill(skill, snapshot, installed.skill_id));
    }
    let next_skill_aliases = next_lock
        .skills
        .iter()
        .map(|skill| skill.alias.as_str())
        .collect::<BTreeSet<_>>();

    let desired_channel_ids = config
        .channels
        .iter()
        .map(|channel| channel.id.as_str())
        .collect::<BTreeSet<_>>();

    for old_channel in &previous_lock.channels {
        if !desired_channel_ids.contains(old_channel.id.as_str()) {
            kernel
                .disable_channel_binding(&old_channel.id, "operator")
                .await
                .map_err(to_anyhow)?;
        }
    }

    for channel in &config.channels {
        installed_skills
            .get(&channel.skill)
            .ok_or_else(|| anyhow!("skill alias '{}' disappeared during apply", channel.skill))?;

        kernel
            .bind_channel(ChannelBindRequest {
                channel_id: channel.id.clone(),
                skill_alias: channel.skill.clone(),
                enabled: Some(channel.enabled),
                config: Some(json!({})),
            })
            .await
            .map_err(to_anyhow)?;

        next_lock.channels.push(LockedChannel {
            id: channel.id.clone(),
            skill: channel.skill.clone(),
            enabled: channel.enabled,
            launch_mode: channel.launch_mode,
        });
    }

    for old_skill in &previous_lock.skills {
        if !next_skill_aliases.contains(old_skill.alias.as_str()) {
            remove_stale_skill_alias_if_present(&kernel, &old_skill.alias).await?;
        }
    }

    next_lock.save(home).await?;

    Ok(ApplyResult {
        config,
        lockfile: next_lock,
    })
}

pub async fn up<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    runtime_id: &str,
    binaries: &StackBinaryPaths,
) -> Result<ApplyResult> {
    let config = OperatorConfig::load(home).await?;
    let runtime_context =
        resolve_runtime_execution_context(home, &config, Some(runtime_id)).await?;
    let home_id = home.ensure_home_id().await?;
    let project_root =
        resolve_project_workspace_root().context("failed to resolve project workspace root")?;
    let project_scope = runtime_project_partition_key(Some(project_root.as_path()));
    let expected_config_fingerprint = runtime_context.daemon_config_fingerprint.clone();
    match classify_daemon(
        &config.daemon.bind,
        &home_id,
        &project_scope,
        &expected_config_fingerprint,
    )
    .await?
    {
        DaemonClassification::Absent => {}
        DaemonClassification::SameHome | DaemonClassification::SameHomeDifferentConfig => {
            let daemon_status = manager.unit_status(DAEMON_UNIT_NAME).await?;
            if !unit_status_is_active(&daemon_status) {
                return Err(anyhow!(
                    "bind '{}' is already served by this LionClaw home, but not by the managed {} unit; stop the foreground daemon before running 'lionclaw service up'",
                    config.daemon.bind,
                    DAEMON_UNIT_NAME
                ));
            }
        }
        DaemonClassification::SameHomeDifferentProject => {
            return Err(anyhow!(
                "bind '{}' is already served by this LionClaw home for a different project; stop that daemon before running 'lionclaw service up' from this project",
                config.daemon.bind
            ));
        }
        DaemonClassification::ForeignHome(info) => {
            return Err(anyhow!(
                "bind '{}' is already served by a different LionClaw home at '{}'; stop that daemon or choose a different bind",
                config.daemon.bind,
                info.home_root
            ));
        }
        DaemonClassification::IncompatibleLionClaw => {
            return Err(anyhow!(
                "bind '{}' is already served by an older LionClaw daemon; restart that daemon before running 'lionclaw service up'",
                config.daemon.bind
            ));
        }
        DaemonClassification::UnknownListener => {
            return Err(anyhow!(
                "bind '{}' is already in use by a non-LionClaw listener",
                config.daemon.bind
            ));
        }
    }

    let previous_units = managed_unit_names(home)?;
    validate_runtime_launch_prerequisites(home, &config, runtime_id).await?;
    let applied = apply(home).await?;
    render_runtime_cache(home, &applied.config, &applied.lockfile, runtime_id).await?;
    let units = build_managed_units(
        home,
        &applied.config,
        &applied.lockfile,
        runtime_id,
        binaries,
        &runtime_context.daemon_config_fingerprint,
        runtime_context.codex_home_override.as_deref(),
    )?;
    let next_units = units
        .iter()
        .map(|unit| unit.name.clone())
        .collect::<BTreeSet<_>>();
    let stale_units = previous_units
        .into_iter()
        .filter(|unit| !next_units.contains(unit))
        .collect::<Vec<_>>();
    if !stale_units.is_empty() {
        manager.down_units(&stale_units).await?;
    }
    let unit_names = units
        .iter()
        .map(|unit| unit.name.clone())
        .collect::<Vec<_>>();
    let changed_units = manager.apply_units(home, &units).await?;
    let mut units_to_start = Vec::new();
    let mut units_to_restart = Vec::new();
    for unit_name in &unit_names {
        let status = manager.unit_status(unit_name).await?;
        if unit_status_is_active(&status) {
            if changed_units.iter().any(|changed| changed == unit_name) {
                units_to_restart.push(unit_name.clone());
            }
        } else {
            units_to_start.push(unit_name.clone());
        }
    }
    if !units_to_start.is_empty() {
        manager.up_units(&units_to_start).await?;
    }
    if !units_to_restart.is_empty() {
        manager.restart_units(&units_to_restart).await?;
    }
    Ok(applied)
}

fn resolve_onboard_bind(
    current_bind: &str,
    selection: Option<&OnboardBindSelection>,
) -> Result<String> {
    match selection {
        None => Ok(current_bind.to_string()),
        Some(OnboardBindSelection::Explicit(bind)) => Ok(bind.trim().to_string()),
        Some(OnboardBindSelection::Auto) => allocate_auto_bind(),
    }
}

fn allocate_auto_bind() -> Result<String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .context("failed to allocate an automatic loopback bind")?;
    let addr = listener
        .local_addr()
        .context("failed to read automatic bind address")?;
    Ok(format!("127.0.0.1:{}", addr.port()))
}

pub async fn down<M: ServiceManager>(home: &LionClawHome, manager: &M) -> Result<()> {
    let units = managed_unit_names(home)?;
    manager.down_units(&units).await
}

pub async fn status<M: ServiceManager>(home: &LionClawHome, manager: &M) -> Result<StackStatus> {
    let config = OperatorConfig::load(home).await?;
    let lockfile = OperatorLockfile::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;

    let mut channels = Vec::new();
    for channel in &lockfile.channels {
        let binding = kernel
            .get_channel_binding(&channel.id)
            .await
            .map_err(to_anyhow)?
            .ok_or_else(|| anyhow!("channel '{}' binding is missing", channel.id))?;
        let health = kernel
            .get_channel_health(&channel.id)
            .await
            .map_err(to_anyhow)?;
        let unit_status = if channel.launch_mode == ChannelLaunchMode::Interactive {
            "interactive".to_string()
        } else {
            manager.unit_status(&channel_unit_name(&channel.id)).await?
        };
        channels.push(ChannelStatus {
            id: channel.id.clone(),
            skill: channel.skill.clone(),
            skill_id: lockfile
                .find_skill(&channel.skill)
                .map(|skill| skill.skill_id.clone())
                .ok_or_else(|| {
                    anyhow!(
                        "channel '{}' skill '{}' is missing",
                        channel.id,
                        channel.skill
                    )
                })?,
            launch_mode: channel.launch_mode.as_str().to_string(),
            binding_enabled: binding.enabled,
            unit_status,
            pending_peers: health.pending_peer_count,
            approved_peers: health.approved_peer_count,
            blocked_peers: health.blocked_peer_count,
            latest_inbound_at: health.latest_inbound_at.map(|value| value.to_rfc3339()),
            latest_outbound_at: health.latest_outbound_at.map(|value| value.to_rfc3339()),
        });
    }

    Ok(StackStatus {
        daemon_status: manager.unit_status(DAEMON_UNIT_NAME).await?,
        channels,
    })
}

pub async fn logs<M: ServiceManager>(
    home: &LionClawHome,
    manager: &M,
    lines: usize,
) -> Result<String> {
    let units = managed_unit_names(home)?;
    manager.logs(&units, lines).await
}

pub async fn pairing_list(
    home: &LionClawHome,
    channel_id: Option<String>,
) -> Result<Vec<crate::contracts::ChannelPeerView>> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    let peers = kernel
        .list_channel_peers(channel_id)
        .await
        .map_err(to_anyhow)?;
    Ok(peers.peers)
}

pub async fn pairing_approve(
    home: &LionClawHome,
    channel_id: String,
    peer_id: String,
    pairing_code: String,
    trust_tier: TrustTier,
) -> Result<ChannelPeerResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    kernel
        .approve_channel_peer(ChannelPeerApproveRequest {
            channel_id,
            peer_id,
            pairing_code,
            trust_tier: Some(trust_tier),
        })
        .await
        .map_err(to_anyhow)
}

pub async fn pairing_block(
    home: &LionClawHome,
    channel_id: String,
    peer_id: String,
) -> Result<ChannelPeerResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    kernel
        .block_channel_peer(crate::contracts::ChannelPeerBlockRequest {
            channel_id,
            peer_id,
        })
        .await
        .map_err(to_anyhow)
}

pub fn resolve_stack_binaries() -> Result<StackBinaryPaths> {
    let current_exe = std::env::current_exe().context("failed to resolve current executable")?;
    let daemon_bin = current_exe.with_file_name("lionclawd");
    if !daemon_bin.exists() {
        return Err(anyhow!(
            "lionclawd binary not found next to '{}'",
            current_exe.display()
        ));
    }

    Ok(StackBinaryPaths { daemon_bin })
}

pub(crate) fn build_managed_units(
    home: &LionClawHome,
    config: &OperatorConfig,
    lockfile: &OperatorLockfile,
    runtime_id: &str,
    binaries: &StackBinaryPaths,
    config_fingerprint: &str,
    codex_home_override: Option<&Path>,
) -> Result<Vec<ManagedServiceUnit>> {
    let mut units = Vec::new();
    let project_workspace_root = resolve_project_workspace_root()
        .context("failed to resolve project workspace root for managed daemon")?;
    units.push(render_daemon_unit(
        home,
        &binaries.daemon_bin,
        DaemonServiceSpec {
            bind_addr: &config.daemon.bind,
            runtime_id,
            workspace: &config.daemon.workspace,
            project_workspace_root: &project_workspace_root,
            config_fingerprint,
            codex_home_override,
        },
    ));

    let base_url = base_url_from_bind(&config.daemon.bind);
    for channel in config
        .channels
        .iter()
        .filter(|channel| channel.enabled && channel.launch_mode == ChannelLaunchMode::Service)
    {
        let locked_skill = lockfile.find_skill(&channel.skill).ok_or_else(|| {
            anyhow!(
                "managed channel '{}' references unknown locked skill '{}'",
                channel.id,
                channel.skill
            )
        })?;
        let worker_path = resolve_worker_entrypoint(home, &locked_skill.snapshot_dir)
            .with_context(|| format!("channel '{}' worker resolution failed", channel.id))?;

        let mut env = vec![
            (
                "LIONCLAW_HOME".to_string(),
                home.root().display().to_string(),
            ),
            ("LIONCLAW_BASE_URL".to_string(), base_url.clone()),
            ("LIONCLAW_CHANNEL_ID".to_string(), channel.id.clone()),
            (
                "LIONCLAW_CHANNEL_RUNTIME_DIR".to_string(),
                home.runtime_channel_dir(&channel.id).display().to_string(),
            ),
        ];
        for key in &channel.required_env {
            let value = std::env::var(key).with_context(|| {
                format!(
                    "required environment variable '{}' is not set for channel '{}'",
                    key, channel.id
                )
            })?;
            env.push((key.clone(), value));
        }

        units.push(render_channel_unit(
            home,
            &ChannelServiceSpec {
                channel_id: channel.id.clone(),
                worker_path,
                env,
            },
        ));
    }

    Ok(units)
}

pub(crate) async fn render_runtime_cache(
    home: &LionClawHome,
    config: &OperatorConfig,
    _lockfile: &OperatorLockfile,
    runtime_id: &str,
) -> Result<()> {
    let workspace = &config.daemon.workspace;
    let project_workspace_root =
        resolve_project_workspace_root().context("failed to resolve project workspace root")?;
    let target_dir = home.runtime_project_dir(runtime_id, workspace, &project_workspace_root);
    for path in [
        target_dir.clone(),
        home.runtime_project_drafts_dir(runtime_id, workspace, &project_workspace_root),
    ] {
        tokio::fs::create_dir_all(&path)
            .await
            .with_context(|| format!("failed to create {}", path.display()))?;
    }
    let target_path = target_dir.join(GENERATED_AGENTS_FILE);

    let mut sections = Vec::new();
    for (name, content) in read_workspace_sections(&config.workspace_root(home)).await? {
        sections.push(format!("## {}\n\n{}", name, content.trim()));
    }

    sections.push(
        "## Draft Outputs\n\nWrite generated files intended for review or keeping to LIONCLAW_DRAFTS_DIR.".to_string(),
    );

    sections.push(
        "## Runtime Secrets\n\nIf this preset mounts runtime secrets, look under /run/secrets for the LionClaw-provided dotenv file whose name starts with lionclaw-runtime-secrets-, read it, and do not print its contents.".to_string(),
    );

    let rendered = render_marker_file(
        &format!(
            "# LionClaw Generated Agent Context\n\nThis file is generated for runtime '{runtime_id}'.\n"
        ),
        &sections.join("\n\n"),
    );

    tokio::fs::write(&target_path, rendered)
        .await
        .with_context(|| format!("failed to write {}", target_path.display()))?;
    Ok(())
}

fn render_marker_file(header: &str, body: &str) -> String {
    let start = "<!-- LIONCLAW:START -->";
    let end = "<!-- LIONCLAW:END -->";
    format!("{header}\n{start}\n{body}\n{end}\n")
}

fn to_locked_skill(
    config: &ManagedSkillConfig,
    snapshot: InstalledSnapshot,
    skill_id: String,
) -> LockedSkill {
    LockedSkill {
        alias: config.alias.clone(),
        source: snapshot.source_uri,
        reference: snapshot.reference,
        skill_id,
        hash: snapshot.hash,
        snapshot_dir: snapshot.snapshot_rel_dir,
    }
}

pub(crate) fn base_url_from_bind(bind: &str) -> String {
    if let Ok(addr) = bind.parse::<std::net::SocketAddr>() {
        match addr {
            std::net::SocketAddr::V4(value) => format!("http://{}:{}", value.ip(), value.port()),
            std::net::SocketAddr::V6(value) => {
                format!("http://[{}]:{}", value.ip(), value.port())
            }
        }
    } else {
        format!("http://{bind}")
    }
}

pub(crate) fn resolve_worker_entrypoint(
    home: &LionClawHome,
    snapshot_dir: &str,
) -> Result<PathBuf> {
    let snapshot_root = resolve_locked_snapshot_dir(home, snapshot_dir)?;
    let candidate = snapshot_root.join("scripts/worker");
    let metadata = match fs::symlink_metadata(&candidate) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(anyhow!(
                "worker entrypoint is missing under '{}'; expected 'scripts/worker'",
                snapshot_root.display()
            ));
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to stat worker entrypoint {}", candidate.display())
            });
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "worker entrypoint '{}' must not be a symlink",
            candidate.display()
        ));
    }
    if !metadata.is_file() {
        return Err(anyhow!(
            "worker entrypoint is missing under '{}'; expected 'scripts/worker'",
            snapshot_root.display()
        ));
    }
    if !is_executable_file(&metadata) {
        return Err(anyhow!(
            "worker entrypoint '{}' is not executable",
            candidate.display()
        ));
    }

    Ok(candidate)
}

pub(crate) async fn resolve_installed_skill_worker_entrypoint(
    home: &LionClawHome,
    alias: &str,
) -> Result<PathBuf> {
    validate_skill_alias(alias)?;
    let lockfile = OperatorLockfile::load(home).await?;
    let locked_skill = lockfile
        .find_skill(alias)
        .ok_or_else(|| anyhow!("skill alias '{alias}' is not installed; run 'lionclaw apply'"))?;

    resolve_worker_entrypoint(home, &locked_skill.snapshot_dir)
}

fn resolve_locked_snapshot_dir(home: &LionClawHome, snapshot_dir: &str) -> Result<PathBuf> {
    let snapshot_path = Path::new(snapshot_dir);
    let mut components = snapshot_path.components();
    let valid = matches!(components.next(), Some(Component::Normal(value)) if value == "skills")
        && matches!(components.next(), Some(Component::Normal(_)))
        && components.next().is_none();
    if snapshot_path.is_absolute() || !valid {
        return Err(anyhow!(
            "locked skill snapshot_dir '{snapshot_dir}' must be a relative skills/<snapshot> path"
        ));
    }

    let snapshot_root = home.root().join(snapshot_path);
    let metadata = fs::symlink_metadata(&snapshot_root).with_context(|| {
        format!(
            "failed to stat locked skill snapshot {}",
            snapshot_root.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "locked skill snapshot '{}' must not be a symlink",
            snapshot_root.display()
        ));
    }
    if !metadata.is_dir() {
        return Err(anyhow!(
            "locked skill snapshot '{}' is not a directory",
            snapshot_root.display()
        ));
    }

    let skills_root = fs::canonicalize(home.skills_dir())
        .with_context(|| format!("failed to resolve {}", home.skills_dir().display()))?;
    let snapshot_root = fs::canonicalize(&snapshot_root)
        .with_context(|| format!("failed to resolve {}", snapshot_root.display()))?;
    if snapshot_root.parent() != Some(skills_root.as_path()) {
        return Err(anyhow!(
            "locked skill snapshot '{}' must stay directly under '{}'",
            snapshot_root.display(),
            skills_root.display()
        ));
    }

    Ok(snapshot_root)
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

fn managed_unit_names(home: &LionClawHome) -> Result<Vec<String>> {
    let mut units = Vec::new();
    let systemd_dir = home.services_systemd_dir();
    if !systemd_dir.exists() {
        return Ok(vec![DAEMON_UNIT_NAME.to_string()]);
    }

    for entry in std::fs::read_dir(&systemd_dir)
        .with_context(|| format!("failed to read directory {}", systemd_dir.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to iterate {}", systemd_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|value| value.to_str()) {
            if name.starts_with("lionclaw") && name.ends_with(".service") {
                units.push(name.to_string());
            }
        }
    }

    units.sort();
    if !units.iter().any(|unit| unit == DAEMON_UNIT_NAME) {
        units.insert(0, DAEMON_UNIT_NAME.to_string());
    }
    Ok(units)
}

pub(crate) async fn open_kernel(
    home: &LionClawHome,
    config: &OperatorConfig,
    default_runtime_id: Option<String>,
) -> Result<Kernel> {
    open_kernel_with_project_root(home, config, default_runtime_id, None, None).await
}

pub(crate) async fn open_runtime_kernel(
    home: &LionClawHome,
    config: &OperatorConfig,
    default_runtime_id: Option<String>,
) -> Result<Kernel> {
    open_runtime_kernel_with_timeouts(home, config, default_runtime_id, None).await
}

pub(crate) async fn open_runtime_kernel_with_timeouts(
    home: &LionClawHome,
    config: &OperatorConfig,
    default_runtime_id: Option<String>,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> Result<Kernel> {
    let project_workspace_root =
        resolve_project_workspace_root().context("failed to resolve project workspace root")?;
    open_kernel_with_project_root(
        home,
        config,
        default_runtime_id,
        Some(project_workspace_root),
        timeout_override,
    )
    .await
}

async fn open_kernel_with_project_root(
    home: &LionClawHome,
    config: &OperatorConfig,
    default_runtime_id: Option<String>,
    project_workspace_root: Option<PathBuf>,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> Result<Kernel> {
    let workspace_root = config.workspace_root(home);
    let runtime_context =
        resolve_runtime_execution_context(home, config, default_runtime_id.as_deref()).await?;
    let timeouts = timeout_override.unwrap_or_else(RuntimeTurnTimeouts::interactive);
    let kernel = Kernel::new_with_options(
        &home.db_path(),
        KernelOptions {
            runtime_turn_idle_timeout: timeouts.idle,
            runtime_turn_hard_timeout: timeouts.hard,
            runtime_execution_policy: project_workspace_root
                .clone()
                .map(RuntimeExecutionPolicy::for_working_dir_root)
                .unwrap_or_default(),
            default_runtime_id: default_runtime_id.or_else(|| config.defaults.runtime.clone()),
            default_preset_name: config.defaults.preset.clone(),
            execution_presets: config.presets.clone(),
            runtime_execution_profiles: runtime_context.execution_profiles,
            runtime_secrets_home: Some(home.clone()),
            codex_home_override: runtime_context.codex_home_override,
            workspace_root: Some(workspace_root),
            project_workspace_root,
            runtime_root: Some(home.runtime_dir()),
            skill_snapshot_root: Some(home.skills_dir()),
            workspace_name: Some(config.daemon.workspace.clone()),
            ..KernelOptions::default()
        },
    )
    .await?;
    register_configured_runtimes(&kernel, config).await?;
    Ok(kernel)
}

fn to_anyhow(err: crate::kernel::KernelError) -> anyhow::Error {
    anyhow!(err.to_string())
}

async fn remove_stale_skill_alias_if_present(kernel: &Kernel, alias: &str) -> Result<()> {
    let removed = kernel
        .remove_skill_with_actor(alias, "operator")
        .await
        .map_err(to_anyhow)?;
    if removed {
        return Ok(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use axum::{routing::get, Json, Router};
    use serde_json::json;

    use super::{
        add_skill, apply, onboard, open_kernel, open_kernel_with_project_root, render_marker_file,
        render_runtime_cache, resolve_installed_skill_worker_entrypoint, resolve_worker_entrypoint,
        status, up, ApplyResult, OnboardBindSelection, StackBinaryPaths,
    };
    use crate::{
        config::resolve_project_workspace_root,
        contracts::{DaemonInfoResponse, SkillInstallRequest},
        home::{runtime_project_partition_key, LionClawHome},
        kernel::{
            runtime::{ConfinementConfig, OciConfinementConfig},
            Kernel, KernelOptions,
        },
        operator::{
            config::{
                ChannelLaunchMode, ManagedChannelConfig, ManagedSkillConfig, OperatorConfig,
                RuntimeProfileConfig,
            },
            lockfile::{LockedSkill, OperatorLockfile},
            runtime::resolve_runtime_execution_context,
            services::{FakeServiceManager, ServiceManager, DAEMON_UNIT_NAME},
        },
        workspace::GENERATED_AGENTS_FILE,
    };

    async fn spawn_probe_server(app: Router, bind_addr: &str) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("bind probe server");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve probe app");
        })
    }

    fn current_project_scope() -> String {
        let project_root =
            resolve_project_workspace_root().expect("resolve project workspace root");
        runtime_project_partition_key(Some(project_root.as_path()))
    }

    async fn current_daemon_config_fingerprint(home: &LionClawHome) -> String {
        let config = OperatorConfig::load(home).await.expect("load config");
        resolve_runtime_execution_context(home, &config, config.defaults.runtime.as_deref())
            .await
            .expect("resolve runtime context")
            .daemon_config_fingerprint
    }

    fn write_channel_skill_fixture(root: &Path, name: &str, description: &str) -> PathBuf {
        let skill_source = root.join(name);
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {description}\n---\n"),
        )
        .expect("skill md");
        let worker = skill_source.join("scripts/worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);
        skill_source
    }

    fn make_executable(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
        }
    }

    #[cfg(unix)]
    fn ensure_fake_podman(reference: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let engine = reference.parent().expect("stub parent").join("podman");
        if !engine.exists() {
            fs::write(
                &engine,
                "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nexit 0\n",
            )
            .expect("write fake podman");
            fs::set_permissions(&engine, fs::Permissions::from_mode(0o755))
                .expect("chmod fake podman");
        }
        engine
    }

    fn test_codex_runtime(runtime_stub: &Path) -> RuntimeProfileConfig {
        RuntimeProfileConfig::Codex {
            executable: "codex".to_string(),
            model: None,
            confinement: ConfinementConfig::Oci(OciConfinementConfig {
                engine: ensure_fake_podman(runtime_stub)
                    .to_string_lossy()
                    .to_string(),
                image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                ..OciConfinementConfig::default()
            }),
        }
    }

    async fn write_test_codex_auth(home: &LionClawHome) {
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
        .expect("write auth file");
    }

    #[tokio::test]
    async fn onboard_bootstraps_workspace_and_config() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, None).await.expect("onboard");

        assert_eq!(config.daemon.workspace, "main");
        assert!(home.config_path().exists());
        assert!(home.home_id_path().exists());
        assert!(home.workspace_dir("main").join("SOUL.md").exists());
    }

    #[tokio::test]
    async fn onboard_with_auto_bind_persists_loopback_port() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");

        assert!(config.daemon.bind.starts_with("127.0.0.1:"));
        assert_ne!(config.daemon.bind, "127.0.0.1:8979");

        let reloaded = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(reloaded.daemon.bind, config.daemon.bind);
    }

    #[tokio::test]
    async fn render_runtime_cache_includes_runtime_secret_guidance() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, None).await.expect("onboard");
        let lockfile = OperatorLockfile::default();

        render_runtime_cache(&home, &config, &lockfile, "codex")
            .await
            .expect("render runtime cache");
        let project_workspace_root =
            resolve_project_workspace_root().expect("resolve project workspace root");

        let rendered = tokio::fs::read_to_string(
            home.runtime_project_dir("codex", &config.daemon.workspace, &project_workspace_root)
                .join(GENERATED_AGENTS_FILE),
        )
        .await
        .expect("read generated agents");
        assert!(rendered.contains("/run/secrets"));
        assert!(rendered.contains("lionclaw-runtime-secrets-"));
        assert!(rendered.contains("do not print its contents"));
    }

    #[tokio::test]
    async fn state_kernel_open_does_not_require_project_workspace_root() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, None).await.expect("onboard");

        open_kernel(&home, &config, None)
            .await
            .expect("state kernel should open without a project root");
        open_kernel_with_project_root(&home, &config, None, None, None)
            .await
            .expect("state kernel helper should allow a missing project root");
    }

    #[test]
    fn marker_file_is_deterministic() {
        let rendered = render_marker_file("# Header", "body");
        assert_eq!(
            rendered,
            "# Header\n<!-- LIONCLAW:START -->\nbody\n<!-- LIONCLAW:END -->\n"
        );
    }

    #[test]
    fn worker_entrypoint_requires_canonical_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let snapshot_dir = "skills/example";
        let scripts_dir = home.root().join(snapshot_dir).join("scripts");
        fs::create_dir_all(&scripts_dir).expect("scripts dir");
        fs::write(scripts_dir.join("worker.sh"), "#!/usr/bin/env bash\n").expect("worker");

        let err = resolve_worker_entrypoint(&home, snapshot_dir).expect_err("should fail");
        assert!(err.to_string().contains("expected 'scripts/worker'"));
    }

    #[test]
    fn worker_entrypoint_rejects_lockfile_paths_outside_skill_snapshots() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        for snapshot_dir in [
            "../outside",
            "/tmp/outside",
            "runtime/example",
            "skills",
            "skills/../outside",
            "skills/example/nested",
        ] {
            let err = resolve_worker_entrypoint(&home, snapshot_dir)
                .expect_err("snapshot path should stay under skills/");
            assert!(
                err.to_string()
                    .contains("must be a relative skills/<snapshot> path"),
                "unexpected error for {snapshot_dir}: {err}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn worker_entrypoint_rejects_symlinked_snapshot_dir() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let outside = temp_dir.path().join("outside-snapshot");
        let worker = outside.join("scripts/worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("outside scripts");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);
        fs::create_dir_all(home.skills_dir()).expect("skills dir");
        symlink(&outside, home.skills_dir().join("example")).expect("snapshot symlink");

        let err = resolve_worker_entrypoint(&home, "skills/example")
            .expect_err("symlinked snapshot should fail");

        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn worker_entrypoint_rejects_directory_worker_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let snapshot_dir = "skills/example";
        fs::create_dir_all(
            home.root()
                .join(snapshot_dir)
                .join("scripts")
                .join("worker"),
        )
        .expect("worker directory");

        let err = resolve_worker_entrypoint(&home, snapshot_dir).expect_err("should fail");
        assert!(err.to_string().contains("expected 'scripts/worker'"));
    }

    #[cfg(unix)]
    #[test]
    fn worker_entrypoint_rejects_non_executable_worker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let snapshot_dir = "skills/example";
        let worker = home
            .root()
            .join(snapshot_dir)
            .join("scripts")
            .join("worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("scripts dir");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");

        let err = resolve_worker_entrypoint(&home, snapshot_dir).expect_err("should fail");
        assert!(
            err.to_string().contains("is not executable"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn worker_entrypoint_rejects_symlinked_worker() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let snapshot_dir = "skills/example";
        let worker = home
            .root()
            .join(snapshot_dir)
            .join("scripts")
            .join("worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("scripts dir");
        let outside_worker = temp_dir.path().join("outside-worker");
        fs::write(&outside_worker, "#!/usr/bin/env bash\n").expect("outside worker");
        make_executable(&outside_worker);
        symlink(&outside_worker, &worker).expect("worker symlink");

        let err = resolve_worker_entrypoint(&home, snapshot_dir).expect_err("should fail");
        assert!(
            err.to_string().contains("must not be a symlink"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn installed_skill_worker_entrypoint_uses_locked_alias() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let snapshot_dir = "skills/example";
        let worker = home
            .root()
            .join(snapshot_dir)
            .join("scripts")
            .join("worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("scripts dir");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);

        OperatorLockfile {
            skills: vec![LockedSkill {
                alias: "telegram".to_string(),
                source: "local:skills/channel-telegram".to_string(),
                reference: "local".to_string(),
                skill_id: "channel-telegram-hash".to_string(),
                hash: "hash".to_string(),
                snapshot_dir: snapshot_dir.to_string(),
            }],
            ..OperatorLockfile::default()
        }
        .save(&home)
        .await
        .expect("save lockfile");

        let resolved = resolve_installed_skill_worker_entrypoint(&home, "telegram")
            .await
            .expect("resolve worker");

        assert_eq!(resolved, worker.canonicalize().expect("canonical worker"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn render_runtime_cache_ignores_symlinked_skill_metadata() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, None).await.expect("onboard");
        let snapshot_root = home.skills_dir().join("example");
        fs::create_dir_all(&snapshot_root).expect("snapshot root");
        let outside_skill_md = temp_dir.path().join("outside-SKILL.md");
        fs::write(
            &outside_skill_md,
            "---\nname: outside\ndescription: outside\n---\n",
        )
        .expect("outside skill md");
        symlink(&outside_skill_md, snapshot_root.join("SKILL.md")).expect("skill md symlink");
        let lockfile = OperatorLockfile {
            skills: vec![LockedSkill {
                alias: "telegram".to_string(),
                source: "local:skills/channel-telegram".to_string(),
                reference: "local".to_string(),
                skill_id: "channel-telegram-hash".to_string(),
                hash: "hash".to_string(),
                snapshot_dir: "skills/example".to_string(),
            }],
            ..OperatorLockfile::default()
        };

        render_runtime_cache(&home, &config, &lockfile, "codex")
            .await
            .expect("symlinked skill metadata should be ignored");
    }

    #[tokio::test]
    async fn up_with_fake_manager_materializes_units() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        write_test_codex_auth(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&runtime_stub, permissions).expect("chmod runtime stub");
        }

        let skill_source = write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "test");

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config.skills = vec![ManagedSkillConfig {
            alias: "telegram".to_string(),
            source: skill_source.to_string_lossy().to_string(),
            reference: "local".to_string(),
        }];
        config.channels = vec![ManagedChannelConfig {
            id: "telegram".to_string(),
            skill: "telegram".to_string(),
            enabled: true,
            launch_mode: ChannelLaunchMode::Service,
            required_env: Vec::new(),
        }];
        config.save(&home).await.expect("save config");
        OperatorLockfile::default()
            .save(&home)
            .await
            .expect("save lock");

        let manager = FakeServiceManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        let applied: ApplyResult = up(&home, &manager, "codex", &binaries).await.expect("up");
        let project_workspace_root =
            resolve_project_workspace_root().expect("resolve project workspace root");

        assert_eq!(applied.config.channels.len(), 1);
        assert!(home
            .runtime_project_dir("codex", "main", &project_workspace_root)
            .join("AGENTS.generated.md")
            .exists());
        assert!(home
            .runtime_project_drafts_dir("codex", "main", &project_workspace_root)
            .exists());
    }

    #[tokio::test]
    async fn up_rejects_missing_codex_runtime_auth() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&runtime_stub, permissions).expect("chmod runtime stub");
        }

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        let skill_source = temp_dir.path().join("test-skill");
        fs::create_dir_all(&skill_source).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: test-skill\ndescription: test\n---\n",
        )
        .expect("skill md");
        config.skills.push(ManagedSkillConfig {
            alias: "test-skill".to_string(),
            source: skill_source.display().to_string(),
            reference: "local".to_string(),
        });
        config.save(&home).await.expect("save config");

        let err = up(
            &home,
            &FakeServiceManager::default(),
            "codex",
            &StackBinaryPaths {
                daemon_bin: "/tmp/lionclawd".into(),
            },
        )
        .await
        .expect_err("missing runtime auth should fail");

        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
        let lockfile = OperatorLockfile::load(&home).await.expect("load lockfile");
        assert!(
            lockfile.skills.is_empty(),
            "auth preflight should fail before apply() installs skill snapshots"
        );
    }

    #[tokio::test]
    async fn apply_disables_old_skill_revision_when_alias_is_updated() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let skill_source = temp_dir.path().join("channel-telegram");
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-telegram\ndescription: first revision\n---\n",
        )
        .expect("skill md v1");
        fs::write(
            skill_source.join("scripts/worker"),
            "#!/usr/bin/env bash\necho v1\n",
        )
        .expect("worker v1");
        make_executable(&skill_source.join("scripts/worker"));

        let config = OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "telegram".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");
        apply(&home).await.expect("apply v1");

        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-telegram\ndescription: second revision\n---\n",
        )
        .expect("skill md v2");

        let applied = apply(&home).await.expect("apply v2");
        let kernel = Kernel::new_with_options(
            &home.db_path(),
            KernelOptions {
                default_runtime_id: None,
                workspace_root: Some(home.workspace_dir("main")),
                project_workspace_root: Some(home.workspace_dir("main")),
                ..KernelOptions::default()
            },
        )
        .await
        .expect("kernel");

        let skills = kernel.list_skills().await.expect("list skills").skills;
        let enabled = skills.iter().collect::<Vec<_>>();
        assert_eq!(
            enabled.len(),
            1,
            "only one current revision should remain listed"
        );
        assert_eq!(
            enabled[0].skill_id, applied.lockfile.skills[0].skill_id,
            "the latest lockfile revision should be the current one"
        );
        assert_eq!(
            skills.len(),
            1,
            "list_skills should only show current aliases"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn apply_converges_when_worker_executable_bit_changes() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let skill_source = temp_dir.path().join("channel-terminal");
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-terminal\ndescription: test\n---\n",
        )
        .expect("skill md");
        let worker = skill_source.join("scripts/worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        fs::set_permissions(&worker, fs::Permissions::from_mode(0o644)).expect("chmod 644");

        let config = OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "mode-test".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");
        let first = apply(&home).await.expect("apply non-executable worker");
        let err = resolve_installed_skill_worker_entrypoint(&home, "mode-test")
            .await
            .expect_err("worker should not be executable yet");
        assert!(
            err.to_string().contains("is not executable"),
            "unexpected error: {err}"
        );

        fs::set_permissions(&worker, fs::Permissions::from_mode(0o755)).expect("chmod 755");
        let second = apply(&home).await.expect("apply executable worker");
        let installed_worker = resolve_installed_skill_worker_entrypoint(&home, "mode-test")
            .await
            .expect("resolve executable worker");

        assert_ne!(
            first.lockfile.skills[0].hash, second.lockfile.skills[0].hash,
            "mode-only changes should create a new snapshot revision"
        );
        assert_ne!(
            installed_worker
                .metadata()
                .expect("installed worker metadata")
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }

    #[tokio::test]
    async fn skill_add_reassigns_source_to_new_alias_without_internal_conflict() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let terminal_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-terminal", "terminal");
        let telegram_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "telegram");

        add_skill(
            &home,
            "alpha".to_string(),
            terminal_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("add alpha");
        add_skill(
            &home,
            "beta".to_string(),
            telegram_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("add beta");
        apply(&home).await.expect("apply original aliases");

        add_skill(
            &home,
            "beta".to_string(),
            terminal_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("reassign terminal to beta");
        let applied = apply(&home).await.expect("apply reassigned alias");

        assert_eq!(applied.config.skills.len(), 1);
        assert_eq!(applied.lockfile.skills.len(), 1);
        assert_eq!(applied.lockfile.skills[0].alias, "beta");

        let kernel = Kernel::new_with_options(
            &home.db_path(),
            KernelOptions {
                default_runtime_id: None,
                workspace_root: Some(home.workspace_dir("main")),
                project_workspace_root: Some(home.workspace_dir("main")),
                ..KernelOptions::default()
            },
        )
        .await
        .expect("kernel");
        let enabled = kernel.list_skills().await.expect("list skills").skills;
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].alias, "beta");
        assert_eq!(enabled[0].skill_id, applied.lockfile.skills[0].skill_id);
    }

    #[tokio::test]
    async fn apply_swaps_skill_aliases_without_disabling_reassigned_skills() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let terminal_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-terminal", "terminal");
        let telegram_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "telegram");

        OperatorConfig {
            skills: vec![
                ManagedSkillConfig {
                    alias: "alpha".to_string(),
                    source: terminal_source.to_string_lossy().to_string(),
                    reference: "local".to_string(),
                },
                ManagedSkillConfig {
                    alias: "beta".to_string(),
                    source: telegram_source.to_string_lossy().to_string(),
                    reference: "local".to_string(),
                },
            ],
            ..OperatorConfig::default()
        }
        .save(&home)
        .await
        .expect("save original config");
        apply(&home).await.expect("apply original aliases");

        OperatorConfig {
            skills: vec![
                ManagedSkillConfig {
                    alias: "alpha".to_string(),
                    source: telegram_source.to_string_lossy().to_string(),
                    reference: "local".to_string(),
                },
                ManagedSkillConfig {
                    alias: "beta".to_string(),
                    source: terminal_source.to_string_lossy().to_string(),
                    reference: "local".to_string(),
                },
            ],
            ..OperatorConfig::default()
        }
        .save(&home)
        .await
        .expect("save swapped config");
        let applied = apply(&home).await.expect("apply swapped aliases");

        let expected = applied
            .lockfile
            .skills
            .iter()
            .map(|skill| (skill.alias.clone(), skill.skill_id.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(expected.len(), 2);

        let kernel = Kernel::new_with_options(
            &home.db_path(),
            KernelOptions {
                default_runtime_id: None,
                workspace_root: Some(home.workspace_dir("main")),
                project_workspace_root: Some(home.workspace_dir("main")),
                ..KernelOptions::default()
            },
        )
        .await
        .expect("kernel");
        let enabled = kernel
            .list_skills()
            .await
            .expect("list skills")
            .skills
            .into_iter()
            .map(|skill| (skill.alias, skill.skill_id))
            .collect::<std::collections::BTreeMap<_, _>>();

        assert_eq!(enabled, expected);
    }

    #[tokio::test]
    async fn apply_preserves_enabled_alias_when_channel_validation_fails() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let terminal_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-terminal", "terminal");
        let telegram_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "telegram");

        OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "alpha".to_string(),
                source: terminal_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            ..OperatorConfig::default()
        }
        .save(&home)
        .await
        .expect("save original config");
        let original = apply(&home).await.expect("apply original alias");
        let original_skill_id = original.lockfile.skills[0].skill_id.clone();

        OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "alpha".to_string(),
                source: telegram_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            channels: vec![ManagedChannelConfig {
                id: "broken".to_string(),
                skill: "missing".to_string(),
                enabled: true,
                launch_mode: ChannelLaunchMode::Service,
                required_env: Vec::new(),
            }],
            ..OperatorConfig::default()
        }
        .save(&home)
        .await
        .expect("save invalid replacement config");

        let err = apply(&home)
            .await
            .expect_err("channel validation should fail before alias switch");
        assert!(
            err.to_string()
                .contains("references missing skill alias 'missing'"),
            "unexpected error: {err}"
        );

        let kernel = Kernel::new_with_options(
            &home.db_path(),
            KernelOptions {
                default_runtime_id: None,
                workspace_root: Some(home.workspace_dir("main")),
                project_workspace_root: Some(home.workspace_dir("main")),
                ..KernelOptions::default()
            },
        )
        .await
        .expect("kernel");
        let enabled = kernel.list_skills().await.expect("list skills").skills;

        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].alias, "alpha");
        assert_eq!(enabled[0].skill_id, original_skill_id);

        let listed = kernel
            .list_skills()
            .await
            .expect("list skills after failure");
        assert_eq!(
            listed.skills.len(),
            1,
            "channel preflight should fail before staging the replacement skill"
        );
    }

    #[tokio::test]
    async fn apply_disables_enabled_alias_conflict_missing_from_lockfile() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let kernel = Kernel::new(&home.db_path()).await.expect("kernel");
        let stale = kernel
            .install_skill(SkillInstallRequest {
                source: "local:/tmp/stale-telegram".to_string(),
                alias: "telegram".to_string(),
                reference: Some("local".to_string()),
                hash: Some("stale-hash".to_string()),
                skill_md: Some(
                    "---\nname: stale-telegram\ndescription: stale telegram\n---\n".to_string(),
                ),
                snapshot_path: None,
            })
            .await
            .expect("install stale skill");
        let skill_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "telegram");
        let config = OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "telegram".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");
        OperatorLockfile::default()
            .save(&home)
            .await
            .expect("save empty lockfile");

        let applied = apply(&home).await.expect("apply should converge");
        let enabled = kernel.list_skills().await.expect("list skills").skills;

        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].alias, "telegram");
        assert_eq!(enabled[0].skill_id, applied.lockfile.skills[0].skill_id);
        assert_ne!(enabled[0].skill_id, stale.skill_id);
    }

    #[tokio::test]
    async fn apply_rejects_duplicate_skill_snapshots() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let skill_source = write_channel_skill_fixture(temp_dir.path(), "channel-terminal", "test");
        let source = skill_source.to_string_lossy().to_string();
        let config = OperatorConfig {
            skills: vec![
                ManagedSkillConfig {
                    alias: "alpha".to_string(),
                    source: source.clone(),
                    reference: "local".to_string(),
                },
                ManagedSkillConfig {
                    alias: "beta".to_string(),
                    source,
                    reference: "other-ref".to_string(),
                },
            ],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");

        let err = apply(&home)
            .await
            .expect_err("duplicate skill snapshots should be rejected");
        assert!(
            err.to_string()
                .contains("configured under both 'alpha' and 'beta'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn apply_rejects_duplicate_skill_aliases() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let terminal_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-terminal", "terminal");
        let telegram_source =
            write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "telegram");
        let config = OperatorConfig {
            skills: vec![
                ManagedSkillConfig {
                    alias: "shared".to_string(),
                    source: terminal_source.to_string_lossy().to_string(),
                    reference: "local".to_string(),
                },
                ManagedSkillConfig {
                    alias: "shared".to_string(),
                    source: telegram_source.to_string_lossy().to_string(),
                    reference: "local".to_string(),
                },
            ],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");

        let err = apply(&home)
            .await
            .expect_err("duplicate aliases should be rejected");
        assert!(
            err.to_string()
                .contains("skill alias 'shared' is configured more than once"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn apply_disables_stale_channel_even_when_old_skill_is_disabled() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let skill_source = write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "test");

        let mut config = OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "telegram".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            channels: vec![ManagedChannelConfig {
                id: "telegram".to_string(),
                skill: "telegram".to_string(),
                enabled: true,
                launch_mode: ChannelLaunchMode::Service,
                required_env: Vec::new(),
            }],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");
        let _applied = apply(&home).await.expect("apply channel");

        let kernel = Kernel::new_with_options(
            &home.db_path(),
            KernelOptions {
                default_runtime_id: None,
                workspace_root: Some(home.workspace_dir("main")),
                project_workspace_root: Some(home.workspace_dir("main")),
                ..KernelOptions::default()
            },
        )
        .await
        .expect("kernel");
        config.skills.clear();
        config.channels.clear();
        config.save(&home).await.expect("save empty config");
        apply(&home).await.expect("apply stale cleanup");

        let binding = kernel
            .get_channel_binding("telegram")
            .await
            .expect("load binding")
            .expect("binding remains for audit history");
        assert!(!binding.enabled, "stale channel binding should be disabled");
    }

    #[tokio::test]
    async fn apply_tolerates_stale_lockfile_skill_missing_from_kernel_db() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let skill_source = write_channel_skill_fixture(temp_dir.path(), "channel-telegram", "test");

        let mut config = OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "telegram".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
            }],
            channels: vec![ManagedChannelConfig {
                id: "telegram".to_string(),
                skill: "telegram".to_string(),
                enabled: true,
                launch_mode: ChannelLaunchMode::Service,
                required_env: Vec::new(),
            }],
            ..OperatorConfig::default()
        };
        config.save(&home).await.expect("save config");
        let applied = apply(&home).await.expect("apply channel");
        assert_eq!(applied.lockfile.skills.len(), 1);

        tokio::fs::remove_file(home.db_path())
            .await
            .expect("remove kernel db");

        config.skills.clear();
        config.channels.clear();
        config.save(&home).await.expect("save empty config");

        apply(&home).await.expect("apply stale lockfile cleanup");

        let lockfile = OperatorLockfile::load(&home).await.expect("load lockfile");
        assert!(
            lockfile.skills.is_empty(),
            "lockfile should converge after stale skill cleanup"
        );
        assert!(
            lockfile.channels.is_empty(),
            "lockfile should converge after stale channel cleanup"
        );
    }

    #[tokio::test]
    async fn up_skips_interactive_channels_for_service_units() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&runtime_stub, fs::Permissions::from_mode(0o755))
                .expect("chmod runtime stub");
        }

        let skill_source = temp_dir.path().join("channel-terminal");
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-terminal\ndescription: test\n---\n",
        )
        .expect("skill md");
        fs::write(skill_source.join("scripts/worker"), "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&skill_source.join("scripts/worker"));

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config.skills = vec![ManagedSkillConfig {
            alias: "terminal".to_string(),
            source: skill_source.to_string_lossy().to_string(),
            reference: "local".to_string(),
        }];
        config.channels = vec![ManagedChannelConfig {
            id: "terminal".to_string(),
            skill: "terminal".to_string(),
            enabled: true,
            launch_mode: ChannelLaunchMode::Interactive,
            required_env: Vec::new(),
        }];
        config.save(&home).await.expect("save config");
        write_test_codex_auth(&home).await;
        OperatorLockfile::default()
            .save(&home)
            .await
            .expect("save lock");

        let manager = FakeServiceManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        let applied = up(&home, &manager, "codex", &binaries).await.expect("up");

        assert_eq!(applied.lockfile.channels.len(), 1);
        assert_eq!(
            manager
                .unit_status("lionclaw-channel-terminal.service")
                .await
                .expect("unit status"),
            "not-found"
        );
    }

    #[tokio::test]
    async fn up_rejects_foreign_home_daemon_on_configured_bind() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let bind_addr = OperatorConfig::load(&home)
            .await
            .expect("load config")
            .daemon
            .bind;
        let config_fingerprint = current_daemon_config_fingerprint(&home).await;
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
                            config_fingerprint: config_fingerprint.clone(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeServiceManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        let err = up(&home, &manager, "codex", &binaries)
            .await
            .expect_err("foreign daemon should fail");
        assert!(err.to_string().contains("/tmp/foreign-home"));
    }

    #[tokio::test]
    async fn up_rejects_same_home_foreground_daemon_without_active_unit() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind;
        let home_root = home.root().display().to_string();
        let config_fingerprint = current_daemon_config_fingerprint(&home).await;
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_id = home_id.clone();
                    let home_root = home_root.clone();
                    let project_scope = current_project_scope();
                    move || async move {
                        Json(DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            config_fingerprint: config_fingerprint.clone(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeServiceManager::default();
        manager
            .set_unit_status(DAEMON_UNIT_NAME, "loaded/inactive/dead")
            .expect("set unit status");
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        let err = up(&home, &manager, "codex", &binaries)
            .await
            .expect_err("foreground daemon without active unit should fail");
        assert!(err.to_string().contains("foreground daemon"));
    }

    #[tokio::test]
    async fn up_rejects_same_home_different_project_daemon() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind;
        let home_root = home.root().display().to_string();
        let config_fingerprint = current_daemon_config_fingerprint(&home).await;
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
                            config_fingerprint: config_fingerprint.clone(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeServiceManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        let err = up(&home, &manager, "codex", &binaries)
            .await
            .expect_err("same-home different-project daemon should fail");
        assert!(err.to_string().contains("different project"));
    }

    #[tokio::test]
    async fn up_reuses_same_home_daemon_when_managed_unit_is_active() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&runtime_stub, fs::Permissions::from_mode(0o755))
                .expect("chmod runtime stub");
        }
        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config.save(&home).await.expect("save config");
        write_test_codex_auth(&home).await;

        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let home_root = home.root().display().to_string();
        let config_fingerprint = current_daemon_config_fingerprint(&home).await;
        let _server = spawn_probe_server(
            Router::new().route(
                "/v0/daemon/info",
                get({
                    let bind_addr = bind_addr.clone();
                    let home_id = home_id.clone();
                    let home_root = home_root.clone();
                    let project_scope = current_project_scope();
                    move || async move {
                        Json(DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            config_fingerprint: config_fingerprint.clone(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeServiceManager::default();
        manager
            .set_unit_status(DAEMON_UNIT_NAME, "loaded/active/running")
            .expect("set unit status");
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        up(&home, &manager, "codex", &binaries)
            .await
            .expect("same-home managed daemon should be reused");
        assert!(
            manager
                .was_restarted(DAEMON_UNIT_NAME)
                .expect("read restart state"),
            "changed active daemon unit should be restarted"
        );
    }

    #[tokio::test]
    async fn up_restarts_same_home_daemon_when_config_is_stale() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let mut config = onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&runtime_stub, fs::Permissions::from_mode(0o755))
                .expect("chmod runtime stub");
        }
        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config.save(&home).await.expect("save config");
        write_test_codex_auth(&home).await;

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
                    let project_scope = current_project_scope();
                    move || async move {
                        Json(DaemonInfoResponse {
                            service: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            config_fingerprint: "daemon-stale-config".to_string(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeServiceManager::default();
        manager
            .set_unit_status(DAEMON_UNIT_NAME, "loaded/active/running")
            .expect("set unit status");
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        up(&home, &manager, "codex", &binaries)
            .await
            .expect("stale managed daemon should be reconciled");
        assert!(
            manager
                .was_restarted(DAEMON_UNIT_NAME)
                .expect("read restart state"),
            "managed daemon should be restarted when daemon config fingerprint changes"
        );
    }

    #[tokio::test]
    async fn up_rejects_unknown_listener_on_bind() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, Some(OnboardBindSelection::Auto))
            .await
            .expect("onboard");
        let bind_addr = OperatorConfig::load(&home)
            .await
            .expect("load config")
            .daemon
            .bind;
        let _server = spawn_probe_server(
            Router::new().route(
                "/health",
                get(|| async { Json(json!({"service": "other", "status": "ok"})) }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeServiceManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        let err = up(&home, &manager, "codex", &binaries)
            .await
            .expect_err("unknown listener should fail");
        assert!(err.to_string().contains("non-LionClaw listener"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn status_ignores_unselected_runtime_image_probe_failures() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home, None).await.expect("onboard");

        let broken_podman = temp_dir.path().join("podman");
        fs::write(
            &broken_podman,
            "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  echo 'storage denied' >&2\n  exit 1\nfi\nexit 0\n",
        )
        .expect("write broken podman");
        fs::set_permissions(&broken_podman, fs::Permissions::from_mode(0o755))
            .expect("chmod broken podman");

        let healthy_runtime = temp_dir.path().join("opencode-stub.sh");
        fs::write(&healthy_runtime, "#!/usr/bin/env bash\ncat >/dev/null\n")
            .expect("write runtime stub");
        fs::set_permissions(&healthy_runtime, fs::Permissions::from_mode(0o755))
            .expect("chmod runtime stub");

        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.upsert_runtime(
            "codex".to_string(),
            RuntimeProfileConfig::Codex {
                executable: "codex".to_string(),
                model: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: broken_podman.to_string_lossy().to_string(),
                    image: Some("ghcr.io/lionclaw/test-codex-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        config.upsert_runtime(
            "opencode".to_string(),
            RuntimeProfileConfig::OpenCode {
                executable: healthy_runtime.to_string_lossy().to_string(),
                model: None,
                agent: None,
                confinement: ConfinementConfig::Oci(OciConfinementConfig {
                    engine: ensure_fake_podman(&healthy_runtime)
                        .to_string_lossy()
                        .to_string(),
                    image: Some("ghcr.io/lionclaw/test-opencode-runtime:latest".to_string()),
                    ..OciConfinementConfig::default()
                }),
            },
        );
        config
            .set_default_runtime("opencode")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");
        OperatorLockfile::default()
            .save(&home)
            .await
            .expect("save lockfile");

        let manager = FakeServiceManager::default();
        let stack = status(&home, &manager).await.expect("status");

        assert_eq!(stack.daemon_status, "not-found");
        assert!(stack.channels.is_empty());
    }
}
