use anyhow::{anyhow, Context, Result};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};
use uuid::Uuid;

use crate::{
    applied::{compute_daemon_fingerprint, AppliedState},
    contracts::{
        ChannelGrantResponse, ChannelGrantRevokeRequest, ChannelGrantRevokeResponse,
        ChannelPairingApproveRequest, ChannelPairingBlockRequest, ChannelPairingBlockResponse,
        ChannelPairingInviteRequest, ChannelPairingInviteResponse, ChannelPairingListResponse,
        ChannelPairingStatus, ChannelRoutingProfile, TrustTier,
    },
    home::{runtime_project_partition_key, LionClawHome},
    kernel::{skills::validate_skill_alias, Kernel, KernelOptions, RuntimeExecutionPolicy},
    operator::{
        channel_metadata::resolve_channel_worker_entrypoint,
        config::{normalize_local_source, ChannelLaunchMode, ManagedChannelConfig, OperatorConfig},
        daemon_probe::{classify_daemon, DaemonClassification},
        managed_units::{
            daemon_unit_name, ensure_unit_identity, render_channel_unit, render_daemon_unit,
            unit_status_is_active, ChannelUnitSpec, DaemonUnitSpec, ManagedUnit, UnitIdentity,
            UnitManager,
        },
        redaction::SecretRedactor,
        runtime::{
            register_configured_runtimes, resolve_runtime_execution_context,
            validate_runtime_launch_prerequisites_for_work_root,
        },
        snapshot::{install_snapshot, resolve_local_source},
    },
    runtime_timeouts::RuntimeTurnTimeouts,
    workspace::{bootstrap_workspace, read_workspace_sections, GENERATED_AGENTS_FILE},
};

#[cfg(test)]
use crate::config::resolve_project_workspace_root;

#[derive(Debug, Clone)]
pub struct OperatorState {
    pub config: OperatorConfig,
    pub applied_state: AppliedState,
}

#[derive(Debug, Clone)]
pub struct StackBinaryPaths {
    pub daemon_bin: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ManagedDaemonContext<'a> {
    pub work_root: &'a Path,
    pub fingerprint: &'a str,
    pub codex_home_override: Option<&'a Path>,
}

pub async fn add_skill(
    home: &LionClawHome,
    alias: String,
    source: String,
    reference: String,
) -> Result<()> {
    validate_skill_alias(&alias)?;
    let source = normalize_local_source(&source)?;
    let config = OperatorConfig::load(home).await?;
    if config.channels.iter().any(|channel| channel.skill == alias) {
        let source_path = resolve_local_source(&source)?;
        resolve_worker_entrypoint(&source_path).with_context(|| {
            format!(
                "skill alias '{alias}' backs a configured channel and must keep a valid 'scripts/worker'"
            )
        })?;
    }
    home.ensure_base_dirs().await?;
    install_snapshot(home, &alias, &source, &reference)?;
    Ok(())
}

pub async fn remove_skill(home: &LionClawHome, alias: &str) -> Result<bool> {
    validate_skill_alias(alias)?;
    let config = OperatorConfig::load(home).await?;
    if let Some(channel) = config
        .channels
        .iter()
        .find(|channel| channel.skill == alias)
    {
        return Err(anyhow!(
            "skill alias '{}' is in use by channel '{}'; remove the channel first with 'lionclaw channel remove {}'",
            alias,
            channel.id,
            channel.id
        ));
    }

    let Some(skills_root) = existing_canonical_skills_root(home)? else {
        return Ok(false);
    };
    let snapshot_root = skills_root.join(alias);
    let metadata = match tokio::fs::symlink_metadata(&snapshot_root).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to stat installed skill {}", snapshot_root.display())
            });
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "installed skill '{}' must not be a symlink",
            snapshot_root.display()
        ));
    }
    if !metadata.is_dir() {
        return Err(anyhow!(
            "installed skill '{}' is not a directory",
            snapshot_root.display()
        ));
    }

    match tokio::fs::remove_dir_all(&snapshot_root).await {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => {
            Err(err).with_context(|| format!("failed to remove {}", snapshot_root.display()))
        }
    }
}

pub async fn add_channel(
    home: &LionClawHome,
    id: String,
    skill: String,
    launch_mode: ChannelLaunchMode,
    required_env: Vec<String>,
) -> Result<()> {
    add_channel_with_worker(
        home,
        id,
        skill,
        launch_mode,
        crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
        required_env,
    )
    .await
}

pub async fn add_channel_with_worker(
    home: &LionClawHome,
    id: String,
    skill: String,
    launch_mode: ChannelLaunchMode,
    worker: String,
    required_env: Vec<String>,
) -> Result<()> {
    validate_skill_alias(&skill)?;
    resolve_installed_skill_worker_entrypoint(home, &skill, Some(&worker)).await?;
    let mut config = OperatorConfig::load(home).await?;
    config.upsert_channel(ManagedChannelConfig {
        id,
        skill,
        launch_mode,
        worker,
        required_env,
    });
    config.save(home).await
}

pub async fn remove_channel(home: &LionClawHome, id: &str) -> Result<bool> {
    let mut config = OperatorConfig::load(home).await?;
    let removed = config.remove_channel(id);
    if !removed {
        return Ok(false);
    }
    config.save(home).await?;
    Ok(removed)
}

pub async fn load_operator_state(home: &LionClawHome) -> Result<OperatorState> {
    home.ensure_base_dirs().await?;
    let config = OperatorConfig::load(home).await?;
    bootstrap_workspace(&config.workspace_root(home)).await?;
    let applied_state = AppliedState::load(home).await?;
    Ok(OperatorState {
        config,
        applied_state,
    })
}

pub async fn up_for_work_root<M: UnitManager>(
    home: &LionClawHome,
    manager: &M,
    runtime_id: &str,
    binaries: &StackBinaryPaths,
    project_root: Option<&Path>,
    work_root: &Path,
) -> Result<OperatorState> {
    let mut state = load_operator_state(home).await?;
    ensure_managed_bind_configured(home, &mut state.config).await?;
    let config = &state.config;
    let unit_identity = ensure_unit_identity(home)?;
    let runtime_context = resolve_runtime_execution_context(home, config, Some(runtime_id)).await?;
    let home_id = home.ensure_home_id().await?;
    let project_scope = runtime_project_partition_key(Some(work_root));
    let runtime_config_fingerprint = runtime_context.daemon_config_fingerprint.clone();
    let expected_daemon_fingerprint =
        compute_daemon_fingerprint(&runtime_config_fingerprint, &state.applied_state);
    match classify_daemon(
        &config.daemon.bind,
        &home_id,
        &project_scope,
        &expected_daemon_fingerprint,
    )
    .await?
    {
        DaemonClassification::Absent => {}
        DaemonClassification::SameHome | DaemonClassification::SameHomeDifferentConfig => {
            let daemon_unit = daemon_unit_name(&unit_identity);
            let daemon_status = manager.unit_status(&daemon_unit).await?;
            if !unit_status_is_active(&daemon_status) {
                return Err(anyhow!(
                    "bind '{}' is already served by this LionClaw home, but not by the managed {} unit; stop the foreground daemon before running 'lionclaw up'",
                    config.daemon.bind,
                    daemon_unit
                ));
            }
        }
        DaemonClassification::SameHomeDifferentProject => {
            return Err(anyhow!(
                "bind '{}' is already served by this LionClaw home for a different project; stop that daemon before running 'lionclaw up' from this project",
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
                "bind '{}' is already served by an older LionClaw daemon; restart that daemon before running 'lionclaw up'",
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

    let previous_units = manager.owned_units(home)?.names();
    validate_runtime_launch_prerequisites_for_work_root(
        home,
        config,
        runtime_id,
        project_root,
        Some(work_root),
    )
    .await?;
    render_runtime_cache_for_work_root(home, &state.config, runtime_id, work_root).await?;
    let units = build_managed_units(
        home,
        &state.config,
        &state.applied_state,
        runtime_id,
        binaries,
        &unit_identity,
        ManagedDaemonContext {
            work_root,
            fingerprint: &expected_daemon_fingerprint,
            codex_home_override: runtime_context.codex_home_override.as_deref(),
        },
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
        if let Err(err) = manager.up_units(&units_to_start).await {
            if let Err(cleanup_err) = manager.down_units(&units_to_start).await {
                return Err(anyhow!(
                    "{err}; additionally failed to stop partially started units: {cleanup_err}"
                ));
            }
            return Err(err);
        }
    }
    if !units_to_restart.is_empty() {
        manager.restart_units(&units_to_restart).await?;
    }
    Ok(state)
}

fn allocate_auto_bind() -> Result<String> {
    static ALLOCATED_AUTO_BIND_PORTS: OnceLock<Mutex<BTreeSet<u16>>> = OnceLock::new();

    let allocated_ports = ALLOCATED_AUTO_BIND_PORTS.get_or_init(|| Mutex::new(BTreeSet::new()));
    for _ in 0..64 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .context("failed to allocate an automatic loopback bind")?;
        let port = listener
            .local_addr()
            .context("failed to read automatic bind address")?
            .port();
        let mut allocated_ports = allocated_ports
            .lock()
            .map_err(|_| anyhow!("automatic bind port registry lock poisoned"))?;
        if allocated_ports.insert(port) {
            return Ok(format!("127.0.0.1:{port}"));
        }
    }

    Err(anyhow!(
        "failed to allocate a unique automatic loopback bind after repeated attempts"
    ))
}

async fn ensure_managed_bind_configured(
    home: &LionClawHome,
    config: &mut OperatorConfig,
) -> Result<()> {
    if config.daemon.bind_configured {
        return Ok(());
    }
    config.daemon.bind = allocate_auto_bind()?;
    config.daemon.bind_configured = true;
    config.save(home).await
}

pub async fn down<M: UnitManager>(home: &LionClawHome, manager: &M) -> Result<()> {
    let units = manager.owned_units(home)?.names();
    manager.down_units(&units).await
}

pub async fn logs<M: UnitManager>(
    home: &LionClawHome,
    manager: &M,
    lines: usize,
) -> Result<String> {
    let units = manager.owned_units(home)?.names();
    if units.is_empty() {
        return Ok(String::new());
    }

    let redactor = SecretRedactor::from_home(home)?;
    match manager.logs(&units, lines).await {
        Ok(output) => Ok(redactor.redact(&output)),
        Err(err) => Err(anyhow!(redactor.redact(&format!("{err:#}")))),
    }
}

pub async fn pairing_list(
    home: &LionClawHome,
    channel_id: Option<String>,
    status: Option<ChannelPairingStatus>,
) -> Result<ChannelPairingListResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    kernel
        .list_channel_pairings(channel_id, status)
        .await
        .map_err(to_anyhow)
}

pub async fn pairing_approve(
    home: &LionClawHome,
    channel_id: String,
    pairing: String,
    routing_profile: Option<ChannelRoutingProfile>,
    trust_tier: TrustTier,
    label: Option<String>,
) -> Result<ChannelGrantResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    let (pairing_id, pairing_code) = pairing_approve_lookup(pairing);
    kernel
        .approve_channel_pairing(ChannelPairingApproveRequest {
            channel_id,
            pairing_id,
            pairing_code,
            routing_profile,
            trust_tier: Some(trust_tier),
            label,
        })
        .await
        .map_err(to_anyhow)
}

pub async fn pairing_invite(
    home: &LionClawHome,
    req: ChannelPairingInviteRequest,
) -> Result<ChannelPairingInviteResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    kernel.invite_channel_pairing(req).await.map_err(to_anyhow)
}

pub async fn pairing_block(
    home: &LionClawHome,
    channel_id: String,
    sender_ref: Option<String>,
    pairing_id: Option<Uuid>,
    conversation_ref: Option<String>,
    thread_ref: Option<String>,
    reason: Option<String>,
) -> Result<ChannelPairingBlockResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    kernel
        .block_channel_pairing(ChannelPairingBlockRequest {
            channel_id,
            pairing_id,
            sender_ref: non_empty_trimmed(sender_ref),
            conversation_ref,
            thread_ref,
            reason,
        })
        .await
        .map_err(to_anyhow)
}

pub async fn pairing_revoke(
    home: &LionClawHome,
    channel_id: String,
    grant_id: Uuid,
    reason: Option<String>,
) -> Result<ChannelGrantRevokeResponse> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config, None).await?;
    kernel
        .revoke_channel_grant(ChannelGrantRevokeRequest {
            channel_id,
            grant_id,
            reason,
        })
        .await
        .map_err(to_anyhow)
}

fn pairing_approve_lookup(raw: String) -> (Option<Uuid>, Option<String>) {
    let trimmed = raw.trim();
    match Uuid::parse_str(trimmed) {
        Ok(pairing_id) => (Some(pairing_id), None),
        Err(_) => (None, Some(trimmed.to_string())),
    }
}

fn non_empty_trimmed(raw: Option<String>) -> Option<String> {
    raw.map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn to_anyhow(err: crate::kernel::KernelError) -> anyhow::Error {
    anyhow!(err.to_string())
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
    applied_state: &AppliedState,
    runtime_id: &str,
    binaries: &StackBinaryPaths,
    unit_identity: &UnitIdentity,
    daemon_context: ManagedDaemonContext<'_>,
) -> Result<Vec<ManagedUnit>> {
    let mut units = Vec::new();
    units.push(render_daemon_unit(
        home,
        unit_identity,
        &binaries.daemon_bin,
        DaemonUnitSpec {
            bind_addr: &config.daemon.bind,
            runtime_id,
            workspace: &config.daemon.workspace,
            project_workspace_root: daemon_context.work_root,
            daemon_fingerprint: daemon_context.fingerprint,
            codex_home_override: daemon_context.codex_home_override,
        },
    ));

    let base_url = base_url_from_bind(&config.daemon.bind);
    for channel in applied_state
        .channels()
        .iter()
        .filter(|channel| channel.launch_mode == ChannelLaunchMode::Background)
    {
        let worker_path = resolve_applied_skill_worker_entrypoint(
            applied_state,
            &channel.skill_alias,
            Some(&channel.worker),
        )
        .with_context(|| format!("channel '{}' worker resolution failed", channel.id))?;

        let env = vec![
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
        let channel_env_path = if channel.required_env.is_empty() {
            None
        } else {
            validate_required_channel_env(home, &channel.id, &channel.required_env)?;
            Some(home.channel_env_path(&channel.id))
        };

        units.push(render_channel_unit(
            home,
            unit_identity,
            &ChannelUnitSpec {
                channel_id: channel.id.clone(),
                worker_path,
                env,
                channel_env_path,
            },
        ));
    }

    Ok(units)
}

pub(crate) fn resolve_required_channel_env(
    home: &LionClawHome,
    channel_id: &str,
    required_env: &[String],
) -> Result<Vec<(String, String)>> {
    crate::operator::channel_env::load_required_channel_env(home, channel_id, required_env)
}

pub(crate) fn validate_required_channel_env(
    home: &LionClawHome,
    channel_id: &str,
    required_env: &[String],
) -> Result<()> {
    crate::operator::channel_env::validate_channel_env_contract(home, channel_id, required_env)
}

#[cfg(test)]
pub(crate) async fn render_runtime_cache(
    home: &LionClawHome,
    config: &OperatorConfig,
    runtime_id: &str,
) -> Result<()> {
    let project_workspace_root =
        resolve_project_workspace_root().context("failed to resolve project workspace root")?;
    render_runtime_cache_for_work_root(home, config, runtime_id, &project_workspace_root).await
}

pub(crate) async fn render_runtime_cache_for_work_root(
    home: &LionClawHome,
    config: &OperatorConfig,
    runtime_id: &str,
    project_workspace_root: &Path,
) -> Result<()> {
    let workspace = &config.daemon.workspace;
    let target_dir = home.runtime_project_dir(runtime_id, workspace, project_workspace_root);
    for path in [
        target_dir.clone(),
        home.runtime_project_drafts_dir(runtime_id, workspace, project_workspace_root),
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

pub(crate) fn resolve_worker_entrypoint(snapshot_root: &Path) -> Result<PathBuf> {
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

pub(crate) fn resolve_applied_skill_worker_entrypoint(
    applied_state: &AppliedState,
    alias: &str,
    worker: Option<&str>,
) -> Result<PathBuf> {
    validate_skill_alias(alias)?;
    let applied_skill = applied_state
        .skill_by_alias(alias)
        .ok_or_else(|| anyhow!("installed skill alias '{alias}' not found"))?;
    resolve_channel_worker_entrypoint(&applied_skill.snapshot_path, worker)
}

pub(crate) async fn resolve_installed_skill_worker_entrypoint(
    home: &LionClawHome,
    alias: &str,
    worker: Option<&str>,
) -> Result<PathBuf> {
    validate_skill_alias(alias)?;
    let snapshot_root = resolve_installed_skill_dir(home, alias)?;
    resolve_channel_worker_entrypoint(&snapshot_root, worker)
}

fn existing_canonical_skills_root(home: &LionClawHome) -> Result<Option<PathBuf>> {
    match fs::symlink_metadata(home.skills_dir()) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(anyhow!(
                    "skills root '{}' must not be a symlink",
                    home.skills_dir().display()
                ));
            }
            if !metadata.is_dir() {
                return Err(anyhow!(
                    "skills root '{}' is not a directory",
                    home.skills_dir().display()
                ));
            }
            fs::canonicalize(home.skills_dir())
                .with_context(|| format!("failed to resolve {}", home.skills_dir().display()))
                .map(Some)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => {
            Err(err).with_context(|| format!("failed to stat {}", home.skills_dir().display()))
        }
    }
}

fn resolve_installed_skill_dir(home: &LionClawHome, alias: &str) -> Result<PathBuf> {
    validate_skill_alias(alias)?;
    let Some(skills_root) = existing_canonical_skills_root(home)? else {
        return Err(anyhow!("installed skill alias '{alias}' not found"));
    };
    let snapshot_root = skills_root.join(alias);
    let metadata = match fs::symlink_metadata(&snapshot_root) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(anyhow!("installed skill alias '{alias}' not found"));
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to stat installed skill {}", snapshot_root.display())
            });
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(anyhow!(
            "installed skill '{}' must not be a symlink",
            snapshot_root.display()
        ));
    }
    if !metadata.is_dir() {
        return Err(anyhow!(
            "installed skill '{}' is not a directory",
            snapshot_root.display()
        ));
    }

    let snapshot_root = fs::canonicalize(&snapshot_root)
        .with_context(|| format!("failed to resolve {}", snapshot_root.display()))?;
    if snapshot_root.parent() != Some(skills_root.as_path()) {
        return Err(anyhow!(
            "installed skill '{}' must stay directly under '{}'",
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

pub(crate) async fn open_kernel(
    home: &LionClawHome,
    config: &OperatorConfig,
    default_runtime_id: Option<String>,
) -> Result<Kernel> {
    open_kernel_with_project_root(home, config, default_runtime_id, None, None).await
}

pub(crate) async fn open_runtime_kernel_for_work_root(
    home: &LionClawHome,
    config: &OperatorConfig,
    default_runtime_id: Option<String>,
    work_root: &Path,
    timeout_override: Option<RuntimeTurnTimeouts>,
) -> Result<Kernel> {
    open_kernel_with_project_root(
        home,
        config,
        default_runtime_id,
        Some(work_root.to_path_buf()),
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
    home.ensure_base_dirs().await?;
    let workspace_root = config.workspace_root(home);
    let applied_state = AppliedState::load(home).await?;
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
            workspace_name: Some(config.daemon.workspace.clone()),
            applied_state,
            ..KernelOptions::default()
        },
    )
    .await?;
    register_configured_runtimes(&kernel, config).await?;
    Ok(kernel)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use super::{
        add_channel, add_skill, down, ensure_managed_bind_configured, logs, open_kernel,
        open_kernel_with_project_root, render_marker_file, render_runtime_cache,
        resolve_installed_skill_worker_entrypoint, resolve_required_channel_env,
        resolve_worker_entrypoint, up_for_work_root, StackBinaryPaths,
    };
    use crate::{
        applied::compute_daemon_fingerprint,
        config::resolve_project_workspace_root,
        contracts::DaemonInfoResponse,
        home::{runtime_project_partition_key, LionClawHome},
        kernel::runtime::{ConfinementConfig, OciConfinementConfig},
        operator::{
            channel_env::{merge_channel_env, ChannelEnv},
            config::{
                ChannelLaunchMode, ManagedChannelConfig, OperatorConfig, RuntimeProfileConfig,
            },
            managed_units::{
                channel_unit_name, daemon_unit_name, ensure_unit_identity, render_daemon_unit,
                DaemonUnitSpec, FakeUnitManager, UnitIdentity, UnitManager,
            },
            reconcile::load_operator_state,
            runtime::resolve_runtime_execution_context,
        },
        workspace::GENERATED_AGENTS_FILE,
    };
    use axum::{routing::get, Json, Router};

    async fn spawn_probe_server(app: Router, bind_addr: &str) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("bind probe server");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve probe app");
        })
    }

    fn current_work_root() -> PathBuf {
        resolve_project_workspace_root().expect("resolve project workspace root")
    }

    fn current_project_scope() -> String {
        let project_root = current_work_root();
        runtime_project_partition_key(Some(project_root.as_path()))
    }

    fn test_project_home(project_root: &Path) -> LionClawHome {
        let project = crate::operator::target::init_project(project_root).expect("init project");
        LionClawHome::new(project.instance.home)
    }

    async fn load_test_config(home: &LionClawHome) -> OperatorConfig {
        OperatorConfig::load(home).await.expect("load config")
    }

    async fn load_test_config_with_managed_bind(home: &LionClawHome) -> OperatorConfig {
        let mut config = OperatorConfig::load(home).await.expect("load config");
        ensure_managed_bind_configured(home, &mut config)
            .await
            .expect("configure managed bind");
        config
    }

    fn test_unit_identity(home: &LionClawHome) -> UnitIdentity {
        ensure_unit_identity(home).expect("unit identity")
    }

    fn test_daemon_unit_name(home: &LionClawHome) -> String {
        daemon_unit_name(&test_unit_identity(home))
    }

    async fn apply_test_daemon_unit(home: &LionClawHome, manager: &FakeUnitManager) -> String {
        let identity = test_unit_identity(home);
        let unit = render_daemon_unit(
            home,
            &identity,
            Path::new("/tmp/lionclawd"),
            DaemonUnitSpec {
                bind_addr: "127.0.0.1:8979",
                runtime_id: "codex",
                workspace: "main",
                project_workspace_root: Path::new("/tmp/project"),
                daemon_fingerprint: "test-fingerprint",
                codex_home_override: None,
            },
        );
        let name = unit.name.clone();
        manager
            .apply_units(home, &[unit])
            .await
            .expect("apply daemon unit");
        name
    }

    #[tokio::test]
    async fn down_ignores_unapplied_derived_unit_names() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let manager = FakeUnitManager::default();
        let daemon_unit = daemon_unit_name(&test_unit_identity(&home));
        manager
            .fail_down_unit(&daemon_unit)
            .expect("configure stop failure");

        down(&home, &manager)
            .await
            .expect("unowned down should be a no-op");
    }

    #[tokio::test]
    async fn down_reports_owned_unit_stop_failures() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let manager = FakeUnitManager::default();
        let daemon_unit = apply_test_daemon_unit(&home, &manager).await;
        manager
            .fail_down_unit(&daemon_unit)
            .expect("configure stop failure");

        let err = down(&home, &manager)
            .await
            .expect_err("down should report stop failure");

        assert!(err.to_string().contains("failed to stop 1 managed unit"));
        assert!(err.to_string().contains("configured unit stop failure"));
    }

    async fn current_daemon_fingerprint(home: &LionClawHome) -> String {
        let config = OperatorConfig::load(home).await.expect("load config");
        let state = load_operator_state(home)
            .await
            .expect("load operator state");
        let runtime_config_fingerprint =
            resolve_runtime_execution_context(home, &config, config.defaults.runtime.as_deref())
                .await
                .expect("resolve runtime context")
                .daemon_config_fingerprint;
        compute_daemon_fingerprint(&runtime_config_fingerprint, &state.applied_state)
    }

    fn write_skill_source(
        root: &Path,
        name: &str,
        description: &str,
        with_worker: bool,
    ) -> PathBuf {
        let skill_source = root.join(name);
        if skill_source.exists() {
            fs::remove_dir_all(&skill_source).expect("clear skill source");
        }
        fs::create_dir_all(&skill_source).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {description}\n---\n"),
        )
        .expect("skill md");
        if with_worker {
            let worker = skill_source.join("scripts/worker");
            fs::create_dir_all(worker.parent().expect("worker parent")).expect("scripts dir");
            fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
            make_executable(&worker);
        }
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
    async fn load_operator_state_bootstraps_instance_workspace() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let state = load_operator_state(&home)
            .await
            .expect("load operator state");

        assert_eq!(state.config.daemon.workspace, "main");
        assert!(home.home_id_path().exists());
        assert!(home.workspace_dir("main").join("SOUL.md").exists());
    }

    #[tokio::test]
    async fn managed_bind_allocation_persists_loopback_port() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let config = load_test_config_with_managed_bind(&home).await;

        assert!(config.daemon.bind.starts_with("127.0.0.1:"));
        assert_ne!(config.daemon.bind, "127.0.0.1:8979");

        let reloaded = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(reloaded.daemon.bind, config.daemon.bind);
    }

    #[tokio::test]
    async fn render_runtime_cache_includes_runtime_secret_guidance() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let config = load_test_config(&home).await;

        render_runtime_cache(&home, &config, "codex")
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
        let home = test_project_home(temp_dir.path());
        let config = load_test_config(&home).await;

        open_kernel(&home, &config, None)
            .await
            .expect("state kernel should open without a project root");
        open_kernel_with_project_root(&home, &config, None, None, None)
            .await
            .expect("state kernel helper should allow a missing project root");
    }

    #[tokio::test]
    async fn state_kernel_open_initializes_fresh_home_dirs() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = OperatorConfig::load(&home)
            .await
            .expect("load default config");

        open_kernel_with_project_root(&home, &config, None, None, None)
            .await
            .expect("state kernel helper should initialize a fresh home");

        assert!(home.skills_dir().is_dir());
        assert!(home.db_dir().is_dir());
    }

    #[test]
    fn resolve_required_channel_env_rejects_invalid_env_keys_without_panicking() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        for key in ["", "BAD=KEY", "BAD\0KEY"] {
            let result = std::panic::catch_unwind(|| {
                resolve_required_channel_env(&home, "loopback", &[key.to_string()])
            })
            .expect("invalid required_env key should not panic");

            let err = result.expect_err("invalid required_env key should fail");
            assert!(err.to_string().contains("environment variable name"));
        }
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
    fn worker_entrypoint_requires_scripts_worker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let snapshot_root = temp_dir.path().join("example");
        fs::create_dir_all(snapshot_root.join("scripts")).expect("scripts dir");
        fs::write(
            snapshot_root.join("scripts/worker.sh"),
            "#!/usr/bin/env bash\n",
        )
        .expect("worker");

        let err = resolve_worker_entrypoint(&snapshot_root).expect_err("should fail");
        assert!(err.to_string().contains("expected 'scripts/worker'"));
    }

    #[test]
    fn worker_entrypoint_rejects_directory_worker_path() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let snapshot_root = temp_dir.path().join("example");
        fs::create_dir_all(snapshot_root.join("scripts/worker")).expect("worker directory");

        let err = resolve_worker_entrypoint(&snapshot_root).expect_err("should fail");
        assert!(err.to_string().contains("expected 'scripts/worker'"));
    }

    #[cfg(unix)]
    #[test]
    fn worker_entrypoint_rejects_non_executable_worker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let snapshot_root = temp_dir.path().join("example");
        let worker = snapshot_root.join("scripts/worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("scripts dir");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");

        let err = resolve_worker_entrypoint(&snapshot_root).expect_err("should fail");
        assert!(err.to_string().contains("is not executable"));
    }

    #[cfg(unix)]
    #[test]
    fn worker_entrypoint_rejects_symlinked_worker() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let snapshot_root = temp_dir.path().join("example");
        let worker = snapshot_root.join("scripts/worker");
        fs::create_dir_all(worker.parent().expect("worker parent")).expect("scripts dir");
        let outside_worker = temp_dir.path().join("outside-worker");
        fs::write(&outside_worker, "#!/usr/bin/env bash\n").expect("outside worker");
        make_executable(&outside_worker);
        symlink(&outside_worker, &worker).expect("worker symlink");

        let err = resolve_worker_entrypoint(&snapshot_root).expect_err("should fail");
        assert!(err.to_string().contains("must not be a symlink"));
    }

    #[tokio::test]
    async fn installed_skill_worker_entrypoint_uses_alias_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let skill_source =
            write_skill_source(temp_dir.path(), "channel-telegram", "telegram", true);
        add_skill(
            &home,
            "telegram".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");

        let resolved = resolve_installed_skill_worker_entrypoint(&home, "telegram", None)
            .await
            .expect("resolve worker");
        assert_eq!(
            resolved,
            home.skills_dir()
                .join("telegram")
                .join("scripts/worker")
                .canonicalize()
                .expect("canonical worker")
        );
    }

    #[tokio::test]
    async fn add_skill_and_remove_skill_manage_installed_directory() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let skill_source = write_skill_source(temp_dir.path(), "test-skill", "test", false);

        add_skill(
            &home,
            "test-skill".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
        assert!(home.skills_dir().join("test-skill").is_dir());

        assert!(super::remove_skill(&home, "test-skill")
            .await
            .expect("remove skill"));
        assert!(!home.skills_dir().join("test-skill").exists());
    }

    #[tokio::test]
    async fn remove_skill_returns_false_when_alias_is_not_installed() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());

        assert!(!super::remove_skill(&home, "missing-skill")
            .await
            .expect("remove missing skill"));
    }

    #[tokio::test]
    async fn remove_skill_returns_false_before_home_exists() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        assert!(!super::remove_skill(&home, "missing-skill")
            .await
            .expect("remove missing skill"));
        assert!(
            !home.root().exists(),
            "missing-skill removal should not bootstrap home state"
        );
    }

    #[tokio::test]
    async fn remove_skill_rejects_channel_bound_alias() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let skill_source =
            write_skill_source(temp_dir.path(), "channel-telegram", "telegram", true);
        add_skill(
            &home,
            "telegram".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect("add channel");

        let err = super::remove_skill(&home, "telegram")
            .await
            .expect_err("channel-bound alias should fail");
        assert!(err.to_string().contains("remove the channel first"));
    }

    #[tokio::test]
    async fn add_channel_requires_installed_worker_skill() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let skill_source = write_skill_source(temp_dir.path(), "broken-skill", "broken", false);
        add_skill(
            &home,
            "broken".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");

        let err = add_channel(
            &home,
            "broken".to_string(),
            "broken".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect_err("workerless skill should fail");
        assert!(err.to_string().contains("expected 'scripts/worker'"));
    }

    #[tokio::test]
    async fn add_channel_reports_missing_installed_alias_before_home_exists() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        let err = add_channel(
            &home,
            "missing".to_string(),
            "missing".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect_err("missing alias should fail");
        assert!(err
            .to_string()
            .contains("installed skill alias 'missing' not found"));
        assert!(
            !home.root().exists(),
            "failing channel add should not bootstrap home state"
        );
    }

    #[tokio::test]
    async fn remove_channel_returns_false_before_home_exists() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));

        assert!(!super::remove_channel(&home, "missing")
            .await
            .expect("remove missing channel"));
        assert!(
            !home.root().exists(),
            "missing channel removal should not bootstrap home state"
        );
    }

    #[tokio::test]
    async fn add_skill_missing_source_does_not_bootstrap_home() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let missing = temp_dir.path().join("missing-skill");

        let err = add_skill(
            &home,
            "missing-skill".to_string(),
            missing.display().to_string(),
            "local".to_string(),
        )
        .await
        .expect_err("missing source should fail");
        let _ = err;
        assert!(
            !home.root().exists(),
            "failing skill install should not bootstrap home state"
        );
    }

    #[tokio::test]
    async fn add_skill_preserves_worker_requirements_for_channel_bound_aliases() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let good_skill = write_skill_source(temp_dir.path(), "channel-telegram", "telegram", true);
        let bad_skill = write_skill_source(temp_dir.path(), "broken-telegram", "telegram", false);

        add_skill(
            &home,
            "telegram".to_string(),
            good_skill.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install channel skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect("add channel");

        let err = add_skill(
            &home,
            "telegram".to_string(),
            bad_skill.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect_err("workerless replacement should fail");
        assert!(err
            .to_string()
            .contains("must keep a valid 'scripts/worker'"));
        assert!(home
            .skills_dir()
            .join("telegram")
            .join("scripts/worker")
            .exists());
    }

    #[tokio::test]
    async fn add_skill_can_repair_missing_channel_bound_snapshot() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let original = write_skill_source(
            temp_dir.path(),
            "channel-telegram-original",
            "telegram",
            true,
        );
        let repaired = write_skill_source(
            temp_dir.path(),
            "channel-telegram-repaired",
            "telegram",
            true,
        );

        add_skill(
            &home,
            "telegram".to_string(),
            original.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install original channel skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect("add channel");

        tokio::fs::remove_dir_all(home.skills_dir().join("telegram"))
            .await
            .expect("remove installed snapshot");

        add_skill(
            &home,
            "telegram".to_string(),
            repaired.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("repair missing channel skill");

        assert!(home
            .skills_dir()
            .join("telegram")
            .join("scripts/worker")
            .exists());
    }

    #[tokio::test]
    async fn add_skill_can_repair_corrupted_channel_bound_snapshot() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let original = write_skill_source(
            temp_dir.path(),
            "channel-telegram-original",
            "telegram",
            true,
        );
        let repaired = write_skill_source(
            temp_dir.path(),
            "channel-telegram-repaired",
            "telegram",
            true,
        );

        add_skill(
            &home,
            "telegram".to_string(),
            original.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install original channel skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect("add channel");

        tokio::fs::remove_file(home.skills_dir().join("telegram").join("SKILL.md"))
            .await
            .expect("remove installed SKILL.md");

        add_skill(
            &home,
            "telegram".to_string(),
            repaired.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("repair corrupted channel skill");

        assert!(home.skills_dir().join("telegram").join("SKILL.md").exists());
        assert!(home
            .skills_dir()
            .join("telegram")
            .join("scripts/worker")
            .exists());
    }

    #[tokio::test]
    async fn up_with_fake_manager_materializes_units() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        write_test_codex_auth(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        let skill_source = write_skill_source(temp_dir.path(), "channel-telegram", "test", true);

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");
        add_skill(
            &home,
            "telegram".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            Vec::new(),
        )
        .await
        .expect("add channel");

        let manager = FakeUnitManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        let state = up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect("up");
        let project_workspace_root =
            resolve_project_workspace_root().expect("resolve project workspace root");

        assert_eq!(state.applied_state.channels().len(), 1);
        assert!(home
            .runtime_project_dir("codex", "main", &project_workspace_root)
            .join("AGENTS.generated.md")
            .exists());
        assert!(home
            .runtime_project_drafts_dir("codex", "main", &project_workspace_root)
            .exists());
    }

    #[tokio::test]
    async fn background_channel_env_uses_private_channel_env_without_secret_copy() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        write_test_codex_auth(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        let skill_source = write_skill_source(temp_dir.path(), "channel-telegram", "test", true);

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");
        add_skill(
            &home,
            "telegram".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            vec!["TELEGRAM_BOT_TOKEN".to_string()],
        )
        .await
        .expect("add channel");
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        merge_channel_env(&home, "telegram", &env).expect("store channel env");

        let manager = FakeUnitManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect("up");
        let telegram_unit_name = channel_unit_name(&test_unit_identity(&home), "telegram");
        let unit = manager
            .managed_unit(&telegram_unit_name)
            .expect("managed unit lookup")
            .expect("telegram unit");

        assert!(!unit.env_content.contains("TELEGRAM_BOT_TOKEN"));
        assert!(!unit.env_content.contains("secret-token"));
        assert_eq!(
            unit.extra_env_files,
            vec![home.channel_env_path("telegram")]
        );
        assert!(unit
            .unit_content
            .contains(&format!("EnvironmentFile={}", unit.env_path.display())));
        assert!(unit.unit_content.contains(&format!(
            "EnvironmentFile={}",
            home.channel_env_path("telegram").display()
        )));
    }

    #[tokio::test]
    async fn background_channel_env_rejects_undeclared_stored_values() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        write_test_codex_auth(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        let skill_source = write_skill_source(temp_dir.path(), "channel-telegram", "test", true);

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");
        add_skill(
            &home,
            "telegram".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
        add_channel(
            &home,
            "telegram".to_string(),
            "telegram".to_string(),
            ChannelLaunchMode::Background,
            vec!["TELEGRAM_BOT_TOKEN".to_string()],
        )
        .await
        .expect("add channel");
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        env.insert("EXTRA_SECRET".to_string(), "do-not-expose".to_string());
        merge_channel_env(&home, "telegram", &env).expect("store channel env");

        let manager = FakeUnitManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        let err = up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect_err("undeclared env should fail");

        assert!(err.to_string().contains("EXTRA_SECRET"));
        assert!(!err.to_string().contains("do-not-expose"));
    }

    #[tokio::test]
    async fn managed_logs_redact_private_channel_env_values() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        merge_channel_env(&home, "telegram", &env).expect("store channel env");
        let manager = FakeUnitManager::default();
        apply_test_daemon_unit(&home, &manager).await;
        manager
            .set_logs("boot secret-token done")
            .expect("set logs");

        let output = logs(&home, &manager, 100).await.expect("logs");

        assert_eq!(output, "boot [REDACTED] done");
    }

    #[tokio::test]
    async fn managed_logs_without_unit_identity_returns_empty() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let manager = FakeUnitManager::default();
        manager
            .fail_logs("log manager should not be called")
            .expect("fail logs");

        let output = logs(&home, &manager, 100).await.expect("logs");

        assert_eq!(output, "");
    }

    #[tokio::test]
    async fn managed_logs_redact_manager_errors() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        merge_channel_env(&home, "telegram", &env).expect("store channel env");
        let manager = FakeUnitManager::default();
        apply_test_daemon_unit(&home, &manager).await;
        manager
            .fail_logs("worker stderr included secret-token")
            .expect("fail logs");

        let err = logs(&home, &manager, 100)
            .await
            .expect_err("logs should fail");

        assert!(err.to_string().contains("[REDACTED]"));
        assert!(!err.to_string().contains("secret-token"));
    }

    #[tokio::test]
    async fn managed_logs_redact_secrets_preserved_after_channel_remove() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("base dirs");
        let mut config = OperatorConfig::default();
        config.upsert_channel(ManagedChannelConfig {
            id: "telegram".to_string(),
            skill: "telegram".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: vec!["TELEGRAM_BOT_TOKEN".to_string()],
        });
        config.save(&home).await.expect("save config");
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        merge_channel_env(&home, "telegram", &env).expect("store channel env");
        assert!(super::remove_channel(&home, "telegram")
            .await
            .expect("remove channel"));
        let manager = FakeUnitManager::default();
        apply_test_daemon_unit(&home, &manager).await;
        manager
            .set_logs("old worker printed secret-token")
            .expect("set logs");

        let output = logs(&home, &manager, 100).await.expect("logs");

        assert_eq!(output, "old worker printed [REDACTED]");
    }

    #[tokio::test]
    async fn up_rejects_missing_codex_runtime_auth() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");

        let err = up_for_work_root(
            &home,
            &FakeUnitManager::default(),
            "codex",
            &StackBinaryPaths {
                daemon_bin: "/tmp/lionclawd".into(),
            },
            None,
            &current_work_root(),
        )
        .await
        .expect_err("missing runtime auth should fail");

        assert!(err.to_string().contains("codex login"));
        assert!(err.to_string().contains("auth.json"));
    }

    #[tokio::test]
    async fn up_rejects_unavailable_private_network() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        let broken_podman = temp_dir.path().join("podman");
        fs::write(
            &broken_podman,
            "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"exists\" ]; then\n  exit 0\nfi\nif [ \"${1:-}\" = \"run\" ]; then\n  cat >&2 <<'EOF'\nError: pasta failed with exit code 1:\nFailed to open() /dev/net/tun: No such device\nFailed to set up tap device in namespace\nEOF\n  exit 125\nfi\nexit 0\n",
        )
        .expect("write broken podman");
        make_executable(&broken_podman);
        write_test_codex_auth(&home).await;

        config.runtimes = [(
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
        )]
        .into_iter()
        .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");

        let err = up_for_work_root(
            &home,
            &FakeUnitManager::default(),
            "codex",
            &StackBinaryPaths {
                daemon_bin: "/tmp/lionclawd".into(),
            },
            None,
            &current_work_root(),
        )
        .await
        .expect_err("private-network failure should block up");

        assert!(err.to_string().contains("requires network-mode 'on'"));
        assert!(err
            .to_string()
            .contains("could not start a private network"));
        assert!(err.to_string().contains("/dev/net/tun"));
    }

    #[tokio::test]
    async fn up_skips_interactive_channels_for_background_units() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        let skill_source = write_skill_source(temp_dir.path(), "channel-fixture", "test", true);

        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");
        write_test_codex_auth(&home).await;
        add_skill(
            &home,
            "test-channel".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install skill");
        add_channel(
            &home,
            "test-channel".to_string(),
            "test-channel".to_string(),
            ChannelLaunchMode::Interactive,
            Vec::new(),
        )
        .await
        .expect("add channel");

        let manager = FakeUnitManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        let state = up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect("up");

        assert_eq!(state.applied_state.channels().len(), 1);
        assert_eq!(
            manager
                .unit_status("lionclaw-channel-test-channel.service")
                .await
                .expect("unit status"),
            "not-found"
        );
    }

    #[tokio::test]
    async fn up_reuses_same_home_daemon_when_managed_unit_is_active() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(&home).await.expect("save config");
        write_test_codex_auth(&home).await;

        let home_id = home.ensure_home_id().await.expect("home id");
        let bind_addr = config.daemon.bind.clone();
        let home_root = home.root().display().to_string();
        let daemon_fingerprint = current_daemon_fingerprint(&home).await;
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
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            daemon_fingerprint: daemon_fingerprint.clone(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;
        let manager = FakeUnitManager::default();
        let daemon_unit = test_daemon_unit_name(&home);
        manager
            .set_unit_status(&daemon_unit, "loaded/active/running")
            .expect("set unit status");
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };

        up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect("same-home managed daemon should be reused");
        assert!(manager
            .was_restarted(&daemon_unit)
            .expect("read restart state"));
    }

    #[tokio::test]
    async fn up_restarts_managed_daemon_when_installed_skills_change() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = test_project_home(temp_dir.path());
        let mut config = load_test_config_with_managed_bind(&home).await;
        let runtime_stub = temp_dir.path().join("codex-stub.sh");
        fs::write(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n").expect("runtime stub");
        make_executable(&runtime_stub);
        config.runtimes = [("codex".to_string(), test_codex_runtime(&runtime_stub))]
            .into_iter()
            .collect();
        config.save(&home).await.expect("save config");
        write_test_codex_auth(&home).await;

        let manager = FakeUnitManager::default();
        let binaries = StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        };
        let daemon_unit = test_daemon_unit_name(&home);
        up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect("initial up");
        assert!(
            !manager
                .was_restarted(&daemon_unit)
                .expect("read restart state"),
            "initial start should not count as a restart"
        );

        let old_daemon_fingerprint = current_daemon_fingerprint(&home).await;
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
                            daemon: "lionclawd".to_string(),
                            status: "ok".to_string(),
                            home_id: home_id.clone(),
                            home_root: home_root.clone(),
                            bind_addr: bind_addr.clone(),
                            project_scope: project_scope.clone(),
                            daemon_fingerprint: old_daemon_fingerprint.clone(),
                        })
                    }
                }),
            ),
            &bind_addr,
        )
        .await;

        let skill_source = write_skill_source(temp_dir.path(), "runtime-extra", "runtime", false);
        add_skill(
            &home,
            "runtime-extra".to_string(),
            skill_source.to_string_lossy().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install runtime-visible skill");

        up_for_work_root(
            &home,
            &manager,
            "codex",
            &binaries,
            None,
            &current_work_root(),
        )
        .await
        .expect("reconcile changed skills");
        assert!(
            manager
                .was_restarted(&daemon_unit)
                .expect("read restart state"),
            "daemon should restart after installed skill changes"
        );
    }
}
