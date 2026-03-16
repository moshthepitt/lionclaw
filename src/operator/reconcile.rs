use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use serde_json::json;

use crate::{
    contracts::{ChannelBindRequest, ChannelPeerApproveRequest, ChannelPeerResponse, TrustTier},
    home::LionClawHome,
    kernel::{Kernel, KernelOptions},
    operator::{
        config::{
            normalize_local_source, ManagedChannelConfig, ManagedSkillConfig, OperatorConfig,
        },
        lockfile::{LockedChannel, LockedSkill, OperatorLockfile},
        services::{
            channel_unit_name, render_channel_unit, render_daemon_unit, ChannelServiceSpec,
            ManagedServiceUnit, ServiceManager, DAEMON_UNIT_NAME,
        },
        snapshot::{install_snapshot, InstalledSnapshot},
    },
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

pub async fn onboard(home: &LionClawHome) -> Result<OperatorConfig> {
    home.ensure_base_dirs().await?;
    let config = OperatorConfig::load(home).await?;
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
    let mut config = OperatorConfig::load(home).await?;
    let source = normalize_local_source(&source)?;
    config.upsert_skill(ManagedSkillConfig {
        alias,
        source,
        reference,
        enabled: true,
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
    required_env: Vec<String>,
) -> Result<()> {
    let mut config = OperatorConfig::load(home).await?;
    config.upsert_channel(ManagedChannelConfig {
        id,
        skill,
        enabled: true,
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

    let kernel = open_kernel(home, &config).await?;
    let mut next_lock = OperatorLockfile::default();
    let mut installed_skills = BTreeMap::new();

    for skill in &config.skills {
        let snapshot = install_snapshot(home, &skill.alias, &skill.source, &skill.reference)?;
        let installed = kernel
            .install_skill(crate::contracts::SkillInstallRequest {
                source: snapshot.source_uri.clone(),
                reference: Some(snapshot.reference.clone()),
                hash: Some(snapshot.hash.clone()),
                skill_md: Some(snapshot.skill_md.clone()),
                snapshot_path: Some(snapshot.snapshot_abs_dir.to_string_lossy().to_string()),
            })
            .await
            .map_err(to_anyhow)?;

        if skill.enabled {
            kernel
                .enable_skill(installed.skill_id.clone())
                .await
                .map_err(to_anyhow)?;
        } else {
            kernel
                .disable_skill(installed.skill_id.clone())
                .await
                .map_err(to_anyhow)?;
        }

        installed_skills.insert(
            skill.alias.clone(),
            (snapshot.clone(), installed.skill_id.clone()),
        );
        next_lock
            .skills
            .push(to_locked_skill(skill, snapshot, installed.skill_id));
    }

    let desired_channel_ids = config
        .channels
        .iter()
        .map(|channel| channel.id.as_str())
        .collect::<BTreeSet<_>>();

    for old_channel in &previous_lock.channels {
        if !desired_channel_ids.contains(old_channel.id.as_str()) {
            let _ = kernel
                .bind_channel(ChannelBindRequest {
                    channel_id: old_channel.id.clone(),
                    skill_id: old_channel.skill_id.clone(),
                    enabled: Some(false),
                    config: Some(json!({})),
                })
                .await;
        }
    }

    for channel in &config.channels {
        let (snapshot, skill_id) = installed_skills.get(&channel.skill).ok_or_else(|| {
            anyhow!(
                "channel '{}' references missing skill alias '{}'",
                channel.id,
                channel.skill
            )
        })?;

        if channel.enabled {
            let skill_config = config
                .skills
                .iter()
                .find(|skill| skill.alias == channel.skill)
                .ok_or_else(|| {
                    anyhow!("skill alias '{}' disappeared during apply", channel.skill)
                })?;
            if !skill_config.enabled {
                return Err(anyhow!(
                    "channel '{}' cannot be enabled while skill '{}' is disabled",
                    channel.id,
                    channel.skill
                ));
            }
        }

        let _ = snapshot;
        kernel
            .bind_channel(ChannelBindRequest {
                channel_id: channel.id.clone(),
                skill_id: skill_id.clone(),
                enabled: Some(channel.enabled),
                config: Some(json!({})),
            })
            .await
            .map_err(to_anyhow)?;

        next_lock.channels.push(LockedChannel {
            id: channel.id.clone(),
            skill: channel.skill.clone(),
            skill_id: skill_id.clone(),
            enabled: channel.enabled,
        });
    }

    let desired_skill_aliases = config
        .skills
        .iter()
        .map(|skill| skill.alias.as_str())
        .collect::<BTreeSet<_>>();
    for old_skill in &previous_lock.skills {
        if !desired_skill_aliases.contains(old_skill.alias.as_str()) {
            let _ = kernel.disable_skill(old_skill.skill_id.clone()).await;
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
    let applied = apply(home).await?;
    render_runtime_cache(home, &applied.config, &applied.lockfile, runtime_id).await?;
    let units = build_managed_units(
        home,
        &applied.config,
        &applied.lockfile,
        runtime_id,
        binaries,
    )?;
    let unit_names = units
        .iter()
        .map(|unit| unit.name.clone())
        .collect::<Vec<_>>();
    manager.apply_units(home, &units).await?;
    manager.up_units(&unit_names).await?;
    Ok(applied)
}

pub async fn down<M: ServiceManager>(home: &LionClawHome, manager: &M) -> Result<()> {
    let lockfile = OperatorLockfile::load(home).await?;
    let units = unit_names_from_lockfile(&lockfile);
    manager.down_units(&units).await
}

pub async fn status<M: ServiceManager>(home: &LionClawHome, manager: &M) -> Result<StackStatus> {
    let config = OperatorConfig::load(home).await?;
    let lockfile = OperatorLockfile::load(home).await?;
    let kernel = open_kernel(home, &config).await?;

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
        let unit_status = manager.unit_status(&channel_unit_name(&channel.id)).await?;
        channels.push(ChannelStatus {
            id: channel.id.clone(),
            skill: channel.skill.clone(),
            skill_id: channel.skill_id.clone(),
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
    let lockfile = OperatorLockfile::load(home).await?;
    let units = unit_names_from_lockfile(&lockfile);
    manager.logs(&units, lines).await
}

pub async fn pairing_list(
    home: &LionClawHome,
    channel_id: Option<String>,
) -> Result<Vec<crate::contracts::ChannelPeerView>> {
    let config = OperatorConfig::load(home).await?;
    let kernel = open_kernel(home, &config).await?;
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
    let kernel = open_kernel(home, &config).await?;
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
    let kernel = open_kernel(home, &config).await?;
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

fn build_managed_units(
    home: &LionClawHome,
    config: &OperatorConfig,
    lockfile: &OperatorLockfile,
    runtime_id: &str,
    binaries: &StackBinaryPaths,
) -> Result<Vec<ManagedServiceUnit>> {
    let mut units = Vec::new();
    units.push(render_daemon_unit(
        home,
        &binaries.daemon_bin,
        &config.daemon.bind,
        runtime_id,
    ));

    let base_url = format!("http://{}", config.daemon.bind);
    for channel in config.channels.iter().filter(|channel| channel.enabled) {
        let locked_skill = lockfile.find_skill(&channel.skill).ok_or_else(|| {
            anyhow!(
                "managed channel '{}' references unknown locked skill '{}'",
                channel.id,
                channel.skill
            )
        })?;
        let worker_path = home
            .root()
            .join(&locked_skill.snapshot_dir)
            .join("scripts/worker.sh");
        if !worker_path.exists() {
            return Err(anyhow!(
                "worker script is missing for channel '{}' at '{}'",
                channel.id,
                worker_path.display()
            ));
        }

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

async fn render_runtime_cache(
    home: &LionClawHome,
    config: &OperatorConfig,
    lockfile: &OperatorLockfile,
    runtime_id: &str,
) -> Result<()> {
    let target_dir = home.runtime_workspace_dir(runtime_id, &config.daemon.workspace);
    tokio::fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("failed to create {}", target_dir.display()))?;
    let target_path = target_dir.join(GENERATED_AGENTS_FILE);

    let mut sections = Vec::new();
    for (name, content) in read_workspace_sections(&config.workspace_root(home)).await? {
        sections.push(format!("## {}\n\n{}", name, content.trim()));
    }

    for skill in &lockfile.skills {
        let skill_md_path = home.root().join(&skill.snapshot_dir).join("SKILL.md");
        if tokio::fs::try_exists(&skill_md_path)
            .await
            .with_context(|| format!("failed to stat {}", skill_md_path.display()))?
        {
            let content = tokio::fs::read_to_string(&skill_md_path)
                .await
                .with_context(|| format!("failed to read {}", skill_md_path.display()))?;
            sections.push(format!(
                "## Skill {} ({})\n\n{}",
                skill.alias,
                skill.hash,
                content.trim()
            ));
        }
    }

    let rendered = render_marker_file(
        &format!(
            "# LionClaw Generated Agent Context\n\nThis file is generated for runtime '{}'.\n",
            runtime_id
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
        enabled: config.enabled,
    }
}

fn unit_names_from_lockfile(lockfile: &OperatorLockfile) -> Vec<String> {
    let mut units = vec![DAEMON_UNIT_NAME.to_string()];
    for channel in &lockfile.channels {
        units.push(channel_unit_name(&channel.id));
    }
    units
}

async fn open_kernel(home: &LionClawHome, config: &OperatorConfig) -> Result<Kernel> {
    Kernel::new_with_options(
        &home.db_path(),
        KernelOptions {
            default_runtime_id: None,
            workspace_root: Some(config.workspace_root(home)),
            ..KernelOptions::default()
        },
    )
    .await
}

fn to_anyhow(err: crate::kernel::KernelError) -> anyhow::Error {
    anyhow!(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{onboard, render_marker_file, up, ApplyResult, StackBinaryPaths};
    use crate::{
        home::LionClawHome,
        operator::{
            config::{ManagedChannelConfig, ManagedSkillConfig, OperatorConfig},
            lockfile::OperatorLockfile,
            services::FakeServiceManager,
        },
    };

    #[tokio::test]
    async fn onboard_bootstraps_workspace_and_config() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        let config = onboard(&home).await.expect("onboard");

        assert_eq!(config.daemon.workspace, "main");
        assert!(home.config_path().exists());
        assert!(home.workspace_dir("main").join("SOUL.md").exists());
    }

    #[test]
    fn marker_file_is_deterministic() {
        let rendered = render_marker_file("# Header", "body");
        assert_eq!(
            rendered,
            "# Header\n<!-- LIONCLAW:START -->\nbody\n<!-- LIONCLAW:END -->\n"
        );
    }

    #[tokio::test]
    async fn up_with_fake_manager_materializes_units() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        onboard(&home).await.expect("onboard");

        let skill_source = temp_dir.path().join("channel-telegram");
        fs::create_dir_all(skill_source.join("scripts")).expect("skill dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-telegram\ndescription: test\n---\n",
        )
        .expect("skill md");
        fs::write(
            skill_source.join("scripts/worker.sh"),
            "#!/usr/bin/env bash\n",
        )
        .expect("worker");

        let config = OperatorConfig {
            skills: vec![ManagedSkillConfig {
                alias: "telegram".to_string(),
                source: skill_source.to_string_lossy().to_string(),
                reference: "local".to_string(),
                enabled: true,
            }],
            channels: vec![ManagedChannelConfig {
                id: "telegram".to_string(),
                skill: "telegram".to_string(),
                enabled: true,
                required_env: Vec::new(),
            }],
            ..OperatorConfig::default()
        };
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

        assert_eq!(applied.config.channels.len(), 1);
        assert!(home
            .runtime_workspace_dir("codex", "main")
            .join("AGENTS.generated.md")
            .exists());
    }
}
