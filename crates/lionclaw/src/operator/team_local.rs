use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::{
    applied::AppliedState,
    contracts::{ChannelGrantApproveRequest, ChannelRoutingProfile, TrustTier},
    home::LionClawHome,
    kernel::{runtime::EscapeClass, Kernel, KernelError, KernelOptions},
    operator::{
        channel_metadata::{bundled_channel_skill_dir, DEFAULT_CHANNEL_WORKER},
        config::{ChannelContactConfig, ChannelLaunchMode, ManagedChannelConfig, OperatorConfig},
        snapshot::{install_snapshot_with_overlays, SnapshotOverlay},
        target::list_project_instances,
    },
};

pub(crate) const CHANNEL_ID: &str = "team-local";
const PRESET_ID: &str = "team-local";
const WORKER_BIN_NAME: &str = "lionclaw-channel-team-local";
const WORKER_BIN_REL_PATH: &str = "runtime/team-local/bin/lionclaw-channel-team-local";
const CONTACT_REF_PREFIX: &str = "team-local:peer:";
const SENDER_REF_PREFIX: &str = "team-local:instance:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TeamLocalProjectSetup {
    pub instances: usize,
    pub grants_ensured: usize,
    pub grants_preserved: usize,
}

#[derive(Debug, Clone)]
struct ProjectMember {
    name: String,
    home: LionClawHome,
    home_id: String,
}

pub(crate) async fn ensure_project_team_local(
    project_root: &Path,
) -> Result<TeamLocalProjectSetup> {
    let worker_binary = worker_binary_path()?;
    ensure_project_team_local_with_worker(project_root, &worker_binary).await
}

pub(crate) async fn ensure_project_team_local_with_worker(
    project_root: &Path,
    worker_binary: &Path,
) -> Result<TeamLocalProjectSetup> {
    let instances = list_project_instances(project_root)?;
    let mut members = Vec::with_capacity(instances.len());
    let mut homes_by_id = BTreeMap::new();
    for instance in instances {
        let home = LionClawHome::new(instance.home);
        let home_id = ensure_instance_team_local_channel(&home, worker_binary)
            .await
            .with_context(|| {
                format!(
                    "failed to configure team-local for instance '{}'",
                    instance.name
                )
            })?;
        if let Some(existing_name) = homes_by_id.insert(home_id.clone(), instance.name.clone()) {
            anyhow::bail!(
                "project instances '{}' and '{}' share home id '{}'",
                existing_name,
                instance.name,
                home_id
            );
        }
        members.push(ProjectMember {
            name: instance.name,
            home,
            home_id,
        });
    }

    let mut grants_ensured = 0;
    let mut grants_preserved = 0;
    for recipient in &members {
        let config = OperatorConfig::load(&recipient.home).await?;
        let kernel = open_channel_admin_kernel(&recipient.home, &config).await?;
        for sender in &members {
            if sender.home_id == recipient.home_id {
                continue;
            }
            match approve_team_member_grant(&kernel, sender).await {
                Ok(()) => grants_ensured += 1,
                Err(KernelError::Conflict(reason))
                    if reason == "scope_blocked" || reason == "scope_revoked" =>
                {
                    grants_preserved += 1;
                }
                Err(err) => return Err(err).context("failed to approve team-local member grant"),
            }
        }
    }

    Ok(TeamLocalProjectSetup {
        instances: members.len(),
        grants_ensured,
        grants_preserved,
    })
}

pub(crate) fn snapshot_overlays_for_source(source_path: &Path) -> Result<Vec<SnapshotOverlay>> {
    if !is_bundled_team_local_source(source_path)? {
        return Ok(Vec::new());
    }
    Ok(vec![SnapshotOverlay::new(
        worker_binary_path()?,
        PathBuf::from(WORKER_BIN_REL_PATH),
    )])
}

pub(crate) fn peer_conversation_ref(home_id: &str) -> String {
    format!("{CONTACT_REF_PREFIX}{home_id}")
}

pub(crate) fn sender_ref(home_id: &str) -> String {
    format!("{SENDER_REF_PREFIX}{home_id}")
}

async fn ensure_instance_team_local_channel(
    home: &LionClawHome,
    worker_binary: &Path,
) -> Result<String> {
    home.ensure_base_dirs().await?;
    install_team_local_skill(home, worker_binary)?;
    let home_id = home.ensure_home_id().await?;
    let mut config = OperatorConfig::load(home).await?;
    ensure_team_local_preset(&mut config);
    let existing_non_team_contact = config
        .channels
        .iter()
        .any(|channel| channel.id != CHANNEL_ID && channel.contact.is_some());
    let existing_team_contact = config
        .channels
        .iter()
        .find(|channel| channel.id == CHANNEL_ID)
        .and_then(|channel| channel.contact.as_ref());
    let should_publish_contact = existing_team_contact.is_some() || !existing_non_team_contact;
    config.upsert_channel(ManagedChannelConfig {
        id: CHANNEL_ID.to_string(),
        skill: CHANNEL_ID.to_string(),
        launch_mode: ChannelLaunchMode::Background,
        worker: DEFAULT_CHANNEL_WORKER.to_string(),
        required_env: Vec::new(),
        contact: should_publish_contact
            .then(|| ChannelContactConfig::new(peer_conversation_ref(&home_id), None)),
    });
    config.save(home).await?;
    Ok(home_id)
}

fn ensure_team_local_preset(config: &mut OperatorConfig) {
    let mut preset = config.preset(PRESET_ID).cloned().unwrap_or_default();
    preset.escape_classes.insert(EscapeClass::ChannelSend);
    config.upsert_preset(PRESET_ID.to_string(), preset);
}

fn install_team_local_skill(home: &LionClawHome, worker_binary: &Path) -> Result<()> {
    let source = bundled_channel_skill_dir(CHANNEL_ID);
    let overlays = [SnapshotOverlay::new(
        worker_binary.to_path_buf(),
        PathBuf::from(WORKER_BIN_REL_PATH),
    )];
    install_snapshot_with_overlays(
        home,
        CHANNEL_ID,
        source.to_string_lossy().as_ref(),
        "bundled",
        &overlays,
    )?;
    Ok(())
}

async fn approve_team_member_grant(
    kernel: &Kernel,
    sender: &ProjectMember,
) -> Result<(), KernelError> {
    kernel
        .approve_channel_grant(ChannelGrantApproveRequest {
            channel_id: CHANNEL_ID.to_string(),
            sender_ref: Some(sender_ref(&sender.home_id)),
            conversation_ref: None,
            thread_ref: None,
            routing_profile: ChannelRoutingProfile::Direct,
            trust_tier: Some(TrustTier::Main),
            label: Some(format!("LionClaw team-local {}", sender.name)),
            reason: Some("team_local_project_member".to_string()),
        })
        .await
        .map(|_| ())
}

async fn open_channel_admin_kernel(home: &LionClawHome, config: &OperatorConfig) -> Result<Kernel> {
    home.ensure_base_dirs().await?;
    let applied_state = AppliedState::load(home).await?;
    Kernel::new_with_options(
        &home.db_path(),
        KernelOptions {
            default_runtime_id: config.defaults.runtime.clone(),
            default_preset_name: config.defaults.preset.clone(),
            execution_presets: config.presets.clone(),
            runtime_execution_profiles: Default::default(),
            runtime_secrets_home: Some(home.clone()),
            workspace_root: Some(config.workspace_root(home)),
            runtime_root: Some(home.runtime_dir()),
            workspace_name: Some(config.daemon.workspace.clone()),
            applied_state,
            ..KernelOptions::default()
        },
    )
    .await
}

fn is_bundled_team_local_source(source_path: &Path) -> Result<bool> {
    let bundled = bundled_channel_skill_dir(CHANNEL_ID);
    if !bundled.exists() {
        return Ok(false);
    }
    let source_path = fs::canonicalize(source_path)
        .with_context(|| format!("failed to resolve {}", source_path.display()))?;
    let bundled = fs::canonicalize(&bundled)
        .with_context(|| format!("failed to resolve {}", bundled.display()))?;
    Ok(source_path == bundled)
}

fn worker_binary_path() -> Result<PathBuf> {
    let exe_name = format!("{WORKER_BIN_NAME}{}", std::env::consts::EXE_SUFFIX);
    let current_exe = std::env::current_exe().context("failed to locate current executable")?;
    let current_dir = current_exe
        .parent()
        .context("current executable path has no parent")?;
    let sibling = current_dir.join(&exe_name);
    if sibling.is_file() {
        return Ok(sibling);
    }
    if current_dir.file_name().and_then(|value| value.to_str()) == Some("deps") {
        if let Some(target_dir) = current_dir.parent() {
            let workspace_sibling = target_dir.join(&exe_name);
            if workspace_sibling.is_file() {
                return Ok(workspace_sibling);
            }
        }
    }
    Err(anyhow::anyhow!(
        "missing {}; build LionClaw with `cargo build --workspace` before installing the bundled team-local channel",
        sibling.display()
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{
        ensure_project_team_local_with_worker, open_channel_admin_kernel, sender_ref, CHANNEL_ID,
        PRESET_ID,
    };
    use crate::{
        contracts::ChannelPairingBlockRequest,
        home::LionClawHome,
        kernel::runtime::{EscapeClass, ExecutionPreset},
        operator::{
            config::OperatorConfig,
            target::{create_project_instance, init_project},
        },
    };

    #[tokio::test]
    async fn project_setup_installs_team_local_and_approves_siblings() {
        let temp_dir = tempdir().expect("temp dir");
        init_project(temp_dir.path()).expect("init project");
        create_project_instance(temp_dir.path(), "reviewer", None, false).expect("reviewer");
        let worker = temp_dir.path().join("worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);

        let setup = ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect("team-local setup");

        assert_eq!(setup.instances, 2);
        assert_eq!(setup.grants_ensured, 2);
        let main_home = LionClawHome::new(temp_dir.path().join(".lionclaw/instances/main"));
        let reviewer_home = LionClawHome::new(temp_dir.path().join(".lionclaw/instances/reviewer"));
        assert_team_local_configured(&main_home).await;
        assert_team_local_configured(&reviewer_home).await;
        assert_team_local_preset(&main_home).await;
        assert_team_local_preset(&reviewer_home).await;
        assert_executable(
            &main_home
                .skills_dir()
                .join(CHANNEL_ID)
                .join("runtime/team-local/bin/lionclaw-channel-team-local"),
        );
        assert!(main_home
            .skills_dir()
            .join(CHANNEL_ID)
            .join("runtime/team-local/SKILL.md")
            .exists());
        assert_executable(
            &main_home
                .skills_dir()
                .join(CHANNEL_ID)
                .join("runtime/team-local/scripts/list"),
        );
        assert_executable(
            &main_home
                .skills_dir()
                .join(CHANNEL_ID)
                .join("runtime/team-local/scripts/resolve"),
        );
        assert_executable(
            &main_home
                .skills_dir()
                .join(CHANNEL_ID)
                .join("runtime/team-local/scripts/send"),
        );
    }

    #[tokio::test]
    async fn project_setup_preserves_existing_non_team_contact_preference() {
        let temp_dir = tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home);
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.upsert_channel(crate::operator::config::ManagedChannelConfig {
            id: "email".to_string(),
            skill: CHANNEL_ID.to_string(),
            launch_mode: crate::operator::config::ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: Vec::new(),
            contact: Some(crate::operator::config::ChannelContactConfig::new(
                "main@example.com".to_string(),
                None,
            )),
        });
        config.save(&home).await.expect("save config");
        let worker = temp_dir.path().join("worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);

        ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect("team-local setup");

        let config = OperatorConfig::load(&home).await.expect("load config");
        let team_local = config
            .channels
            .iter()
            .find(|channel| channel.id == CHANNEL_ID)
            .expect("team-local channel");
        assert!(team_local.contact.is_none());
        let email = config
            .channels
            .iter()
            .find(|channel| channel.id == "email")
            .expect("email channel");
        assert!(email.contact.is_some());
    }

    #[tokio::test]
    async fn project_setup_preserves_existing_default_preset() {
        let temp_dir = tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home);
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.upsert_preset("locked-down".to_string(), ExecutionPreset::default());
        config.save(&home).await.expect("save config");
        let worker = temp_dir.path().join("worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);

        ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect("team-local setup");

        let config = OperatorConfig::load(&home).await.expect("load config");
        assert_eq!(config.defaults.preset.as_deref(), Some("locked-down"));
        let preset = config.preset(PRESET_ID).expect("team-local preset");
        assert!(preset.escape_classes.contains(&EscapeClass::ChannelSend));
    }

    #[tokio::test]
    async fn project_setup_updates_existing_team_local_preset() {
        let temp_dir = tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let home = LionClawHome::new(project.instance.home);
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.upsert_preset(PRESET_ID.to_string(), ExecutionPreset::default());
        config.save(&home).await.expect("save config");
        let worker = temp_dir.path().join("worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);

        ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect("team-local setup");

        let config = OperatorConfig::load(&home).await.expect("load config");
        let preset = config.preset(PRESET_ID).expect("team-local preset");
        assert!(preset.escape_classes.contains(&EscapeClass::ChannelSend));
        assert_eq!(config.defaults.preset.as_deref(), Some(PRESET_ID));
    }

    #[tokio::test]
    async fn project_setup_rejects_duplicate_home_ids() {
        let temp_dir = tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let reviewer =
            create_project_instance(temp_dir.path(), "reviewer", None, false).expect("reviewer");
        let main_home = LionClawHome::new(project.instance.home);
        let reviewer_home = LionClawHome::new(reviewer.home);
        let main_home_id = main_home.ensure_home_id().await.expect("main home id");
        fs::write(reviewer_home.home_id_path(), format!("{main_home_id}\n"))
            .expect("duplicate home id");
        let worker = temp_dir.path().join("worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);

        let err = ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect_err("duplicate home ids should fail");

        assert!(err.to_string().contains("share home id"));
    }

    #[tokio::test]
    async fn project_setup_preserves_blocked_team_member_grants() {
        let temp_dir = tempdir().expect("temp dir");
        let project = init_project(temp_dir.path()).expect("init project");
        let reviewer =
            create_project_instance(temp_dir.path(), "reviewer", None, false).expect("reviewer");
        let worker = temp_dir.path().join("worker");
        fs::write(&worker, "#!/usr/bin/env bash\n").expect("worker");
        make_executable(&worker);
        ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect("initial setup");
        let main_home = LionClawHome::new(project.instance.home);
        let reviewer_home = LionClawHome::new(reviewer.home);
        let main_home_id = main_home.ensure_home_id().await.expect("main home id");
        let reviewer_config = OperatorConfig::load(&reviewer_home)
            .await
            .expect("reviewer config");
        let reviewer_kernel = open_channel_admin_kernel(&reviewer_home, &reviewer_config)
            .await
            .expect("reviewer kernel");

        reviewer_kernel
            .block_channel_pairing(ChannelPairingBlockRequest {
                channel_id: CHANNEL_ID.to_string(),
                pairing_id: None,
                sender_ref: Some(sender_ref(&main_home_id)),
                conversation_ref: None,
                thread_ref: None,
                reason: Some("test block".to_string()),
            })
            .await
            .expect("block main");
        let setup = ensure_project_team_local_with_worker(temp_dir.path(), &worker)
            .await
            .expect("setup preserves block");

        assert_eq!(setup.grants_preserved, 1);
        let access = reviewer_kernel
            .list_channel_pairings(Some(CHANNEL_ID.to_string()), None)
            .await
            .expect("list grants");
        assert!(access.grants.iter().any(|grant| {
            grant.sender_ref.as_deref() == Some(sender_ref(&main_home_id).as_str())
                && grant.status == "blocked"
        }));
    }

    async fn assert_team_local_configured(home: &LionClawHome) {
        let config = OperatorConfig::load(home).await.expect("load config");
        let channel = config
            .channels
            .iter()
            .find(|channel| channel.id == CHANNEL_ID)
            .expect("team-local channel");
        assert_eq!(channel.skill, CHANNEL_ID);
        assert!(channel.contact.is_some());
    }

    async fn assert_team_local_preset(home: &LionClawHome) {
        let config = OperatorConfig::load(home).await.expect("load config");
        let preset = config.preset(PRESET_ID).expect("team-local preset");
        assert!(preset.escape_classes.contains(&EscapeClass::ChannelSend));
        assert_eq!(config.defaults.preset.as_deref(), Some(PRESET_ID));
    }

    fn make_executable(path: &std::path::Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(path).expect("metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).expect("chmod");
        }
    }

    fn assert_executable(path: &std::path::Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(path).expect("metadata").permissions().mode();
            assert_ne!(mode & 0o111, 0, "{} is not executable", path.display());
        }
        #[cfg(not(unix))]
        assert!(path.exists(), "{} does not exist", path.display());
    }
}
