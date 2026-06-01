use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    io::{BufRead, ErrorKind, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{anyhow, bail, Context, Result};
use uuid::Uuid;

use crate::{
    home::LionClawHome,
    operator::{
        attach::{attach_channel_with_binaries, ChannelAttachContext},
        channel_env::{
            collect_from_process_env, load_channel_env, merge_channel_env, missing_required_env,
            parse_env_file, render_missing_env_repair, save_channel_env,
            validate_no_undeclared_channel_env, ChannelEnv,
        },
        channel_metadata::{
            discover_channel_skill, resolve_channel_setup_command_entrypoint,
            validate_channel_env_name, ChannelSetupMetadata, ChannelSkillSource,
            DiscoveredChannelSkill,
        },
        command_display::lionclaw_home_command_prefix,
        config::{ChannelLaunchMode, ManagedChannelConfig, OperatorConfig},
        managed_units::UnitManager,
        private_paths::{
            create_private_dir_all, ensure_private_file_readable, ensure_private_file_write_target,
            private_dir_exists, private_file_exists, remove_private_dir_all_if_exists,
            remove_private_file_if_exists,
        },
        reconcile::{
            add_channel_with_worker, add_skill, resolve_channel_contact_config,
            resolve_stack_binaries, up_for_work_root, ChannelContactSetup, ChannelWorkerSetup,
            StackBinaryPaths,
        },
        runtime::resolve_runtime_id,
    },
};

const CHANNEL_SETUP_ENV_ALLOWLIST: &[&str] = &[
    "ALL_PROXY",
    "APPDATA",
    "BROWSER",
    "COLORTERM",
    "ComSpec",
    "CURL_CA_BUNDLE",
    "DBUS_SESSION_BUS_ADDRESS",
    "DESKTOP_SESSION",
    "DISPLAY",
    "FORCE_COLOR",
    "GNOME_DESKTOP_SESSION_ID",
    "HOME",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "KDE_FULL_SESSION",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LOCALAPPDATA",
    "NO_COLOR",
    "NO_PROXY",
    "PATH",
    "PATHEXT",
    "ProgramData",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "SystemDrive",
    "SystemRoot",
    "TEMP",
    "TERM",
    "TMP",
    "TMPDIR",
    "USERPROFILE",
    "WAYLAND_DISPLAY",
    "WINDIR",
    "XAUTHORITY",
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_CURRENT_DESKTOP",
    "XDG_RUNTIME_DIR",
    "XDG_SESSION_TYPE",
    "XDG_STATE_HOME",
    "all_proxy",
    "http_proxy",
    "https_proxy",
    "no_proxy",
];

#[derive(Debug, Clone, Default)]
pub struct ConnectEnvInputs {
    pub env_file: Option<PathBuf>,
    pub from_env: Vec<String>,
    pub setup_profile: Option<String>,
    pub setup_args: Vec<String>,
}

pub struct ConnectChannelRequest<'a, M> {
    pub home: &'a LionClawHome,
    pub manager: &'a M,
    pub project_root: Option<&'a Path>,
    pub work_root: &'a Path,
    pub channel_or_path: &'a str,
    pub env_inputs: ConnectEnvInputs,
    pub contact: ChannelContactSetup,
    pub interactive: bool,
    pub hide_prompt_input: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectAction {
    InteractiveAttach,
    BackgroundStarted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectOutcome {
    pub channel_id: String,
    pub skill_alias: String,
    pub launch: ChannelLaunchMode,
    pub action: ConnectAction,
}

pub async fn connect_channel<R, W, M>(
    request: ConnectChannelRequest<'_, M>,
    input: &mut R,
    output: &mut W,
) -> Result<ConnectOutcome>
where
    R: BufRead,
    W: Write,
    M: UnitManager,
{
    let binaries = resolve_stack_binaries()?;
    connect_channel_with_binaries(request, &binaries, input, output).await
}

async fn connect_channel_with_binaries<R, W, M>(
    request: ConnectChannelRequest<'_, M>,
    binaries: &StackBinaryPaths,
    input: &mut R,
    output: &mut W,
) -> Result<ConnectOutcome>
where
    R: BufRead,
    W: Write,
    M: UnitManager,
{
    let ConnectChannelRequest {
        home,
        manager,
        project_root,
        work_root,
        channel_or_path,
        env_inputs,
        contact,
        interactive,
        hide_prompt_input,
    } = request;
    let config = OperatorConfig::load(home).await?;
    let runtime_id = resolve_runtime_id(&config, None).map_err(|_| {
        anyhow!(
            "no default runtime configured for selected instance\nRun:\n  lionclaw configure --runtime codex"
        )
    })?;
    if !config.runtimes.contains_key(&runtime_id) {
        return Err(anyhow!(
            "default runtime profile \"{runtime_id}\" is not configured\nRun:\n  lionclaw configure --runtime codex"
        ));
    }

    let discovered = discover_channel_skill(home, channel_or_path)?;
    let channel_id = discovered.metadata.id.clone();
    let contact = resolve_channel_contact_config(contact, Some(&discovered.metadata), &channel_id)?;
    let rollback =
        ConnectRollback::capture(home, &channel_id, config.channels.clone(), &discovered)?;
    let skill_alias = match install_or_select_skill(home, &discovered, &config).await {
        Ok(skill_alias) => skill_alias,
        Err(err) => return rollback_all_and_return(home, &channel_id, rollback, err).await,
    };
    let installed_skill_dir = match installed_channel_skill_dir(home, &skill_alias) {
        Ok(skill_dir) => skill_dir,
        Err(err) => return rollback_all_and_return(home, &channel_id, rollback, err).await,
    };
    let mut prepared_env = match prepare_connect_env_inputs(PrepareConnectEnvRequest {
        home,
        channel_id: &channel_id,
        required_env: &discovered.metadata.env,
        optional_env: &discovered.metadata.optional_env,
        env_inputs,
        interactive,
        installed_skill_dir: &installed_skill_dir,
        discovered: &discovered,
    }) {
        Ok(prepared) => prepared,
        Err(err) => return rollback_all_and_return(home, &channel_id, rollback, err).await,
    };
    if let Err(err) = ensure_required_env(
        RequiredEnvRequest {
            home,
            channel_id: &channel_id,
            required_env: &discovered.metadata.env,
            optional_env: &discovered.metadata.optional_env,
            env_inputs: prepared_env.inputs.clone(),
            interactive,
            hide_prompt_input,
        },
        input,
        output,
    ) {
        let err = prepared_env.rollback(home, err);
        return rollback_all_and_return(home, &channel_id, rollback, err).await;
    }
    if let Err(err) = prepared_env.cleanup_generated_env(home) {
        let err = prepared_env.rollback(home, err);
        return rollback_all_and_return(home, &channel_id, rollback, err).await;
    }
    if let Err(err) = add_channel_with_worker(
        home,
        channel_id.clone(),
        skill_alias.clone(),
        discovered.metadata.launch,
        ChannelWorkerSetup {
            worker: discovered.metadata.worker.clone(),
            required_env: discovered.metadata.env.clone(),
            optional_env: discovered.metadata.optional_env.clone(),
            contact,
        },
    )
    .await
    {
        let err = prepared_env.rollback(home, err);
        return rollback_all_and_return(home, &channel_id, rollback, err).await;
    }

    let action = match discovered.metadata.launch {
        ChannelLaunchMode::Interactive => {
            if let Err(err) = prepared_env.commit(home) {
                let err = prepared_env.rollback(home, err);
                return rollback_all_and_return(home, &channel_id, rollback, err).await;
            }
            rollback.commit()?;
            attach_channel_with_binaries(
                ChannelAttachContext {
                    home,
                    manager,
                    project_root,
                    work_root,
                },
                channel_id.clone(),
                None,
                None,
                binaries,
            )
            .await?;
            ConnectAction::InteractiveAttach
        }
        ChannelLaunchMode::Background => {
            let channel_was_active =
                background_channel_is_active(home, manager, &channel_id).await?;
            if let Err(err) = up_for_work_root(
                home,
                manager,
                &runtime_id,
                binaries,
                project_root,
                work_root,
            )
            .await
            {
                let err = prepared_env.rollback(home, err);
                return rollback_all_and_return(home, &channel_id, rollback, err).await;
            }
            if channel_was_active {
                if let Err(err) = restart_background_channel(home, manager, &channel_id).await {
                    let err = prepared_env.rollback(home, err);
                    return rollback_all_and_return(home, &channel_id, rollback, err).await;
                }
            }
            if let Err(err) = prepared_env.commit(home) {
                let err = prepared_env.rollback(home, err);
                return rollback_all_and_return(home, &channel_id, rollback, err).await;
            }
            rollback.commit()?;
            ConnectAction::BackgroundStarted
        }
    };

    Ok(ConnectOutcome {
        channel_id,
        skill_alias,
        launch: discovered.metadata.launch,
        action,
    })
}

async fn install_or_select_skill(
    home: &LionClawHome,
    discovered: &DiscoveredChannelSkill,
    config: &OperatorConfig,
) -> Result<String> {
    match &discovered.source {
        ChannelSkillSource::Installed { alias } => Ok(alias.clone()),
        ChannelSkillSource::Path | ChannelSkillSource::Bundled => {
            let alias = discovered.metadata.id.clone();
            validate_channel_skill_alias_install_target(
                home,
                config,
                &alias,
                &discovered.metadata.id,
            )?;
            add_skill(
                home,
                alias.clone(),
                discovered.skill_dir.display().to_string(),
                "local".to_string(),
            )
            .await?;
            Ok(alias)
        }
    }
}

fn validate_channel_skill_alias_install_target(
    home: &LionClawHome,
    config: &OperatorConfig,
    alias: &str,
    channel_id: &str,
) -> Result<()> {
    let alias_path = home.skills_dir().join(alias);
    let exists = match fs::symlink_metadata(&alias_path) {
        Ok(_) => true,
        Err(err) if err.kind() == ErrorKind::NotFound => false,
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", alias_path.display()))
        }
    };
    if !exists {
        return Ok(());
    }

    let bound_channels = config
        .channels
        .iter()
        .filter(|channel| channel.skill == alias)
        .map(|channel| channel.id.as_str())
        .collect::<Vec<_>>();
    match bound_channels.as_slice() {
        [existing_channel] if *existing_channel == channel_id => Ok(()),
        [] => bail!(
            "skill alias '{alias}' already exists and is not bound to channel '{channel_id}'; remove or rename it before connecting this channel"
        ),
        channels => bail!(
            "skill alias '{alias}' is already used by configured channel(s): {}; remove the channel before replacing the alias",
            channels.join(", ")
        ),
    }
}

struct PrepareConnectEnvRequest<'a> {
    home: &'a LionClawHome,
    channel_id: &'a str,
    required_env: &'a [String],
    optional_env: &'a [String],
    env_inputs: ConnectEnvInputs,
    interactive: bool,
    installed_skill_dir: &'a Path,
    discovered: &'a DiscoveredChannelSkill,
}

struct PreparedConnectEnvInputs {
    inputs: ConnectEnvInputs,
    generated_env_file: Option<PathBuf>,
    setup_state: Option<SetupStateRollback>,
}

impl PreparedConnectEnvInputs {
    fn cleanup_generated_env(&mut self, home: &LionClawHome) -> Result<()> {
        if let Some(path) = self.generated_env_file.take() {
            remove_private_file_if_exists(home, &path, "channel setup env file")?;
        }
        Ok(())
    }

    fn rollback(mut self, home: &LionClawHome, err: anyhow::Error) -> anyhow::Error {
        let err = self.cleanup_generated_env_lossy(home, err);
        let Some(setup_state) = self.setup_state.take() else {
            return err;
        };
        if let Err(rollback_err) = setup_state.restore(home) {
            return anyhow!(
                "{err}; additionally failed to roll back channel setup state: {rollback_err}"
            );
        }
        err
    }

    fn commit(&mut self, home: &LionClawHome) -> Result<()> {
        if let Some(setup_state) = self.setup_state.as_mut() {
            setup_state.discard(home)?;
        }
        self.setup_state = None;
        Ok(())
    }

    fn cleanup_generated_env_lossy(
        &mut self,
        home: &LionClawHome,
        err: anyhow::Error,
    ) -> anyhow::Error {
        let Some(path) = self.generated_env_file.take() else {
            return err;
        };
        cleanup_generated_setup_env(home, &path, err)
    }
}

fn prepare_connect_env_inputs(
    request: PrepareConnectEnvRequest<'_>,
) -> Result<PreparedConnectEnvInputs> {
    let PrepareConnectEnvRequest {
        home,
        channel_id,
        required_env,
        optional_env,
        env_inputs,
        interactive,
        installed_skill_dir,
        discovered,
    } = request;
    reject_conflicting_setup_inputs(&env_inputs)?;
    if env_inputs.env_file.is_some() || !env_inputs.from_env.is_empty() {
        return Ok(PreparedConnectEnvInputs {
            inputs: env_inputs,
            generated_env_file: None,
            setup_state: None,
        });
    }

    validate_no_undeclared_channel_env(home, channel_id, required_env, optional_env)?;
    let stored = load_channel_env(home, channel_id)?;
    let missing = missing_required_env(&stored, required_env)?;
    let setup_requested = setup_inputs_requested(&env_inputs);
    if missing.is_empty() && !setup_requested {
        return Ok(PreparedConnectEnvInputs {
            inputs: env_inputs,
            generated_env_file: None,
            setup_state: None,
        });
    }
    let setup = discovered.metadata.setup.as_ref();
    if !setup_requested && (!interactive || setup.is_none()) {
        return Ok(PreparedConnectEnvInputs {
            inputs: env_inputs,
            generated_env_file: None,
            setup_state: None,
        });
    }

    let setup =
        setup.ok_or_else(|| anyhow!("channel '{channel_id}' does not declare a setup command"))?;
    let env_file = channel_setup_env_file(home, channel_id);
    let state_dir = channel_setup_state_dir(home, channel_id);
    let setup_state = SetupStateRollback::capture(home, channel_id)?;
    if let Err(err) =
        remove_private_dir_all_if_exists(home, &state_dir, "channel setup state directory")
    {
        return Err(rollback_setup_state(home, setup_state, err));
    }
    if let Err(err) = create_private_dir_all(home, &state_dir, "channel setup state directory") {
        return Err(rollback_setup_state(home, setup_state, err));
    }
    if let Err(err) = remove_private_file_if_exists(home, &env_file, "channel setup env file") {
        return Err(rollback_setup_state(home, setup_state, err));
    }
    if let Err(err) = ensure_private_file_write_target(home, &env_file, "channel setup env file") {
        return Err(rollback_setup_state(home, setup_state, err));
    }
    if let Err(err) = run_channel_setup_command(ChannelSetupRunRequest {
        skill_dir: installed_skill_dir,
        setup,
        home,
        channel_id,
        setup_profile: env_inputs.setup_profile.as_deref(),
        setup_args: &env_inputs.setup_args,
        env_file: &env_file,
        state_dir: &state_dir,
    }) {
        return Err(PreparedConnectEnvInputs {
            inputs: ConnectEnvInputs::default(),
            generated_env_file: Some(env_file),
            setup_state: Some(setup_state),
        }
        .rollback(home, err));
    }
    if let Err(err) = ensure_private_file_readable(home, &env_file, "channel setup env file") {
        return Err(PreparedConnectEnvInputs {
            inputs: ConnectEnvInputs::default(),
            generated_env_file: Some(env_file),
            setup_state: Some(setup_state),
        }
        .rollback(home, err));
    }

    Ok(PreparedConnectEnvInputs {
        inputs: ConnectEnvInputs {
            env_file: Some(env_file.clone()),
            from_env: Vec::new(),
            setup_profile: None,
            setup_args: Vec::new(),
        },
        generated_env_file: Some(env_file),
        setup_state: Some(setup_state),
    })
}

fn cleanup_generated_setup_env(
    home: &LionClawHome,
    env_file: &Path,
    err: anyhow::Error,
) -> anyhow::Error {
    if let Err(cleanup_err) =
        remove_private_file_if_exists(home, env_file, "channel setup env file")
    {
        return anyhow!(
            "{err}; additionally failed to clean up channel setup env file: {cleanup_err}"
        );
    }
    err
}

fn rollback_setup_state(
    home: &LionClawHome,
    setup_state: SetupStateRollback,
    err: anyhow::Error,
) -> anyhow::Error {
    PreparedConnectEnvInputs {
        inputs: ConnectEnvInputs::default(),
        generated_env_file: None,
        setup_state: Some(setup_state),
    }
    .rollback(home, err)
}

fn reject_conflicting_setup_inputs(env_inputs: &ConnectEnvInputs) -> Result<()> {
    if setup_inputs_requested(env_inputs)
        && (env_inputs.env_file.is_some() || !env_inputs.from_env.is_empty())
    {
        bail!("channel setup arguments cannot be combined with --env-file or --from-env");
    }
    Ok(())
}

fn setup_inputs_requested(env_inputs: &ConnectEnvInputs) -> bool {
    env_inputs.setup_profile.is_some() || !env_inputs.setup_args.is_empty()
}

struct SetupStateRollback {
    state_dir: PathBuf,
    backup_dir: Option<PathBuf>,
}

impl SetupStateRollback {
    fn capture(home: &LionClawHome, channel_id: &str) -> Result<Self> {
        let state_dir = channel_setup_state_dir(home, channel_id);
        let backup_dir = if private_dir_exists(home, &state_dir, "channel setup state directory")? {
            let backup_dir = channel_setup_dir(home, channel_id)
                .join(format!(".state.rollback-{}", Uuid::new_v4()));
            copy_regular_tree_for_rollback(
                &state_dir,
                &backup_dir,
                "channel setup state directory",
            )?;
            Some(backup_dir)
        } else {
            None
        };
        Ok(Self {
            state_dir,
            backup_dir,
        })
    }

    fn restore(self, home: &LionClawHome) -> Result<()> {
        remove_private_dir_all_if_exists(home, &self.state_dir, "channel setup state directory")?;
        if let Some(backup_dir) = self.backup_dir {
            fs::rename(&backup_dir, &self.state_dir).with_context(|| {
                format!(
                    "failed to restore '{}' from '{}'",
                    self.state_dir.display(),
                    backup_dir.display()
                )
            })?;
        }
        Ok(())
    }

    fn discard(&mut self, home: &LionClawHome) -> Result<()> {
        let Some(backup_dir) = self.backup_dir.take() else {
            return Ok(());
        };
        if let Err(err) = remove_private_dir_all_if_exists(
            home,
            &backup_dir,
            "channel setup state rollback directory",
        ) {
            self.backup_dir = Some(backup_dir);
            return Err(err);
        }
        Ok(())
    }
}

struct ChannelSetupRunRequest<'a> {
    skill_dir: &'a Path,
    setup: &'a ChannelSetupMetadata,
    home: &'a LionClawHome,
    channel_id: &'a str,
    setup_profile: Option<&'a str>,
    setup_args: &'a [String],
    env_file: &'a Path,
    state_dir: &'a Path,
}

fn run_channel_setup_command(request: ChannelSetupRunRequest<'_>) -> Result<()> {
    let ChannelSetupRunRequest {
        skill_dir,
        setup,
        home,
        channel_id,
        setup_profile,
        setup_args,
        env_file,
        state_dir,
    } = request;
    let command = resolve_channel_setup_command_entrypoint(skill_dir, setup)?;
    let home_root = absolute_setup_env_path(&home.root(), "LionClaw home")?;
    let env_file = absolute_setup_env_path(env_file, "channel setup env file")?;
    let state_dir = absolute_setup_env_path(state_dir, "channel setup state directory")?;
    let mut process = Command::new(&command);
    process.args(&setup.args);
    if let Some(profile) = setup_profile {
        process.arg(profile);
    }
    process.args(setup_args);
    process
        .current_dir(skill_dir)
        .env_clear()
        .envs(channel_setup_ambient_env())
        .env("LIONCLAW_HOME", home_root)
        .env("LIONCLAW_CHANNEL_ID", channel_id)
        .env("LIONCLAW_CHANNEL_SETUP_ENV_FILE", env_file)
        .env("LIONCLAW_CHANNEL_SETUP_STATE_DIR", state_dir)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = process.status().with_context(|| {
        format!(
            "failed to start channel setup command {}",
            command.display()
        )
    })?;
    if !status.success() {
        bail!(
            "channel setup command {} exited with {status}",
            command.display()
        );
    }
    Ok(())
}

fn absolute_setup_env_path(path: &Path, label: &str) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let current_dir = std::env::current_dir()
        .with_context(|| format!("failed to resolve current directory for {label}"))?;
    Ok(current_dir.join(path))
}

fn channel_setup_ambient_env() -> Vec<(&'static str, OsString)> {
    CHANNEL_SETUP_ENV_ALLOWLIST
        .iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (*name, value)))
        .collect()
}

fn installed_channel_skill_dir(home: &LionClawHome, alias: &str) -> Result<PathBuf> {
    let path = home.skills_dir().join(alias);
    let metadata = fs::symlink_metadata(&path)
        .with_context(|| format!("failed to stat {}", path.display()))?;
    ensure_skill_snapshot_dir(&path, &metadata)?;
    fs::canonicalize(&path).with_context(|| format!("failed to resolve {}", path.display()))
}

fn channel_setup_env_file(home: &LionClawHome, channel_id: &str) -> PathBuf {
    channel_setup_dir(home, channel_id).join("generated.env")
}

fn channel_setup_state_dir(home: &LionClawHome, channel_id: &str) -> PathBuf {
    channel_setup_dir(home, channel_id).join("state")
}

fn channel_setup_dir(home: &LionClawHome, channel_id: &str) -> PathBuf {
    home.config_dir().join("channel-setup").join(channel_id)
}

struct ConnectRollback {
    previous_channels: Vec<ManagedChannelConfig>,
    previous_env: ChannelEnvSnapshot,
    skill: SkillRollback,
}

impl ConnectRollback {
    fn capture(
        home: &LionClawHome,
        channel_id: &str,
        previous_channels: Vec<ManagedChannelConfig>,
        discovered: &DiscoveredChannelSkill,
    ) -> Result<Self> {
        Ok(Self {
            previous_channels,
            previous_env: ChannelEnvSnapshot::capture(home, channel_id)?,
            skill: SkillRollback::capture(home, discovered)?,
        })
    }

    async fn rollback_all(self, home: &LionClawHome, channel_id: &str) -> Result<()> {
        restore_channel_config(home, self.previous_channels).await?;
        self.previous_env.restore(home, channel_id)?;
        self.skill.restore()?;
        Ok(())
    }

    fn commit(self) -> Result<()> {
        self.skill.discard()
    }
}

struct ChannelEnvSnapshot {
    existed: bool,
    values: ChannelEnv,
}

impl ChannelEnvSnapshot {
    fn capture(home: &LionClawHome, channel_id: &str) -> Result<Self> {
        let values = load_channel_env(home, channel_id)?;
        Ok(Self {
            existed: channel_env_file_exists(home, channel_id)?,
            values,
        })
    }

    fn restore(self, home: &LionClawHome, channel_id: &str) -> Result<()> {
        if self.existed {
            save_channel_env(home, channel_id, &self.values)
        } else {
            remove_channel_env_file(home, channel_id)
        }
    }
}

enum SkillRollback {
    None,
    Snapshot {
        snapshot_dir: PathBuf,
        backup_dir: Option<PathBuf>,
    },
}

impl SkillRollback {
    fn capture(home: &LionClawHome, discovered: &DiscoveredChannelSkill) -> Result<Self> {
        if matches!(discovered.source, ChannelSkillSource::Installed { .. }) {
            return Ok(Self::None);
        }

        let alias = &discovered.metadata.id;
        let snapshot_dir = home.skills_dir().join(alias);
        let backup_dir = match fs::symlink_metadata(&snapshot_dir) {
            Ok(metadata) => {
                ensure_skill_snapshot_dir(&snapshot_dir, &metadata)?;
                let backup_dir = home
                    .skills_dir()
                    .join(format!(".{alias}.rollback-{}", Uuid::new_v4()));
                copy_skill_snapshot_for_rollback(&snapshot_dir, &backup_dir)?;
                Some(backup_dir)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to stat {}", snapshot_dir.display()));
            }
        };
        Ok(Self::Snapshot {
            snapshot_dir,
            backup_dir,
        })
    }

    fn restore(self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Snapshot {
                snapshot_dir,
                backup_dir,
            } => {
                remove_skill_snapshot(&snapshot_dir)?;
                if let Some(backup_dir) = backup_dir {
                    fs::rename(&backup_dir, &snapshot_dir).with_context(|| {
                        format!(
                            "failed to restore '{}' from '{}'",
                            snapshot_dir.display(),
                            backup_dir.display()
                        )
                    })?;
                }
                Ok(())
            }
        }
    }

    fn discard(self) -> Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Snapshot {
                backup_dir: Some(backup_dir),
                ..
            } => remove_skill_snapshot(&backup_dir),
            Self::Snapshot {
                backup_dir: None, ..
            } => Ok(()),
        }
    }
}

async fn rollback_all_and_return<T>(
    home: &LionClawHome,
    channel_id: &str,
    rollback: ConnectRollback,
    err: anyhow::Error,
) -> Result<T> {
    if let Err(rollback_err) = rollback.rollback_all(home, channel_id).await {
        return Err(anyhow!(
            "{err}; additionally failed to roll back partial channel state: {rollback_err}"
        ));
    }
    Err(err)
}

async fn restore_channel_config(
    home: &LionClawHome,
    previous_channels: Vec<ManagedChannelConfig>,
) -> Result<()> {
    let mut config = OperatorConfig::load(home).await?;
    config.channels = previous_channels;
    config.save(home).await?;
    Ok(())
}

fn channel_env_file_exists(home: &LionClawHome, channel_id: &str) -> Result<bool> {
    let path = home.channel_env_path(channel_id);
    private_file_exists(home, &path, "channel env file")
}

fn remove_channel_env_file(home: &LionClawHome, channel_id: &str) -> Result<()> {
    let path = home.channel_env_path(channel_id);
    remove_private_file_if_exists(home, &path, "channel env file")
}

fn ensure_skill_snapshot_dir(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        bail!("skill snapshot {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("skill snapshot {} is not a directory", path.display());
    }
    Ok(())
}

fn copy_skill_snapshot_for_rollback(source: &Path, destination: &Path) -> Result<()> {
    copy_regular_tree_for_rollback(source, destination, "skill snapshot")
}

fn copy_regular_tree_for_rollback(source: &Path, destination: &Path, label: &str) -> Result<()> {
    let source_metadata = fs::symlink_metadata(source)
        .with_context(|| format!("failed to stat {}", source.display()))?;
    if source_metadata.file_type().is_symlink() {
        bail!("{label} {} must not be a symlink", source.display());
    }
    if !source_metadata.is_dir() {
        bail!("{label} {} is not a directory", source.display());
    }
    create_rollback_destination_dir(destination, label, &source_metadata)?;
    let mut entries = fs::read_dir(source)
        .with_context(|| format!("failed to read {}", source.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("failed to iterate {}", source.display()))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let target = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to stat {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("{label} entry {} must not be a symlink", path.display());
        }
        if metadata.is_dir() {
            copy_regular_tree_for_rollback(&path, &target, label)?;
        } else if metadata.is_file() {
            fs::copy(&path, &target).with_context(|| {
                format!(
                    "failed to copy '{}' to '{}'",
                    path.display(),
                    target.display()
                )
            })?;
            fs::set_permissions(&target, metadata.permissions())
                .with_context(|| format!("failed to chmod {}", target.display()))?;
        } else {
            bail!(
                "{label} entry {} is not a regular file or directory",
                path.display()
            );
        }
    }
    Ok(())
}

fn create_rollback_destination_dir(
    destination: &Path,
    label: &str,
    source_metadata: &fs::Metadata,
) -> Result<()> {
    match fs::symlink_metadata(destination) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "{label} rollback destination {} must not be a symlink",
                    destination.display()
                );
            }
            bail!(
                "{label} rollback destination {} already exists",
                destination.display()
            );
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", destination.display()));
        }
    }
    fs::create_dir(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
    fs::set_permissions(destination, source_metadata.permissions())
        .with_context(|| format!("failed to chmod {}", destination.display()))?;
    Ok(())
}

fn remove_skill_snapshot(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure_skill_snapshot_dir(path, &metadata)?;
            fs::remove_dir_all(path).with_context(|| format!("failed to remove {}", path.display()))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

async fn background_channel_is_active<M: UnitManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: &str,
) -> Result<bool> {
    let owned_units = manager.owned_units(home)?;
    let Some(unit) = owned_units.channel(channel_id) else {
        return Ok(false);
    };
    let status = manager.unit_status(unit).await?;
    Ok(crate::operator::managed_units::unit_status_is_active(
        &status,
    ))
}

async fn restart_background_channel<M: UnitManager>(
    home: &LionClawHome,
    manager: &M,
    channel_id: &str,
) -> Result<()> {
    let owned_units = manager.owned_units(home)?;
    let Some(unit) = owned_units.channel(channel_id) else {
        return Ok(());
    };
    let units = vec![unit.to_string()];
    manager.restart_units(&units).await
}

struct RequiredEnvRequest<'a> {
    home: &'a LionClawHome,
    channel_id: &'a str,
    required_env: &'a [String],
    optional_env: &'a [String],
    env_inputs: ConnectEnvInputs,
    interactive: bool,
    hide_prompt_input: bool,
}

fn ensure_required_env<R: BufRead, W: Write>(
    request: RequiredEnvRequest<'_>,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let RequiredEnvRequest {
        home,
        channel_id,
        required_env,
        optional_env,
        env_inputs,
        interactive,
        hide_prompt_input,
    } = request;
    let mut updates = ChannelEnv::new();
    if let Some(path) = env_inputs.env_file.as_deref() {
        let file_updates = parse_env_file(path)?;
        validate_declared_env_updates(
            channel_id,
            required_env,
            optional_env,
            &file_updates,
            "env file",
        )?;
        updates.extend(file_updates);
    }
    validate_declared_env_input_names(
        channel_id,
        required_env,
        optional_env,
        &env_inputs.from_env,
    )?;
    updates.extend(collect_from_process_env(&env_inputs.from_env)?);
    validate_no_undeclared_channel_env(home, channel_id, required_env, optional_env)?;
    if !updates.is_empty() {
        merge_channel_env_if_changed(home, channel_id, &updates)?;
    }

    let stored = load_channel_env(home, channel_id)?;
    validate_no_undeclared_channel_env(home, channel_id, required_env, optional_env)?;
    let missing = missing_required_env(&stored, required_env)?;
    if missing.is_empty() {
        return Ok(());
    }
    let repair_command = lionclaw_home_command_prefix(home);
    if !interactive {
        return Err(anyhow!(render_missing_env_repair(
            &repair_command,
            channel_id,
            &missing
        )));
    }

    let prompted = prompt_required_env(channel_id, &missing, hide_prompt_input, input, output)?;
    merge_channel_env_if_changed(home, channel_id, &prompted)?;
    let stored = load_channel_env(home, channel_id)?;
    validate_no_undeclared_channel_env(home, channel_id, required_env, optional_env)?;
    let missing = missing_required_env(&stored, required_env)?;
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(render_missing_env_repair(
            &repair_command,
            channel_id,
            &missing
        )))
    }
}

fn merge_channel_env_if_changed(
    home: &LionClawHome,
    channel_id: &str,
    updates: &ChannelEnv,
) -> Result<()> {
    let existing = load_channel_env(home, channel_id)?;
    let changed = updates.iter().any(|(key, value)| {
        if value.is_empty() {
            existing.contains_key(key)
        } else {
            existing.get(key) != Some(value)
        }
    });
    if changed {
        merge_channel_env(home, channel_id, updates)?;
    }
    Ok(())
}

fn prompt_required_env<R: BufRead, W: Write>(
    channel_id: &str,
    missing: &[String],
    hide_input: bool,
    input: &mut R,
    output: &mut W,
) -> Result<ChannelEnv> {
    let mut values = BTreeMap::new();
    for key in missing {
        write!(output, "{channel_id} {key}: ")?;
        output.flush()?;
        let value = read_prompt_line(input, hide_input)
            .with_context(|| format!("failed to read value for {key}"))?;
        writeln!(output)?;
        let value = value.trim_end_matches(['\r', '\n']).to_string();
        if value.is_empty() {
            return Err(anyhow!(
                "required environment value '{key}' cannot be empty"
            ));
        }
        values.insert(key.clone(), value);
    }
    Ok(values)
}

fn validate_declared_env_updates(
    channel_id: &str,
    required_env: &[String],
    optional_env: &[String],
    updates: &ChannelEnv,
    source: &str,
) -> Result<()> {
    let declared = declared_env_set(required_env, optional_env)?;
    let mut undeclared = Vec::new();
    for key in updates.keys() {
        if !declared.contains(key.as_str()) {
            undeclared.push(key.as_str());
        }
    }
    if undeclared.is_empty() {
        return Ok(());
    }
    bail!(
        "{source} contains environment values not declared by channel '{channel_id}' metadata: {}",
        undeclared.join(", ")
    )
}

fn validate_declared_env_input_names(
    channel_id: &str,
    required_env: &[String],
    optional_env: &[String],
    input_names: &[String],
) -> Result<()> {
    let declared = declared_env_set(required_env, optional_env)?;
    let mut undeclared = Vec::new();
    for key in input_names {
        validate_channel_env_name(key)?;
        if !declared.contains(key.as_str()) {
            undeclared.push(key.as_str());
        }
    }
    if undeclared.is_empty() {
        return Ok(());
    }
    bail!(
        "--from-env references environment values not declared by channel '{channel_id}' metadata: {}",
        undeclared.join(", ")
    )
}

fn declared_env_set<'a>(
    required_env: &'a [String],
    optional_env: &'a [String],
) -> Result<BTreeSet<&'a str>> {
    let mut declared = BTreeSet::new();
    for key in required_env {
        validate_channel_env_name(key)?;
        declared.insert(key.as_str());
    }
    for key in optional_env {
        validate_channel_env_name(key)?;
        declared.insert(key.as_str());
    }
    Ok(declared)
}

fn read_prompt_line<R: BufRead>(input: &mut R, hide_input: bool) -> Result<String> {
    if hide_input {
        return read_secret_line();
    }

    let mut value = String::new();
    input.read_line(&mut value)?;
    Ok(value)
}

#[cfg(unix)]
fn read_secret_line() -> Result<String> {
    use std::{fs::OpenOptions, io::BufReader};

    use rustix::termios::{tcgetattr, tcsetattr, LocalModes, OptionalActions, Termios};

    struct EchoGuard<'a> {
        tty: &'a std::fs::File,
        original: Termios,
    }

    impl Drop for EchoGuard<'_> {
        fn drop(&mut self) {
            let _restore_result = tcsetattr(self.tty, OptionalActions::Now, &self.original);
        }
    }

    let tty = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
        .context("failed to open controlling terminal")?;
    let original = tcgetattr(&tty).context("failed to read terminal mode")?;
    let mut no_echo = original.clone();
    no_echo.local_modes.remove(LocalModes::ECHO);
    tcsetattr(&tty, OptionalActions::Now, &no_echo).context("failed to disable terminal echo")?;
    let _guard = EchoGuard {
        tty: &tty,
        original,
    };

    let mut value = String::new();
    BufReader::new(&tty).read_line(&mut value)?;
    Ok(value)
}

#[cfg(not(unix))]
fn read_secret_line() -> Result<String> {
    Err(anyhow!(
        "interactive hidden input is only supported on Unix-like systems"
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        ffi::OsString,
        fs,
        io::Cursor,
        net::TcpListener,
        path::{Path, PathBuf},
        sync::{Mutex, MutexGuard},
    };

    use super::{
        channel_setup_env_file, channel_setup_state_dir, connect_channel_with_binaries,
        copy_regular_tree_for_rollback, ensure_required_env, prepare_connect_env_inputs,
        run_channel_setup_command, ChannelSetupRunRequest, ConnectAction, ConnectChannelRequest,
        ConnectEnvInputs, PrepareConnectEnvRequest, RequiredEnvRequest,
    };
    use crate::{
        home::LionClawHome,
        kernel::runtime::{ConfinementConfig, OciConfinementConfig},
        operator::{
            channel_env::{load_channel_env, save_channel_env, ChannelEnv},
            channel_metadata::{discover_channel_skill, ChannelSetupMetadata},
            config::{
                ChannelContactConfig, ChannelLaunchMode, ManagedChannelConfig, OperatorConfig,
                RuntimeProfileConfig,
            },
            managed_units::{
                channel_unit_name, daemon_unit_name, ensure_unit_identity, FakeUnitManager,
                UnitManager,
            },
            reconcile::{add_skill, ChannelContactSetup, StackBinaryPaths},
        },
    };

    fn binaries() -> StackBinaryPaths {
        StackBinaryPaths {
            daemon_bin: "/tmp/lionclawd".into(),
        }
    }

    fn test_bind() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("allocate test bind");
        let addr = listener.local_addr().expect("read test bind");
        format!("127.0.0.1:{}", addr.port())
    }

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[cfg(unix)]
    fn write_executable(path: &Path, content: &str) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, content).expect("write executable");
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }

    #[cfg(unix)]
    async fn seed_configured_runtime(home: &LionClawHome, temp_dir: &Path) {
        home.ensure_base_dirs().await.expect("create base dirs");
        let codex_home = home.root().join(".codex");
        fs::create_dir_all(&codex_home).expect("codex home");
        fs::write(
            codex_home.join("auth.json"),
            r#"{
  "OPENAI_API_KEY": "sk-test"
}"#,
        )
        .expect("runtime auth");
        let runtime_stub = temp_dir.join("codex-stub.sh");
        write_executable(&runtime_stub, "#!/usr/bin/env bash\ncat >/dev/null\n");
        let podman = temp_dir.join("podman");
        write_executable(
            &podman,
            "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"image\" ] && [ \"${2:-}\" = \"inspect\" ]; then\n  printf 'sha256:test-runtime-image\\n'\n  exit 0\nfi\nexit 0\n",
        );

        let mut config = OperatorConfig::load(home).await.expect("load config");
        config.daemon.bind = test_bind();
        config.daemon.bind_configured = true;
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
        config
            .set_default_runtime("codex")
            .expect("set default runtime");
        config.save(home).await.expect("save config");
    }

    #[cfg(unix)]
    fn write_channel_skill(root: &Path, skill_name: &str, token: &str) {
        write_channel_skill_with_id(root, skill_name, "telegram", token);
    }

    #[cfg(unix)]
    fn write_channel_skill_with_id(root: &Path, skill_name: &str, channel_id: &str, token: &str) {
        fs::create_dir_all(root.join("scripts")).expect("scripts dir");
        fs::write(
            root.join("SKILL.md"),
            format!("---\nname: {skill_name}\ndescription: test channel\n---\n{token}\n"),
        )
        .expect("skill md");
        fs::write(
            root.join("lionclaw.toml"),
            format!(
                r#"version = 1

[channel]
id = "{channel_id}"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]
optional_env = ["TELEGRAM_POLL_MS"]
"#,
            ),
        )
        .expect("channel metadata");
        write_executable(
            root.join("scripts/worker").as_path(),
            "#!/usr/bin/env bash\n",
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_setup_script(root: &Path, metadata: &str, setup_script: &str) {
        fs::create_dir_all(root.join("scripts")).expect("scripts dir");
        fs::write(
            root.join("SKILL.md"),
            "---\nname: Telegram\ndescription: test channel\n---\nsetup\n",
        )
        .expect("skill md");
        fs::write(root.join("lionclaw.toml"), metadata).expect("channel metadata");
        write_executable(
            root.join("scripts/worker").as_path(),
            "#!/usr/bin/env bash\n",
        );
        write_executable(root.join("scripts/setup").as_path(), setup_script);
    }

    #[cfg(unix)]
    fn write_channel_skill_with_setup(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]
optional_env = ["TELEGRAM_POLL_MS"]

[channel.setup]
command = "scripts/setup"
args = ["bootstrap"]
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
test "${1:-}" = "bootstrap"
test "${2:-}" = "testprofile"
test "${3:-}" = "--flag"
test "${4:-}" = "value"
expected_setup_root="$(cd "$(dirname "$0")/.." && pwd -P)"
test "$(pwd -P)" = "$expected_setup_root"
test "${LIONCLAW_CHANNEL_ID:-}" = "telegram"
test -n "${LIONCLAW_HOME:-}"
test -n "${LIONCLAW_CHANNEL_SETUP_STATE_DIR:-}"
mkdir -p "$LIONCLAW_CHANNEL_SETUP_STATE_DIR/check"
printf 'TELEGRAM_BOT_TOKEN=setup-token\nTELEGRAM_POLL_MS=250\n' > "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
"#,
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_setup_env_probe(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]

[channel.setup]
command = "scripts/setup"
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
test -z "${LIONCLAW_TEST_CHANNEL_SETUP_SECRET+x}"
test -n "${PATH:-}"
test -n "${LIONCLAW_HOME:-}"
test "${LIONCLAW_CHANNEL_ID:-}" = "telegram"
test -n "${LIONCLAW_CHANNEL_SETUP_ENV_FILE:-}"
test -n "${LIONCLAW_CHANNEL_SETUP_STATE_DIR:-}"
printf 'TELEGRAM_BOT_TOKEN=setup-token\n' > "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
"#,
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_interactive_setup(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]

[channel.setup]
command = "scripts/setup"
args = ["bootstrap"]
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
test "${1:-}" = "bootstrap"
test "${2:-}" = ""
printf 'TELEGRAM_BOT_TOKEN=interactive-token\n' > "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
"#,
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_public_setup_env(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]

[channel.setup]
command = "scripts/setup"
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
printf 'TELEGRAM_BOT_TOKEN=setup-token\n' > "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
chmod 0644 "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
"#,
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_setup_state(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]

[channel.setup]
command = "scripts/setup"
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_CHANNEL_SETUP_STATE_DIR/gmail"
printf 'new-state\n' > "$LIONCLAW_CHANNEL_SETUP_STATE_DIR/gmail/new.json"
printf 'TELEGRAM_BOT_TOKEN=setup-token\n' > "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
"#,
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_failing_setup_state(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]

[channel.setup]
command = "scripts/setup"
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$LIONCLAW_CHANNEL_SETUP_STATE_DIR/gmail"
printf 'failed-state\n' > "$LIONCLAW_CHANNEL_SETUP_STATE_DIR/gmail/state.json"
printf 'TELEGRAM_BOT_TOKEN=setup-token\n' > "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
exit 17
"#,
        );
    }

    #[cfg(unix)]
    fn write_channel_skill_with_symlinked_setup_outputs(root: &Path) {
        write_channel_skill_with_setup_script(
            root,
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]

[channel.setup]
command = "scripts/setup"
"#,
            r#"#!/usr/bin/env bash
set -euo pipefail
outside_env="${1:?}"
outside_state="${2:?}"
rm -f "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
ln -s "$outside_env" "$LIONCLAW_CHANNEL_SETUP_ENV_FILE"
rm -rf "$LIONCLAW_CHANNEL_SETUP_STATE_DIR"
ln -s "$outside_state" "$LIONCLAW_CHANNEL_SETUP_STATE_DIR"
exit 17
"#,
        );
    }

    #[cfg(unix)]
    fn write_normal_skill(root: &Path, skill_name: &str, token: &str) {
        fs::create_dir_all(root).expect("skill dir");
        fs::write(
            root.join("SKILL.md"),
            format!("---\nname: {skill_name}\ndescription: normal skill\n---\n{token}\n"),
        )
        .expect("skill md");
    }

    struct EnvRestore {
        _guard: MutexGuard<'static, ()>,
        original: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvRestore {
        fn set<const N: usize>(values: [(&'static str, Option<&str>); N]) -> Self {
            let guard = ENV_LOCK.lock().expect("env lock");
            let original = values
                .iter()
                .map(|(name, _)| (*name, env::var_os(name)))
                .collect::<Vec<_>>();
            for (name, value) in values {
                if let Some(value) = value {
                    env::set_var(name, value);
                } else {
                    env::remove_var(name);
                }
            }
            Self {
                _guard: guard,
                original,
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (name, value) in self.original.drain(..) {
                if let Some(value) = value {
                    env::set_var(name, value);
                } else {
                    env::remove_var(name);
                }
            }
        }
    }

    #[test]
    fn noninteractive_missing_env_reports_repair_commands() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &[],
                env_inputs: ConnectEnvInputs::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect_err("missing env should fail");

        assert!(err.to_string().contains("TELEGRAM_BOT_TOKEN"));
        assert!(err.to_string().contains("lionclaw --home"));
        assert!(err.to_string().contains(&home.root().display().to_string()));
        assert!(err.to_string().contains("--env-file"));
        assert!(err.to_string().contains("--from-env TELEGRAM_BOT_TOKEN"));
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn env_file_rejects_undeclared_values_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(
            &env_file,
            "TELEGRAM_BOT_TOKEN=secret-token\nEXTRA_SECRET=do-not-store\n",
        )
        .expect("env file");
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &[],
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect_err("undeclared env should fail");

        assert!(err.to_string().contains("EXTRA_SECRET"));
        assert!(load_channel_env(&home, "telegram")
            .expect("load env")
            .is_empty());
    }

    #[test]
    fn env_file_accepts_declared_optional_values_without_requiring_them() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(
            &env_file,
            "TELEGRAM_BOT_TOKEN=secret-token\nTELEGRAM_POLL_MS=1000\n",
        )
        .expect("env file");
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &[
                    "TELEGRAM_POLL_MS".to_string(),
                    "TELEGRAM_TIMEOUT_MS".to_string(),
                ],
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect("declared optional env should be accepted");

        let stored = load_channel_env(&home, "telegram").expect("load env");
        assert_eq!(
            stored.get("TELEGRAM_BOT_TOKEN").map(String::as_str),
            Some("secret-token")
        );
        assert_eq!(
            stored.get("TELEGRAM_POLL_MS").map(String::as_str),
            Some("1000")
        );
        assert!(!stored.contains_key("TELEGRAM_TIMEOUT_MS"));
    }

    #[test]
    fn env_file_empty_optional_value_clears_stored_channel_env() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut existing = ChannelEnv::new();
        existing.insert("TELEGRAM_BOT_TOKEN".to_string(), "old-token".to_string());
        existing.insert("TELEGRAM_POLL_MS".to_string(), "1000".to_string());
        save_channel_env(&home, "telegram", &existing).expect("save env");
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(
            &env_file,
            "TELEGRAM_BOT_TOKEN=new-token\nTELEGRAM_POLL_MS=\n",
        )
        .expect("env file");
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &["TELEGRAM_POLL_MS".to_string()],
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect("env update should be accepted");

        let stored = load_channel_env(&home, "telegram").expect("load env");
        assert_eq!(
            stored.get("TELEGRAM_BOT_TOKEN").map(String::as_str),
            Some("new-token")
        );
        assert!(!stored.contains_key("TELEGRAM_POLL_MS"));
    }

    #[test]
    fn from_env_rejects_undeclared_names_before_reading_process_env() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &[],
                env_inputs: ConnectEnvInputs {
                    env_file: None,
                    from_env: vec!["EXTRA_SECRET".to_string()],
                    ..ConnectEnvInputs::default()
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect_err("undeclared env should fail");

        assert!(err.to_string().contains("EXTRA_SECRET"));
        assert!(!err.to_string().contains("is not set"));
    }

    #[test]
    fn stored_env_rejects_values_outside_channel_metadata_contract() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut env = ChannelEnv::new();
        env.insert("TELEGRAM_BOT_TOKEN".to_string(), "secret-token".to_string());
        env.insert("EXTRA_SECRET".to_string(), "do-not-expose".to_string());
        save_channel_env(&home, "telegram", &env).expect("save env");
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &[],
                env_inputs: ConnectEnvInputs::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect_err("stored extra env should fail");

        assert!(err.to_string().contains("EXTRA_SECRET"));
        assert!(!err.to_string().contains("do-not-expose"));
    }

    #[test]
    fn interactive_env_prompt_persists_without_echoing_value() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(b"secret-token\n".to_vec());
        let mut output = Vec::new();

        ensure_required_env(
            RequiredEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &["TELEGRAM_BOT_TOKEN".to_string()],
                optional_env: &[],
                env_inputs: ConnectEnvInputs::default(),
                interactive: true,
                hide_prompt_input: false,
            },
            &mut input,
            &mut output,
        )
        .expect("prompt env");

        let rendered = String::from_utf8(output).expect("utf8");
        assert!(rendered.contains("TELEGRAM_BOT_TOKEN"));
        assert!(!rendered.contains("secret-token"));
        let stored = load_channel_env(&home, "telegram").expect("load env");
        assert_eq!(
            stored.get("TELEGRAM_BOT_TOKEN").map(String::as_str),
            Some("secret-token")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_telegram_stores_env_and_starts_background_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let outcome = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut input,
            &mut output,
        )
        .await
        .expect("connect telegram");

        assert_eq!(outcome.channel_id, "telegram");
        assert_eq!(outcome.launch, ChannelLaunchMode::Background);
        assert_eq!(outcome.action, ConnectAction::BackgroundStarted);
        assert_eq!(
            load_channel_env(&home, "telegram")
                .expect("channel env")
                .get("TELEGRAM_BOT_TOKEN")
                .map(String::as_str),
            Some("secret-token")
        );
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(config.channels.iter().any(|channel| {
            channel.id == "telegram"
                && channel.skill == "telegram"
                && channel.launch_mode == ChannelLaunchMode::Background
                && channel.required_env == ["TELEGRAM_BOT_TOKEN"]
        }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn setup_generated_env_file_is_hardened_before_ingest() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("create base dirs");
        let skill_source = temp_dir.path().join("telegram-public-setup-env");
        write_channel_skill_with_public_setup_env(&skill_source);
        let discovered =
            discover_channel_skill(&home, skill_source.to_str().expect("utf8 skill source"))
                .expect("discover skill");

        let mut prepared = prepare_connect_env_inputs(PrepareConnectEnvRequest {
            home: &home,
            channel_id: "telegram",
            required_env: &discovered.metadata.env,
            optional_env: &discovered.metadata.optional_env,
            env_inputs: ConnectEnvInputs {
                setup_profile: Some("setup".to_string()),
                ..ConnectEnvInputs::default()
            },
            interactive: false,
            installed_skill_dir: &skill_source,
            discovered: &discovered,
        })
        .expect("prepare setup env");

        let mode = fs::metadata(channel_setup_env_file(&home, "telegram"))
            .expect("setup env metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        prepared
            .cleanup_generated_env(&home)
            .expect("cleanup setup env");
        prepared.commit(&home).expect("commit setup state");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn setup_hook_receives_only_contract_and_allowlisted_ambient_env() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        home.ensure_base_dirs().await.expect("create base dirs");
        let skill_source = temp_dir.path().join("telegram-setup-env-probe");
        write_channel_skill_with_setup_env_probe(&skill_source);
        let discovered =
            discover_channel_skill(&home, skill_source.to_str().expect("utf8 skill source"))
                .expect("discover skill");

        let mut prepared = {
            let _guard = EnvRestore::set([(
                "LIONCLAW_TEST_CHANNEL_SETUP_SECRET",
                Some("should-not-reach-setup"),
            )]);
            prepare_connect_env_inputs(PrepareConnectEnvRequest {
                home: &home,
                channel_id: "telegram",
                required_env: &discovered.metadata.env,
                optional_env: &discovered.metadata.optional_env,
                env_inputs: ConnectEnvInputs {
                    setup_profile: Some("testprofile".to_string()),
                    ..ConnectEnvInputs::default()
                },
                interactive: false,
                installed_skill_dir: &skill_source,
                discovered: &discovered,
            })
            .expect("prepare setup env")
        };

        prepared
            .cleanup_generated_env(&home)
            .expect("cleanup setup env");
        prepared.commit(&home).expect("commit setup state");
    }

    #[cfg(unix)]
    #[test]
    fn setup_hook_receives_absolute_contract_paths_with_relative_home() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let skill_source = temp_dir.path().join("telegram-relative-home-setup");
        fs::create_dir_all(skill_source.join("scripts")).expect("setup scripts dir");
        write_executable(
            skill_source.join("scripts/setup").as_path(),
            r#"#!/usr/bin/env bash
set -euo pipefail
test "${LIONCLAW_HOME:-}" = "${1:?}"
test "${LIONCLAW_CHANNEL_SETUP_ENV_FILE:-}" = "${2:?}"
test "${LIONCLAW_CHANNEL_SETUP_STATE_DIR:-}" = "${3:?}"
case "$LIONCLAW_HOME" in /*) ;; *) exit 44;; esac
case "$LIONCLAW_CHANNEL_SETUP_ENV_FILE" in /*) ;; *) exit 45;; esac
case "$LIONCLAW_CHANNEL_SETUP_STATE_DIR" in /*) ;; *) exit 46;; esac
"#,
        );
        let home = LionClawHome::new(PathBuf::from(".relative-lionclaw-home"));
        let env_file = channel_setup_env_file(&home, "telegram");
        let state_dir = channel_setup_state_dir(&home, "telegram");
        let current_dir = env::current_dir().expect("current dir");
        let setup_args = vec![
            current_dir.join(home.root()).display().to_string(),
            current_dir.join(&env_file).display().to_string(),
            current_dir.join(&state_dir).display().to_string(),
        ];
        let setup = ChannelSetupMetadata {
            command: "scripts/setup".to_string(),
            args: Vec::new(),
        };

        run_channel_setup_command(ChannelSetupRunRequest {
            skill_dir: &skill_source,
            setup: &setup,
            home: &home,
            channel_id: "telegram",
            setup_profile: None,
            setup_args: &setup_args,
            env_file: &env_file,
            state_dir: &state_dir,
        })
        .expect("setup command receives absolute paths");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_setup_hook_removes_generated_env_and_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let skill_source = temp_dir.path().join("telegram-failing-setup-state");
        write_channel_skill_with_failing_setup_state(&skill_source);
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs {
                    setup_profile: Some("testprofile".to_string()),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("failing setup hook should fail connect");

        assert!(err.to_string().contains("exited with"));
        assert!(!channel_setup_env_file(&home, "telegram").exists());
        assert!(!channel_setup_state_dir(&home, "telegram").exists());
        assert!(!home.channel_env_path("telegram").exists());
        assert!(!home.skills_dir().join("telegram").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_setup_hook_unlinks_symlinked_generated_outputs() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let outside_env = temp_dir.path().join("outside.env");
        let outside_state = temp_dir.path().join("outside-state");
        fs::write(&outside_env, "outside\n").expect("outside env");
        fs::create_dir_all(&outside_state).expect("outside state");
        fs::write(outside_state.join("token.json"), "outside\n").expect("outside token");
        let skill_source = temp_dir.path().join("telegram-symlinked-setup-outputs");
        write_channel_skill_with_symlinked_setup_outputs(&skill_source);
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs {
                    setup_args: vec![
                        outside_env.display().to_string(),
                        outside_state.display().to_string(),
                    ],
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("failing setup hook should fail connect");

        assert!(err.to_string().contains("exited with"));
        assert!(fs::symlink_metadata(channel_setup_env_file(&home, "telegram")).is_err());
        assert!(fs::symlink_metadata(channel_setup_state_dir(&home, "telegram")).is_err());
        assert_eq!(
            fs::read_to_string(&outside_env).expect("outside env"),
            "outside\n"
        );
        assert_eq!(
            fs::read_to_string(outside_state.join("token.json")).expect("outside token"),
            "outside\n"
        );
        assert!(!home.skills_dir().join("telegram").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rollback_copy_rejects_symlinked_destination() {
        use std::os::unix::fs::symlink;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let source = temp_dir.path().join("state");
        fs::create_dir_all(&source).expect("source");
        fs::write(source.join("token.json"), "secret\n").expect("source token");
        let outside = temp_dir.path().join("outside");
        fs::create_dir_all(&outside).expect("outside");
        let destination = temp_dir.path().join("backup");
        symlink(&outside, &destination).expect("destination symlink");

        let err = copy_regular_tree_for_rollback(&source, &destination, "channel setup state")
            .expect_err("symlinked rollback destination should fail");

        assert!(err.to_string().contains("must not be a symlink"));
        assert!(!outside.join("token.json").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_connect_after_setup_restores_previous_setup_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let state_file = channel_setup_state_dir(&home, "telegram").join("gmail/old.json");
        fs::create_dir_all(state_file.parent().expect("state parent")).expect("state dir");
        fs::write(&state_file, "old-state\n").expect("old state");
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve daemon bind");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.daemon.bind = format!(
            "127.0.0.1:{}",
            listener.local_addr().expect("listener addr").port()
        );
        config.daemon.bind_configured = true;
        config.save(&home).await.expect("save config");
        let skill_source = temp_dir.path().join("telegram-setup-state");
        write_channel_skill_with_setup_state(&skill_source);
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs {
                    setup_profile: Some("testprofile".to_string()),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("reserved non-LionClaw listener should block background startup");

        assert!(err.to_string().contains("non-LionClaw listener"));
        assert_eq!(
            fs::read_to_string(&state_file).expect("restored state"),
            "old-state\n"
        );
        assert!(
            !channel_setup_state_dir(&home, "telegram")
                .join("gmail/new.json")
                .exists(),
            "failed connect must not leave setup-created OAuth state"
        );
        assert!(!channel_setup_env_file(&home, "telegram").exists());
        assert!(!home.channel_env_path("telegram").exists());
        assert!(!home.skills_dir().join("telegram").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn successful_setup_replaces_previous_setup_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let old_state_file = channel_setup_state_dir(&home, "telegram").join("gmail/old.json");
        fs::create_dir_all(old_state_file.parent().expect("state parent")).expect("state dir");
        fs::write(&old_state_file, "old-state\n").expect("old state");
        let skill_source = temp_dir.path().join("telegram-setup-state-success");
        write_channel_skill_with_setup_state(&skill_source);
        let manager = FakeUnitManager::default();

        let outcome = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs {
                    setup_profile: Some("testprofile".to_string()),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect("connect with replacement setup state");

        assert_eq!(outcome.channel_id, "telegram");
        assert!(!old_state_file.exists());
        assert_eq!(
            fs::read_to_string(channel_setup_state_dir(&home, "telegram").join("gmail/new.json"))
                .expect("new state"),
            "new-state\n"
        );
        assert_eq!(
            load_channel_env(&home, "telegram")
                .expect("channel env")
                .get("TELEGRAM_BOT_TOKEN")
                .map(String::as_str),
            Some("setup-token")
        );
        assert!(!channel_setup_env_file(&home, "telegram").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_runs_declared_setup_hook_when_env_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let skill_source = temp_dir.path().join("telegram-setup");
        write_channel_skill_with_setup(&skill_source);
        let manager = FakeUnitManager::default();
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let outcome = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs {
                    setup_profile: Some("testprofile".to_string()),
                    setup_args: vec!["--flag".to_string(), "value".to_string()],
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut input,
            &mut output,
        )
        .await
        .expect("connect with setup hook");

        assert_eq!(outcome.channel_id, "telegram");
        let stored = load_channel_env(&home, "telegram").expect("channel env");
        assert_eq!(
            stored.get("TELEGRAM_BOT_TOKEN").map(String::as_str),
            Some("setup-token")
        );
        assert_eq!(
            stored.get("TELEGRAM_POLL_MS").map(String::as_str),
            Some("250")
        );
        assert!(
            !channel_setup_env_file(&home, "telegram").exists(),
            "generated setup env should be transient after validation"
        );
        assert!(channel_setup_state_dir(&home, "telegram")
            .join("check")
            .is_dir());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interactive_connect_runs_declared_setup_hook_without_profile() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let skill_source = temp_dir.path().join("telegram-interactive-setup");
        write_channel_skill_with_interactive_setup(&skill_source);
        let manager = FakeUnitManager::default();

        connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs::default(),
                contact: ChannelContactSetup::default(),
                interactive: true,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect("interactive connect with setup hook");

        assert_eq!(
            load_channel_env(&home, "telegram")
                .expect("channel env")
                .get("TELEGRAM_BOT_TOKEN")
                .map(String::as_str),
            Some("interactive-token")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interactive_connect_without_setup_hook_prompts_for_missing_env() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let skill_source = temp_dir.path().join("telegram-prompt-setup");
        write_channel_skill(&skill_source, "Telegram", "prompt");
        let manager = FakeUnitManager::default();
        let mut input = Cursor::new(b"prompt-token\n".to_vec());
        let mut output = Vec::new();

        let outcome = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs::default(),
                contact: ChannelContactSetup::default(),
                interactive: true,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut input,
            &mut output,
        )
        .await
        .expect("interactive connect with env prompt");

        assert_eq!(outcome.channel_id, "telegram");
        assert_eq!(
            load_channel_env(&home, "telegram")
                .expect("channel env")
                .get("TELEGRAM_BOT_TOKEN")
                .map(String::as_str),
            Some("prompt-token")
        );
        let rendered = String::from_utf8(output).expect("utf8");
        assert!(rendered.contains("TELEGRAM_BOT_TOKEN"));
        assert!(!rendered.contains("prompt-token"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_background_connect_does_not_record_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let manager = FakeUnitManager::default();
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs::default(),
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut input,
            &mut output,
        )
        .await
        .expect_err("missing env should fail");

        assert!(err.to_string().contains("TELEGRAM_BOT_TOKEN"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
        assert!(
            !home.skills_dir().join("telegram").exists(),
            "failed connect must not leave an unbound channel skill visible to runtimes"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_contact_requires_project_instance_target() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs::default(),
                contact: ChannelContactSetup {
                    enabled: true,
                    conversation_ref: Some("member:reviewer".to_string()),
                    thread_ref: None,
                    instance_name: None,
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("contact without project instance should fail");

        assert!(err
            .to_string()
            .contains("requires a project instance target"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_contact_stores_explicit_route() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();

        connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup {
                    enabled: true,
                    conversation_ref: Some("member:reviewer".to_string()),
                    thread_ref: Some("thread-a".to_string()),
                    instance_name: Some("reviewer".to_string()),
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect("connect telegram contact");

        let config = OperatorConfig::load(&home).await.expect("load config");
        let channel = config
            .channels
            .iter()
            .find(|channel| channel.id == "telegram")
            .expect("telegram channel");
        let contact = channel.contact.as_ref().expect("contact");
        assert_eq!(contact.conversation_ref.as_deref(), Some("member:reviewer"));
        assert_eq!(contact.thread_ref.as_deref(), Some("thread-a"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_contact_connect_restores_previous_preferred_contact() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let old_skill = temp_dir.path().join("email-skill");
        write_channel_skill_with_id(&old_skill, "Email", "email", "old email snapshot");
        add_skill(
            &home,
            "email".to_string(),
            old_skill.display().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install email skill");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.upsert_channel(ManagedChannelConfig {
            id: "email".to_string(),
            skill: "email".to_string(),
            launch_mode: ChannelLaunchMode::Background,
            worker: crate::operator::channel_metadata::DEFAULT_CHANNEL_WORKER.to_string(),
            required_env: Vec::new(),
            optional_env: Vec::new(),
            contact: Some(ChannelContactConfig::new(
                "reviewer@example.com".to_string(),
                Some("inbox".to_string()),
            )),
        });
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve daemon bind");
        config.daemon.bind = format!(
            "127.0.0.1:{}",
            listener.local_addr().expect("listener addr").port()
        );
        config.daemon.bind_configured = true;
        config.save(&home).await.expect("save config");
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();

        connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup {
                    enabled: true,
                    conversation_ref: Some("member:reviewer".to_string()),
                    thread_ref: None,
                    instance_name: Some("reviewer".to_string()),
                },
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("reserved non-LionClaw listener should block background startup");

        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
        let email = config
            .channels
            .iter()
            .find(|channel| channel.id == "email")
            .expect("email channel");
        let contact = email.contact.as_ref().expect("restored contact");
        assert_eq!(
            contact.conversation_ref.as_deref(),
            Some("reviewer@example.com")
        );
        assert_eq!(contact.thread_ref.as_deref(), Some("inbox"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn background_connect_rejects_empty_required_env_without_persisting_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=\n").expect("env file");
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("empty required env should fail");

        assert!(err.to_string().contains("TELEGRAM_BOT_TOKEN"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
        assert!(!home.skills_dir().join("telegram").exists());
        assert!(
            !home.channel_env_path("telegram").exists(),
            "failed connect must not keep empty required env"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_rejects_existing_normal_skill_alias_collision() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let normal_skill = temp_dir.path().join("normal-telegram");
        write_normal_skill(&normal_skill, "Normal Telegram", "normal snapshot");
        add_skill(
            &home,
            "telegram".to_string(),
            normal_skill.display().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install normal skill");
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("normal skill alias collision should fail");

        assert!(err
            .to_string()
            .contains("skill alias 'telegram' already exists"));
        let installed = fs::read_to_string(home.skills_dir().join("telegram/SKILL.md"))
            .expect("installed skill");
        assert!(installed.contains("Normal Telegram"));
        assert!(installed.contains("normal snapshot"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_background_start_rolls_back_channel_skill_and_env() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve daemon bind");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.daemon.bind = format!(
            "127.0.0.1:{}",
            listener.local_addr().expect("listener addr").port()
        );
        config.daemon.bind_configured = true;
        config.save(&home).await.expect("save config");
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("reserved non-LionClaw listener should block background startup");

        assert!(err.to_string().contains("non-LionClaw listener"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
        assert!(
            !home.skills_dir().join("telegram").exists(),
            "failed background startup must roll back the installed channel skill"
        );
        assert!(
            !home.channel_env_path("telegram").exists(),
            "failed background startup must roll back newly stored channel env"
        );
        assert!(load_channel_env(&home, "telegram")
            .expect("channel env")
            .is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_partial_background_start_stops_units_and_rolls_back_state() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();
        manager
            .fail_up_after_started(1)
            .expect("configure start failure");

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("configured unit start failure should fail");

        assert!(err.to_string().contains("configured unit start failure"));
        let identity = ensure_unit_identity(&home).expect("unit identity");
        let daemon_unit = daemon_unit_name(&identity);
        let channel_unit = channel_unit_name(&identity, "telegram");
        assert_eq!(
            manager
                .unit_status(&daemon_unit)
                .await
                .expect("daemon status"),
            "loaded/inactive/dead"
        );
        assert_eq!(
            manager
                .unit_status(&channel_unit)
                .await
                .expect("channel status"),
            "loaded/inactive/dead"
        );
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
        assert!(!home.skills_dir().join("telegram").exists());
        assert!(!home.channel_env_path("telegram").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_path_background_start_restores_existing_skill_snapshot() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let old_skill = temp_dir.path().join("old-telegram");
        write_channel_skill(&old_skill, "Old Telegram", "old snapshot");
        add_skill(
            &home,
            "telegram".to_string(),
            old_skill.display().to_string(),
            "local".to_string(),
        )
        .await
        .expect("install old skill");

        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve daemon bind");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.daemon.bind = format!(
            "127.0.0.1:{}",
            listener.local_addr().expect("listener addr").port()
        );
        config.daemon.bind_configured = true;
        config.save(&home).await.expect("save config");
        let new_skill = temp_dir.path().join("new-telegram");
        write_channel_skill(&new_skill, "New Telegram", "new snapshot");
        let env_file = temp_dir.path().join("telegram.env");
        fs::write(&env_file, "TELEGRAM_BOT_TOKEN=secret-token\n").expect("env file");
        let manager = FakeUnitManager::default();

        connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: new_skill.to_str().expect("utf8 path"),
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect_err("reserved non-LionClaw listener should block background startup");

        let restored_skill = fs::read_to_string(home.skills_dir().join("telegram/SKILL.md"))
            .expect("restored skill");
        assert!(restored_skill.contains("Old Telegram"));
        assert!(restored_skill.contains("old snapshot"));
        assert!(home
            .skills_dir()
            .join("telegram/.lionclaw-skill.toml")
            .exists());
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(!config
            .channels
            .iter()
            .any(|channel| channel.id == "telegram"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_background_restarts_active_channel_when_env_changes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let first_env_file = temp_dir.path().join("telegram-first.env");
        fs::write(&first_env_file, "TELEGRAM_BOT_TOKEN=old-token\n").expect("first env file");
        let second_env_file = temp_dir.path().join("telegram-second.env");
        fs::write(&second_env_file, "TELEGRAM_BOT_TOKEN=new-token\n").expect("second env file");
        let manager = FakeUnitManager::default();

        connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(first_env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect("first connect");
        let identity = ensure_unit_identity(&home).expect("unit identity");
        let channel_unit = channel_unit_name(&identity, "telegram");
        assert!(
            !manager.was_restarted(&channel_unit).expect("restart state"),
            "initial start should not restart the channel"
        );

        connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: "telegram",
                env_inputs: ConnectEnvInputs {
                    env_file: Some(second_env_file),
                    from_env: Vec::new(),
                    ..ConnectEnvInputs::default()
                },
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
        )
        .await
        .expect("second connect");

        assert!(
            manager.was_restarted(&channel_unit).expect("restart state"),
            "active background channel should restart after env changes"
        );
        assert_eq!(
            load_channel_env(&home, "telegram")
                .expect("channel env")
                .get("TELEGRAM_BOT_TOKEN")
                .map(String::as_str),
            Some("new-token")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_explicit_interactive_channel_configures_before_attaching() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
        let skill_source = temp_dir.path().join("channel-fixture");
        fs::create_dir_all(skill_source.join("scripts")).expect("scripts dir");
        fs::write(
            skill_source.join("SKILL.md"),
            "---\nname: channel-fixture\ndescription: test channel\n---\n",
        )
        .expect("skill md");
        fs::write(
            skill_source.join("lionclaw.toml"),
            r#"version = 1

[channel]
id = "test-channel"
launch = "interactive"
worker = "scripts/worker"
optional_env = ["OPTIONAL_PATH"]
"#,
        )
        .expect("channel metadata");
        write_executable(
            skill_source.join("scripts/worker").as_path(),
            "#!/usr/bin/env bash\n",
        );
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve daemon bind");
        let mut config = OperatorConfig::load(&home).await.expect("load config");
        config.daemon.bind = format!(
            "127.0.0.1:{}",
            listener.local_addr().expect("listener addr").port()
        );
        config.save(&home).await.expect("save config");
        let manager = FakeUnitManager::default();
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = connect_channel_with_binaries(
            ConnectChannelRequest {
                home: &home,
                manager: &manager,
                project_root: None,
                work_root: temp_dir.path(),
                channel_or_path: skill_source.to_str().expect("utf8 skill source"),
                env_inputs: ConnectEnvInputs::default(),
                contact: ChannelContactSetup::default(),
                interactive: false,
                hide_prompt_input: false,
            },
            &binaries(),
            &mut input,
            &mut output,
        )
        .await
        .expect_err("reserved non-LionClaw listener should block attach");

        assert!(err.to_string().contains("non-LionClaw listener"));
        let config = OperatorConfig::load(&home).await.expect("load config");
        assert!(config.channels.iter().any(|channel| {
            channel.id == "test-channel"
                && channel.skill == "test-channel"
                && channel.launch_mode == ChannelLaunchMode::Interactive
                && channel.optional_env == ["OPTIONAL_PATH"]
        }));
    }
}
