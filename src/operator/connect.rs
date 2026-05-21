use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, ErrorKind, Write},
    path::{Path, PathBuf},
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
            discover_channel_skill, validate_channel_env_name, ChannelSkillSource,
            DiscoveredChannelSkill,
        },
        command_display::lionclaw_home_command_prefix,
        config::{ChannelLaunchMode, ManagedChannelConfig, OperatorConfig},
        managed_units::UnitManager,
        private_paths::{private_file_exists, remove_private_file_if_exists},
        reconcile::{
            add_channel_with_worker, add_skill, resolve_stack_binaries, up_for_work_root,
            StackBinaryPaths,
        },
        runtime::resolve_runtime_id,
    },
};

#[derive(Debug, Clone, Default)]
pub struct ConnectEnvInputs {
    pub env_file: Option<PathBuf>,
    pub from_env: Vec<String>,
}

pub struct ConnectChannelRequest<'a, M> {
    pub home: &'a LionClawHome,
    pub manager: &'a M,
    pub project_root: Option<&'a Path>,
    pub work_root: &'a Path,
    pub channel_or_path: &'a str,
    pub env_inputs: ConnectEnvInputs,
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
    let previous_channel = config
        .channels
        .iter()
        .find(|channel| channel.id == channel_id)
        .cloned();
    let rollback = ConnectRollback::capture(home, &channel_id, previous_channel, &discovered)?;
    let required_env = match ensure_required_env(
        RequiredEnvRequest {
            home,
            channel_id: &channel_id,
            required_env: &discovered.metadata.env,
            env_inputs,
            interactive,
            hide_prompt_input,
        },
        input,
        output,
    ) {
        Ok(outcome) => outcome,
        Err(err) => return Err(rollback.restore_channel_env_only(home, &channel_id, err)),
    };
    let skill_alias = match install_or_select_skill(home, &discovered, &config).await {
        Ok(skill_alias) => skill_alias,
        Err(err) => return rollback_all_and_return(home, &channel_id, rollback, err).await,
    };
    if let Err(err) = add_channel_with_worker(
        home,
        channel_id.clone(),
        skill_alias.clone(),
        discovered.metadata.launch,
        discovered.metadata.worker.clone(),
        discovered.metadata.env.clone(),
    )
    .await
    {
        return rollback_all_and_return(home, &channel_id, rollback, err).await;
    }

    let action = match discovered.metadata.launch {
        ChannelLaunchMode::Interactive => {
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
                return rollback_all_and_return(home, &channel_id, rollback, err).await;
            }
            if required_env.changed && channel_was_active {
                if let Err(err) = restart_background_channel(home, manager, &channel_id).await {
                    return rollback_all_and_return(home, &channel_id, rollback, err).await;
                }
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

struct ConnectRollback {
    previous_channel: Option<ManagedChannelConfig>,
    previous_env: ChannelEnvSnapshot,
    skill: SkillRollback,
}

impl ConnectRollback {
    fn capture(
        home: &LionClawHome,
        channel_id: &str,
        previous_channel: Option<ManagedChannelConfig>,
        discovered: &DiscoveredChannelSkill,
    ) -> Result<Self> {
        Ok(Self {
            previous_channel,
            previous_env: ChannelEnvSnapshot::capture(home, channel_id)?,
            skill: SkillRollback::capture(home, discovered)?,
        })
    }

    fn restore_channel_env_only(
        self,
        home: &LionClawHome,
        channel_id: &str,
        err: anyhow::Error,
    ) -> anyhow::Error {
        if let Err(rollback_err) = self
            .previous_env
            .restore(home, channel_id)
            .and_then(|()| self.skill.discard())
        {
            return anyhow!(
                "{err}; additionally failed to roll back partial channel env state: {rollback_err}"
            );
        }
        err
    }

    async fn rollback_all(self, home: &LionClawHome, channel_id: &str) -> Result<()> {
        restore_channel_config(home, channel_id, self.previous_channel).await?;
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
    channel_id: &str,
    previous_channel: Option<ManagedChannelConfig>,
) -> Result<()> {
    let mut config = OperatorConfig::load(home).await?;
    match previous_channel {
        Some(channel) => {
            config.upsert_channel(channel);
            config.save(home).await?;
        }
        None => {
            if config.remove_channel(channel_id) {
                config.save(home).await?;
            }
        }
    }
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
    fs::create_dir_all(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
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
            bail!(
                "skill snapshot entry {} must not be a symlink",
                path.display()
            );
        }
        if metadata.is_dir() {
            copy_skill_snapshot_for_rollback(&path, &target)?;
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
                "skill snapshot entry {} is not a regular file or directory",
                path.display()
            );
        }
    }
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
    env_inputs: ConnectEnvInputs,
    interactive: bool,
    hide_prompt_input: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RequiredEnvOutcome {
    changed: bool,
}

fn ensure_required_env<R: BufRead, W: Write>(
    request: RequiredEnvRequest<'_>,
    input: &mut R,
    output: &mut W,
) -> Result<RequiredEnvOutcome> {
    let RequiredEnvRequest {
        home,
        channel_id,
        required_env,
        env_inputs,
        interactive,
        hide_prompt_input,
    } = request;
    let mut updates = ChannelEnv::new();
    let mut changed = false;
    if let Some(path) = env_inputs.env_file.as_deref() {
        let file_updates = parse_env_file(path)?;
        validate_declared_env_updates(channel_id, required_env, &file_updates, "env file")?;
        updates.extend(file_updates);
    }
    validate_declared_env_input_names(channel_id, required_env, &env_inputs.from_env)?;
    updates.extend(collect_from_process_env(&env_inputs.from_env)?);
    validate_no_undeclared_channel_env(home, channel_id, required_env)?;
    if !updates.is_empty() {
        changed |= merge_changed_channel_env(home, channel_id, &updates)?;
    }

    let stored = load_channel_env(home, channel_id)?;
    validate_no_undeclared_channel_env(home, channel_id, required_env)?;
    let missing = missing_required_env(&stored, required_env)?;
    if missing.is_empty() {
        return Ok(RequiredEnvOutcome { changed });
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
    changed |= merge_changed_channel_env(home, channel_id, &prompted)?;
    let stored = load_channel_env(home, channel_id)?;
    validate_no_undeclared_channel_env(home, channel_id, required_env)?;
    let missing = missing_required_env(&stored, required_env)?;
    if missing.is_empty() {
        Ok(RequiredEnvOutcome { changed })
    } else {
        Err(anyhow!(render_missing_env_repair(
            &repair_command,
            channel_id,
            &missing
        )))
    }
}

fn merge_changed_channel_env(
    home: &LionClawHome,
    channel_id: &str,
    updates: &ChannelEnv,
) -> Result<bool> {
    let existing = load_channel_env(home, channel_id)?;
    let changed = updates
        .iter()
        .any(|(key, value)| existing.get(key) != Some(value));
    if changed {
        merge_channel_env(home, channel_id, updates)?;
    }
    Ok(changed)
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
    updates: &ChannelEnv,
    source: &str,
) -> Result<()> {
    let declared = declared_env_set(required_env)?;
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
    input_names: &[String],
) -> Result<()> {
    let declared = declared_env_set(required_env)?;
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

fn declared_env_set(required_env: &[String]) -> Result<BTreeSet<&str>> {
    let mut declared = BTreeSet::new();
    for key in required_env {
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
    use std::{fs, io::Cursor, net::TcpListener, path::Path};

    use super::{
        connect_channel_with_binaries, ensure_required_env, ConnectAction, ConnectChannelRequest,
        ConnectEnvInputs, RequiredEnvRequest,
    };
    use crate::{
        home::LionClawHome,
        kernel::runtime::{ConfinementConfig, OciConfinementConfig},
        operator::{
            channel_env::{load_channel_env, save_channel_env, ChannelEnv},
            config::{ChannelLaunchMode, OperatorConfig, RuntimeProfileConfig},
            managed_units::{
                channel_unit_name, daemon_unit_name, ensure_unit_identity, FakeUnitManager,
                UnitManager,
            },
            reconcile::{add_skill, StackBinaryPaths},
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
        fs::create_dir_all(root.join("scripts")).expect("scripts dir");
        fs::write(
            root.join("SKILL.md"),
            format!("---\nname: {skill_name}\ndescription: test channel\n---\n{token}\n"),
        )
        .expect("skill md");
        fs::write(
            root.join("lionclaw.toml"),
            r#"version = 1

[channel]
id = "telegram"
launch = "background"
worker = "scripts/worker"
env = ["TELEGRAM_BOT_TOKEN"]
"#,
        )
        .expect("channel metadata");
        write_executable(
            root.join("scripts/worker").as_path(),
            "#!/usr/bin/env bash\n",
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
                env_inputs: ConnectEnvInputs {
                    env_file: Some(env_file),
                    from_env: Vec::new(),
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
                env_inputs: ConnectEnvInputs {
                    env_file: None,
                    from_env: vec!["EXTRA_SECRET".to_string()],
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
                },
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
                },
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
                },
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
                },
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
                },
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
                },
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
    async fn connect_terminal_configures_interactive_channel_before_attaching() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join(".lionclaw"));
        seed_configured_runtime(&home, temp_dir.path()).await;
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
                channel_or_path: "terminal",
                env_inputs: ConnectEnvInputs::default(),
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
            channel.id == "terminal"
                && channel.skill == "terminal"
                && channel.launch_mode == ChannelLaunchMode::Interactive
        }));
    }
}
