use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};

use crate::{
    home::LionClawHome,
    operator::{
        attach::attach_channel,
        channel_env::{
            collect_from_process_env, load_channel_env, merge_channel_env, missing_required_env,
            parse_env_file, render_missing_env_repair, ChannelEnv,
        },
        channel_metadata::{discover_channel_skill, ChannelSkillSource, DiscoveredChannelSkill},
        config::{ChannelLaunchMode, OperatorConfig},
        reconcile::{add_channel_with_worker, add_skill, resolve_stack_binaries, up_for_work_root},
        runtime::resolve_runtime_id,
        services::ServiceManager,
    },
};

#[derive(Debug, Clone, Default)]
pub struct ConnectEnvInputs {
    pub env_file: Option<PathBuf>,
    pub from_env: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectAction {
    InteractiveAttach,
    ServiceStarted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectOutcome {
    pub channel_id: String,
    pub skill_alias: String,
    pub launch: ChannelLaunchMode,
    pub action: ConnectAction,
}

pub async fn connect_channel<R, W, M>(
    home: &LionClawHome,
    manager: &M,
    work_root: &Path,
    channel_or_path: &str,
    env_inputs: ConnectEnvInputs,
    interactive: bool,
    hide_prompt_input: bool,
    input: &mut R,
    output: &mut W,
) -> Result<ConnectOutcome>
where
    R: BufRead,
    W: Write,
    M: ServiceManager,
{
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
    let skill_alias = install_or_select_skill(home, &discovered).await?;
    add_channel_with_worker(
        home,
        channel_id.clone(),
        skill_alias.clone(),
        discovered.metadata.launch,
        discovered.metadata.worker.clone(),
        discovered.metadata.env.clone(),
    )
    .await?;

    ensure_required_env(
        home,
        &channel_id,
        &discovered.metadata.env,
        env_inputs,
        interactive,
        hide_prompt_input,
        input,
        output,
    )?;

    let action = match discovered.metadata.launch {
        ChannelLaunchMode::Interactive => {
            attach_channel(home, manager, work_root, channel_id.clone(), None, None).await?;
            ConnectAction::InteractiveAttach
        }
        ChannelLaunchMode::Service => {
            let binaries = resolve_stack_binaries()?;
            up_for_work_root(home, manager, &runtime_id, &binaries, work_root).await?;
            ConnectAction::ServiceStarted
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
) -> Result<String> {
    match &discovered.source {
        ChannelSkillSource::Installed { alias } => Ok(alias.clone()),
        ChannelSkillSource::Path | ChannelSkillSource::Bundled => {
            let alias = discovered.metadata.id.clone();
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

fn ensure_required_env<R: BufRead, W: Write>(
    home: &LionClawHome,
    channel_id: &str,
    required_env: &[String],
    env_inputs: ConnectEnvInputs,
    interactive: bool,
    hide_prompt_input: bool,
    input: &mut R,
    output: &mut W,
) -> Result<()> {
    let mut updates = ChannelEnv::new();
    if let Some(path) = env_inputs.env_file.as_deref() {
        updates.extend(parse_env_file(path)?);
    }
    updates.extend(collect_from_process_env(&env_inputs.from_env)?);
    if !updates.is_empty() {
        merge_channel_env(home, channel_id, &updates)?;
    }

    let stored = load_channel_env(home, channel_id)?;
    let missing = missing_required_env(&stored, required_env)?;
    if missing.is_empty() {
        return Ok(());
    }
    if !interactive {
        return Err(anyhow!(render_missing_env_repair(channel_id, &missing)));
    }

    let prompted = prompt_required_env(channel_id, &missing, hide_prompt_input, input, output)?;
    merge_channel_env(home, channel_id, &prompted)?;
    let stored = load_channel_env(home, channel_id)?;
    let missing = missing_required_env(&stored, required_env)?;
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(render_missing_env_repair(channel_id, &missing)))
    }
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
            let _ = tcsetattr(self.tty, OptionalActions::Now, &self.original);
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
    use std::io::Cursor;

    use super::{ensure_required_env, ConnectEnvInputs};
    use crate::{home::LionClawHome, operator::channel_env::load_channel_env};

    #[test]
    fn noninteractive_missing_env_reports_repair_commands() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();

        let err = ensure_required_env(
            &home,
            "telegram",
            &["TELEGRAM_BOT_TOKEN".to_string()],
            ConnectEnvInputs::default(),
            false,
            false,
            &mut input,
            &mut output,
        )
        .expect_err("missing env should fail");

        assert!(err.to_string().contains("TELEGRAM_BOT_TOKEN"));
        assert!(err.to_string().contains("--env-file"));
        assert!(err.to_string().contains("--from-env TELEGRAM_BOT_TOKEN"));
        assert!(!err.to_string().contains("secret"));
    }

    #[test]
    fn interactive_env_prompt_persists_without_echoing_value() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let home = LionClawHome::new(temp_dir.path().join("home"));
        let mut input = Cursor::new(b"secret-token\n".to_vec());
        let mut output = Vec::new();

        ensure_required_env(
            &home,
            "telegram",
            &["TELEGRAM_BOT_TOKEN".to_string()],
            ConnectEnvInputs::default(),
            true,
            false,
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
}
