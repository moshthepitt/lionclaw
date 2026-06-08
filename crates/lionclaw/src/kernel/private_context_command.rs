use std::{
    cmp,
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
};

#[cfg(unix)]
use rustix::process::{kill_process_group, Pid, Signal};

pub(crate) const PRIVATE_CONTEXT_STDERR_MAX_BYTES: usize = 4096;

#[cfg(unix)]
pub(crate) type PrivateContextProcessGroup = Pid;

#[cfg(not(unix))]
pub(crate) type PrivateContextProcessGroup = ();

pub(crate) struct PrivateContextCommandConfig<'a> {
    pub(crate) skill_root: &'a Path,
    pub(crate) state_dir: &'a Path,
    pub(crate) private_context_id: &'a str,
    pub(crate) private_context_id_env: &'static str,
}

pub(crate) fn configure_private_context_command(
    command: &mut Command,
    config: &PrivateContextCommandConfig<'_>,
) {
    command
        .current_dir(config.skill_root)
        .kill_on_drop(true)
        .env_clear()
        .envs(private_context_ambient_env())
        .env(config.private_context_id_env, config.private_context_id)
        .env("LIONCLAW_SKILL_STATE_DIR", config.state_dir);
    configure_private_context_process_group(command);
}

pub(crate) fn ensure_private_context_state_dir(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {
                current.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                anyhow::bail!("state directory {} contains '..'", path.display());
            }
            Component::Normal(name) => {
                current.push(name);
                ensure_private_context_state_component(&current, current == path)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn child_process_group(child: &Child) -> Option<PrivateContextProcessGroup> {
    child_process_group_for_platform(child)
}

pub(crate) fn terminate_private_context_process_group(
    process_group: &mut Option<PrivateContextProcessGroup>,
) {
    terminate_private_context_process_group_for_platform(process_group);
}

pub(crate) fn spawn_bounded_output_reader<R>(
    mut reader: R,
    max_bytes: usize,
) -> tokio::task::JoinHandle<Vec<u8>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut captured = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read = match reader.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            let remaining = max_bytes.saturating_sub(captured.len());
            if remaining > 0 {
                let take = cmp::min(read, remaining);
                captured.extend(buffer.iter().take(take).copied());
            }
        }
        captured
    })
}

fn private_context_ambient_env() -> Vec<(String, OsString)> {
    let mut env = Vec::new();
    for key in ["PATH", "PATHEXT", "SYSTEMROOT", "SystemRoot"] {
        if let Some(value) = std::env::var_os(key) {
            env.push((key.to_string(), value));
        }
    }
    env
}

fn ensure_private_context_state_component(path: &Path, harden_existing: bool) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                anyhow::bail!("state directory {} must not be a symlink", path.display());
            }
            if !metadata.is_dir() {
                anyhow::bail!("state directory {} is not a directory", path.display());
            }
            if harden_existing {
                harden_private_context_state_dir(path, &metadata)?;
            }
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| format!("failed to create {}", path.display()))?;
            let metadata = fs::symlink_metadata(path)
                .with_context(|| format!("failed to stat {}", path.display()))?;
            harden_private_context_state_dir(path, &metadata)
        }
        Err(err) => Err(err).with_context(|| format!("failed to stat {}", path.display())),
    }
}

#[cfg(unix)]
fn harden_private_context_state_dir(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o077 != 0 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn harden_private_context_state_dir(_path: &Path, _metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

fn configure_private_context_process_group(command: &mut Command) {
    configure_private_context_process_group_for_platform(command);
}

#[cfg(unix)]
fn configure_private_context_process_group_for_platform(command: &mut Command) {
    // This is the v1 cleanup boundary; helper subprocesses must not detach from it.
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_private_context_process_group_for_platform(_command: &mut Command) {}

#[cfg(unix)]
fn child_process_group_for_platform(child: &Child) -> Option<PrivateContextProcessGroup> {
    child
        .id()
        .and_then(|pid| i32::try_from(pid).ok())
        .and_then(Pid::from_raw)
}

#[cfg(not(unix))]
fn child_process_group_for_platform(_child: &Child) -> Option<PrivateContextProcessGroup> {
    None
}

#[cfg(unix)]
fn terminate_private_context_process_group_for_platform(
    process_group: &mut Option<PrivateContextProcessGroup>,
) {
    if let Some(process_group) = process_group.take() {
        let _group_kill_result = kill_process_group(process_group, Signal::KILL);
    }
}

#[cfg(not(unix))]
fn terminate_private_context_process_group_for_platform(
    _process_group: &mut Option<PrivateContextProcessGroup>,
) {
}
