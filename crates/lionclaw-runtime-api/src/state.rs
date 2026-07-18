use std::{
    ffi::OsString,
    fs::File,
    io::Read,
    path::{Component, Path},
};

use anyhow::{anyhow, Context, Result};
use lionclaw_durable_fs::write_file_atomically;
use rustix::{
    fs::{open, openat, unlinkat, AtFlags, Mode, OFlags},
    io::Errno,
};

pub const RUNTIME_SESSION_READY_MARKER: &str = ".lionclaw-runtime-session";
const RECONSTRUCTED_RESUME_MODE: &str = "reconstructed";
const RESUMED_RESUME_MODE: &str = "resumed";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeSessionReady {
    marker_present: bool,
}

impl RuntimeSessionReady {
    pub const fn not_ready() -> Self {
        Self {
            marker_present: false,
        }
    }

    pub fn from_runtime_state_root(runtime_state_root: &Path) -> Result<Self> {
        Ok(Self {
            marker_present: runtime_session_ready_marker_exists(runtime_state_root)?,
        })
    }

    pub const fn is_ready(self) -> bool {
        self.marker_present
    }
}

pub fn load_ready_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    label: &str,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    if !runtime_session_ready.is_ready() {
        return Ok(None);
    }
    load_state_value(runtime_state_root, file_name, label)
}

pub fn runtime_session_ready_marker_exists(runtime_state_root: &Path) -> Result<bool> {
    let marker_path = runtime_state_root.join(RUNTIME_SESSION_READY_MARKER);
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(false);
    };
    let marker = match openat(
        &root,
        RUNTIME_SESSION_READY_MARKER,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(marker) => File::from(marker),
        Err(Errno::NOENT) => return Ok(false),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "runtime session marker '{}' cannot be a symlink",
                marker_path.display()
            ))
        }
        Err(err) => {
            return Err(err).with_context(|| format!("failed to open {}", marker_path.display()))
        }
    };
    let metadata = marker
        .metadata()
        .with_context(|| format!("failed to stat {}", marker_path.display()))?;
    Ok(metadata.is_file())
}

/// Records the adapter-observed mode in mission-private conversation state.
/// The same file remains the commit marker which permits native state to be
/// considered by the next process.
pub fn record_runtime_resume_mode(
    runtime_state_root: &Path,
    mode: crate::RuntimeResumeMode,
) -> Result<()> {
    let value = match mode {
        crate::RuntimeResumeMode::Reconstructed => RECONSTRUCTED_RESUME_MODE,
        crate::RuntimeResumeMode::Resumed => RESUMED_RESUME_MODE,
    };
    save_state_value(
        runtime_state_root,
        RUNTIME_SESSION_READY_MARKER,
        value,
        "runtime resume mode",
    )
}

/// Reads the last adapter-observed mode without exposing native identity.
pub fn recorded_runtime_resume_mode(
    runtime_state_root: &Path,
) -> Result<Option<crate::RuntimeResumeMode>> {
    match load_state_value(
        runtime_state_root,
        RUNTIME_SESSION_READY_MARKER,
        "runtime resume mode",
    )?
    .as_deref()
    {
        None => Ok(None),
        Some(RECONSTRUCTED_RESUME_MODE) => Ok(Some(crate::RuntimeResumeMode::Reconstructed)),
        Some(RESUMED_RESUME_MODE) => Ok(Some(crate::RuntimeResumeMode::Resumed)),
        Some(value) => Err(anyhow!("unknown recorded runtime resume mode '{value}'")),
    }
}

pub fn load_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    label: &str,
) -> Result<Option<String>> {
    let target_name = state_file_name(file_name)?;
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(None);
    };
    let file = match openat(
        &root,
        &target_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(Errno::NOENT) => return Ok(None),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} state file '{}' cannot be a symlink",
                runtime_state_root.join(file_name).display()
            ))
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to open {label} state file '{}' in '{}': {err}",
                file_name,
                runtime_state_root.display()
            ))
        }
    };

    let mut file = File::from(file);
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?
        .is_file()
    {
        return Ok(None);
    }

    let mut contents = String::new();
    file.read_to_string(&mut contents).with_context(|| {
        format!(
            "failed to read {label} state file '{}' in '{}'",
            file_name,
            runtime_state_root.display()
        )
    })?;
    normalize_state_value(contents, label)
}

pub fn save_state_value(
    runtime_state_root: &Path,
    file_name: &str,
    value: &str,
    label: &str,
) -> Result<()> {
    let target_name = state_file_name(file_name)?;
    let value = match normalize_state_value(value, label)? {
        Some(value) => value,
        None => return Ok(()),
    };
    let root = open_state_root(runtime_state_root)?;
    validate_existing_state_file(&root, runtime_state_root, file_name, &target_name, label)?;

    let mut contents = value.into_bytes();
    contents.push(b'\n');
    write_file_atomically(
        &root,
        runtime_state_root,
        &target_name,
        &contents,
        0o600,
        None,
        &format!("{label} state file"),
    )
}

pub fn clear_state_value(runtime_state_root: &Path, file_name: &str, label: &str) -> Result<()> {
    let target_name = state_file_name(file_name)?;
    let Some(root) = open_existing_state_root(runtime_state_root)? else {
        return Ok(());
    };
    validate_existing_state_file(&root, runtime_state_root, file_name, &target_name, label)?;
    match unlinkat(&root, &target_name, AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => Ok(()),
        Err(err) => Err(anyhow!(
            "failed to remove {label} state file '{}' in '{}': {err}",
            file_name,
            runtime_state_root.display()
        )),
    }
}

fn validate_existing_state_file(
    root: &File,
    runtime_state_root: &Path,
    file_name: &str,
    target_name: &OsString,
    label: &str,
) -> Result<()> {
    let file = match openat(
        root,
        target_name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(file) => file,
        Err(Errno::NOENT) => return Ok(()),
        Err(Errno::LOOP) => {
            return Err(anyhow!(
                "{label} state file '{}' cannot be a symlink",
                runtime_state_root.join(file_name).display()
            ))
        }
        Err(err) => {
            return Err(anyhow!(
                "failed to open existing {label} state file '{}' in '{}': {err}",
                file_name,
                runtime_state_root.display()
            ))
        }
    };
    let file = File::from(file);
    if !file
        .metadata()
        .with_context(|| {
            format!(
                "failed to stat existing {label} state file '{}' in '{}'",
                file_name,
                runtime_state_root.display()
            )
        })?
        .is_file()
    {
        return Err(anyhow!(
            "{label} state path '{}' must be a regular file",
            runtime_state_root.join(file_name).display()
        ));
    }
    Ok(())
}

fn open_state_root(runtime_state_root: &Path) -> Result<File> {
    let root = match open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(root) => root,
        Err(Errno::LOOP | Errno::NOTDIR) => {
            return Err(anyhow!(
                "runtime state root '{}' must be a directory and cannot be a symlink",
                runtime_state_root.display()
            ))
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to open '{}'", runtime_state_root.display()))
        }
    };
    Ok(File::from(root))
}

fn open_existing_state_root(runtime_state_root: &Path) -> Result<Option<File>> {
    match open(
        runtime_state_root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(root) => Ok(Some(File::from(root))),
        Err(Errno::NOENT) => Ok(None),
        Err(Errno::LOOP | Errno::NOTDIR) => Err(anyhow!(
            "runtime state root '{}' must be a directory and cannot be a symlink",
            runtime_state_root.display()
        )),
        Err(err) => Err(anyhow!(
            "failed to open runtime state root '{}': {err}",
            runtime_state_root.display()
        )),
    }
}

fn state_file_name(file_name: &str) -> Result<OsString> {
    let path = Path::new(file_name);
    let mut components = path.components();
    let Some(Component::Normal(name)) = components.next() else {
        return Err(anyhow!("runtime state file name '{file_name}' is invalid"));
    };
    if components.next().is_some() {
        return Err(anyhow!("runtime state file name '{file_name}' is invalid"));
    }
    Ok(OsString::from(name))
}

fn normalize_state_value(value: impl AsRef<str>, label: &str) -> Result<Option<String>> {
    let value = value.as_ref().trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.contains(['\n', '\r']) {
        return Err(anyhow!("{label} state value must be a single line"));
    }
    Ok(Some(value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeResumeMode;

    #[test]
    fn recorded_resume_mode_is_truthful_and_replaces_prior_observation() {
        let root = tempfile::tempdir().unwrap();

        assert_eq!(recorded_runtime_resume_mode(root.path()).unwrap(), None);
        record_runtime_resume_mode(root.path(), RuntimeResumeMode::Reconstructed).unwrap();
        assert!(runtime_session_ready_marker_exists(root.path()).unwrap());
        assert_eq!(
            recorded_runtime_resume_mode(root.path()).unwrap(),
            Some(RuntimeResumeMode::Reconstructed)
        );

        record_runtime_resume_mode(root.path(), RuntimeResumeMode::Resumed).unwrap();
        assert_eq!(
            recorded_runtime_resume_mode(root.path()).unwrap(),
            Some(RuntimeResumeMode::Resumed)
        );
    }

    #[test]
    fn invalid_recorded_resume_mode_is_not_projected_as_native() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(RUNTIME_SESSION_READY_MARKER), b"pretend\n").unwrap();

        let error = recorded_runtime_resume_mode(root.path()).unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown recorded runtime resume mode 'pretend'"));
    }
}
