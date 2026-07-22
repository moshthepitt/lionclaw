use std::{ffi::OsStr, path::Path};

use anyhow::{anyhow, Context, Result};
use lionclaw_durable_fs::RootedDirectory;

pub const RUNTIME_SESSION_READY_MARKER: &str = ".lionclaw-runtime-session";
pub const RUNTIME_STATE_VALUE_LIMIT: usize = 4 * 1024;
const RECONSTRUCTED_RESUME_MODE: &str = "reconstructed";
const RESUMED_RESUME_MODE: &str = "resumed";

/// Host-owned native runtime state beneath one explicit trusted state anchor.
///
/// Keeping both coordinates in one value prevents adapters from inferring a
/// security boundary by walking parents from the runtime leaf.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStateDir {
    files: RootedDirectory,
}

impl RuntimeStateDir {
    pub fn new(
        state_anchor: impl AsRef<Path>,
        runtime_state_root: impl AsRef<Path>,
    ) -> Result<Self> {
        Ok(Self {
            files: RootedDirectory::new(
                state_anchor.as_ref().to_path_buf(),
                runtime_state_root.as_ref().to_path_buf(),
            )?,
        })
    }

    pub fn path(&self) -> &Path {
        self.files.path()
    }
}

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

    pub fn from_state_dir(runtime_state: &RuntimeStateDir) -> Result<Self> {
        Ok(Self {
            marker_present: runtime_session_ready_marker_exists(runtime_state)?,
        })
    }

    pub const fn is_ready(self) -> bool {
        self.marker_present
    }
}

pub fn load_ready_state_value(
    runtime_state: &RuntimeStateDir,
    file_name: &str,
    label: &str,
    runtime_session_ready: RuntimeSessionReady,
) -> Result<Option<String>> {
    if !runtime_session_ready.is_ready() {
        return Ok(None);
    }
    load_state_value(runtime_state, file_name, label)
}

pub fn runtime_session_ready_marker_exists(runtime_state: &RuntimeStateDir) -> Result<bool> {
    Ok(runtime_state
        .files
        .read_bounded(
            OsStr::new(RUNTIME_SESSION_READY_MARKER),
            RUNTIME_STATE_VALUE_LIMIT,
            "runtime session marker",
        )?
        .is_some())
}

/// Records the adapter-observed mode in mission-private conversation state.
/// The same file remains the commit marker which permits native state to be
/// considered by the next process.
pub fn record_runtime_resume_mode(
    runtime_state: &RuntimeStateDir,
    mode: crate::RuntimeResumeMode,
) -> Result<()> {
    let value = match mode {
        crate::RuntimeResumeMode::Reconstructed => RECONSTRUCTED_RESUME_MODE,
        crate::RuntimeResumeMode::Resumed => RESUMED_RESUME_MODE,
    };
    save_state_value(
        runtime_state,
        RUNTIME_SESSION_READY_MARKER,
        value,
        "runtime resume mode",
    )
}

/// Reads the last adapter-observed mode without exposing native identity.
pub fn recorded_runtime_resume_mode(
    runtime_state: &RuntimeStateDir,
) -> Result<Option<crate::RuntimeResumeMode>> {
    match load_state_value(
        runtime_state,
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
    runtime_state: &RuntimeStateDir,
    file_name: &str,
    label: &str,
) -> Result<Option<String>> {
    let Some(contents) = runtime_state.files.read_bounded(
        OsStr::new(file_name),
        RUNTIME_STATE_VALUE_LIMIT,
        &format!("{label} state file"),
    )?
    else {
        return Ok(None);
    };
    let contents = String::from_utf8(contents).with_context(|| {
        format!(
            "{label} state file '{}' is not UTF-8",
            runtime_state.path().join(file_name).display()
        )
    })?;
    normalize_state_value(contents, label)
}

pub fn save_state_value(
    runtime_state: &RuntimeStateDir,
    file_name: &str,
    value: &str,
    label: &str,
) -> Result<()> {
    let value = match normalize_state_value(value, label)? {
        Some(value) => value,
        None => return Ok(()),
    };
    let mut contents = value.into_bytes();
    contents.push(b'\n');
    runtime_state.files.write_private_atomic(
        OsStr::new(file_name),
        &contents,
        RUNTIME_STATE_VALUE_LIMIT,
        &format!("{label} state file"),
    )
}

pub fn clear_state_value(
    runtime_state: &RuntimeStateDir,
    file_name: &str,
    label: &str,
) -> Result<()> {
    let _removed = runtime_state
        .files
        .remove_file(OsStr::new(file_name), &format!("{label} state file"))?;
    Ok(())
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

    fn runtime_state(root: &tempfile::TempDir) -> RuntimeStateDir {
        RuntimeStateDir::new(root.path(), root.path().join("missions/m1/runtime")).unwrap()
    }

    fn prepare(runtime_state: &RuntimeStateDir) {
        std::fs::create_dir_all(runtime_state.path()).unwrap();
    }

    #[test]
    fn recorded_resume_mode_is_truthful_and_replaces_prior_observation() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);

        assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
        record_runtime_resume_mode(&runtime_state, RuntimeResumeMode::Reconstructed).unwrap();
        assert!(runtime_session_ready_marker_exists(&runtime_state).unwrap());
        assert_eq!(
            recorded_runtime_resume_mode(&runtime_state).unwrap(),
            Some(RuntimeResumeMode::Reconstructed)
        );

        record_runtime_resume_mode(&runtime_state, RuntimeResumeMode::Resumed).unwrap();
        assert_eq!(
            recorded_runtime_resume_mode(&runtime_state).unwrap(),
            Some(RuntimeResumeMode::Resumed)
        );
    }

    #[test]
    fn invalid_recorded_resume_mode_is_not_projected_as_native() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        std::fs::write(
            runtime_state.path().join(RUNTIME_SESSION_READY_MARKER),
            b"pretend\n",
        )
        .unwrap();

        let error = recorded_runtime_resume_mode(&runtime_state).unwrap_err();
        assert!(error
            .to_string()
            .contains("unknown recorded runtime resume mode 'pretend'"));
    }

    #[test]
    fn missing_runtime_root_reads_absent_but_cannot_be_written() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);

        assert_eq!(
            load_state_value(&runtime_state, "session", "test").unwrap(),
            None
        );
        assert!(!runtime_session_ready_marker_exists(&runtime_state).unwrap());
        assert!(save_state_value(&runtime_state, "session", "one", "test")
            .unwrap_err()
            .to_string()
            .contains("does not exist"));
    }

    #[test]
    fn oversized_runtime_state_is_rejected_before_allocation() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        std::fs::write(
            runtime_state.path().join("session"),
            vec![b'x'; RUNTIME_STATE_VALUE_LIMIT + 1],
        )
        .unwrap();

        let error = load_state_value(&runtime_state, "session", "test").unwrap_err();
        assert!(error.to_string().contains("4096-byte limit"));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_runtime_ancestor_is_rejected() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("missions")).unwrap();
        symlink(outside.path(), root.path().join("missions/m1")).unwrap();
        let runtime_state = runtime_state(&root);

        let error = load_state_value(&runtime_state, "session", "test").unwrap_err();
        assert!(error.to_string().contains("must be a real directory"));
    }
}
