use std::{ffi::OsStr, path::Path};

use anyhow::{anyhow, Context, Result};
use lionclaw_durable_fs::{BoundedRead, RootedDirectory};

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
    marker_files: RootedDirectory,
    profile_files: RootedDirectory,
    profile_key: String,
}

impl RuntimeStateDir {
    pub fn new(
        state_anchor: impl AsRef<Path>,
        marker_root: impl AsRef<Path>,
        profile_key: impl Into<String>,
    ) -> Result<Self> {
        let profile_key = profile_key.into();
        validate_profile_key(&profile_key)?;
        let marker_root = marker_root.as_ref();
        Ok(Self {
            marker_files: RootedDirectory::new(
                state_anchor.as_ref().to_path_buf(),
                marker_root.to_path_buf(),
            )?,
            profile_files: RootedDirectory::new(
                state_anchor.as_ref().to_path_buf(),
                marker_root.join("profiles").join(&profile_key),
            )?,
            profile_key,
        })
    }

    pub fn path(&self) -> &Path {
        self.profile_files.path()
    }

    pub fn marker_path(&self) -> &Path {
        self.marker_files.path()
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

    pub const fn is_ready(self) -> bool {
        self.marker_present
    }
}

#[must_use = "dropping an attempt intentionally leaves native state uncommitted"]
#[derive(Debug)]
pub struct RuntimeSessionAttempt {
    runtime_state: RuntimeStateDir,
    previous_ready: RuntimeSessionReady,
}

impl RuntimeSessionAttempt {
    pub const fn previous_ready(&self) -> RuntimeSessionReady {
        self.previous_ready
    }

    /// Commits adapter-observed, reopenable native state for the next attempt.
    pub fn commit(self, observation: crate::RuntimeNativeSessionObservation) -> Result<()> {
        let Some(mode) = observation.committable_mode() else {
            return Ok(());
        };
        let value = match mode {
            crate::RuntimeResumeMode::Reconstructed => RECONSTRUCTED_RESUME_MODE,
            crate::RuntimeResumeMode::Resumed => RESUMED_RESUME_MODE,
        };
        write_value(
            &self.runtime_state.marker_files,
            RUNTIME_SESSION_READY_MARKER,
            &format!("{} {value}", self.runtime_state.profile_key),
            "runtime resume mode",
        )
    }
}

/// Begins one native-session attempt by consuming the prior commit marker.
///
/// A valid marker authorizes exactly one reopen attempt for the matching
/// profile. Invalid content degrades to reconstruction, while filesystem
/// authority violations remain errors. In every non-error case the marker is
/// removed before external runtime work can begin. The mission scheduler
/// serializes role effects, so only the owning attempt may later commit a new
/// marker; concurrent takers still have exactly one winner.
pub fn begin_runtime_session_attempt(
    runtime_state: &RuntimeStateDir,
) -> Result<RuntimeSessionAttempt> {
    let marker_present = take_marker(runtime_state)?
        .is_some_and(|marker| marker.profile_key == runtime_state.profile_key);
    Ok(RuntimeSessionAttempt {
        runtime_state: runtime_state.clone(),
        previous_ready: RuntimeSessionReady { marker_present },
    })
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

/// Reads the last adapter-observed mode without exposing native identity.
pub fn recorded_runtime_resume_mode(
    runtime_state: &RuntimeStateDir,
) -> Result<Option<crate::RuntimeResumeMode>> {
    Ok(read_marker_files(&runtime_state.marker_files)?.map(|marker| marker.mode))
}

pub fn recorded_runtime_resume_mode_at(
    state_anchor: impl AsRef<Path>,
    marker_root: impl AsRef<Path>,
) -> Result<Option<crate::RuntimeResumeMode>> {
    let files = RootedDirectory::new(
        state_anchor.as_ref().to_path_buf(),
        marker_root.as_ref().to_path_buf(),
    )?;
    Ok(read_marker_files(&files)?.map(|marker| marker.mode))
}

pub fn load_state_value(
    runtime_state: &RuntimeStateDir,
    file_name: &str,
    label: &str,
) -> Result<Option<String>> {
    let Some(contents) = runtime_state.profile_files.read_bounded(
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
    runtime_state.profile_files.write_private_atomic(
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
        .profile_files
        .remove_file(OsStr::new(file_name), &format!("{label} state file"))?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeMarker {
    profile_key: String,
    mode: crate::RuntimeResumeMode,
}

fn read_marker_files(files: &RootedDirectory) -> Result<Option<RuntimeMarker>> {
    parse_marker(files.read_bounded_status(
        OsStr::new(RUNTIME_SESSION_READY_MARKER),
        RUNTIME_STATE_VALUE_LIMIT,
        "runtime session marker",
    )?)
}

fn take_marker(runtime_state: &RuntimeStateDir) -> Result<Option<RuntimeMarker>> {
    parse_marker(runtime_state.marker_files.take_bounded_status(
        OsStr::new(RUNTIME_SESSION_READY_MARKER),
        RUNTIME_STATE_VALUE_LIMIT,
        "runtime session marker",
    )?)
}

fn parse_marker(contents: BoundedRead) -> Result<Option<RuntimeMarker>> {
    let contents = match contents {
        BoundedRead::Missing | BoundedRead::TooLarge => return Ok(None),
        BoundedRead::Contents(contents) => contents,
    };
    let Ok(contents) = String::from_utf8(contents) else {
        return Ok(None);
    };
    let mut fields = contents.split_whitespace();
    let (Some(profile_key), Some(mode), None) = (fields.next(), fields.next(), fields.next())
    else {
        return Ok(None);
    };
    if validate_profile_key(profile_key).is_err() {
        return Ok(None);
    }
    let mode = match mode {
        RECONSTRUCTED_RESUME_MODE => crate::RuntimeResumeMode::Reconstructed,
        RESUMED_RESUME_MODE => crate::RuntimeResumeMode::Resumed,
        _ => return Ok(None),
    };
    Ok(Some(RuntimeMarker {
        profile_key: profile_key.to_string(),
        mode,
    }))
}

fn write_value(files: &RootedDirectory, file_name: &str, value: &str, label: &str) -> Result<()> {
    let value = normalize_state_value(value, label)?.ok_or_else(|| anyhow!("{label} is empty"))?;
    let mut contents = value.into_bytes();
    contents.push(b'\n');
    files.write_private_atomic(
        OsStr::new(file_name),
        &contents,
        RUNTIME_STATE_VALUE_LIMIT,
        &format!("{label} state file"),
    )
}

fn validate_profile_key(profile_key: &str) -> Result<()> {
    if profile_key.len() != 64
        || !profile_key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(anyhow!(
            "runtime profile key must be a 64-character lowercase hexadecimal digest"
        ));
    }
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
    use crate::{
        RuntimeNativeSessionObservation, RuntimeNativeStateAvailability, RuntimeResumeMode,
    };

    const PROFILE_KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn runtime_state(root: &tempfile::TempDir) -> RuntimeStateDir {
        RuntimeStateDir::new(
            root.path(),
            root.path().join("missions/m1/session-control"),
            PROFILE_KEY,
        )
        .unwrap()
    }

    fn prepare(runtime_state: &RuntimeStateDir) {
        std::fs::create_dir_all(runtime_state.marker_path()).unwrap();
        std::fs::create_dir_all(runtime_state.path()).unwrap();
    }

    fn reopenable(mode: RuntimeResumeMode) -> RuntimeNativeSessionObservation {
        match mode {
            RuntimeResumeMode::Reconstructed => RuntimeNativeSessionObservation::Reconstructed {
                state: RuntimeNativeStateAvailability::Reopenable,
            },
            RuntimeResumeMode::Resumed => RuntimeNativeSessionObservation::Resumed,
        }
    }

    fn publish(runtime_state: &RuntimeStateDir, mode: RuntimeResumeMode) {
        begin_runtime_session_attempt(runtime_state)
            .unwrap()
            .commit(reopenable(mode))
            .unwrap();
    }

    #[test]
    fn recorded_resume_mode_is_truthful_and_replaces_prior_observation() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);

        assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
        publish(&runtime_state, RuntimeResumeMode::Reconstructed);
        assert_eq!(
            recorded_runtime_resume_mode(&runtime_state).unwrap(),
            Some(RuntimeResumeMode::Reconstructed)
        );

        let attempt = begin_runtime_session_attempt(&runtime_state).unwrap();
        assert!(attempt.previous_ready().is_ready());
        attempt
            .commit(RuntimeNativeSessionObservation::Resumed)
            .unwrap();
        assert_eq!(
            recorded_runtime_resume_mode(&runtime_state).unwrap(),
            Some(RuntimeResumeMode::Resumed)
        );
        assert_eq!(
            std::fs::metadata(
                runtime_state
                    .marker_path()
                    .join(RUNTIME_SESSION_READY_MARKER)
            )
            .unwrap()
            .permissions()
            .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn begin_consumes_readiness_before_loading_the_authorized_identity() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        save_state_value(&runtime_state, "session", "native-id", "test").unwrap();
        publish(&runtime_state, RuntimeResumeMode::Resumed);

        let attempt = begin_runtime_session_attempt(&runtime_state).unwrap();

        assert!(attempt.previous_ready().is_ready());
        assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
        assert_eq!(
            load_ready_state_value(&runtime_state, "session", "test", attempt.previous_ready(),)
                .unwrap()
                .as_deref(),
            Some("native-id")
        );
    }

    #[test]
    fn noncommittable_observations_cannot_publish_readiness() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);

        for observation in [
            RuntimeNativeSessionObservation::Reconstructed {
                state: RuntimeNativeStateAvailability::Unavailable,
            },
            RuntimeNativeSessionObservation::ReopenFailed,
        ] {
            begin_runtime_session_attempt(&runtime_state)
                .unwrap()
                .commit(observation)
                .unwrap();
            assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
        }
    }

    #[test]
    fn invalid_recorded_resume_mode_is_not_projected_as_native() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        std::fs::write(
            runtime_state
                .marker_path()
                .join(RUNTIME_SESSION_READY_MARKER),
            b"pretend\n",
        )
        .unwrap();

        assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
    }

    #[test]
    fn ready_marker_is_bound_to_the_exact_profile_key() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        publish(&runtime_state, RuntimeResumeMode::Resumed);

        let other_profile = RuntimeStateDir::new(
            root.path(),
            runtime_state.marker_path(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .unwrap();
        std::fs::create_dir_all(other_profile.path()).unwrap();

        assert_eq!(
            recorded_runtime_resume_mode(&other_profile).unwrap(),
            Some(RuntimeResumeMode::Resumed),
            "operator projection reports the last observation without granting another profile readiness"
        );
        let attempt = begin_runtime_session_attempt(&other_profile).unwrap();
        assert!(!attempt.previous_ready().is_ready());
        assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
    }

    #[test]
    fn corrupt_marker_content_degrades_to_reconstruction() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        let marker = runtime_state
            .marker_path()
            .join(RUNTIME_SESSION_READY_MARKER);

        for contents in [
            Vec::new(),
            vec![0xff, 0xfe],
            vec![b'x'; RUNTIME_STATE_VALUE_LIMIT + 1],
        ] {
            std::fs::write(&marker, contents).unwrap();
            assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
            let attempt = begin_runtime_session_attempt(&runtime_state).unwrap();
            assert!(!attempt.previous_ready().is_ready());
            assert!(!marker.exists(), "corrupt marker must be consumed");
        }
    }

    #[test]
    fn runtime_profile_key_must_be_a_digest() {
        let root = tempfile::tempdir().unwrap();
        let error = RuntimeStateDir::new(root.path(), root.path(), "profile-name").unwrap_err();
        assert!(error.to_string().contains("lowercase hexadecimal"));
        let error = RuntimeStateDir::new(
            root.path(),
            root.path(),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        )
        .unwrap_err();
        assert!(error.to_string().contains("lowercase hexadecimal"));
    }

    #[test]
    fn missing_runtime_root_reads_absent_but_cannot_be_written() {
        let root = tempfile::tempdir().unwrap();
        let runtime_state = runtime_state(&root);

        assert_eq!(
            load_state_value(&runtime_state, "session", "test").unwrap(),
            None
        );
        assert!(!begin_runtime_session_attempt(&runtime_state)
            .unwrap()
            .previous_ready()
            .is_ready());
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

    #[cfg(unix)]
    #[test]
    fn symlinked_runtime_marker_is_an_authority_error() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        symlink(
            outside.path(),
            runtime_state
                .marker_path()
                .join(RUNTIME_SESSION_READY_MARKER),
        )
        .unwrap();

        let error = recorded_runtime_resume_mode(&runtime_state).unwrap_err();
        assert!(error.to_string().contains("cannot be a symlink"));
        let error = begin_runtime_session_attempt(&runtime_state).unwrap_err();
        assert!(error.to_string().contains("cannot be a symlink"));
        assert!(
            outside.path().exists(),
            "authority target must remain untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn consumed_readiness_preserves_identity_authority_checks() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        symlink(outside.path(), runtime_state.path().join("session")).unwrap();
        publish(&runtime_state, RuntimeResumeMode::Resumed);

        let attempt = begin_runtime_session_attempt(&runtime_state).unwrap();
        assert!(attempt.previous_ready().is_ready());
        let error =
            load_ready_state_value(&runtime_state, "session", "test", attempt.previous_ready())
                .unwrap_err();
        assert!(error.to_string().contains("cannot be a symlink"));
        assert_eq!(recorded_runtime_resume_mode(&runtime_state).unwrap(), None);
        assert!(
            outside.path().exists(),
            "identity target must remain untouched"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unready_attempt_does_not_inspect_stale_identity() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let runtime_state = runtime_state(&root);
        prepare(&runtime_state);
        symlink(outside.path(), runtime_state.path().join("session")).unwrap();

        let attempt = begin_runtime_session_attempt(&runtime_state).unwrap();
        assert!(!attempt.previous_ready().is_ready());
        assert_eq!(
            load_ready_state_value(&runtime_state, "session", "test", attempt.previous_ready(),)
                .unwrap(),
            None
        );
        assert!(
            outside.path().exists(),
            "identity target must remain untouched"
        );
    }
}
