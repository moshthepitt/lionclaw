//! Canonical host paths for mission-owned resources.
//!
//! Path identity and resource lifetime are separate concerns: conversation
//! resources survive role effects, while effect resources are disposable after
//! settlement. Callers derive paths here and perform only the preparation their
//! ownership boundary requires.

use std::ffi::{OsStr, OsString};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStringExt;
use std::path::{Component, Path, PathBuf};

use lionclaw_durable_fs::RootedDirectory;
use rustix::fs::{mkdirat, open, openat, unlinkat, AtFlags, Dir, Mode, OFlags};
use rustix::io::Errno;

use crate::model::{ConversationId, EffectId, MissionId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MissionDirs {
    state_dir: PathBuf,
    mission_id: MissionId,
    root: PathBuf,
}

impl MissionDirs {
    pub(crate) fn new(state_dir: &Path, mission_id: &MissionId) -> Self {
        Self {
            state_dir: state_dir.to_path_buf(),
            mission_id: mission_id.clone(),
            root: state_dir.join("missions").join(mission_id.as_str()),
        }
    }

    pub(crate) fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub(crate) fn mission_id(&self) -> &MissionId {
        &self.mission_id
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(&self.state_dir, [&self.root])
    }

    pub(crate) fn files(&self) -> anyhow::Result<RootedDirectory> {
        RootedDirectory::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) fn conversation(&self, conversation_id: &ConversationId) -> ConversationDirs {
        ConversationDirs::new(
            self.state_dir.clone(),
            self.root
                .join("conversations")
                .join(conversation_id.as_str()),
        )
    }

    pub(crate) fn effect(&self, effect_id: &EffectId) -> EffectDirs {
        EffectDirs::new(
            self.state_dir.clone(),
            self.root.join("effects").join(effect_id.as_str()),
        )
    }
}

/// Durable resources for one exact folded conversation generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversationDirs {
    state_dir: PathBuf,
    root: PathBuf,
    role_state: RoleStateDirs,
    observer_index: PathBuf,
}

impl ConversationDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            role_state: RoleStateDirs::new(state_dir.clone(), &root),
            observer_index: root.join("observer.index"),
            state_dir,
            root,
        }
    }

    pub(crate) fn files(&self) -> anyhow::Result<RootedDirectory> {
        RootedDirectory::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) fn work(&self) -> &Path {
        self.role_state.work()
    }

    pub(crate) fn observer_index(&self) -> &Path {
        &self.observer_index
    }

    pub(crate) fn role_state(&self) -> &RoleStateDirs {
        &self.role_state
    }

    /// Remove only disposable build/scratch data after this conversation has
    /// settled. Retained work, runtime state, and observer evidence remain in
    /// the conversation tree.
    pub(crate) async fn remove_disposable_scratch(&self) -> std::io::Result<()> {
        let state_dir = self.role_state.state_dir.clone();
        let scratch = self.role_state.scratch.clone();
        tokio::task::spawn_blocking(move || remove_tree_beneath(&state_dir, &scratch))
            .await
            .map_err(|error| {
                std::io::Error::other(format!("conversation scratch cleanup task failed: {error}"))
            })?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoleStateDirs {
    state_dir: PathBuf,
    work: PathBuf,
    scratch: PathBuf,
    runtime: PathBuf,
    session_control: PathBuf,
}

impl RoleStateDirs {
    fn new(state_dir: PathBuf, root: &Path) -> Self {
        Self {
            state_dir,
            work: root.join("work"),
            scratch: root.join("scratch"),
            runtime: root.join("runtime"),
            session_control: root.join("session-control"),
        }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(
            &self.state_dir,
            [&self.scratch, &self.runtime, &self.session_control],
        )
    }

    pub(crate) fn work(&self) -> &Path {
        &self.work
    }

    pub(crate) fn scratch(&self) -> &Path {
        &self.scratch
    }

    pub(crate) fn runtime(&self) -> &Path {
        &self.runtime
    }

    pub(crate) fn session_control_root(&self) -> &Path {
        &self.session_control
    }

    pub(crate) fn session_control(&self, profile_key: &str) -> SessionControlDirs {
        SessionControlDirs {
            state_dir: self.state_dir.clone(),
            marker_root: self.session_control.clone(),
            profile_key: profile_key.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionControlDirs {
    state_dir: PathBuf,
    marker_root: PathBuf,
    profile_key: String,
}

impl SessionControlDirs {
    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        let state = self
            .state()
            .map_err(|error| std::io::Error::other(format!("{error:#}")))?;
        ensure_dirs_beneath(&self.state_dir, [state.marker_path(), state.path()])
    }

    pub(crate) fn state(&self) -> anyhow::Result<lionclaw_runtime_api::RuntimeStateDir> {
        lionclaw_runtime_api::RuntimeStateDir::new(
            &self.state_dir,
            &self.marker_root,
            self.profile_key.clone(),
        )
    }
}

/// Disposable resources for one effect. No retained conversation work or
/// native runtime state belongs in this tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectDirs {
    state_dir: PathBuf,
    root: PathBuf,
}

impl EffectDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self { state_dir, root }
    }

    pub(crate) fn role(&self) -> RoleEffectDirs {
        RoleEffectDirs::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) fn oracle(&self) -> OracleEffectDirs {
        OracleEffectDirs::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) async fn remove(&self) -> std::io::Result<()> {
        let state_dir = self.state_dir.clone();
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || remove_tree_beneath(&state_dir, &root))
            .await
            .map_err(|error| {
                std::io::Error::other(format!("effect cleanup task failed: {error}"))
            })?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoleEffectDirs {
    state_dir: PathBuf,
    root: PathBuf,
    handoff: PathBuf,
    runtime_home: PathBuf,
    role_state: RoleStateDirs,
}

impl RoleEffectDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            handoff: root.join("handoff"),
            runtime_home: root.join("runtime-home"),
            role_state: RoleStateDirs::new(state_dir.clone(), &root),
            state_dir,
            root,
        }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(&self.state_dir, [&self.handoff, &self.runtime_home])
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn handoff(&self) -> &Path {
        &self.handoff
    }

    pub(crate) fn runtime_home(&self) -> &Path {
        &self.runtime_home
    }

    pub(crate) fn role_state(&self) -> &RoleStateDirs {
        &self.role_state
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OracleEffectDirs {
    state_dir: PathBuf,
    root: PathBuf,
    scratch: PathBuf,
    work: PathBuf,
    program: PathBuf,
}

impl OracleEffectDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            scratch: root.join("scratch"),
            work: root.join("work"),
            program: root.join("oracle"),
            state_dir,
            root,
        }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(&self.state_dir, [&self.scratch, &self.program])
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn scratch(&self) -> &Path {
        &self.scratch
    }

    pub(crate) fn work(&self) -> &Path {
        &self.work
    }

    pub(crate) fn program(&self) -> &Path {
        &self.program
    }
}

const RESOURCE_TREE_RETRIES: usize = 8;
const RESOURCE_TREE_MAX_DEPTH: usize = 128;
const RESOURCE_TREE_MAX_ENTRIES: usize = 100_000;

fn directory_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK
}

fn resource_components<'a>(state_dir: &Path, target: &'a Path) -> std::io::Result<Vec<&'a OsStr>> {
    let relative = target.strip_prefix(state_dir).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "mission resource '{}' is not beneath state directory '{}'",
                target.display(),
                state_dir.display()
            ),
        )
    })?;
    relative
        .components()
        .map(|component| match component {
            Component::Normal(name) => Ok(name),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "mission resource '{}' has an invalid component",
                    target.display()
                ),
            )),
        })
        .collect()
}

fn open_state_dir(state_dir: &Path) -> std::io::Result<OwnedFd> {
    open(state_dir, directory_flags(), Mode::empty()).map_err(|error| {
        contextual_io(
            error,
            format!(
                "state directory '{}' must be a real directory",
                state_dir.display()
            ),
        )
    })
}

fn contextual_io(error: Errno, context: String) -> std::io::Error {
    let source = std::io::Error::from(error);
    std::io::Error::new(source.kind(), format!("{context}: {source}"))
}

fn ensure_dirs_beneath(
    state_dir: &Path,
    targets: impl IntoIterator<Item = impl AsRef<Path>>,
) -> std::io::Result<()> {
    for target in targets {
        let target = target.as_ref();
        let mut parent = open_state_dir(state_dir)?;
        let mut display = state_dir.to_path_buf();
        for name in resource_components(state_dir, target)? {
            display.push(name);
            match mkdirat(&parent, name, Mode::from_raw_mode(0o777)) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(error) => {
                    return Err(contextual_io(
                        error,
                        format!("failed to create mission resource '{}'", display.display()),
                    ))
                }
            }
            parent = openat(&parent, name, directory_flags(), Mode::empty()).map_err(|error| {
                contextual_io(
                    error,
                    format!(
                        "mission resource '{}' must be a real directory",
                        display.display()
                    ),
                )
            })?;
        }
    }
    Ok(())
}

fn remove_tree_beneath(state_dir: &Path, target: &Path) -> std::io::Result<()> {
    let components = resource_components(state_dir, target)?;
    let Some((name, parents)) = components.split_last() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "refusing to remove the state directory",
        ));
    };
    let mut parent = match open_state_dir(state_dir) {
        Ok(parent) => parent,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for component in parents {
        parent = match openat(&parent, *component, directory_flags(), Mode::empty()) {
            Ok(directory) => directory,
            Err(Errno::NOENT) => return Ok(()),
            Err(error) => {
                return Err(contextual_io(
                    error,
                    format!(
                        "mission resource parent '{}' is unavailable or unsafe",
                        target.display()
                    ),
                ))
            }
        };
    }
    remove_entry_at(&parent, name, target)
}

fn remove_entry_at(parent: &OwnedFd, name: &OsStr, display: &Path) -> std::io::Result<()> {
    let parent =
        openat(parent, ".", directory_flags(), Mode::empty()).map_err(std::io::Error::from)?;
    let Some(root) = open_removal_frame(parent, name.to_owned(), display.to_path_buf(), 0)? else {
        return Ok(());
    };
    let mut stack = vec![root];
    let mut visited = 0_usize;

    while let Some(frame) = stack.last_mut() {
        if let Some(entry) = frame.entries.next() {
            let entry = entry.map_err(std::io::Error::from)?;
            let bytes = entry.file_name().to_bytes();
            if matches!(bytes, b"." | b"..") {
                continue;
            }
            visited = visited.saturating_add(1);
            if visited > RESOURCE_TREE_MAX_ENTRIES {
                return Err(resource_tree_limit(display, "entry"));
            }
            let child_name = OsString::from_vec(bytes.to_vec());
            let child_display = frame.display.join(&child_name);
            let frame_parent = openat(
                frame.entries.fd().map_err(std::io::Error::from)?,
                ".",
                directory_flags(),
                Mode::empty(),
            )
            .map_err(std::io::Error::from)?;
            if let Some(child) = open_removal_frame(frame_parent, child_name, child_display, 0)? {
                if stack.len() >= RESOURCE_TREE_MAX_DEPTH {
                    return Err(resource_tree_limit(display, "depth"));
                }
                stack.push(child);
            }
            continue;
        }

        let frame = stack.pop().expect("last_mut established a frame");
        match unlinkat(&frame.parent, &frame.name, AtFlags::REMOVEDIR) {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(Errno::NOTEMPTY | Errno::NOTDIR) if frame.retries < RESOURCE_TREE_RETRIES => {
                if let Some(reopened) =
                    open_removal_frame(frame.parent, frame.name, frame.display, frame.retries + 1)?
                {
                    stack.push(reopened);
                }
            }
            Err(Errno::NOTEMPTY | Errno::NOTDIR) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    format!(
                        "mission resource '{}' changed repeatedly during cleanup",
                        frame.display.display()
                    ),
                ));
            }
            Err(error) => return Err(std::io::Error::from(error)),
        }
    }

    Ok(())
}

struct RemovalFrame {
    parent: OwnedFd,
    name: OsString,
    display: PathBuf,
    entries: Dir,
    retries: usize,
}

fn open_removal_frame(
    parent: OwnedFd,
    name: OsString,
    display: PathBuf,
    retries: usize,
) -> std::io::Result<Option<RemovalFrame>> {
    for _ in 0..RESOURCE_TREE_RETRIES {
        match openat(&parent, &name, directory_flags(), Mode::empty()) {
            Ok(directory) => {
                return Ok(Some(RemovalFrame {
                    parent,
                    name,
                    display,
                    entries: Dir::new(directory).map_err(std::io::Error::from)?,
                    retries,
                }))
            }
            Err(Errno::NOENT) => return Ok(None),
            Err(Errno::LOOP | Errno::NOTDIR) => match unlinkat(&parent, &name, AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => return Ok(None),
                Err(Errno::ISDIR) => continue,
                Err(error) => return Err(std::io::Error::from(error)),
            },
            Err(error) => return Err(std::io::Error::from(error)),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::WouldBlock,
        format!(
            "mission resource '{}' changed repeatedly during cleanup",
            display.display()
        ),
    ))
}

fn resource_tree_limit(display: &Path, dimension: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "mission resource '{}' exceeds the cleanup {dimension} limit",
            display.display()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{RoleName, TaskId, TaskNamespace};
    use std::os::unix::fs::symlink;

    #[test]
    fn one_mission_graph_separates_conversation_and_effect_lifetimes() {
        let state = tempfile::tempdir().unwrap();
        let mission = MissionId::for_creation("/workspace", "test", 1);
        let conversation = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &TaskId::new("worker").unwrap(),
            &RoleName::new("implementer").unwrap(),
            1,
        );
        let effect = EffectId::for_parts(&["resource", "test"]);
        let mission_dirs = MissionDirs::new(state.path(), &mission);
        let conversation_dirs = mission_dirs.conversation(&conversation);
        let effect_dirs = mission_dirs.effect(&effect);
        let role_effect = effect_dirs.role();
        let oracle_effect = effect_dirs.oracle();

        assert!(!mission_dirs.root().exists());
        let replacement = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &TaskId::new("worker").unwrap(),
            &RoleName::new("implementer").unwrap(),
            2,
        );
        assert_ne!(
            conversation_dirs.root,
            mission_dirs.conversation(&replacement).root
        );
        assert_ne!(
            role_effect.root(),
            mission_dirs
                .effect(&EffectId::for_parts(&["resource", "other"]))
                .role()
                .root()
        );

        assert!(conversation_dirs
            .root
            .starts_with(mission_dirs.root().join("conversations")));
        assert!(role_effect
            .root()
            .starts_with(mission_dirs.root().join("effects")));
        assert!(!conversation_dirs.root.starts_with(role_effect.root()));
        assert!(!role_effect.root().starts_with(&conversation_dirs.root));

        conversation_dirs.role_state().prepare().unwrap();
        role_effect.prepare().unwrap();
        role_effect.role_state().prepare().unwrap();
        assert!(conversation_dirs.role_state().scratch().is_dir());
        assert!(conversation_dirs.role_state().runtime().is_dir());
        assert!(role_effect.handoff().is_dir());
        assert!(role_effect.runtime_home().is_dir());
        assert!(role_effect.role_state().scratch().is_dir());
        assert!(role_effect.role_state().runtime().is_dir());
        assert!(!oracle_effect.work().exists());
        assert!(!oracle_effect.program().exists());

        std::fs::remove_dir_all(role_effect.root()).unwrap();
        oracle_effect.prepare().unwrap();
        assert!(oracle_effect.scratch().is_dir());
        assert!(oracle_effect.program().is_dir());
        assert!(!role_effect.handoff().exists());
        assert!(!role_effect.runtime_home().exists());

        let mounts = crate::runner::effect_mounts(&role_effect, conversation_dirs.role_state());
        let source_for = |target: &str| {
            mounts
                .iter()
                .find(|mount| mount.target == target)
                .map(|mount| mount.source.as_path())
                .unwrap()
        };
        assert_eq!(
            source_for(crate::runner::SCRATCH_MOUNT_TARGET),
            conversation_dirs.role_state().scratch()
        );
        assert_eq!(
            source_for(lionclaw_confinement::RUNTIME_MOUNT_TARGET),
            conversation_dirs.role_state().runtime()
        );

        let transient_mounts = crate::runner::effect_mounts(&role_effect, role_effect.role_state());
        let transient_source_for = |target: &str| {
            transient_mounts
                .iter()
                .find(|mount| mount.target == target)
                .map(|mount| mount.source.as_path())
                .unwrap()
        };
        assert_eq!(
            transient_source_for(crate::runner::SCRATCH_MOUNT_TARGET),
            role_effect.role_state().scratch()
        );
        assert_eq!(
            transient_source_for(lionclaw_confinement::RUNTIME_MOUNT_TARGET),
            role_effect.role_state().runtime()
        );
        assert_eq!(
            source_for(crate::runner::HANDOFF_MOUNT_TARGET),
            role_effect.handoff()
        );
        assert_eq!(
            source_for(lionclaw_confinement::RUNTIME_HOME_MOUNT_TARGET),
            role_effect.runtime_home()
        );
    }

    #[test]
    fn preparation_rejects_symlinked_resource_ancestors() {
        for target in [
            "missions",
            "mission",
            "effects",
            "effect",
            "conversations",
            "conversation",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let state_dir = temp.path().join("state");
            let outside = temp.path().join("outside");
            std::fs::create_dir(&state_dir).unwrap();
            std::fs::create_dir(&outside).unwrap();
            let mission = MissionId::parse("mabc123def456").unwrap();
            let conversation = ConversationId::for_role_instance(
                &mission,
                TaskNamespace::Execution,
                &TaskId::new("worker").unwrap(),
                &RoleName::new("implementer").unwrap(),
                1,
            );
            let effect = EffectId::for_parts(&["resource", "symlink"]);
            let mission_dirs = MissionDirs::new(&state_dir, &mission);
            let link = match target {
                "missions" => state_dir.join("missions"),
                "mission" => mission_dirs.root().to_path_buf(),
                "effects" => mission_dirs.root().join("effects"),
                "effect" => mission_dirs.effect(&effect).role().root().to_path_buf(),
                "conversations" => mission_dirs.root().join("conversations"),
                "conversation" => mission_dirs.conversation(&conversation).root.clone(),
                _ => unreachable!(),
            };
            std::fs::create_dir_all(link.parent().unwrap()).unwrap();
            symlink(&outside, &link).unwrap();

            let result = if matches!(target, "conversations" | "conversation") {
                mission_dirs
                    .conversation(&conversation)
                    .role_state()
                    .prepare()
            } else {
                mission_dirs.effect(&effect).role().prepare()
            };
            let error = result.expect_err("symlinked resource ancestor must fail closed");
            assert!(
                error.to_string().contains("real directory"),
                "{target}: {error}"
            );
            assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0, "{target}");
        }
    }

    #[tokio::test]
    async fn effect_cleanup_is_descriptor_rooted_and_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), "preserve\n").unwrap();
        let mission = MissionId::parse("mabc123def456").unwrap();
        let conversation = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &TaskId::new("worker").unwrap(),
            &RoleName::new("implementer").unwrap(),
            1,
        );
        let current = EffectId::for_parts(&["resource", "current"]);
        let adjacent = EffectId::for_parts(&["resource", "adjacent"]);
        let mission_dirs = MissionDirs::new(&state_dir, &mission);
        let current_dirs = mission_dirs.effect(&current);
        let role = current_dirs.role();
        role.prepare().unwrap();
        role.role_state().prepare().unwrap();
        std::fs::write(role.role_state().scratch().join("private"), "delete\n").unwrap();
        symlink(&outside, role.root().join("outside-link")).unwrap();
        mission_dirs.effect(&adjacent).role().prepare().unwrap();
        mission_dirs
            .conversation(&conversation)
            .role_state()
            .prepare()
            .unwrap();

        current_dirs.remove().await.unwrap();
        current_dirs.remove().await.unwrap();
        assert!(!current_dirs.role().root().exists());
        assert!(mission_dirs.effect(&adjacent).role().root().is_dir());
        assert!(mission_dirs
            .conversation(&conversation)
            .role_state()
            .runtime()
            .is_dir());
        assert_eq!(
            std::fs::read_to_string(outside.join("sentinel")).unwrap(),
            "preserve\n"
        );

        symlink(&outside, current_dirs.role().root()).unwrap();
        current_dirs.remove().await.unwrap();
        assert!(std::fs::symlink_metadata(current_dirs.role().root()).is_err());
        assert!(outside.join("sentinel").is_file());
    }

    #[tokio::test]
    async fn conversation_cleanup_removes_only_scratch_and_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        std::fs::create_dir(&state_dir).unwrap();
        let mission = MissionId::parse("mabc123def456").unwrap();
        let conversation = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &TaskId::new("worker").unwrap(),
            &RoleName::new("implementer").unwrap(),
            1,
        );
        let adjacent = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &TaskId::new("reviewer").unwrap(),
            &RoleName::new("reviewer").unwrap(),
            1,
        );
        let mission_dirs = MissionDirs::new(&state_dir, &mission);
        let current = mission_dirs.conversation(&conversation);
        current.role_state().prepare().unwrap();
        std::fs::create_dir_all(current.work()).unwrap();
        std::fs::write(current.work().join("checkout"), "preserve\n").unwrap();
        std::fs::write(
            current.role_state().runtime().join("native-session"),
            "preserve\n",
        )
        .unwrap();
        std::fs::write(current.observer_index(), "preserve\n").unwrap();
        std::fs::write(current.role_state().scratch().join("build"), "delete\n").unwrap();
        let adjacent = mission_dirs.conversation(&adjacent);
        adjacent.role_state().prepare().unwrap();
        std::fs::write(adjacent.role_state().scratch().join("build"), "preserve\n").unwrap();

        current.remove_disposable_scratch().await.unwrap();
        current.remove_disposable_scratch().await.unwrap();

        assert!(!current.role_state().scratch().exists());
        assert_eq!(
            std::fs::read_to_string(current.work().join("checkout")).unwrap(),
            "preserve\n"
        );
        assert_eq!(
            std::fs::read_to_string(current.role_state().runtime().join("native-session"),)
                .unwrap(),
            "preserve\n"
        );
        assert_eq!(
            std::fs::read_to_string(current.observer_index()).unwrap(),
            "preserve\n"
        );
        assert!(adjacent.role_state().scratch().join("build").is_file());
    }

    #[tokio::test]
    async fn conversation_cleanup_rejects_a_symlinked_conversation_parent() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), "preserve\n").unwrap();
        let mission = MissionId::parse("mabc123def456").unwrap();
        let conversation = ConversationId::for_role_instance(
            &mission,
            TaskNamespace::Execution,
            &TaskId::new("worker").unwrap(),
            &RoleName::new("implementer").unwrap(),
            1,
        );
        let mission_dirs = MissionDirs::new(&state_dir, &mission);
        std::fs::create_dir_all(mission_dirs.root().join("conversations")).unwrap();
        symlink(
            &outside,
            mission_dirs
                .root()
                .join("conversations")
                .join(conversation.as_str()),
        )
        .unwrap();

        let error = mission_dirs
            .conversation(&conversation)
            .remove_disposable_scratch()
            .await
            .expect_err("symlinked conversation parent must fail closed");
        assert!(error.to_string().contains("unavailable or unsafe"));
        assert!(outside.join("sentinel").is_file());
    }

    #[tokio::test]
    async fn effect_cleanup_rejects_a_symlinked_effects_parent() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("sentinel"), "preserve\n").unwrap();
        let mission = MissionId::parse("mabc123def456").unwrap();
        let effect = EffectId::for_parts(&["resource", "unsafe-parent"]);
        let mission_dirs = MissionDirs::new(&state_dir, &mission);
        std::fs::create_dir_all(mission_dirs.root()).unwrap();
        symlink(&outside, mission_dirs.root().join("effects")).unwrap();

        let error = mission_dirs
            .effect(&effect)
            .remove()
            .await
            .expect_err("symlinked effects parent must fail closed");
        assert!(error.to_string().contains("unavailable or unsafe"));
        assert!(outside.join("sentinel").is_file());
    }

    #[tokio::test]
    async fn effect_cleanup_retains_an_excessively_deep_tree() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        std::fs::create_dir(&state_dir).unwrap();
        let mission = MissionId::parse("mabc123def456").unwrap();
        let effect = EffectId::for_parts(&["resource", "deep"]);
        let effect_dirs = MissionDirs::new(&state_dir, &mission).effect(&effect);
        let role = effect_dirs.role();
        role.prepare().unwrap();
        let mut deepest = role.handoff().to_path_buf();
        for index in 0..RESOURCE_TREE_MAX_DEPTH {
            deepest.push(format!("d{index}"));
        }
        std::fs::create_dir_all(&deepest).unwrap();

        let error = effect_dirs
            .remove()
            .await
            .expect_err("cleanup depth must be bounded");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("cleanup depth limit"));
        assert!(effect_dirs.role().root().is_dir());
    }
}
