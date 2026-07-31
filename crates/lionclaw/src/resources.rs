//! Canonical host paths for mission-owned resources.
//!
//! Path identity and resource lifetime are separate concerns: conversation
//! resources survive role effects, while effect resources are disposable after
//! settlement. Callers derive paths here and perform only the preparation their
//! ownership boundary requires.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use lionclaw_durable_fs::{
    MetadataTreeLimit, MetadataTreeLimitExceeded, MetadataTreeLimits, RootedDirectory,
};
use rustix::fs::{chmodat, fchmod, mkdirat, open, openat, unlinkat, AtFlags, Dir, Mode, OFlags};
use rustix::io::Errno;

use crate::model::{EffectId, MissionId, RoleInstanceId, TaskId};

/// Repository-scoped resources for the one everyday orchestrator session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EverydayDirs {
    state_dir: PathBuf,
    root: PathBuf,
    runtime_root: PathBuf,
    role_state: RoleStateDirs,
    auth_staging: PathBuf,
    operator_skill: PathBuf,
    driver_lock: PathBuf,
}

impl EverydayDirs {
    pub(crate) fn new(state_dir: &Path, runtime_root: PathBuf) -> Self {
        let root = state_dir.join("everyday");
        Self {
            role_state: RoleStateDirs::new(runtime_root.clone(), &runtime_root),
            auth_staging: runtime_root.join("auth-staging"),
            operator_skill: root.join("operator-skill").join("lionclaw"),
            driver_lock: root.join("driver.lock"),
            state_dir: state_dir.to_path_buf(),
            root,
            runtime_root,
        }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.runtime_root)?;
        std::fs::set_permissions(&self.runtime_root, std::fs::Permissions::from_mode(0o700))?;
        self.role_state.prepare()?;
        ensure_private_dirs_beneath(&self.state_dir, [&self.root, &self.operator_skill])?;
        ensure_private_dirs_beneath(&self.runtime_root, [&self.auth_staging])
    }

    pub(crate) fn role_state(&self) -> &RoleStateDirs {
        &self.role_state
    }

    pub(crate) fn files(&self) -> anyhow::Result<RootedDirectory> {
        RootedDirectory::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) fn auth_staging(&self) -> &Path {
        &self.auth_staging
    }

    pub(crate) fn operator_skill(&self) -> &Path {
        &self.operator_skill
    }

    pub(crate) fn driver_lock(&self) -> &Path {
        &self.driver_lock
    }
}

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

    pub(crate) fn role(&self, role_instance: &RoleInstanceId) -> RoleDirs {
        RoleDirs::new(
            self.state_dir.clone(),
            self.root.join("conversations").join(role_instance.as_str()),
        )
    }

    pub(crate) fn task(&self, task_id: &TaskId) -> TaskDirs {
        TaskDirs::new(
            self.state_dir.clone(),
            self.root.join("tasks").join(task_id.as_str()),
        )
    }

    pub(crate) fn effect(&self, effect_id: &EffectId) -> EffectDirs {
        EffectDirs::new(
            self.state_dir.clone(),
            self.root.join("effects").join(effect_id.as_str()),
        )
    }

    pub(crate) fn external_oracle_request_budget(
        &self,
        effect_id: &EffectId,
    ) -> ExternalOracleRequestBudgetDirs {
        self.external_oracle_request_budgets().effect(effect_id)
    }

    pub(crate) fn external_oracle_request_budgets(&self) -> ExternalOracleRequestBudgets {
        ExternalOracleRequestBudgets::new(
            self.state_dir.clone(),
            self.root.join("external-oracle-request-budgets"),
        )
    }
}

/// Durable resources for one exact folded conversation generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoleDirs {
    role_state: RoleStateDirs,
}

impl RoleDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            role_state: RoleStateDirs::new(state_dir.clone(), &root),
        }
    }

    pub(crate) fn role_state(&self) -> &RoleStateDirs {
        &self.role_state
    }

    pub(crate) async fn remove_disposable_scratch(&self) -> std::io::Result<()> {
        let state_dir = self.role_state.state_dir.clone();
        let scratch = self.role_state.scratch.clone();
        tokio::task::spawn_blocking(move || remove_tree_beneath(&state_dir, &scratch))
            .await
            .map_err(|error| {
                std::io::Error::other(format!("role scratch cleanup task failed: {error}"))
            })?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskDirs {
    state_dir: PathBuf,
    root: PathBuf,
    work: PathBuf,
    observer_index: PathBuf,
    workspace_archives: PathBuf,
}

impl TaskDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            work: root.join("work"),
            observer_index: root.join("observer.index"),
            workspace_archives: root.join("workspace-archives"),
            state_dir,
            root,
        }
    }

    pub(crate) fn files(&self) -> anyhow::Result<RootedDirectory> {
        RootedDirectory::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) fn work(&self) -> &Path {
        &self.work
    }

    pub(crate) fn observer_index(&self) -> &Path {
        &self.observer_index
    }

    pub(crate) fn workspace_archive(&self, effect_id: &EffectId) -> PathBuf {
        self.workspace_archives.join(effect_id.as_str())
    }

    pub(crate) fn prepare_workspace_archives(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(&self.state_dir, [&self.workspace_archives])
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeRetentionUsage {
    pub(crate) bytes: u64,
    pub(crate) entries: usize,
    pub(crate) max_depth: usize,
    pub(crate) profiles: usize,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeRetentionPolicy {
    tree: MetadataTreeLimits,
    max_profiles: usize,
}

const RUNTIME_RETENTION_POLICY: RuntimeRetentionPolicy = RuntimeRetentionPolicy {
    tree: MetadataTreeLimits {
        max_bytes: 512 * 1024 * 1024,
        max_entries: 100_000,
        max_depth: 128,
    },
    max_profiles: 8,
};

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

    pub(crate) fn runtime_profile(&self, profile_key: &str) -> anyhow::Result<RuntimeProfileDirs> {
        RuntimeProfileDirs::new(self.clone(), profile_key)
    }

    /// Admit one retained native-state profile under the fixed product policy.
    ///
    /// This is a metadata-only check. It neither reads runtime-owned contents
    /// nor deletes retained state.
    pub(crate) fn admit_runtime_profile(
        &self,
        profile_key: &str,
    ) -> anyhow::Result<RuntimeRetentionUsage> {
        self.assess_runtime_retention_with_profile(Some(profile_key))
    }

    pub(crate) async fn admit_runtime_profile_async(
        &self,
        profile_key: String,
    ) -> anyhow::Result<RuntimeRetentionUsage> {
        account_runtime_retention(self.clone(), Some(profile_key)).await
    }

    /// Account all retained writable runtime state under the fixed product
    /// policy. `/runtime` and host-owned session control are one combined
    /// budget; profile count is the exact immediate directory namespace.
    pub(crate) fn assess_runtime_retention(&self) -> anyhow::Result<RuntimeRetentionUsage> {
        self.assess_runtime_retention_with_profile(None)
    }

    pub(crate) async fn assess_runtime_retention_async(
        &self,
    ) -> anyhow::Result<RuntimeRetentionUsage> {
        account_runtime_retention(self.clone(), None).await
    }

    fn assess_runtime_retention_with_profile(
        &self,
        prospective_profile: Option<&str>,
    ) -> anyhow::Result<RuntimeRetentionUsage> {
        let policy = RUNTIME_RETENTION_POLICY;
        let runtime = RootedDirectory::new(self.state_dir.clone(), self.runtime.clone())?
            .account_metadata(policy.tree, "retained role runtime state")?;
        let session_control =
            RootedDirectory::new(self.state_dir.clone(), self.session_control.clone())?
                .account_metadata(policy.tree, "retained runtime session control")?;
        let profiles_root = RootedDirectory::new(
            self.state_dir.clone(),
            self.session_control.join("profiles"),
        )?;
        let profiles = profiles_root
            .immediate_directory_names(policy.max_profiles, "retained runtime profiles")?;
        let profile_count = profiles.len()
            + usize::from(
                prospective_profile.is_some_and(|profile| !profiles.contains(OsStr::new(profile))),
            );
        if profile_count > policy.max_profiles {
            return Err(anyhow::anyhow!(
                "retained runtime profile limit exceeded: observed {}, maximum {}",
                profile_count,
                policy.max_profiles
            ));
        }

        let usage = RuntimeRetentionUsage {
            bytes: runtime
                .bytes
                .checked_add(session_control.bytes)
                .ok_or_else(|| {
                    runtime_retention_limit_error(
                        MetadataTreeLimit::Bytes,
                        u64::MAX,
                        policy.tree.max_bytes,
                    )
                })?,
            entries: runtime
                .entries
                .checked_add(session_control.entries)
                .ok_or_else(|| {
                    runtime_retention_limit_error(
                        MetadataTreeLimit::Entries,
                        u64::MAX,
                        policy.tree.max_entries as u64,
                    )
                })?,
            max_depth: runtime.max_depth.max(session_control.max_depth),
            profiles: profile_count,
        };
        enforce_runtime_retention_usage(usage, policy)?;
        Ok(usage)
    }
}

async fn account_runtime_retention(
    role_state: RoleStateDirs,
    prospective_profile: Option<String>,
) -> anyhow::Result<RuntimeRetentionUsage> {
    tokio::task::spawn_blocking(move || match prospective_profile {
        Some(profile) => role_state.admit_runtime_profile(&profile),
        None => role_state.assess_runtime_retention(),
    })
    .await
    .map_err(|error| anyhow::anyhow!("retained runtime accounting task failed: {error}"))?
}

fn enforce_runtime_retention_usage(
    usage: RuntimeRetentionUsage,
    policy: RuntimeRetentionPolicy,
) -> anyhow::Result<()> {
    if usage.bytes > policy.tree.max_bytes {
        return Err(runtime_retention_limit_error(
            MetadataTreeLimit::Bytes,
            usage.bytes,
            policy.tree.max_bytes,
        ));
    }
    if usage.entries > policy.tree.max_entries {
        return Err(runtime_retention_limit_error(
            MetadataTreeLimit::Entries,
            usage.entries as u64,
            policy.tree.max_entries as u64,
        ));
    }
    if usage.max_depth > policy.tree.max_depth {
        return Err(runtime_retention_limit_error(
            MetadataTreeLimit::Depth,
            usage.max_depth as u64,
            policy.tree.max_depth as u64,
        ));
    }
    Ok(())
}

fn runtime_retention_limit_error(
    limit: MetadataTreeLimit,
    observed: u64,
    maximum: u64,
) -> anyhow::Error {
    anyhow::anyhow!(MetadataTreeLimitExceeded {
        limit,
        observed,
        maximum,
    })
}

/// Retained runtime-owned state for one exact role-state and compatible runtime
/// profile. Only `native_home` is projected into the confined runtime; control
/// files remain host-owned siblings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeProfileDirs {
    role_state: RoleStateDirs,
    runtime_state: lionclaw_runtime_api::RuntimeStateDir,
    native_home: PathBuf,
}

impl RuntimeProfileDirs {
    fn new(role_state: RoleStateDirs, profile_key: &str) -> anyhow::Result<Self> {
        let runtime_state = lionclaw_runtime_api::RuntimeStateDir::new(
            &role_state.state_dir,
            &role_state.session_control,
            profile_key,
        )?;
        let native_home = runtime_state.path().join("native-home");
        Ok(Self {
            role_state,
            runtime_state,
            native_home,
        })
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_private_dirs_beneath(
            &self.role_state.state_dir,
            [self.runtime_state.control_path(), self.runtime_state.path()],
        )?;
        ensure_dirs_beneath(&self.role_state.state_dir, [&self.native_home])
    }

    pub(crate) fn runtime_state(&self) -> &lionclaw_runtime_api::RuntimeStateDir {
        &self.runtime_state
    }

    pub(crate) fn role_state(&self) -> &RoleStateDirs {
        &self.role_state
    }

    pub(crate) fn native_home(&self) -> &Path {
        &self.native_home
    }

    pub(crate) fn prepare_native_home_dir(&self, relative: &Path) -> anyhow::Result<PathBuf> {
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            anyhow::bail!(
                "runtime native-home directory '{}' must be a non-empty relative path",
                relative.display()
            );
        }
        let path = self.native_home.join(relative);
        ensure_dirs_beneath(&self.role_state.state_dir, [&path])?;
        Ok(path)
    }
}

/// Mission-owned namespace for durable external-oracle request authority.
/// Entries survive disposable effect cleanup until their outcome is recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalOracleRequestBudgets {
    state_dir: PathBuf,
    root: PathBuf,
}

impl ExternalOracleRequestBudgets {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self { state_dir, root }
    }

    pub(crate) fn effect(&self, effect_id: &EffectId) -> ExternalOracleRequestBudgetDirs {
        ExternalOracleRequestBudgetDirs::new(
            self.state_dir.clone(),
            self.root.join(effect_id.as_str()),
        )
    }

    pub(crate) fn effect_ids(&self, max_effects: usize) -> anyhow::Result<BTreeSet<EffectId>> {
        RootedDirectory::new(self.state_dir.clone(), self.root.clone())?
            .immediate_directory_names(max_effects, "external oracle request budgets")?
            .into_iter()
            .map(|name| {
                let name = name.into_string().map_err(|_| {
                    anyhow::anyhow!("external oracle request budget name is not UTF-8")
                })?;
                EffectId::parse(name).map_err(anyhow::Error::from)
            })
            .collect()
    }
}

/// One effect's durable external-oracle request authority accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternalOracleRequestBudgetDirs {
    state_dir: PathBuf,
    root: PathBuf,
}

impl ExternalOracleRequestBudgetDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self { state_dir, root }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_private_dirs_beneath(&self.state_dir, [&self.root])
    }

    pub(crate) fn files(&self) -> anyhow::Result<RootedDirectory> {
        RootedDirectory::new(self.state_dir.clone(), self.root.clone())
    }

    pub(crate) async fn remove(&self) -> std::io::Result<()> {
        let state_dir = self.state_dir.clone();
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || remove_tree_beneath(&state_dir, &root))
            .await
            .map_err(|error| {
                std::io::Error::other(format!(
                    "external oracle request budget cleanup task failed: {error}"
                ))
            })?
    }
}

/// Disposable resources for one effect. No retained conversation work,
/// native runtime state, or durable authority accounting belongs in this tree.
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
    auth_staging: PathBuf,
    role_state: RoleStateDirs,
}

impl RoleEffectDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            handoff: root.join("handoff"),
            auth_staging: root.join("auth-staging"),
            role_state: RoleStateDirs::new(state_dir.clone(), &root),
            state_dir,
            root,
        }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(&self.state_dir, [&self.handoff, &self.auth_staging])
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn handoff(&self) -> &Path {
        &self.handoff
    }

    pub(crate) fn auth_staging(&self) -> &Path {
        &self.auth_staging
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
}

impl OracleEffectDirs {
    fn new(state_dir: PathBuf, root: PathBuf) -> Self {
        Self {
            scratch: root.join("scratch"),
            work: root.join("work"),
            state_dir,
            root,
        }
    }

    pub(crate) fn prepare(&self) -> std::io::Result<()> {
        ensure_dirs_beneath(&self.state_dir, [&self.scratch])
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
    let directory = open(state_dir, directory_flags(), Mode::empty()).map_err(|error| {
        contextual_io(
            error,
            format!(
                "state directory '{}' must be a real directory",
                state_dir.display()
            ),
        )
    })?;
    fchmod(&directory, Mode::from_raw_mode(0o700)).map_err(|error| {
        contextual_io(
            error,
            format!(
                "failed to protect state directory '{}'",
                state_dir.display()
            ),
        )
    })?;
    Ok(directory)
}

fn contextual_io(error: Errno, context: String) -> std::io::Error {
    let source = std::io::Error::from(error);
    std::io::Error::new(source.kind(), format!("{context}: {source}"))
}

fn ensure_dirs_beneath(
    state_dir: &Path,
    targets: impl IntoIterator<Item = impl AsRef<Path>>,
) -> std::io::Result<()> {
    ensure_dirs_beneath_with_policy(state_dir, targets, false)
}

fn ensure_private_dirs_beneath(
    state_dir: &Path,
    targets: impl IntoIterator<Item = impl AsRef<Path>>,
) -> std::io::Result<()> {
    ensure_dirs_beneath_with_policy(state_dir, targets, true)
}

fn ensure_dirs_beneath_with_policy(
    state_dir: &Path,
    targets: impl IntoIterator<Item = impl AsRef<Path>>,
    protect_existing: bool,
) -> std::io::Result<()> {
    for target in targets {
        let target = target.as_ref();
        let mut parent = open_state_dir(state_dir)?;
        let mut display = state_dir.to_path_buf();
        for name in resource_components(state_dir, target)? {
            display.push(name);
            let created = match mkdirat(&parent, name, Mode::from_raw_mode(0o700)) {
                Ok(()) => true,
                Err(Errno::EXIST) => false,
                Err(error) => {
                    return Err(contextual_io(
                        error,
                        format!("failed to create mission resource '{}'", display.display()),
                    ))
                }
            };
            if created {
                // An adversarial umask may create the directory with no search
                // permission, so make the exact new entry openable before
                // validating and hardening its descriptor below.
                chmodat(&parent, name, Mode::from_raw_mode(0o700), AtFlags::empty()).map_err(
                    |error| {
                        contextual_io(
                            error,
                            format!("failed to protect mission resource '{}'", display.display()),
                        )
                    },
                )?;
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
            if created || protect_existing {
                fchmod(&parent, Mode::from_raw_mode(0o700)).map_err(|error| {
                    contextual_io(
                        error,
                        format!("failed to protect mission resource '{}'", display.display()),
                    )
                })?;
            }
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
mod team_resource_tests {
    use super::*;

    #[test]
    fn task_workspace_identity_is_independent_of_role_and_runtime_identity() {
        let state = Path::new("/state");
        let mission = MissionId::parse("mabc123abc123").unwrap();
        let task = TaskId::new("implement").unwrap();
        let engineer = RoleInstanceId::new("engineer").unwrap();
        let specialist = RoleInstanceId::new("specialist").unwrap();
        let dirs = MissionDirs::new(state, &mission);

        let task_work = dirs.task(&task).work().to_path_buf();
        assert_eq!(
            task_work,
            state.join("missions/mabc123abc123/tasks/implement/work")
        );
        assert_eq!(dirs.task(&task).work(), task_work);

        let engineer_runtime = dirs.role(&engineer).role_state().runtime().to_path_buf();
        let specialist_runtime = dirs.role(&specialist).role_state().runtime().to_path_buf();
        assert_eq!(
            engineer_runtime,
            state.join("missions/mabc123abc123/conversations/engineer/runtime")
        );
        assert_ne!(engineer_runtime, specialist_runtime);
        assert!(!task_work.starts_with(state.join("missions/mabc123abc123/conversations")));
    }
}
