//! Bounded, disposable activity projection. Mission authority remains the
//! append-only event log; this file exists only for cheap live observation.

use std::ffi::OsStr;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::model::{
    InflightEffect, MissionState, OutputSemantics, TaskId, TaskKind, TaskNamespace,
};
use crate::resources::MissionDirs;

const MAX_EFFECTS: usize = 32;
const MAX_TEXT: usize = 4 * 1024;
pub const MAX_PROJECTION_BYTES: u64 = 256 * 1024;
const MAX_DRIVER_STDERR_BYTES: u64 = 64 * 1024;
const ACTIVITY_FILE: &str = "activity.json";
const DRIVER_ERROR_FILE: &str = "driver-error.txt";
const DRIVER_STDERR_FILE: &str = "driver-stderr.txt";
const MAX_CONCURRENT_OBSERVERS: usize = 4;
const TOTAL_OBSERVATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum WorkspaceObservation {
    NotApplicable,
    NotCreated,
    Clean,
    Changed { diffstat: String },
    Unavailable { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityProjection {
    pub version: u32,
    pub mission_id: String,
    pub event_head: u64,
    pub generated_at_ms: i64,
    pub effects: Vec<EffectActivity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectActivity {
    pub effect_id: String,
    pub applied_model: Option<String>,
    pub model_confirmation: Option<lionclaw_runtime_api::RuntimeConfigurationConfirmation>,
    pub applied_mode: Option<String>,
    pub mode_confirmation: Option<lionclaw_runtime_api::RuntimeConfigurationConfirmation>,
    pub elapsed_ms: i64,
    pub last_activity: String,
    pub tool_activity: Option<String>,
    pub workspace: WorkspaceObservation,
}

/// Load the disposable observer only when it describes this exact fresh fold.
/// Any read, shape, bound, or authority mismatch is deliberately suppressed.
pub(crate) fn load_validated(
    mission_dirs: &MissionDirs,
    state: &MissionState,
) -> Option<ActivityProjection> {
    let bytes = mission_dirs
        .files()
        .ok()?
        .read_bounded(
            OsStr::new(ACTIVITY_FILE),
            MAX_PROJECTION_BYTES as usize,
            "activity projection",
        )
        .ok()??;
    let projection: ActivityProjection = serde_json::from_slice(&bytes).ok()?;
    let expected = state
        .inflight
        .keys()
        .map(|id| id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let observed = projection
        .effects
        .iter()
        .map(|effect| effect.effect_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let bounded_effect = |effect: &EffectActivity| {
        effect.effect_id.len() <= MAX_TEXT
            && effect.last_activity.len() <= MAX_TEXT
            && effect
                .tool_activity
                .as_ref()
                .is_none_or(|text| text.len() <= MAX_TEXT)
            && match &effect.workspace {
                WorkspaceObservation::Changed { diffstat } => diffstat.len() <= MAX_TEXT,
                WorkspaceObservation::Unavailable { reason } => reason.len() <= MAX_TEXT,
                _ => true,
            }
    };
    (projection.version == 4
        && projection.mission_id == state.mission_id.as_str()
        && projection.event_head == state.head
        && projection.effects.len() <= MAX_EFFECTS
        && observed.len() == projection.effects.len()
        && observed == expected
        && projection.effects.iter().all(bounded_effect))
    .then_some(projection)
}

pub fn path(mission_dir: &Path) -> PathBuf {
    mission_dir.join(ACTIVITY_FILE)
}

#[expect(
    clippy::disallowed_methods,
    reason = "timestamps a disposable observer projection, never mission policy or event facts"
)]
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
fn driver_error_path(mission_dir: &Path) -> PathBuf {
    mission_dir.join(DRIVER_ERROR_FILE)
}

#[cfg(test)]
fn driver_stderr_path(mission_dir: &Path) -> PathBuf {
    mission_dir.join(DRIVER_STDERR_FILE)
}

pub(crate) fn clear_driver_run_evidence(mission_dirs: &MissionDirs) -> Result<()> {
    let files = mission_dirs.files()?;
    for name in [DRIVER_ERROR_FILE, DRIVER_STDERR_FILE] {
        let _removed = files.remove_file(OsStr::new(name), "driver evidence")?;
    }
    Ok(())
}

pub(crate) fn clear_driver_error(mission_dirs: &MissionDirs) -> Result<()> {
    let _removed = mission_dirs
        .files()?
        .remove_file(OsStr::new(DRIVER_ERROR_FILE), "driver error")?;
    Ok(())
}

pub(crate) fn spool_driver_stderr(mut input: impl Read, mission_dirs: &MissionDirs) -> Result<()> {
    let mut bytes = Vec::with_capacity(MAX_DRIVER_STDERR_BYTES as usize);
    let mut retained = 0_u64;
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let available = MAX_DRIVER_STDERR_BYTES.saturating_sub(retained) as usize;
        let keep = read.min(available);
        if keep > 0 {
            bytes.extend_from_slice(&buffer[..keep]);
            retained += keep as u64;
        }
    }
    mission_dirs.prepare()?;
    mission_dirs.files()?.write_private_atomic(
        OsStr::new(DRIVER_STDERR_FILE),
        &bytes,
        MAX_DRIVER_STDERR_BYTES as usize,
        "driver stderr",
    )
}

pub(crate) fn record_driver_error(mission_dirs: &MissionDirs, error: &anyhow::Error) -> Result<()> {
    mission_dirs.prepare()?;
    mission_dirs.files()?.write_private_atomic(
        OsStr::new(DRIVER_ERROR_FILE),
        bounded(&format!("{error:#}")).as_bytes(),
        MAX_TEXT,
        "driver error",
    )
}

pub(crate) fn driver_error(mission_dirs: &MissionDirs) -> Option<String> {
    read_driver_diagnostic(mission_dirs, DRIVER_ERROR_FILE)
        .filter(|text| !text.trim().is_empty())
        .or_else(|| {
            read_driver_diagnostic(mission_dirs, DRIVER_STDERR_FILE)
                .map(|text| bounded(&text))
                .filter(|text| !text.trim().is_empty())
        })
}

fn read_driver_diagnostic(mission_dirs: &MissionDirs, file_name: &str) -> Option<String> {
    let bytes = mission_dirs
        .files()
        .ok()?
        .read_bounded(
            OsStr::new(file_name),
            MAX_DRIVER_STDERR_BYTES as usize,
            "driver diagnostic",
        )
        .ok()??;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

pub async fn publish_observed(
    store: &crate::store::MissionStore,
    state: &MissionState,
    now_ms: i64,
    observation: Option<&(crate::model::EffectId, lionclaw_runtime_api::TurnEvent)>,
) -> Result<()> {
    let workspace_root = store
        .lionclaw_dir()
        .parent()
        .context(".lionclaw directory has no workspace parent")?;
    let mission_dirs = MissionDirs::new(store.lionclaw_dir(), &state.mission_id);
    mission_dirs.prepare().with_context(|| {
        format!(
            "creating mission directory '{}'",
            mission_dirs.root().display()
        )
    })?;
    let previous = load_validated(&mission_dirs, state);
    let mut workspace_requests = Vec::new();
    let mut workspace_observations = std::collections::BTreeMap::new();
    for (effect_id, effect) in state.inflight.iter().take(MAX_EFFECTS) {
        if !matches!(effect, InflightEffect::RoleRun { .. }) {
            continue;
        }
        match effect {
            InflightEffect::RoleRun { output, .. } => {
                let authority = if *output == OutputSemantics::ProducesArtifact {
                    state.active_workspace_conversation(effect_id)
                } else {
                    state.active_role_conversation(effect_id)
                };
                match authority {
                    Ok((conversation_id, conversation)) => {
                        if *output != OutputSemantics::ProducesArtifact {
                            continue;
                        }
                        let dirs = mission_dirs.conversation(conversation_id);
                        workspace_requests.push((
                            effect_id.clone(),
                            workspace_root.to_path_buf(),
                            dirs.work().to_path_buf(),
                            dirs.observer_index().to_path_buf(),
                            Some(conversation.workspace_base_sha.clone()),
                        ));
                    }
                    Err(reason) => {
                        workspace_observations.insert(
                            effect_id.clone(),
                            WorkspaceObservation::Unavailable {
                                reason: reason.into(),
                            },
                        );
                    }
                }
            }
            InflightEffect::OracleRun { .. } | InflightEffect::TerminalReview { .. } => {}
        }
    }
    workspace_observations.extend(observe_workspaces(workspace_requests).await);
    let effects = state
        .inflight
        .iter()
        .take(MAX_EFFECTS)
        .map(|(effect_id, effect)| {
            let requested_at_ms = match effect {
                InflightEffect::RoleRun {
                    requested_at_ms, ..
                } => *requested_at_ms,
                InflightEffect::OracleRun {
                    requested_at_ms, ..
                } => *requested_at_ms,
                InflightEffect::TerminalReview {
                    requested_at_ms, ..
                } => *requested_at_ms,
            };
            let workspace = workspace_observations
                .get(effect_id)
                .cloned()
                .unwrap_or(WorkspaceObservation::NotApplicable);
            let prior = previous.as_ref().and_then(|projection| {
                projection
                    .effects
                    .iter()
                    .find(|prior| prior.effect_id == effect_id.as_str())
            });
            let configuration = match effect {
                InflightEffect::RoleRun {
                    runtime_configuration,
                    ..
                }
                | InflightEffect::TerminalReview {
                    runtime_configuration,
                    ..
                } => runtime_configuration.as_ref(),
                InflightEffect::OracleRun { .. } => None,
            };
            let not_before_ms = effect.not_before_ms();
            let scheduled = now_ms < not_before_ms;
            EffectActivity {
                effect_id: effect_id.as_str().to_string(),
                // Prefer the adapter confirmation folded for this exact
                // effect; the prior file supplies only same-effect live data.
                applied_model: configuration
                    .and_then(|configuration| configuration.applied_model.clone())
                    .or_else(|| prior.and_then(|prior| prior.applied_model.clone())),
                model_confirmation: configuration
                    .and_then(|configuration| configuration.model_confirmation)
                    .or_else(|| prior.and_then(|prior| prior.model_confirmation)),
                applied_mode: configuration
                    .and_then(|configuration| configuration.applied_mode.clone())
                    .or_else(|| prior.and_then(|prior| prior.applied_mode.clone())),
                mode_confirmation: configuration
                    .and_then(|configuration| configuration.mode_confirmation)
                    .or_else(|| prior.and_then(|prior| prior.mode_confirmation)),
                elapsed_ms: now_ms
                    .saturating_sub(not_before_ms.max(requested_at_ms))
                    .max(0),
                last_activity: if scheduled {
                    format!("retry scheduled for {not_before_ms}")
                } else {
                    prior.map_or_else(
                        || "effect running".into(),
                        |prior| prior.last_activity.clone(),
                    )
                },
                tool_activity: prior.and_then(|prior| prior.tool_activity.clone()),
                workspace,
            }
        })
        .collect();
    let mut projection = ActivityProjection {
        version: 4,
        mission_id: state.mission_id.as_str().to_string(),
        event_head: state.head,
        generated_at_ms: now_ms,
        effects,
    };
    if let Some((effect_id, event)) = observation {
        apply_runtime_event(&mut projection, effect_id, event, now_ms);
    }
    write(&mission_dirs, &projection).await
}

fn apply_runtime_event(
    projection: &mut ActivityProjection,
    effect_id: &crate::model::EffectId,
    event: &lionclaw_runtime_api::TurnEvent,
    now_ms: i64,
) {
    use lionclaw_runtime_api::{RuntimeEvent, RuntimeMessageLane};

    let Some(effect) = projection
        .effects
        .iter_mut()
        .find(|effect| effect.effect_id == effect_id.as_str())
    else {
        return;
    };
    match event.event() {
        RuntimeEvent::Configuration { configuration } => {
            effect.applied_model = configuration.applied_model.clone();
            effect.model_confirmation = configuration.model_confirmation;
            effect.applied_mode = configuration.applied_mode.clone();
            effect.mode_confirmation = configuration.mode_confirmation;
            effect.last_activity = "runtime configuration applied".into();
        }
        RuntimeEvent::Status { code, text } => {
            effect.last_activity = bounded(text);
            effect.tool_activity = code.clone().map(|code| bounded(&code));
        }
        RuntimeEvent::Artifact { artifact } => {
            effect.last_activity = "runtime produced an artifact".into();
            effect.tool_activity = Some(bounded(&artifact.artifact_id));
        }
        RuntimeEvent::FileChange { change } => {
            effect.last_activity = format!("runtime file change: {:?}", change.status);
            effect.tool_activity = Some(bounded(&change.paths.join(", ")));
        }
        RuntimeEvent::MessageDelta { lane, .. } => {
            effect.last_activity = match lane {
                RuntimeMessageLane::Answer => "agent response streaming",
                RuntimeMessageLane::Reasoning => "agent reasoning",
            }
            .into();
        }
        RuntimeEvent::MessageBoundary { lane } => {
            effect.last_activity = match lane {
                RuntimeMessageLane::Answer => "agent response boundary",
                RuntimeMessageLane::Reasoning => "agent reasoning boundary",
            }
            .into();
        }
        RuntimeEvent::Done => effect.last_activity = "runtime turn completed".into(),
        RuntimeEvent::Error { code, text } => {
            effect.last_activity = bounded(text);
            effect.tool_activity = code.clone().map(|code| bounded(&code));
        }
    }
    projection.generated_at_ms = now_ms;
}

#[cfg(test)]
async fn read(mission_dirs: &MissionDirs) -> Result<ActivityProjection> {
    let bytes = mission_dirs
        .files()?
        .read_bounded(
            OsStr::new(ACTIVITY_FILE),
            MAX_PROJECTION_BYTES as usize,
            "activity projection",
        )?
        .context("activity projection is absent")?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn write(mission_dirs: &MissionDirs, projection: &ActivityProjection) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(projection)?;
    let files = mission_dirs.files()?;
    tokio::task::spawn_blocking(move || {
        files.write_private_atomic(
            OsStr::new(ACTIVITY_FILE),
            &bytes,
            MAX_PROJECTION_BYTES as usize,
            "activity projection",
        )
    })
    .await
    .context("joining activity projection publication")?
}

async fn workspace_observation(
    repo: &Path,
    workspace: &Path,
    observer_index: &Path,
    base_sha: Option<&str>,
) -> WorkspaceObservation {
    if let Err(observation) =
        classify_workspace_metadata(tokio::fs::symlink_metadata(workspace).await)
    {
        return observation;
    }
    let Some(base_sha) = base_sha else {
        return WorkspaceObservation::Unavailable {
            reason: "workspace exists before its base was recorded".into(),
        };
    };
    match crate::workspace::observe_checkout(repo, workspace, observer_index, base_sha).await {
        Ok(summary) if summary.is_empty() => WorkspaceObservation::Clean,
        Ok(diffstat) => WorkspaceObservation::Changed {
            diffstat: bounded(&diffstat),
        },
        Err(error) => WorkspaceObservation::Unavailable {
            reason: bounded(&format!("{error:#}")),
        },
    }
}

fn classify_workspace_metadata(
    metadata: std::io::Result<std::fs::Metadata>,
) -> std::result::Result<(), WorkspaceObservation> {
    match metadata {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(WorkspaceObservation::Unavailable {
            reason: "workspace path is not a directory".into(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(WorkspaceObservation::NotCreated)
        }
        Err(error) => Err(WorkspaceObservation::Unavailable {
            reason: bounded(&format!("workspace metadata unavailable: {error}")),
        }),
    }
}

pub async fn task_workspace_observations(
    lionclaw_dir: &Path,
    state: &MissionState,
) -> std::collections::BTreeMap<crate::model::TaskId, WorkspaceObservation> {
    let mut observations = state
        .tasks
        .keys()
        .cloned()
        .map(|task_id| (task_id, WorkspaceObservation::NotApplicable))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mission_dirs = MissionDirs::new(lionclaw_dir, &state.mission_id);
    let repo = lionclaw_dir
        .parent()
        .expect(".lionclaw directory has a workspace parent")
        .to_path_buf();
    let mut requests = Vec::new();
    for task_id in state
        .tasks
        .keys()
        .filter(|task_id| task_workspace_applicable(state, TaskNamespace::Execution, task_id))
    {
        match state.task_workspace_conversation(TaskNamespace::Execution, task_id) {
            Ok(Some((conversation_id, conversation))) => {
                let dirs = mission_dirs.conversation(conversation_id);
                requests.push((
                    task_id.clone(),
                    repo.clone(),
                    dirs.work().to_path_buf(),
                    dirs.observer_index().to_path_buf(),
                    Some(conversation.workspace_base_sha.clone()),
                ));
            }
            Ok(None) => {
                observations.insert(task_id.clone(), WorkspaceObservation::NotCreated);
            }
            Err(reason) => {
                observations.insert(
                    task_id.clone(),
                    WorkspaceObservation::Unavailable {
                        reason: reason.into(),
                    },
                );
            }
        }
    }
    observations.extend(observe_workspaces(requests).await);
    observations
}

fn task_workspace_applicable(
    state: &MissionState,
    namespace: TaskNamespace,
    task_id: &TaskId,
) -> bool {
    if namespace != TaskNamespace::Execution {
        return false;
    }
    state
        .tasks
        .get(task_id)
        .is_some_and(|task| task.workspace_provenance.is_some())
        || state.plan.as_ref().is_some_and(|plan| {
            plan.tasks
                .iter()
                .any(|task| task.id == *task_id && task.kind == TaskKind::Work)
        })
}

async fn observe_workspaces<K>(
    requests: Vec<(K, PathBuf, PathBuf, PathBuf, Option<String>)>,
) -> std::collections::BTreeMap<K, WorkspaceObservation>
where
    K: Clone + Ord + Send + 'static,
{
    let mut observed: std::collections::BTreeMap<K, WorkspaceObservation> = requests
        .iter()
        .map(|(key, _, _, _, _)| {
            (
                key.clone(),
                WorkspaceObservation::Unavailable {
                    reason: "observation budget exhausted".into(),
                },
            )
        })
        .collect();
    let mut tasks = tokio::task::JoinSet::new();
    let mut pending = requests.into_iter();
    let deadline = tokio::time::Instant::now() + TOTAL_OBSERVATION_BUDGET;
    loop {
        while tasks.len() < MAX_CONCURRENT_OBSERVERS {
            let Some((key, repo, workspace, observer_index, base_sha)) = pending.next() else {
                break;
            };
            tasks.spawn(async move {
                let observation =
                    workspace_observation(&repo, &workspace, &observer_index, base_sha.as_deref())
                        .await;
                (key, observation)
            });
        }
        if tasks.is_empty() {
            break;
        }
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(Ok((key, observation)))) => {
                observed.insert(key, observation);
            }
            Ok(Some(Err(_))) => {}
            Ok(None) => break,
            Err(_) => {
                tasks.abort_all();
                break;
            }
        }
    }
    observed
}

fn bounded(text: &str) -> String {
    text.chars().take(MAX_TEXT).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace;
    use lionclaw_runtime_api::{
        AppliedRuntimeConfiguration, RuntimeConfigurationConfirmation, RuntimeEvent, TurnEvent,
    };
    use std::io::Cursor;

    fn test_mission_dirs(temp: &tempfile::TempDir) -> MissionDirs {
        let mission_id = crate::model::MissionId::parse("mabc123def456").unwrap();
        let dirs = MissionDirs::new(temp.path(), &mission_id);
        dirs.prepare().unwrap();
        dirs
    }

    fn observer_files(index: &Path) -> lionclaw_durable_fs::RootedDirectory {
        let parent = index.parent().expect("observer index has a parent");
        lionclaw_durable_fs::RootedDirectory::new(parent, parent).unwrap()
    }

    #[test]
    fn bounded_projection_text_is_capped() {
        assert_eq!(bounded(&"x".repeat(MAX_TEXT + 10)).len(), MAX_TEXT);
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn init_repo(repo: &Path) -> String {
        git(repo, &["init", "-q"]);
        git(repo, &["config", "user.name", "test"]);
        git(repo, &["config", "user.email", "test@local"]);
        git(repo, &["config", "commit.gpgsign", "false"]);
        std::fs::write(repo.join("tracked"), "base\n").unwrap();
        git(repo, &["add", "tracked"]);
        git(repo, &["commit", "-q", "-m", "base"]);
        git(repo, &["rev-parse", "HEAD"])
    }

    #[tokio::test]
    async fn clean_committed_work_is_visible_relative_to_the_recorded_base() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let work = temp.path().join("work");
        let index = temp.path().join("observer.index");
        std::fs::create_dir(&source).unwrap();
        let base = init_repo(&source);
        workspace::create_checkout(&source, &work, &base)
            .await
            .unwrap();
        workspace::prepare_checkout_observer_index(&source, &observer_files(&index), &base, true)
            .await
            .unwrap();
        std::fs::write(work.join("tracked"), "committed result\n").unwrap();
        git(&work, &["add", "tracked"]);
        git(&work, &["commit", "-q", "-m", "worker progress"]);

        let WorkspaceObservation::Changed { diffstat: summary } =
            workspace_observation(&source, &work, &index, Some(&base)).await
        else {
            panic!("committed work must be observed as changed");
        };
        assert!(summary.contains("tracked"));
        assert!(summary.contains(" M tracked"));
    }

    #[tokio::test]
    async fn workspace_observation_never_executes_worker_git_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let work = temp.path().join("work");
        let index = temp.path().join("observer.index");
        std::fs::create_dir(&source).unwrap();
        let base = init_repo(&source);
        workspace::create_checkout(&source, &work, &base)
            .await
            .unwrap();
        workspace::prepare_checkout_observer_index(&source, &observer_files(&index), &base, true)
            .await
            .unwrap();
        let marker = temp.path().join("host-command-ran");
        let monitor = temp.path().join("hostile-monitor");
        std::fs::write(
            &monitor,
            format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
        )
        .unwrap();
        workspace::make_executable(&monitor).unwrap();
        git(
            &work,
            &["config", "core.fsmonitor", monitor.to_str().unwrap()],
        );

        let clean = workspace_observation(&source, &work, &index, Some(&base)).await;
        assert_eq!(clean, WorkspaceObservation::Clean);
        assert!(
            !marker.exists(),
            "observer Git must not execute worker-controlled local config"
        );

        let filter = temp.path().join("hostile-filter");
        std::fs::write(
            &filter,
            format!("#!/bin/sh\ntouch '{}'\ncat\n", marker.display()),
        )
        .unwrap();
        workspace::make_executable(&filter).unwrap();
        std::fs::write(work.join(".gitattributes"), "tracked filter=hostile\n").unwrap();
        git(
            &work,
            &["config", "filter.hostile.clean", filter.to_str().unwrap()],
        );
        std::fs::write(work.join("tracked"), "worker change\n").unwrap();
        std::fs::write(
            work.join(".git/HEAD"),
            "worker metadata must not be opened\n",
        )
        .unwrap();

        let changed = workspace_observation(&source, &work, &index, Some(&base)).await;
        assert!(matches!(changed, WorkspaceObservation::Changed { .. }));
        assert!(
            !marker.exists(),
            "observer Git must not execute worker-controlled filter drivers"
        );
    }

    #[tokio::test]
    async fn failed_workspace_observation_does_not_look_clean() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("not-a-repository"), "retained work\n").unwrap();

        let observation =
            workspace_observation(temp.path(), temp.path(), &temp.path().join("index"), None).await;

        assert!(matches!(
            observation,
            WorkspaceObservation::Unavailable { .. }
        ));
    }

    #[tokio::test]
    async fn non_directory_workspace_is_unavailable_not_uncreated() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("work");
        std::fs::write(&workspace, "retained work\n").unwrap();

        let observation =
            workspace_observation(temp.path(), &workspace, &temp.path().join("index"), None).await;

        assert!(matches!(
            observation,
            WorkspaceObservation::Unavailable { .. }
        ));
    }

    #[tokio::test]
    async fn only_execution_work_tasks_have_task_workspace_observations() {
        use crate::model::{
            Assertion, AssertionId, ConversationId, ConversationLifecycle, ConversationState,
            DecisionAction, EffectId, EventEnvelope, InflightEffect, MissionConfig, MissionEvent,
            MissionId, MissionTypeRef, OracleName, OutputSemantics, Plan, PlanInventory,
            PlanProposal, Requirement, RequirementDisposition, RequirementId, RequirementKind,
            RoleName, RolePromptTemplate, Task, TaskId, TaskKind, TaskNamespace,
            TaskRoleAssignment, TaskWorkspaceProvenance, VersionStamps,
        };
        use lionclaw_model::state::ActiveDelivery;

        let mission_id = MissionId::parse("mabc123abc123").unwrap();
        let task = |id: &str, kind: TaskKind, role: Option<&str>| Task {
            id: TaskId::new(id).unwrap(),
            kind,
            body: "test".into(),
            targets: vec![],
            role: role.map(|role| RoleName::new(role).unwrap()),
            depends_on: vec![],
        };
        let events = vec![
            MissionEvent::MissionCreated {
                objective: "observe workspace applicability".into(),
                mission_type: MissionTypeRef {
                    name: "test".into(),
                    digest: "digest".into(),
                },
                runtime: "codex".into(),
                image_id: "image".into(),
                workspace_dir: "/workspace".into(),
                base_sha: "base".into(),
                config: MissionConfig {
                    plan_inventory: PlanInventory {
                        roles: std::collections::BTreeMap::from([
                            (
                                RoleName::new("worker").unwrap(),
                                OutputSemantics::ProducesArtifact,
                            ),
                            (
                                RoleName::new("validator").unwrap(),
                                OutputSemantics::EmitsVerdict,
                            ),
                        ]),
                        oracles: std::collections::BTreeSet::from([
                            OracleName::new("test-oracle").unwrap()
                        ]),
                    },
                    ..Default::default()
                },
            },
            MissionEvent::PlanProposed {
                proposal: PlanProposal {
                    base_revision: 0,
                    requirement_changes: vec![],
                    assertion_supersessions: vec![],
                    plan: Plan {
                        requirements: vec![Requirement {
                            id: RequirementId::new("REQ-1").unwrap(),
                            kind: RequirementKind::Capability,
                            prose: "observe workspace applicability".into(),
                            disposition: RequirementDisposition::Covered {
                                assertion_ids: vec![AssertionId::new("OBSERVABLE").unwrap()],
                            },
                        }],
                        assertions: vec![Assertion {
                            id: AssertionId::new("OBSERVABLE").unwrap(),
                            prose: "workspace applicability is observable".into(),
                            oracle: Some(OracleName::new("test-oracle").unwrap()),
                        }],
                        tasks: vec![
                            {
                                let mut task = task("work", TaskKind::Work, Some("worker"));
                                task.targets = vec![AssertionId::new("OBSERVABLE").unwrap()];
                                task
                            },
                            {
                                let mut task =
                                    task("validate", TaskKind::Validate, Some("validator"));
                                task.targets = vec![AssertionId::new("OBSERVABLE").unwrap()];
                                task
                            },
                            {
                                let mut task = task("gate", TaskKind::Gate, None);
                                task.body.clear();
                                task.targets = vec![AssertionId::new("OBSERVABLE").unwrap()];
                                task.depends_on = vec![TaskId::new("validate").unwrap()];
                                task
                            },
                        ],
                    },
                },
                plan_hash: "hash".into(),
            },
            MissionEvent::DecisionRecorded {
                attention_id: "plan_proposal:mission".into(),
                action: DecisionAction::Approve,
                justification: "approve".into(),
                requirement_changes: vec![],
            },
        ];
        let mut state = crate::model::fold(events.into_iter().enumerate().map(|(index, event)| {
            EventEnvelope {
                mission_id: mission_id.clone(),
                sequence_no: index as u64,
                recorded_at_ms: 0,
                stamps: VersionStamps::default(),
                event,
            }
        }))
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let observations =
            task_workspace_observations(&temp.path().join(".lionclaw"), &state).await;

        assert_eq!(
            observations[&TaskId::new("work").unwrap()],
            WorkspaceObservation::NotCreated
        );
        assert_eq!(
            observations[&TaskId::new("validate").unwrap()],
            WorkspaceObservation::NotApplicable
        );
        assert_eq!(
            observations[&TaskId::new("gate").unwrap()],
            WorkspaceObservation::NotApplicable
        );
        assert!(!task_workspace_applicable(
            &state,
            TaskNamespace::Planning,
            &TaskId::new("work").unwrap()
        ));

        let base = init_repo(temp.path());
        let task_id = TaskId::new("work").unwrap();
        let role = RoleName::new("worker").unwrap();
        let conversation_id = ConversationId::for_role_instance(
            &mission_id,
            TaskNamespace::Execution,
            &task_id,
            &role,
            1,
        );
        let conversation = ConversationState {
            role,
            namespace: TaskNamespace::Execution,
            task_id: task_id.clone(),
            assignment_epoch: 1,
            workspace_base_sha: base.clone(),
            lifecycle: ConversationLifecycle::Ready,
            queued: vec![],
            consumed_through: 0,
            active_delivery: None,
            final_response: None,
            invalid_handoff_reworks: 0,
        };
        let prepared_effect = EffectId::for_parts(&["settled", "prepared"]);
        let task = state.tasks.get_mut(&task_id).unwrap();
        task.workspace_provenance = Some(TaskWorkspaceProvenance {
            effect_id: prepared_effect,
            conversation_id: conversation_id.clone(),
            base_sha: base.clone(),
            assignment_epoch: 1,
        });
        state
            .conversations
            .insert(conversation_id.clone(), conversation.clone());

        let state_dir = temp.path().join(".lionclaw");
        std::fs::create_dir(&state_dir).unwrap();
        let mission_dirs = MissionDirs::new(&state_dir, &mission_id);
        let conversation_dirs = mission_dirs.conversation(&conversation_id);
        conversation_dirs.role_state().prepare().unwrap();
        workspace::create_checkout(temp.path(), conversation_dirs.work(), &base)
            .await
            .unwrap();
        workspace::prepare_checkout_observer_index(
            temp.path(),
            &conversation_dirs.files().unwrap(),
            &base,
            true,
        )
        .await
        .unwrap();
        std::fs::write(
            conversation_dirs.work().join("tracked"),
            "conversation change\n",
        )
        .unwrap();

        // A populated legacy task path must not compete with the folded
        // conversation generation for observation authority.
        let legacy_root = mission_dirs.root().join("tasks/work");
        let legacy = legacy_root.join("work");
        workspace::create_checkout(temp.path(), &legacy, &base)
            .await
            .unwrap();
        workspace::prepare_checkout_observer_index(
            temp.path(),
            &observer_files(&legacy_root.join("observer.index")),
            &base,
            true,
        )
        .await
        .unwrap();
        std::fs::write(legacy.join("misleading"), "legacy\n").unwrap();
        let observations = task_workspace_observations(&state_dir, &state).await;
        let WorkspaceObservation::Changed { diffstat } = &observations[&task_id] else {
            panic!("exact conversation work must be observed")
        };
        assert!(diffstat.contains("tracked"));
        assert!(!diffstat.contains("misleading"));

        let mut missing = state.clone();
        missing.conversations.remove(&conversation_id);
        let observations = task_workspace_observations(&state_dir, &missing).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("prepared workspace without conversation authority must fail closed")
        };
        assert!(reason.contains("no authoritative conversation"));

        let mut orphaned_running = state.clone();
        orphaned_running
            .conversations
            .get_mut(&conversation_id)
            .unwrap()
            .lifecycle = ConversationLifecycle::Running;
        let observations = task_workspace_observations(&state_dir, &orphaned_running).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("running conversation without an effect must fail closed")
        };
        assert!(reason.contains("without an active effect"));

        let mut stale_delivery = state.clone();
        stale_delivery
            .conversations
            .get_mut(&conversation_id)
            .unwrap()
            .active_delivery = Some(ActiveDelivery {
            effect_id: EffectId::for_parts(&["stale", "delivery"]),
            message_boundary: state.head,
            presented_messages: vec![],
        });
        let observations = task_workspace_observations(&state_dir, &stale_delivery).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("settled conversation with active delivery must fail closed")
        };
        assert!(reason.contains("without an active effect"));

        let mut corrupt = state.clone();
        let conflicting_id = ConversationId::for_role_instance(
            &mission_id,
            TaskNamespace::Execution,
            &task_id,
            &RoleName::new("conflicting-worker").unwrap(),
            1,
        );
        corrupt.conversations.insert(
            conflicting_id,
            ConversationState {
                role: RoleName::new("conflicting-worker").unwrap(),
                ..conversation.clone()
            },
        );
        let observations = task_workspace_observations(&state_dir, &corrupt).await;
        let WorkspaceObservation::Changed { diffstat } = &observations[&task_id] else {
            panic!("unreferenced conversation state must not compete with exact provenance")
        };
        assert!(diffstat.contains("tracked"));

        let mut wrong_role = state.clone();
        wrong_role.conversations.remove(&conversation_id);
        wrong_role.conversations.insert(
            ConversationId::for_role_instance(
                &mission_id,
                TaskNamespace::Execution,
                &task_id,
                &RoleName::new("other-worker").unwrap(),
                1,
            ),
            ConversationState {
                role: RoleName::new("other-worker").unwrap(),
                ..conversation.clone()
            },
        );
        let observations = task_workspace_observations(&state_dir, &wrong_role).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("a different role must not own the current plan task's workspace")
        };
        assert!(reason.contains("no authoritative conversation"));

        // A same-base plan generation moves conversation identity without a
        // workspace recreation decision, but it cannot claim the checkout
        // until its own preparation fact replaces the retained provenance.
        let mut active = state.clone();
        active.revision = 2;
        active.current_sha = base.clone();
        let replacement_id = ConversationId::for_role_instance(
            &mission_id,
            TaskNamespace::Execution,
            &task_id,
            &conversation.role,
            2,
        );
        let effect_id = EffectId::for_role_request(
            TaskNamespace::Execution,
            &mission_id,
            &task_id,
            2,
            2,
            "replacement-prompt",
        );
        let active_task = active.tasks.get_mut(&task_id).unwrap();
        active_task.status = crate::model::TaskStatus::Running;
        active_task.attempts = 2;
        active_task.role_assignment = Some(TaskRoleAssignment {
            base_sha: base.clone(),
            assignment_epoch: 2,
        });
        active.conversations.insert(
            replacement_id.clone(),
            ConversationState {
                assignment_epoch: 2,
                lifecycle: ConversationLifecycle::Running,
                active_delivery: Some(ActiveDelivery {
                    effect_id: effect_id.clone(),
                    message_boundary: active.head.saturating_sub(1),
                    presented_messages: vec![],
                }),
                ..conversation.clone()
            },
        );
        active.inflight.insert(
            effect_id.clone(),
            InflightEffect::RoleRun {
                conversation_id: replacement_id.clone(),
                namespace: TaskNamespace::Execution,
                task_id: task_id.clone(),
                attempt_no: 2,
                role: conversation.role.clone(),
                output: OutputSemantics::ProducesArtifact,
                runtime: "codex".into(),
                prompt_template: RolePromptTemplate::Execution,
                prompt_hash: "replacement-prompt".into(),
                base_sha: base.clone(),
                assignment_epoch: 2,
                message_boundary: active.head.saturating_sub(1),
                presented_messages: vec![],
                recreate_workspace: false,
                runtime_configuration: None,
                requested_at_ms: 0,
                not_before_ms: 0,
                deadline_ms: 1,
                budget_deadline_ms: 1,
                requested_seq: active.head,
            },
        );
        let replacement_dirs = mission_dirs.conversation(&replacement_id);
        workspace::create_checkout(temp.path(), replacement_dirs.work(), &base)
            .await
            .unwrap();
        workspace::prepare_checkout_observer_index(
            temp.path(),
            &replacement_dirs.files().unwrap(),
            &base,
            true,
        )
        .await
        .unwrap();
        std::fs::write(replacement_dirs.work().join("replacement"), "new\n").unwrap();

        let observations = task_workspace_observations(&state_dir, &active).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("pre-preparation active workspace must fail closed")
        };
        assert!(reason.contains("exact prepared workspace authority"));

        active.tasks.get_mut(&task_id).unwrap().workspace_provenance =
            Some(TaskWorkspaceProvenance {
                effect_id: effect_id.clone(),
                conversation_id: replacement_id.clone(),
                base_sha: base.clone(),
                assignment_epoch: 2,
            });
        let reloaded: MissionState = serde_json::from_slice(
            &serde_json::to_vec(&active).expect("serialize valid inflight state"),
        )
        .expect("reload valid inflight state");
        let observations = task_workspace_observations(&state_dir, &reloaded).await;
        let WorkspaceObservation::Changed { diffstat } = &observations[&task_id] else {
            panic!(
                "active conversation must outrank the last prepared task epoch: {:?}",
                observations[&task_id]
            )
        };
        assert!(diffstat.contains("replacement"));
        assert!(!diffstat.contains("tracked"));

        let forged_generation = 3;
        let forged_generation_id = ConversationId::for_role_instance(
            &mission_id,
            TaskNamespace::Execution,
            &task_id,
            &conversation.role,
            forged_generation,
        );
        let forged_effect_id = EffectId::for_role_request(
            TaskNamespace::Execution,
            &mission_id,
            &task_id,
            2,
            forged_generation,
            "replacement-prompt",
        );
        let mut coherent_forgery = active.clone();
        let mut forged_effect = coherent_forgery.inflight.remove(&effect_id).unwrap();
        let InflightEffect::RoleRun {
            conversation_id: forged_conversation_id,
            assignment_epoch: forged_assignment_epoch,
            ..
        } = &mut forged_effect
        else {
            unreachable!("known role effect")
        };
        *forged_conversation_id = forged_generation_id.clone();
        *forged_assignment_epoch = forged_generation;
        coherent_forgery
            .inflight
            .insert(forged_effect_id.clone(), forged_effect);
        let mut forged_conversation = coherent_forgery
            .conversations
            .remove(&replacement_id)
            .unwrap();
        forged_conversation.assignment_epoch = forged_generation;
        forged_conversation.active_delivery = Some(ActiveDelivery {
            effect_id: forged_effect_id.clone(),
            message_boundary: active.head.saturating_sub(1),
            presented_messages: vec![],
        });
        coherent_forgery
            .conversations
            .insert(forged_generation_id.clone(), forged_conversation);
        coherent_forgery
            .tasks
            .get_mut(&task_id)
            .unwrap()
            .workspace_provenance = Some(TaskWorkspaceProvenance {
            effect_id: forged_effect_id.clone(),
            conversation_id: forged_generation_id.clone(),
            base_sha: base.clone(),
            assignment_epoch: forged_generation,
        });
        let forged_dirs = mission_dirs.conversation(&forged_generation_id);
        workspace::create_checkout(temp.path(), forged_dirs.work(), &base)
            .await
            .unwrap();
        workspace::prepare_checkout_observer_index(
            temp.path(),
            &forged_dirs.files().unwrap(),
            &base,
            true,
        )
        .await
        .unwrap();
        std::fs::write(forged_dirs.work().join("forged-generation"), "forged\n").unwrap();
        let observations = task_workspace_observations(&state_dir, &coherent_forgery).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("coherently forged active generation must not select a workspace")
        };
        assert!(reason.contains("folded conversation workspace authority"));

        let forged_id = ConversationId::for_role_instance(
            &mission_id,
            TaskNamespace::Execution,
            &task_id,
            &RoleName::new("forged-key").unwrap(),
            2,
        );
        let mut forged_active = active.clone();
        let replacement = forged_active.conversations.remove(&replacement_id).unwrap();
        forged_active
            .conversations
            .insert(forged_id.clone(), replacement);
        let store = crate::store::MissionStore::open(temp.path()).await.unwrap();
        publish_observed(&store, &forged_active, 0, None)
            .await
            .unwrap();
        let projection: ActivityProjection = serde_json::from_slice(
            &std::fs::read(path(mission_dirs.root())).expect("read activity projection"),
        )
        .expect("decode activity projection");
        let WorkspaceObservation::Unavailable { reason } = &projection.effects[0].workspace else {
            panic!("forged active identity must not select a workspace")
        };
        assert!(reason.contains("absent from folded state"));

        let mut wrong_output = active.clone();
        let InflightEffect::RoleRun { output, .. } = wrong_output
            .inflight
            .get_mut(&effect_id)
            .expect("active role effect")
        else {
            unreachable!("known role effect")
        };
        *output = OutputSemantics::EmitsVerdict;
        publish_observed(&store, &wrong_output, 0, None)
            .await
            .unwrap();
        let projection: ActivityProjection = serde_json::from_slice(
            &std::fs::read(path(mission_dirs.root())).expect("read activity projection"),
        )
        .expect("decode activity projection");
        let WorkspaceObservation::Unavailable { reason } = &projection.effects[0].workspace else {
            panic!("a role output that disagrees with the plan must fail closed")
        };
        assert!(reason.contains("workspace authority disagree"));

        let mut forged = active;
        forged.inflight.clear();
        let task = forged.tasks.get_mut(&task_id).unwrap();
        task.workspace_provenance = Some(TaskWorkspaceProvenance {
            effect_id: EffectId::for_parts(&["forged", "prepared"]),
            conversation_id: forged_id.clone(),
            base_sha: base,
            assignment_epoch: 2,
        });
        let replacement = forged.conversations.remove(&replacement_id).unwrap();
        forged.conversations.insert(forged_id, replacement);
        let observations = task_workspace_observations(&state_dir, &forged).await;
        let WorkspaceObservation::Unavailable { reason } = &observations[&task_id] else {
            panic!("a forged conversation key must not become filesystem authority")
        };
        assert!(reason.contains("does not match its identity"));
    }

    #[test]
    fn workspace_metadata_errors_distinguish_absence_from_unavailability() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("file");
        std::fs::write(&file, "not a workspace\n").unwrap();
        assert_eq!(
            classify_workspace_metadata(std::fs::symlink_metadata(temp.path())),
            Ok(())
        );
        assert!(matches!(
            classify_workspace_metadata(std::fs::symlink_metadata(file)),
            Err(WorkspaceObservation::Unavailable { .. })
        ));
        assert_eq!(
            classify_workspace_metadata(Err(std::io::Error::from(std::io::ErrorKind::NotFound))),
            Err(WorkspaceObservation::NotCreated)
        );
        assert!(matches!(
            classify_workspace_metadata(Err(std::io::Error::from(
                std::io::ErrorKind::PermissionDenied
            ))),
            Err(WorkspaceObservation::Unavailable { .. })
        ));
    }

    #[test]
    fn runtime_activity_channel_coalesces_without_a_queue() {
        let effect_id = crate::model::EffectId::for_parts(&["activity", "coalesced"]);
        let (activity, observed) = tokio::sync::watch::channel(None);
        for index in 0..10_000 {
            activity.send_replace(Some((
                effect_id.clone(),
                lionclaw_runtime_api::TurnEvent::canonical(RuntimeEvent::Status {
                    code: Some("progress".into()),
                    text: format!("event {index}"),
                }),
            )));
        }
        let latest = observed.borrow().clone().unwrap();
        assert_eq!(latest.0, effect_id);
        assert!(matches!(
            latest.1.event(),
            RuntimeEvent::Status { ref text, .. } if text == "event 9999"
        ));
    }

    #[tokio::test]
    async fn runtime_journal_updates_configuration_and_activity_without_message_content() {
        let temp = tempfile::tempdir().unwrap();
        let mission_dirs = test_mission_dirs(&temp);
        let effect_id = crate::model::EffectId::for_parts(&["activity", "effect"]);
        write(
            &mission_dirs,
            &ActivityProjection {
                version: 4,
                mission_id: "mission".into(),
                event_head: 4,
                generated_at_ms: 10,
                effects: vec![EffectActivity {
                    effect_id: effect_id.as_str().into(),
                    applied_model: None,
                    model_confirmation: None,
                    applied_mode: None,
                    mode_confirmation: None,
                    elapsed_ms: 0,
                    last_activity: "effect running".into(),
                    tool_activity: None,
                    workspace: WorkspaceObservation::Clean,
                }],
            },
        )
        .await
        .unwrap();

        let mut projection = read(&mission_dirs).await.unwrap();
        apply_runtime_event(
            &mut projection,
            &effect_id,
            &TurnEvent::canonical(RuntimeEvent::Configuration {
                configuration: AppliedRuntimeConfiguration {
                    requested_model: Some("friendly-name".into()),
                    applied_model: Some("canonical-id".into()),
                    model_confirmation: Some(RuntimeConfigurationConfirmation::Acknowledged),
                    requested_mode: Some("build".into()),
                    applied_mode: Some("build".into()),
                    mode_confirmation: Some(RuntimeConfigurationConfirmation::Observed),
                },
            }),
            20,
        );
        apply_runtime_event(
            &mut projection,
            &effect_id,
            &TurnEvent::canonical(RuntimeEvent::MessageDelta {
                lane: lionclaw_runtime_api::RuntimeMessageLane::Answer,
                text: "secret response text".into(),
            }),
            21,
        );
        write(&mission_dirs, &projection).await.unwrap();

        let projection = read(&mission_dirs).await.unwrap();
        let effect = &projection.effects[0];
        assert_eq!(effect.applied_model.as_deref(), Some("canonical-id"));
        assert_eq!(
            effect.model_confirmation,
            Some(RuntimeConfigurationConfirmation::Acknowledged)
        );
        assert_eq!(effect.applied_mode.as_deref(), Some("build"));
        assert_eq!(
            effect.mode_confirmation,
            Some(RuntimeConfigurationConfirmation::Observed)
        );
        assert_eq!(effect.last_activity, "agent response streaming");
        assert!(!serde_json::to_string(&projection)
            .unwrap()
            .contains("secret response text"));
    }

    #[test]
    fn driver_diagnostics_preserve_unexpected_stderr_but_prefer_structured_errors() {
        let temp = tempfile::tempdir().unwrap();
        let mission_dirs = test_mission_dirs(&temp);
        clear_driver_run_evidence(&mission_dirs).unwrap();
        std::fs::write(
            driver_stderr_path(mission_dirs.root()),
            "panic from detached driver\n",
        )
        .unwrap();
        assert_eq!(
            driver_error(&mission_dirs).as_deref(),
            Some("panic from detached driver\n")
        );

        record_driver_error(&mission_dirs, &anyhow::anyhow!("typed driver failure")).unwrap();
        assert_eq!(
            driver_error(&mission_dirs).as_deref(),
            Some("typed driver failure")
        );
        clear_driver_run_evidence(&mission_dirs).unwrap();
        assert_eq!(driver_error(&mission_dirs), None);
    }

    #[test]
    fn detached_driver_stderr_retention_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let mission_dirs = test_mission_dirs(&temp);
        let source = vec![b'x'; 128 * 1024];
        let mut stderr = Cursor::new(source);
        spool_driver_stderr(&mut stderr, &mission_dirs).unwrap();

        assert_eq!(stderr.position(), 128 * 1024, "the spool keeps draining");
        assert_eq!(
            std::fs::metadata(driver_stderr_path(mission_dirs.root()))
                .unwrap()
                .len(),
            MAX_DRIVER_STDERR_BYTES,
            "retained diagnostics have a fixed disk bound"
        );
        assert_eq!(driver_error(&mission_dirs).unwrap().len(), MAX_TEXT);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(driver_stderr_path(mission_dirs.root()))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn detached_driver_stderr_keeps_draining_when_retention_fails() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("missions"), "occupied").unwrap();
        let mission_id = crate::model::MissionId::parse("mabc123def456").unwrap();
        let invalid_mission_dirs = MissionDirs::new(temp.path(), &mission_id);
        let source = vec![b'x'; 128 * 1024];
        let mut stderr = Cursor::new(source);

        spool_driver_stderr(&mut stderr, &invalid_mission_dirs)
            .expect_err("invalid retention path must be reported");
        assert_eq!(
            stderr.position(),
            128 * 1024,
            "diagnostic failure must not backpressure mission execution"
        );
    }

    #[cfg(unix)]
    #[test]
    fn driver_diagnostic_reads_reject_indirection_without_blocking() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let mission_dirs = test_mission_dirs(&temp);
        let external = temp.path().join("external.txt");
        std::fs::write(&external, "must not be disclosed").unwrap();
        symlink(&external, driver_error_path(mission_dirs.root())).unwrap();

        assert_eq!(driver_error(&mission_dirs), None);
        std::fs::remove_file(driver_error_path(mission_dirs.root())).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            driver_error_path(mission_dirs.root()),
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let started = tokio::time::Instant::now();
        assert_eq!(driver_error(&mission_dirs), None);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[test]
    fn driver_evidence_rejects_a_symlinked_mission_ancestor() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("missions")).unwrap();
        let mission_id = crate::model::MissionId::parse("mabc123def456").unwrap();
        let mission_dirs = MissionDirs::new(temp.path(), &mission_id);
        symlink(outside.path(), mission_dirs.root()).unwrap();
        std::fs::write(outside.path().join(DRIVER_ERROR_FILE), "private\n").unwrap();

        assert_eq!(driver_error(&mission_dirs), None);
        record_driver_error(&mission_dirs, &anyhow::anyhow!("must not escape"))
            .expect_err("symlinked mission root must fail closed");
        assert_eq!(
            std::fs::read_to_string(outside.path().join(DRIVER_ERROR_FILE)).unwrap(),
            "private\n"
        );
    }
}
