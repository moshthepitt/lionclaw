//! Bounded, disposable activity projection. Mission authority remains the
//! append-only event log; this file exists only for cheap live observation.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rustix::fs::{open, Mode, OFlags};
use serde::{Deserialize, Serialize};

use crate::model::{
    ConversationLifecycle, DeliveryMarker, InflightEffect, MissionState, TaskId, TaskKind,
    TaskNamespace,
};

const MAX_EFFECTS: usize = 32;
const MAX_TEXT: usize = 4 * 1024;
const MAX_DRIVER_STDERR_BYTES: u64 = 64 * 1024;
const MAX_DRIVER_DIAGNOSTIC_BYTES: u64 = (MAX_TEXT * 4) as u64;
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
    pub role: Option<String>,
    pub task: Option<String>,
    pub runtime: Option<String>,
    pub applied_model: Option<String>,
    pub model_confirmation: Option<lionclaw_runtime_api::RuntimeConfigurationConfirmation>,
    pub applied_mode: Option<String>,
    pub mode_confirmation: Option<lionclaw_runtime_api::RuntimeConfigurationConfirmation>,
    pub environment: String,
    pub elapsed_ms: i64,
    pub deadline_ms: i64,
    pub last_activity: String,
    pub tool_activity: Option<String>,
    pub workspace: WorkspaceObservation,
    pub queued_controls: Vec<String>,
    pub queued_messages: usize,
    #[serde(default)]
    pub conversation_lifecycle: Option<ConversationLifecycle>,
    #[serde(default)]
    pub message_boundary: Option<u64>,
    #[serde(default)]
    pub delivery_markers: Vec<DeliveryMarker>,
    pub legal_controls: Vec<String>,
}

pub fn path(mission_dir: &Path) -> PathBuf {
    mission_dir.join("activity.json")
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

fn driver_error_path(mission_dir: &Path) -> PathBuf {
    mission_dir.join("driver-error.txt")
}

fn driver_stderr_path(mission_dir: &Path) -> PathBuf {
    mission_dir.join("driver-stderr.txt")
}

fn create_private_diagnostic(path: &Path) -> Result<std::fs::File> {
    let descriptor = open(
        path,
        OFlags::WRONLY | OFlags::CREATE | OFlags::TRUNC | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::RUSR | Mode::WUSR,
    )?;
    let file = std::fs::File::from(descriptor);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

pub fn clear_driver_run_evidence(mission_dir: &Path) -> Result<()> {
    for path in [
        driver_error_path(mission_dir),
        driver_stderr_path(mission_dir),
    ] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub fn clear_driver_error(mission_dir: &Path) -> Result<()> {
    match std::fs::remove_file(driver_error_path(mission_dir)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn spool_driver_stderr(mut input: impl Read, mission_dir: &Path) -> Result<()> {
    let mut failure = None;
    let mut output = match std::fs::create_dir_all(mission_dir)
        .map_err(anyhow::Error::from)
        .and_then(|()| create_private_diagnostic(&driver_stderr_path(mission_dir)))
    {
        Ok(output) => Some(output),
        Err(error) => {
            failure = Some(error);
            None
        }
    };
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
            if let Some(writer) = output.as_mut() {
                match writer.write_all(&buffer[..keep]) {
                    Ok(()) => retained += keep as u64,
                    Err(error) => {
                        failure = Some(error.into());
                        output = None;
                    }
                }
            }
        }
    }
    if let Some(writer) = output.as_mut() {
        if let Err(error) = writer.flush() {
            failure = Some(error.into());
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub fn record_driver_error(mission_dir: &Path, error: &anyhow::Error) -> Result<()> {
    let target = driver_error_path(mission_dir);
    let temporary = target.with_extension("tmp");
    let mut output = create_private_diagnostic(&temporary)?;
    output.write_all(bounded(&format!("{error:#}")).as_bytes())?;
    output.flush()?;
    drop(output);
    std::fs::rename(temporary, target)?;
    Ok(())
}

pub fn driver_error(mission_dir: &Path) -> Option<String> {
    read_driver_diagnostic(&driver_error_path(mission_dir))
        .filter(|text| !text.trim().is_empty())
        .or_else(|| {
            read_driver_diagnostic(&driver_stderr_path(mission_dir))
                .map(|text| bounded(&text))
                .filter(|text| !text.trim().is_empty())
        })
}

fn read_driver_diagnostic(path: &Path) -> Option<String> {
    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .ok()?;
    let file = std::fs::File::from(descriptor);
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::with_capacity(MAX_DRIVER_DIAGNOSTIC_BYTES as usize);
    file.take(MAX_DRIVER_DIAGNOSTIC_BYTES)
        .read_to_end(&mut bytes)
        .ok()?;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

pub async fn publish_observed(
    workspace_root: &Path,
    mission_dir: &Path,
    state: &MissionState,
    now_ms: i64,
    observation: Option<&(crate::model::EffectId, lionclaw_runtime_api::TurnEvent)>,
) -> Result<()> {
    tokio::fs::create_dir_all(mission_dir)
        .await
        .with_context(|| format!("creating mission directory '{}'", mission_dir.display()))?;
    let previous = read(mission_dir).await.ok();
    let workspace_requests = state
        .inflight
        .iter()
        .take(MAX_EFFECTS)
        .filter_map(|(effect_id, effect)| match effect {
            InflightEffect::RoleRun {
                namespace, task_id, ..
            } if task_workspace_applicable(state, *namespace, task_id) => Some((
                effect_id.clone(),
                workspace_root.to_path_buf(),
                mission_dir
                    .join("tasks")
                    .join(task_id.as_str())
                    .join("work"),
                mission_dir
                    .join("tasks")
                    .join(task_id.as_str())
                    .join("observer.index"),
                state
                    .tasks
                    .get(task_id)
                    .and_then(|task| task.workspace_base_sha.clone()),
            )),
            InflightEffect::RoleRun { .. }
            | InflightEffect::OracleRun { .. }
            | InflightEffect::TerminalReview { .. } => None,
        })
        .collect();
    let workspace_observations = observe_workspaces(workspace_requests).await;
    let effects = state
        .inflight
        .iter()
        .take(MAX_EFFECTS)
        .map(|(effect_id, effect)| {
            let (role, task, runtime, requested_at_ms, conversation) = match effect {
                InflightEffect::RoleRun {
                    namespace,
                    role,
                    task_id,
                    runtime,
                    assignment_epoch,
                    requested_at_ms,
                    ..
                } => (
                    Some(role.as_str().to_string()),
                    Some(task_id.as_str().to_string()),
                    Some(runtime.clone()),
                    *requested_at_ms,
                    state
                        .conversations
                        .get(&crate::model::ConversationId::for_role_instance(
                            &state.mission_id,
                            *namespace,
                            task_id,
                            role,
                            *assignment_epoch,
                        )),
                ),
                InflightEffect::OracleRun {
                    oracle,
                    requested_at_ms,
                    ..
                } => (
                    Some(oracle.as_str().to_string()),
                    None,
                    Some("oracle".into()),
                    *requested_at_ms,
                    None,
                ),
                InflightEffect::TerminalReview {
                    role,
                    runtime,
                    requested_at_ms,
                    ..
                } => (
                    Some(role.as_str().to_string()),
                    None,
                    Some(runtime.clone()),
                    *requested_at_ms,
                    None,
                ),
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
            let deadline_reached = state.reached_deadlines.contains_key(effect_id);
            let mut queued_controls = state
                .stop_requests
                .get(effect_id)
                .map(|reason| vec![format!("stop: {}", bounded(reason))])
                .unwrap_or_default();
            if deadline_reached {
                queued_controls.push("deadline cancellation".into());
            }
            EffectActivity {
                effect_id: effect_id.as_str().to_string(),
                role,
                task,
                runtime,
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
                environment: format!(
                    "confinement-image:{}",
                    crate::model::short_hex(&state.image_id)
                ),
                elapsed_ms: now_ms
                    .saturating_sub(not_before_ms.max(requested_at_ms))
                    .max(0),
                deadline_ms: effect.deadline_ms(),
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
                queued_controls,
                queued_messages: conversation.map_or(0, |conversation| {
                    conversation
                        .queued
                        .iter()
                        .filter(|message| {
                            conversation
                                .active_delivery
                                .as_ref()
                                .map(|delivery| delivery.message_boundary)
                                .is_none_or(|boundary| message.sequence_no > boundary)
                        })
                        .count()
                }),
                conversation_lifecycle: conversation.map(|conversation| conversation.lifecycle),
                message_boundary: conversation.and_then(|conversation| {
                    conversation
                        .active_delivery
                        .as_ref()
                        .map(|delivery| delivery.message_boundary)
                }),
                delivery_markers: conversation.map_or_else(Vec::new, |conversation| {
                    conversation
                        .queued
                        .iter()
                        .map(|message| message.marker)
                        .collect()
                }),
                legal_controls: if deadline_reached {
                    Vec::new()
                } else {
                    vec!["stop".into(), "extend_deadline".into()]
                },
            }
        })
        .collect();
    let mut projection = ActivityProjection {
        version: 3,
        mission_id: state.mission_id.as_str().to_string(),
        event_head: state.head,
        generated_at_ms: now_ms,
        effects,
    };
    if let Some((effect_id, event)) = observation {
        apply_runtime_event(&mut projection, effect_id, event, now_ms);
    }
    write(mission_dir, &projection).await
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

async fn read(mission_dir: &Path) -> Result<ActivityProjection> {
    let bytes = tokio::fs::read(path(mission_dir)).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn write(mission_dir: &Path, projection: &ActivityProjection) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(projection)?;
    let target = path(mission_dir);
    let temporary = target.with_extension("tmp");
    tokio::fs::write(&temporary, bytes)
        .await
        .with_context(|| format!("writing activity projection '{}'", temporary.display()))?;
    tokio::fs::rename(&temporary, &target)
        .await
        .with_context(|| format!("publishing activity projection '{}'", target.display()))?;
    Ok(())
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
    match crate::workspace::observe_task_workspace(repo, workspace, observer_index, base_sha).await
    {
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
    let requests = state
        .tasks
        .iter()
        .filter(|(task_id, _)| task_workspace_applicable(state, TaskNamespace::Execution, task_id))
        .map(|(task_id, task)| {
            (
                task_id.clone(),
                lionclaw_dir
                    .parent()
                    .expect(".lionclaw directory has a workspace parent")
                    .to_path_buf(),
                lionclaw_dir
                    .join("missions")
                    .join(state.mission_id.as_str())
                    .join("tasks")
                    .join(task_id.as_str())
                    .join("work"),
                lionclaw_dir
                    .join("missions")
                    .join(state.mission_id.as_str())
                    .join("tasks")
                    .join(task_id.as_str())
                    .join("observer.index"),
                task.workspace_base_sha.clone(),
            )
        })
        .collect();
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
        .is_some_and(|task| task.workspace_base_sha.is_some())
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
        workspace::prepare_task_observer_index(&source, &index, &base, true)
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
        workspace::prepare_task_observer_index(&source, &index, &base, true)
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
            Assertion, AssertionId, DecisionAction, EventEnvelope, MissionConfig, MissionEvent,
            MissionId, MissionTypeRef, OracleName, OutputSemantics, Plan, PlanInventory,
            PlanProposal, Requirement, RequirementDisposition, RequirementId, RequirementKind,
            RoleName, Task, TaskId, TaskKind, TaskNamespace, VersionStamps,
        };

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
            },
        ];
        let state = crate::model::fold(events.into_iter().enumerate().map(|(index, event)| {
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
        let effect_id = crate::model::EffectId::for_parts(&["activity", "effect"]);
        write(
            temp.path(),
            &ActivityProjection {
                version: 3,
                mission_id: "mission".into(),
                event_head: 4,
                generated_at_ms: 10,
                effects: vec![EffectActivity {
                    effect_id: effect_id.as_str().into(),
                    role: Some("worker".into()),
                    task: Some("task".into()),
                    runtime: Some("acp".into()),
                    applied_model: None,
                    model_confirmation: None,
                    applied_mode: None,
                    mode_confirmation: None,
                    environment: "image:test".into(),
                    elapsed_ms: 0,
                    deadline_ms: 100,
                    last_activity: "effect running".into(),
                    tool_activity: None,
                    workspace: WorkspaceObservation::Clean,
                    queued_controls: Vec::new(),
                    queued_messages: 0,
                    conversation_lifecycle: None,
                    message_boundary: None,
                    delivery_markers: Vec::new(),
                    legal_controls: vec!["stop".into()],
                }],
            },
        )
        .await
        .unwrap();

        let mut projection = read(temp.path()).await.unwrap();
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
        write(temp.path(), &projection).await.unwrap();

        let projection = read(temp.path()).await.unwrap();
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
        clear_driver_run_evidence(temp.path()).unwrap();
        std::fs::write(
            driver_stderr_path(temp.path()),
            "panic from detached driver\n",
        )
        .unwrap();
        assert_eq!(
            driver_error(temp.path()).as_deref(),
            Some("panic from detached driver\n")
        );

        record_driver_error(temp.path(), &anyhow::anyhow!("typed driver failure")).unwrap();
        assert_eq!(
            driver_error(temp.path()).as_deref(),
            Some("typed driver failure")
        );
        clear_driver_run_evidence(temp.path()).unwrap();
        assert_eq!(driver_error(temp.path()), None);
    }

    #[test]
    fn detached_driver_stderr_retention_is_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let source = vec![b'x'; 128 * 1024];
        let mut stderr = Cursor::new(source);
        spool_driver_stderr(&mut stderr, temp.path()).unwrap();

        assert_eq!(stderr.position(), 128 * 1024, "the spool keeps draining");
        assert_eq!(
            std::fs::metadata(driver_stderr_path(temp.path()))
                .unwrap()
                .len(),
            MAX_DRIVER_STDERR_BYTES,
            "retained diagnostics have a fixed disk bound"
        );
        assert_eq!(driver_error(temp.path()).unwrap().len(), MAX_TEXT);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(driver_stderr_path(temp.path()))
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
        let invalid_mission_dir = temp.path().join("not-a-directory");
        std::fs::write(&invalid_mission_dir, "occupied").unwrap();
        let source = vec![b'x'; 128 * 1024];
        let mut stderr = Cursor::new(source);

        spool_driver_stderr(&mut stderr, &invalid_mission_dir)
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
        let external = temp.path().join("external.txt");
        std::fs::write(&external, "must not be disclosed").unwrap();
        symlink(&external, driver_error_path(temp.path())).unwrap();

        assert_eq!(driver_error(temp.path()), None);
        std::fs::remove_file(driver_error_path(temp.path())).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            driver_error_path(temp.path()),
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let started = tokio::time::Instant::now();
        assert_eq!(driver_error(temp.path()), None);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    }
}
