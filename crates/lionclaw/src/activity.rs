//! Bounded, disposable activity projection. Mission authority remains the
//! append-only event log; this file exists only for cheap live observation.

use std::ffi::OsStr;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::model::{InflightEffect, MissionState, OutputSemantics, TaskId};
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
        if !matches!(effect, InflightEffect::RoleTurn { .. }) {
            continue;
        }
        match effect {
            InflightEffect::RoleTurn { output, .. } => {
                if *output != OutputSemantics::ProducesArtifact {
                    continue;
                }
                match state.active_workspace_task(effect_id) {
                    Ok((task_id, task)) => {
                        let dirs = mission_dirs.task(task_id);
                        workspace_requests.push((
                            effect_id.clone(),
                            workspace_root.to_path_buf(),
                            dirs.work().to_path_buf(),
                            dirs.observer_index().to_path_buf(),
                            task.workspace_provenance
                                .as_ref()
                                .map(|provenance| provenance.base_sha.clone()),
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
            InflightEffect::OracleRun { .. } => {}
        }
    }
    workspace_observations.extend(observe_workspaces(workspace_requests).await);
    let effects = state
        .inflight
        .iter()
        .take(MAX_EFFECTS)
        .map(|(effect_id, effect)| {
            let requested_at_ms = match effect {
                InflightEffect::RoleTurn {
                    requested_at_ms, ..
                } => *requested_at_ms,
                InflightEffect::OracleRun {
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
            let configuration = state
                .role_attempt_receipts
                .get(effect_id)
                .and_then(crate::model::RoleAttemptReceipt::effective_runtime_configuration);
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
        .filter(|task_id| task_workspace_applicable(state, task_id))
    {
        match state.tasks.get(task_id) {
            Some(task) if task.workspace_provenance.is_some() => {
                let dirs = mission_dirs.task(task_id);
                requests.push((
                    task_id.clone(),
                    repo.clone(),
                    dirs.work().to_path_buf(),
                    dirs.observer_index().to_path_buf(),
                    task.workspace_provenance
                        .as_ref()
                        .map(|provenance| provenance.base_sha.clone()),
                ));
            }
            Some(_) | None => {
                observations.insert(task_id.clone(), WorkspaceObservation::NotCreated);
            }
        }
    }
    observations.extend(observe_workspaces(requests).await);
    observations
}

fn task_workspace_applicable(state: &MissionState, task_id: &TaskId) -> bool {
    state
        .tasks
        .get(task_id)
        .is_some_and(|task| task.workspace_provenance.is_some())
        || state
            .plan
            .as_ref()
            .is_some_and(|plan| plan.tasks.iter().any(|task| task.id == *task_id))
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
