//! Bounded, disposable activity projection. Mission authority remains the
//! append-only event log; this file exists only for cheap live observation.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::model::{InflightEffect, MissionState};

const MAX_EFFECTS: usize = 32;
const MAX_TEXT: usize = 4 * 1024;
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

pub fn open_driver_stderr(mission_dir: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(mission_dir)?;
    Ok(std::fs::File::create(driver_stderr_path(mission_dir))?)
}

pub fn record_driver_error(mission_dir: &Path, error: &anyhow::Error) -> Result<()> {
    let target = driver_error_path(mission_dir);
    let temporary = target.with_extension("tmp");
    std::fs::write(&temporary, bounded(&format!("{error:#}")))?;
    std::fs::rename(temporary, target)?;
    Ok(())
}

pub fn driver_error(mission_dir: &Path) -> Option<String> {
    std::fs::read_to_string(driver_error_path(mission_dir))
        .ok()
        .filter(|text| !text.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string(driver_stderr_path(mission_dir))
                .ok()
                .map(|text| bounded(&text))
                .filter(|text| !text.trim().is_empty())
        })
}

pub async fn publish_observed(
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
            InflightEffect::RoleRun { task_id, .. } => Some((
                effect_id.clone(),
                mission_dir
                    .join("tasks")
                    .join(task_id.as_str())
                    .join("work"),
                state
                    .tasks
                    .get(task_id)
                    .and_then(|task| task.workspace_base_sha.clone()),
            )),
            InflightEffect::OracleRun { .. } | InflightEffect::TerminalReview { .. } => None,
        })
        .collect();
    let workspace_observations = observe_workspaces(workspace_requests).await;
    let effects = state
        .inflight
        .iter()
        .take(MAX_EFFECTS)
        .map(|(effect_id, effect)| {
            let (role, task, runtime, requested_at_ms) = match effect {
                InflightEffect::RoleRun {
                    role,
                    task_id,
                    runtime,
                    requested_at_ms,
                    ..
                } => (
                    Some(role.as_str().to_string()),
                    Some(task_id.as_str().to_string()),
                    Some(runtime.clone()),
                    *requested_at_ms,
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
                queued_messages: prior.map_or(0, |prior| prior.queued_messages),
                legal_controls: if deadline_reached {
                    Vec::new()
                } else {
                    vec!["stop".into(), "extend_deadline".into()]
                },
            }
        })
        .collect();
    let mut projection = ActivityProjection {
        version: 2,
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
    match &event.event {
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

async fn git_output(
    workspace: &Path,
    args: &[&str],
) -> Result<lionclaw_runtime_api::ExecutionOutput> {
    crate::workspace::observed_git_output(workspace, args)
        .await
        .with_context(|| format!("Git observation failed for {}", args.join(" ")))
}

async fn workspace_observation(workspace: &Path, base_sha: Option<&str>) -> WorkspaceObservation {
    match tokio::fs::symlink_metadata(workspace).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return WorkspaceObservation::Unavailable {
                reason: "workspace path is not a directory".into(),
            };
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return WorkspaceObservation::NotCreated;
        }
        Err(error) => {
            return WorkspaceObservation::Unavailable {
                reason: bounded(&format!("workspace metadata unavailable: {error}")),
            };
        }
    }
    match observe_existing_workspace(workspace, base_sha).await {
        Ok(summary) if summary.is_empty() => WorkspaceObservation::Clean,
        Ok(diffstat) => WorkspaceObservation::Changed {
            diffstat: bounded(&diffstat),
        },
        Err(error) => WorkspaceObservation::Unavailable {
            reason: bounded(&format!("{error:#}")),
        },
    }
}

async fn observe_existing_workspace(workspace: &Path, base_sha: Option<&str>) -> Result<String> {
    let mut summary = String::new();
    if let Some(base_sha) = base_sha {
        let head_output = git_output(workspace, &["rev-parse", "HEAD"]).await?;
        ensure_git_success(&head_output, "resolve workspace HEAD")?;
        let head = String::from_utf8_lossy(&head_output.stdout)
            .trim()
            .to_string();
        if head != base_sha {
            let ancestry =
                git_output(workspace, &["merge-base", "--is-ancestor", base_sha, &head]).await?;
            let is_ancestor = if ancestry.success() {
                true
            } else if ancestry.exit_code == Some(1) && ancestry.exit_signal.is_none() {
                false
            } else {
                ensure_git_success(&ancestry, "compare workspace ancestry")?;
                unreachable!("successful ancestry check returned above")
            };
            let relation = if is_ancestor {
                "committed work"
            } else {
                "diverged work"
            };
            summary.push_str(&format!(
                "{relation}: {} (recorded base {})\n",
                crate::model::short_hex(&head),
                crate::model::short_hex(base_sha),
            ));
            if is_ancestor {
                let diffstat = git_output(
                    workspace,
                    &[
                        "diff",
                        "--no-ext-diff",
                        "--no-textconv",
                        "--stat",
                        &format!("{base_sha}..{head}"),
                    ],
                )
                .await?;
                ensure_git_success(&diffstat, "read committed workspace diffstat")?;
                summary.push_str(&String::from_utf8_lossy(&diffstat.stdout));
            }
        }
    }
    let status = git_output(workspace, &["status", "--short"]).await?;
    ensure_git_success(&status, "read workspace status")?;
    summary.push_str(&String::from_utf8_lossy(&status.stdout));
    Ok(summary)
}

fn ensure_git_success(
    output: &lionclaw_runtime_api::ExecutionOutput,
    operation: &str,
) -> Result<()> {
    if !output.success() {
        anyhow::bail!(
            "{operation} failed ({}): {}",
            output.status_description(),
            bounded(&String::from_utf8_lossy(&output.stderr))
        );
    }
    Ok(())
}

pub async fn task_workspace_observations(
    lionclaw_dir: &Path,
    state: &MissionState,
) -> std::collections::BTreeMap<crate::model::TaskId, WorkspaceObservation> {
    let requests = state
        .tasks
        .iter()
        .map(|(task_id, task)| {
            (
                task_id.clone(),
                lionclaw_dir
                    .join("missions")
                    .join(state.mission_id.as_str())
                    .join("tasks")
                    .join(task_id.as_str())
                    .join("work"),
                task.workspace_base_sha.clone(),
            )
        })
        .collect();
    observe_workspaces(requests).await
}

async fn observe_workspaces<K>(
    requests: Vec<(K, PathBuf, Option<String>)>,
) -> std::collections::BTreeMap<K, WorkspaceObservation>
where
    K: Clone + Ord + Send + 'static,
{
    let mut observed: std::collections::BTreeMap<K, WorkspaceObservation> = requests
        .iter()
        .map(|(key, _, _)| {
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
            let Some((key, workspace, base_sha)) = pending.next() else {
                break;
            };
            tasks.spawn(async move {
                let observation = workspace_observation(&workspace, base_sha.as_deref()).await;
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

    #[test]
    fn bounded_projection_text_is_capped() {
        assert_eq!(bounded(&"x".repeat(MAX_TEXT + 10)).len(), MAX_TEXT);
    }

    #[tokio::test]
    async fn clean_committed_work_is_visible_relative_to_the_recorded_base() {
        let temp = tempfile::tempdir().unwrap();
        let run = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(temp.path())
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        run(&["init", "-q"]);
        run(&["config", "user.name", "test"]);
        run(&["config", "user.email", "test@local"]);
        run(&["config", "commit.gpgsign", "false"]);
        std::fs::write(temp.path().join("work.txt"), "base\n").unwrap();
        run(&["add", "work.txt"]);
        run(&["commit", "-q", "-m", "base"]);
        let base = String::from_utf8(
            git_output(temp.path(), &["rev-parse", "HEAD"])
                .await
                .unwrap()
                .stdout,
        )
        .unwrap();
        std::fs::write(temp.path().join("work.txt"), "committed result\n").unwrap();
        run(&["add", "work.txt"]);
        run(&["commit", "-q", "-m", "worker progress"]);

        let WorkspaceObservation::Changed { diffstat: summary } =
            workspace_observation(temp.path(), Some(base.trim())).await
        else {
            panic!("committed work must be observed as changed");
        };
        assert!(summary.contains("committed work:"));
        assert!(summary.contains("work.txt"));
        assert!(!summary.contains(" M work.txt"));
    }

    #[tokio::test]
    async fn workspace_observation_never_executes_worker_git_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("host-command-ran");
        let monitor = temp.path().join("hostile-monitor");
        std::fs::write(
            &monitor,
            format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
        )
        .unwrap();
        workspace::make_executable(&monitor).unwrap();
        let run = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(temp.path())
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?}");
        };
        run(&["init", "-q"]);
        run(&["config", "user.name", "test"]);
        run(&["config", "user.email", "test@local"]);
        run(&["config", "commit.gpgsign", "false"]);
        std::fs::write(temp.path().join("tracked"), "base\n").unwrap();
        run(&["add", "tracked"]);
        run(&["commit", "-q", "-m", "base"]);
        run(&["config", "core.fsmonitor", monitor.to_str().unwrap()]);

        let _ = workspace_observation(temp.path(), None).await;
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
        std::fs::write(
            temp.path().join(".gitattributes"),
            "tracked filter=hostile\n",
        )
        .unwrap();
        run(&["config", "filter.hostile.clean", filter.to_str().unwrap()]);
        std::fs::write(temp.path().join("tracked"), "worker change\n").unwrap();

        let _ = workspace_observation(temp.path(), None).await;
        assert!(
            !marker.exists(),
            "observer Git must not execute worker-controlled filter drivers"
        );
    }

    #[tokio::test]
    async fn failed_workspace_observation_does_not_look_clean() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("not-a-repository"), "retained work\n").unwrap();

        let observation = workspace_observation(temp.path(), None).await;

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

        let observation = workspace_observation(&workspace, None).await;

        assert!(matches!(
            observation,
            WorkspaceObservation::Unavailable { .. }
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
            latest.1.event,
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
                version: 2,
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
}
