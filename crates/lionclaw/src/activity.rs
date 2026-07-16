//! Bounded, disposable activity projection. Mission authority remains the
//! append-only event log; this file exists only for cheap live observation.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::model::{InflightEffect, MissionState};

const MAX_EFFECTS: usize = 32;
const MAX_TEXT: usize = 4 * 1024;

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
    pub dirty_diffstat: Option<String>,
    pub queued_controls: Vec<String>,
    pub queued_messages: usize,
    pub legal_controls: Vec<String>,
}

pub fn path(mission_dir: &Path) -> PathBuf {
    mission_dir.join("activity.json")
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

pub fn publish(mission_dir: &Path, state: &MissionState, now_ms: i64) -> Result<()> {
    std::fs::create_dir_all(mission_dir)
        .with_context(|| format!("creating mission directory '{}'", mission_dir.display()))?;
    let previous = read(mission_dir).ok();
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
            let dirty_diffstat = task.as_ref().and_then(|task| {
                workspace_diffstat(&mission_dir.join("tasks").join(task).join("work"))
            });
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
                dirty_diffstat,
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
    let projection = ActivityProjection {
        version: 1,
        mission_id: state.mission_id.as_str().to_string(),
        event_head: state.head,
        generated_at_ms: now_ms,
        effects,
    };
    write(mission_dir, &projection)
}

pub fn record_runtime_event(
    mission_dir: &Path,
    effect_id: &crate::model::EffectId,
    event: &lionclaw_runtime_api::TurnEvent,
    now_ms: i64,
) -> Result<()> {
    use lionclaw_runtime_api::{RuntimeEvent, RuntimeMessageLane};

    let mut projection = read(mission_dir)?;
    let Some(effect) = projection
        .effects
        .iter_mut()
        .find(|effect| effect.effect_id == effect_id.as_str())
    else {
        return Ok(());
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
    write(mission_dir, &projection)
}

fn read(mission_dir: &Path) -> Result<ActivityProjection> {
    let bytes = std::fs::read(path(mission_dir))?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn write(mission_dir: &Path, projection: &ActivityProjection) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(projection)?;
    let target = path(mission_dir);
    let temporary = target.with_extension("tmp");
    std::fs::write(&temporary, bytes)
        .with_context(|| format!("writing activity projection '{}'", temporary.display()))?;
    std::fs::rename(&temporary, &target)
        .with_context(|| format!("publishing activity projection '{}'", target.display()))?;
    Ok(())
}

fn workspace_diffstat(workspace: &Path) -> Option<String> {
    if !workspace.is_dir() {
        return None;
    }
    let output = std::process::Command::new("git")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .args(["-C", workspace.to_str()?, "status", "--short"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| bounded(&String::from_utf8_lossy(&output.stdout)))
        .filter(|text| !text.is_empty())
}

pub fn task_workspace_diffstat(
    lionclaw_dir: &Path,
    mission_id: &crate::model::MissionId,
    task_id: &crate::model::TaskId,
) -> Option<String> {
    workspace_diffstat(
        &lionclaw_dir
            .join("missions")
            .join(mission_id.as_str())
            .join("tasks")
            .join(task_id.as_str())
            .join("work"),
    )
}

fn bounded(text: &str) -> String {
    text.chars().take(MAX_TEXT).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use lionclaw_runtime_api::{
        AppliedRuntimeConfiguration, RuntimeConfigurationConfirmation, RuntimeEvent, TurnEvent,
    };

    #[test]
    fn bounded_projection_text_is_capped() {
        assert_eq!(bounded(&"x".repeat(MAX_TEXT + 10)).len(), MAX_TEXT);
    }

    #[test]
    fn runtime_journal_updates_configuration_and_activity_without_message_content() {
        let temp = tempfile::tempdir().unwrap();
        let effect_id = crate::model::EffectId::for_parts(&["activity", "effect"]);
        write(
            temp.path(),
            &ActivityProjection {
                version: 1,
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
                    dirty_diffstat: None,
                    queued_controls: Vec::new(),
                    queued_messages: 0,
                    legal_controls: vec!["stop".into()],
                }],
            },
        )
        .unwrap();

        record_runtime_event(
            temp.path(),
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
        )
        .unwrap();
        record_runtime_event(
            temp.path(),
            &effect_id,
            &TurnEvent::canonical(RuntimeEvent::MessageDelta {
                lane: lionclaw_runtime_api::RuntimeMessageLane::Answer,
                text: "secret response text".into(),
            }),
            21,
        )
        .unwrap();

        let projection = read(temp.path()).unwrap();
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
