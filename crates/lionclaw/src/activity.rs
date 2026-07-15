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
    pub applied_mode: Option<String>,
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

pub fn publish(mission_dir: &Path, state: &MissionState, now_ms: i64) -> Result<()> {
    std::fs::create_dir_all(mission_dir)
        .with_context(|| format!("creating mission directory '{}'", mission_dir.display()))?;
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
            EffectActivity {
                effect_id: effect_id.as_str().to_string(),
                role,
                task,
                runtime,
                // Adapter configuration is authoritative only after the effect
                // reports its outcome. Never project a prior attempt as current.
                applied_model: None,
                applied_mode: None,
                environment: format!(
                    "confinement-image:{}",
                    crate::model::short_hex(&state.image_id)
                ),
                elapsed_ms: now_ms.saturating_sub(requested_at_ms).max(0),
                deadline_ms: effect.deadline_ms(),
                last_activity: "effect running".into(),
                tool_activity: None,
                dirty_diffstat,
                queued_controls: state
                    .stop_requests
                    .get(effect_id)
                    .map(|reason| vec![format!("stop: {}", bounded(reason))])
                    .unwrap_or_default(),
                queued_messages: 0,
                legal_controls: vec!["stop".into(), "extend_deadline".into()],
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
    let bytes = serde_json::to_vec_pretty(&projection)?;
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
        .args(["-C", workspace.to_str()?, "status", "--short"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| bounded(&String::from_utf8_lossy(&output.stdout)))
        .filter(|text| !text.is_empty())
}

fn bounded(text: &str) -> String {
    text.chars().take(MAX_TEXT).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_projection_text_is_capped() {
        assert_eq!(bounded(&"x".repeat(MAX_TEXT + 10)).len(), MAX_TEXT);
    }
}
