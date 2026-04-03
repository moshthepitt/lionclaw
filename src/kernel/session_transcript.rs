use std::collections::HashSet;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::{SessionHistoryPolicy, SessionTurnStatus, SessionTurnView};

use super::session_turns::{SessionTurnRecord, SessionTurnStore};

const HISTORY_OVERFETCH_MULTIPLIER: usize = 3;
const HISTORY_OVERFETCH_CAP: usize = 100;
const INTERRUPTED_ERROR_CODE: &str = "runtime.interrupted";
const INTERRUPTED_ERROR_TEXT: &str = "turn interrupted by kernel restart";
pub const COMPACTION_RAW_KEEP: u64 = 12;
const COMPACTION_OLDEST_KEEP: usize = 2;
const COMPACTION_FAILURE_KEEP: usize = 4;
const COMPACTION_RECENT_KEEP: usize = 8;

#[derive(Debug, Clone, Copy)]
pub enum TranscriptMode {
    Prompt(SessionHistoryPolicy),
    History,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CompactionSummaryState {
    #[serde(default)]
    pub oldest_anchors: Vec<CompactionTurnDigest>,
    #[serde(default)]
    pub notable_failures: Vec<CompactionTurnDigest>,
    #[serde(default)]
    pub recent_turns: Vec<CompactionTurnDigest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionTurnDigest {
    pub sequence_no: u64,
    pub user: String,
    pub outcome: String,
}

pub async fn load_repaired_turns(
    store: &SessionTurnStore,
    session_id: Uuid,
    limit: usize,
    mode: TranscriptMode,
) -> Result<Vec<SessionTurnRecord>> {
    let limit = limit.max(1);
    let fetch_limit = limit
        .saturating_mul(HISTORY_OVERFETCH_MULTIPLIER)
        .min(HISTORY_OVERFETCH_CAP)
        .max(limit);
    let turns = store.list_recent(session_id, fetch_limit).await?;
    let mut repaired = repair_turns(turns, mode);
    if repaired.len() > limit {
        let keep_from = repaired.len() - limit;
        repaired.drain(0..keep_from);
    }
    Ok(repaired)
}

pub fn repair_turns(
    mut turns: Vec<SessionTurnRecord>,
    mode: TranscriptMode,
) -> Vec<SessionTurnRecord> {
    turns.sort_by(|left, right| {
        left.sequence_no
            .cmp(&right.sequence_no)
            .then_with(|| left.started_at.cmp(&right.started_at))
            .then_with(|| left.turn_id.cmp(&right.turn_id))
    });

    let map_running_to_interrupted = matches!(mode, TranscriptMode::Prompt(_));
    let mut seen_turn_ids = HashSet::new();
    let mut seen_sequence_nos = HashSet::new();
    let mut repaired = Vec::with_capacity(turns.len());

    for mut turn in turns {
        if !seen_turn_ids.insert(turn.turn_id) || !seen_sequence_nos.insert(turn.sequence_no) {
            continue;
        }
        if map_running_to_interrupted && turn.status == SessionTurnStatus::Running {
            turn.status = SessionTurnStatus::Interrupted;
            if turn.error_code.is_none() {
                turn.error_code = Some(INTERRUPTED_ERROR_CODE.to_string());
            }
            if turn.error_text.is_none() {
                turn.error_text = Some(INTERRUPTED_ERROR_TEXT.to_string());
            }
        }
        if turn.prompt_user_text.trim().is_empty()
            && turn.assistant_text.trim().is_empty()
            && turn
                .error_text
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
        {
            continue;
        }
        repaired.push(turn);
    }

    repaired
}

pub fn turns_to_history_views(turns: Vec<SessionTurnRecord>) -> Vec<SessionTurnView> {
    turns.into_iter().map(SessionTurnView::from).collect()
}

pub fn render_turns_for_prompt(
    turns: &[SessionTurnRecord],
    history_policy: SessionHistoryPolicy,
) -> Vec<String> {
    turns
        .iter()
        .map(|turn| render_turn_for_prompt(turn, history_policy))
        .collect()
}

pub fn merge_compaction_summary_state(
    previous_state: Option<&CompactionSummaryState>,
    turns: &[SessionTurnRecord],
) -> CompactionSummaryState {
    let turn_summaries = turns
        .iter()
        .map(summarize_turn_for_compaction)
        .collect::<Vec<_>>();
    let failure_summaries = turns
        .iter()
        .filter(|turn| {
            matches!(
                turn.status,
                SessionTurnStatus::TimedOut
                    | SessionTurnStatus::Failed
                    | SessionTurnStatus::Cancelled
                    | SessionTurnStatus::Interrupted
            )
        })
        .map(summarize_turn_for_compaction)
        .collect::<Vec<_>>();

    let mut oldest_anchors = previous_state
        .map(|state| state.oldest_anchors.clone())
        .unwrap_or_default();
    if oldest_anchors.len() < COMPACTION_OLDEST_KEEP {
        oldest_anchors.extend(
            turn_summaries
                .iter()
                .cloned()
                .take(COMPACTION_OLDEST_KEEP.saturating_sub(oldest_anchors.len())),
        );
    }

    let mut notable_failures = previous_state
        .map(|state| state.notable_failures.clone())
        .unwrap_or_default();
    notable_failures.extend(failure_summaries);
    notable_failures = keep_tail(notable_failures, COMPACTION_FAILURE_KEEP);

    let mut recent_turns = previous_state
        .map(|state| state.recent_turns.clone())
        .unwrap_or_default();
    recent_turns.extend(turn_summaries);
    recent_turns = keep_tail(recent_turns, COMPACTION_RECENT_KEEP);

    CompactionSummaryState {
        oldest_anchors,
        notable_failures,
        recent_turns,
    }
}

pub fn render_compaction_summary(
    start_sequence_no: u64,
    through_sequence_no: u64,
    state: &CompactionSummaryState,
) -> String {
    if start_sequence_no == 0 || through_sequence_no < start_sequence_no {
        return String::new();
    }

    let mut lines = vec![
        format!(
            "## Compacted Prior Turns {}-{}",
            start_sequence_no, through_sequence_no
        ),
        format!(
            "- Total compacted turns: {}",
            through_sequence_no - start_sequence_no + 1
        ),
        "- This is a bounded handoff summary of compacted turns. Recent raw turns remain below."
            .to_string(),
    ];

    push_compaction_section(&mut lines, "Oldest Anchors", state.oldest_anchors.iter());
    push_compaction_section(
        &mut lines,
        "Notable Failures",
        state.notable_failures.iter().filter(|digest| {
            !state
                .oldest_anchors
                .iter()
                .any(|anchor| anchor.sequence_no == digest.sequence_no)
        }),
    );
    push_compaction_section(
        &mut lines,
        "Recent Compacted Turns",
        state.recent_turns.iter().filter(|digest| {
            !state
                .oldest_anchors
                .iter()
                .any(|anchor| anchor.sequence_no == digest.sequence_no)
                && !state
                    .notable_failures
                    .iter()
                    .any(|failure| failure.sequence_no == digest.sequence_no)
        }),
    );
    lines.join("\n")
}

pub fn partial_marker(status: SessionTurnStatus) -> &'static str {
    match status {
        SessionTurnStatus::TimedOut => {
            "[Partial assistant reply; previous turn timed out before completion]"
        }
        SessionTurnStatus::Failed => {
            "[Partial assistant reply; previous turn failed before completion]"
        }
        SessionTurnStatus::Cancelled => {
            "[Partial assistant reply; previous turn was cancelled before completion]"
        }
        SessionTurnStatus::Interrupted => {
            "[Partial assistant reply; previous turn was interrupted before completion]"
        }
        _ => "",
    }
}

pub fn format_failure_note(status: SessionTurnStatus, error_text: Option<&str>) -> String {
    let reason = error_text.unwrap_or("no additional error text recorded");
    match status {
        SessionTurnStatus::TimedOut => format!(
            "The previous assistant turn timed out before completion. Recorded error: {}",
            reason
        ),
        SessionTurnStatus::Failed => format!(
            "The previous assistant turn failed before completion. Recorded error: {}",
            reason
        ),
        SessionTurnStatus::Cancelled => format!(
            "The previous assistant turn was cancelled before completion. Recorded error: {}",
            reason
        ),
        SessionTurnStatus::Interrupted => format!(
            "The previous assistant turn was interrupted before completion. Recorded error: {}",
            reason
        ),
        SessionTurnStatus::Completed => "The previous assistant turn completed.".to_string(),
        SessionTurnStatus::Running => {
            "The previous assistant turn is still marked running.".to_string()
        }
    }
}

fn render_turn_for_prompt(
    turn: &SessionTurnRecord,
    history_policy: SessionHistoryPolicy,
) -> String {
    let mut sections = vec![format!("## Prior Turn {}", turn.sequence_no)];
    if !turn.prompt_user_text.trim().is_empty() {
        sections.push(format!("### User\n\n{}", turn.prompt_user_text.trim()));
    }

    match turn.status {
        SessionTurnStatus::Completed => {
            if !turn.assistant_text.trim().is_empty() {
                sections.push(format!("### Assistant\n\n{}", turn.assistant_text.trim()));
            }
        }
        SessionTurnStatus::TimedOut
        | SessionTurnStatus::Failed
        | SessionTurnStatus::Cancelled
        | SessionTurnStatus::Interrupted => {
            if history_policy == SessionHistoryPolicy::Interactive
                && !turn.assistant_text.trim().is_empty()
            {
                sections.push(format!(
                    "### Assistant\n\n{}\n\n{}",
                    partial_marker(turn.status),
                    turn.assistant_text.trim()
                ));
            }
            sections.push(format!(
                "### Outcome\n\n{}",
                format_failure_note(turn.status, turn.error_text.as_deref())
            ));
        }
        SessionTurnStatus::Running => {
            sections
                .push("### Outcome\n\nThe previous assistant turn is still running.".to_string());
        }
    }

    sections.join("\n\n")
}

fn truncate_for_compaction(value: &str, max_chars: usize) -> String {
    let single_line = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.chars().count() <= max_chars {
        single_line
    } else {
        single_line
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>()
            + "…"
    }
}

fn summarize_turn_for_compaction(turn: &SessionTurnRecord) -> CompactionTurnDigest {
    let user = truncate_for_compaction(&turn.prompt_user_text, 120);
    let outcome = match turn.status {
        SessionTurnStatus::Completed => {
            let assistant = truncate_for_compaction(&turn.assistant_text, 160);
            if assistant.is_empty() {
                "completed".to_string()
            } else {
                format!("completed; assistant: {}", assistant)
            }
        }
        SessionTurnStatus::TimedOut
        | SessionTurnStatus::Failed
        | SessionTurnStatus::Cancelled
        | SessionTurnStatus::Interrupted => format!(
            "{}; {}",
            turn.status.as_str(),
            truncate_for_compaction(
                turn.error_text
                    .as_deref()
                    .unwrap_or("no additional error text recorded"),
                120
            )
        ),
        SessionTurnStatus::Running => "running".to_string(),
    };

    CompactionTurnDigest {
        sequence_no: turn.sequence_no,
        user: if user.is_empty() {
            "<empty>".to_string()
        } else {
            user
        },
        outcome,
    }
}

fn keep_tail<T>(mut items: Vec<T>, limit: usize) -> Vec<T> {
    if items.len() > limit {
        let keep_from = items.len() - limit;
        items.drain(0..keep_from);
    }
    items
}

fn push_compaction_section<'a>(
    lines: &mut Vec<String>,
    title: &str,
    digests: impl Iterator<Item = &'a CompactionTurnDigest>,
) {
    let digests = digests.collect::<Vec<_>>();
    if digests.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(format!("### {}", title));
    for digest in digests {
        lines.push(format!(
            "- Turn {}: user: {}; outcome: {}",
            digest.sequence_no, digest.user, digest.outcome
        ));
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::contracts::{SessionHistoryPolicy, SessionTurnKind, SessionTurnStatus};

    use super::{render_compaction_summary, repair_turns, TranscriptMode};
    use crate::kernel::session_turns::SessionTurnRecord;

    fn turn(
        sequence_no: u64,
        turn_id: Uuid,
        status: SessionTurnStatus,
        prompt_user_text: &str,
        assistant_text: &str,
        error_text: Option<&str>,
    ) -> SessionTurnRecord {
        SessionTurnRecord {
            turn_id,
            session_id: Uuid::new_v4(),
            sequence_no,
            kind: SessionTurnKind::Normal,
            status,
            display_user_text: prompt_user_text.to_string(),
            prompt_user_text: prompt_user_text.to_string(),
            assistant_text: assistant_text.to_string(),
            error_code: None,
            error_text: error_text.map(ToString::to_string),
            runtime_id: "mock".to_string(),
            started_at: Utc::now(),
            finished_at: None,
        }
    }

    #[test]
    fn repair_drops_duplicate_sequence_numbers_and_empty_turns() {
        let duplicate_seq = 7_u64;
        let kept = turn(
            duplicate_seq,
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").expect("uuid"),
            SessionTurnStatus::Completed,
            "user",
            "assistant",
            None,
        );
        let dropped_duplicate = turn(
            duplicate_seq,
            Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").expect("uuid"),
            SessionTurnStatus::Completed,
            "duplicate",
            "assistant",
            None,
        );
        let dropped_empty = turn(
            8,
            Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").expect("uuid"),
            SessionTurnStatus::Failed,
            "",
            "",
            None,
        );

        let repaired = repair_turns(
            vec![dropped_empty, dropped_duplicate, kept.clone()],
            TranscriptMode::History,
        );

        assert_eq!(repaired.len(), 1);
        assert_eq!(repaired[0].turn_id, kept.turn_id);
    }

    #[test]
    fn prompt_repair_maps_running_turns_to_interrupted() {
        let running = turn(
            1,
            Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").expect("uuid"),
            SessionTurnStatus::Running,
            "user",
            "partial",
            None,
        );

        let repaired = repair_turns(
            vec![running],
            TranscriptMode::Prompt(SessionHistoryPolicy::Interactive),
        );

        assert_eq!(repaired.len(), 1);
        assert_eq!(repaired[0].status, SessionTurnStatus::Interrupted);
        assert_eq!(
            repaired[0].error_code.as_deref(),
            Some("runtime.interrupted")
        );
        assert_eq!(
            repaired[0].error_text.as_deref(),
            Some("turn interrupted by kernel restart")
        );
    }

    #[test]
    fn compaction_summary_preserves_prior_compacted_turns() {
        let first_state = super::merge_compaction_summary_state(
            None,
            &[
                turn(
                    1,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 0",
                    "assistant 0",
                    None,
                ),
                turn(
                    2,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 1",
                    "assistant 1",
                    None,
                ),
                turn(
                    3,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 2",
                    "assistant 2",
                    None,
                ),
                turn(
                    4,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 3",
                    "assistant 3",
                    None,
                ),
            ],
        );

        let merged_state = super::merge_compaction_summary_state(
            Some(&first_state),
            &[
                turn(
                    5,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 4",
                    "assistant 4",
                    None,
                ),
                turn(
                    6,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 5",
                    "assistant 5",
                    None,
                ),
                turn(
                    7,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 6",
                    "assistant 6",
                    None,
                ),
                turn(
                    8,
                    Uuid::new_v4(),
                    SessionTurnStatus::Failed,
                    "turn 7",
                    "",
                    Some("boom"),
                ),
                turn(
                    9,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 8",
                    "assistant 8",
                    None,
                ),
                turn(
                    10,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 9",
                    "assistant 9",
                    None,
                ),
                turn(
                    11,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 10",
                    "assistant 10",
                    None,
                ),
                turn(
                    12,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "turn 11",
                    "assistant 11",
                    None,
                ),
            ],
        );
        let merged = render_compaction_summary(1, 12, &merged_state);

        assert!(merged.contains("## Compacted Prior Turns 1-12"));
        assert!(merged.contains("Total compacted turns: 12"));
        assert!(merged.contains("user: turn 0"));
        assert!(merged.contains("user: turn 1"));
        assert!(merged.contains("user: turn 11"));
        assert!(merged.contains("failed; boom"));
        assert!(!merged.contains("user: turn 2"));
        assert_eq!(merged.matches("## Compacted Prior Turns").count(), 1);
    }
}
