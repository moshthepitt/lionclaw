use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::contracts::{SessionHistoryPolicy, SessionTurnStatus, SessionTurnView};

use super::continuity::normalized_title_key;
use super::session_turns::{SessionTurnRecord, SessionTurnStore};

const HISTORY_OVERFETCH_MULTIPLIER: usize = 3;
const HISTORY_OVERFETCH_CAP: usize = 100;
const INTERRUPTED_ERROR_CODE: &str = "runtime.interrupted";
const INTERRUPTED_ERROR_TEXT: &str = "turn interrupted by kernel restart";
pub const COMPACTION_RAW_KEEP: u64 = 12;
const COMPACTION_LIST_KEEP: usize = 6;
const COMPACTION_MAX_ITEM_LEN: usize = 160;

#[derive(Debug, Clone, Copy)]
pub enum TranscriptMode {
    Prompt(SessionHistoryPolicy),
    History,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CompactionSummaryState {
    #[serde(default)]
    pub goal: Option<String>,
    #[serde(default)]
    pub constraints_preferences: Vec<String>,
    #[serde(default)]
    pub progress_done: Vec<String>,
    #[serde(default)]
    pub progress_in_progress: Vec<String>,
    #[serde(default)]
    pub progress_blocked: Vec<String>,
    #[serde(default)]
    pub key_decisions: Vec<String>,
    #[serde(default)]
    pub relevant_files: Vec<String>,
    #[serde(default)]
    pub next_steps: Vec<String>,
    #[serde(default)]
    pub critical_context: Vec<String>,
    #[serde(default)]
    pub memory_proposals: Vec<CompactionMemoryProposal>,
    #[serde(default)]
    pub open_loops: Vec<CompactionOpenLoop>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionMemoryProposal {
    pub title: String,
    pub rationale: String,
    pub entries: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionOpenLoop {
    pub title: String,
    pub summary: String,
    pub next_step: String,
}

#[derive(Debug, Default)]
struct TurnSignals {
    goal: Option<String>,
    constraints_preferences: Vec<String>,
    progress_done: Vec<String>,
    progress_blocked: Vec<String>,
    relevant_files: Vec<String>,
    key_decisions: Vec<String>,
    next_steps: Vec<String>,
    critical_context: Vec<String>,
    memory_proposals: Vec<CompactionMemoryProposal>,
    open_loops: Vec<CompactionOpenLoop>,
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
    let mut state = previous_state.cloned().unwrap_or_default();
    let signals = collect_turn_signals(turns);
    state.goal = signals.goal.or_else(|| state.goal.clone());
    state.constraints_preferences = merge_unique_strings(
        state.constraints_preferences,
        signals.constraints_preferences,
        COMPACTION_LIST_KEEP,
    );
    state.progress_done = merge_unique_strings(
        state.progress_done,
        signals.progress_done,
        COMPACTION_LIST_KEEP,
    );
    state.progress_blocked = merge_unique_strings(
        state.progress_blocked,
        signals.progress_blocked,
        COMPACTION_LIST_KEEP,
    );
    state.relevant_files = merge_unique_strings(
        state.relevant_files,
        signals.relevant_files,
        COMPACTION_LIST_KEEP,
    );
    state.key_decisions = merge_unique_strings(
        state.key_decisions,
        signals.key_decisions,
        COMPACTION_LIST_KEEP,
    );
    state.next_steps =
        merge_unique_strings(state.next_steps, signals.next_steps, COMPACTION_LIST_KEEP);
    state.critical_context = merge_unique_strings(
        state.critical_context,
        signals.critical_context,
        COMPACTION_LIST_KEEP,
    );
    state.memory_proposals =
        merge_unique_memory_proposals(state.memory_proposals, signals.memory_proposals);
    state.open_loops = merge_unique_open_loops(state.open_loops, signals.open_loops);

    state.progress_in_progress = merge_unique_strings(
        state.progress_in_progress,
        state
            .open_loops
            .iter()
            .map(|open_loop| format!("{} (next: {})", open_loop.title, open_loop.next_step))
            .collect(),
        COMPACTION_LIST_KEEP,
    );
    if state.progress_in_progress.is_empty() {
        state.progress_in_progress = state.next_steps.clone();
    }
    state
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
        "- This is the persisted handoff summary for older turns. Recent raw turns remain below."
            .to_string(),
    ];

    if let Some(goal) = state
        .goal
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        lines.push(String::new());
        lines.push("### Goal".to_string());
        lines.push(goal.trim().to_string());
    }
    push_string_section(
        &mut lines,
        "Constraints & Preferences",
        &state.constraints_preferences,
    );
    push_string_section(&mut lines, "Progress Done", &state.progress_done);
    push_string_section(&mut lines, "In Progress", &state.progress_in_progress);
    push_string_section(&mut lines, "Blocked", &state.progress_blocked);
    push_string_section(&mut lines, "Key Decisions", &state.key_decisions);
    push_string_section(&mut lines, "Relevant Files", &state.relevant_files);
    push_string_section(&mut lines, "Next Steps", &state.next_steps);
    push_string_section(&mut lines, "Critical Context", &state.critical_context);
    push_memory_proposal_section(&mut lines, &state.memory_proposals);
    push_open_loop_section(&mut lines, &state.open_loops);

    lines.join("\n")
}

pub fn build_compaction_prompt(
    previous_state: Option<&CompactionSummaryState>,
    turns: &[SessionTurnRecord],
) -> String {
    let previous_summary = previous_state
        .map(|state| render_compaction_summary(1, 1, state))
        .unwrap_or_else(|| "No previous compaction summary.".to_string());
    let transcript = turns
        .iter()
        .map(serialize_turn_for_compaction)
        .collect::<Vec<_>>()
        .join("\n\n");

    format!(
        concat!(
            "lionclaw_compaction_handoff_v1\n\n",
            "You are updating LionClaw's persisted transcript handoff summary.\n",
            "Return ONLY valid JSON. Do not use markdown fences. Do not include any commentary.\n",
            "Use this schema exactly:\n",
            "{{\n",
            "  \"goal\": string|null,\n",
            "  \"constraints_preferences\": [string],\n",
            "  \"progress_done\": [string],\n",
            "  \"progress_in_progress\": [string],\n",
            "  \"progress_blocked\": [string],\n",
            "  \"key_decisions\": [string],\n",
            "  \"relevant_files\": [string],\n",
            "  \"next_steps\": [string],\n",
            "  \"critical_context\": [string],\n",
            "  \"memory_proposals\": [{{\"title\": string, \"rationale\": string, \"entries\": [string]}}],\n",
            "  \"open_loops\": [{{\"title\": string, \"summary\": string, \"next_step\": string}}]\n",
            "}}\n\n",
            "Rules:\n",
            "- Keep every string concise and factual.\n",
            "- Preserve durable context needed to continue the work.\n",
            "- memory_proposals and open_loops must describe the full current active set after these turns, not just new deltas.\n",
            "- relevant_files must only include explicit file paths from the transcript.\n",
            "- memory_proposals must only contain durable facts, preferences, or conventions suitable for MEMORY.md.\n",
            "- open_loops must only contain active commitments or blockers that should remain visible.\n",
            "- Never invent facts that are not in the transcript.\n\n",
            "Previous persisted handoff summary:\n",
            "{}\n\n",
            "Newly compacted turns:\n",
            "{}\n"
        ),
        previous_summary, transcript
    )
}

pub fn parse_compaction_summary_state(raw: &str) -> Result<CompactionSummaryState> {
    let json_text = extract_json_object(raw)
        .ok_or_else(|| anyhow!("runtime did not return a compaction summary JSON object"))?;
    let mut state: CompactionSummaryState = serde_json::from_str(&json_text)
        .map_err(|err| anyhow!("invalid compaction summary JSON: {}", err))?;
    normalize_compaction_summary_state(&mut state);
    Ok(state)
}

pub fn merge_compaction_summary_updates(
    previous_state: Option<&CompactionSummaryState>,
    current_state: CompactionSummaryState,
) -> CompactionSummaryState {
    let Some(previous) = previous_state else {
        return current_state;
    };

    let mut merged = current_state;
    merged.goal = merged.goal.or_else(|| previous.goal.clone());
    merged.constraints_preferences = merge_unique_strings(
        previous.constraints_preferences.clone(),
        merged.constraints_preferences,
        COMPACTION_LIST_KEEP,
    );
    merged.progress_done = merge_unique_strings(
        previous.progress_done.clone(),
        merged.progress_done,
        COMPACTION_LIST_KEEP,
    );
    merged.progress_in_progress = merge_unique_strings(
        previous.progress_in_progress.clone(),
        merged.progress_in_progress,
        COMPACTION_LIST_KEEP,
    );
    merged.progress_blocked = merge_unique_strings(
        previous.progress_blocked.clone(),
        merged.progress_blocked,
        COMPACTION_LIST_KEEP,
    );
    merged.key_decisions = merge_unique_strings(
        previous.key_decisions.clone(),
        merged.key_decisions,
        COMPACTION_LIST_KEEP,
    );
    merged.relevant_files = merge_unique_strings(
        previous.relevant_files.clone(),
        merged.relevant_files,
        COMPACTION_LIST_KEEP,
    );
    merged.next_steps = merge_unique_strings(
        previous.next_steps.clone(),
        merged.next_steps,
        COMPACTION_LIST_KEEP,
    );
    merged.critical_context = merge_unique_strings(
        previous.critical_context.clone(),
        merged.critical_context,
        COMPACTION_LIST_KEEP,
    );
    merged.memory_proposals = normalize_memory_proposals(&merged.memory_proposals);
    merged.open_loops = normalize_open_loops(&merged.open_loops);
    merged
}

pub fn normalize_compaction_summary_state(state: &mut CompactionSummaryState) {
    state.goal = state
        .goal
        .take()
        .and_then(|value| compact_line(&value, COMPACTION_MAX_ITEM_LEN));
    state.constraints_preferences =
        normalize_lines(&state.constraints_preferences, COMPACTION_LIST_KEEP);
    state.progress_done = normalize_lines(&state.progress_done, COMPACTION_LIST_KEEP);
    state.progress_in_progress = normalize_lines(&state.progress_in_progress, COMPACTION_LIST_KEEP);
    state.progress_blocked = normalize_lines(&state.progress_blocked, COMPACTION_LIST_KEEP);
    state.key_decisions = normalize_lines(&state.key_decisions, COMPACTION_LIST_KEEP);
    state.relevant_files = normalize_lines(&state.relevant_files, COMPACTION_LIST_KEEP);
    state.next_steps = normalize_lines(&state.next_steps, COMPACTION_LIST_KEEP);
    state.critical_context = normalize_lines(&state.critical_context, COMPACTION_LIST_KEEP);
    state.memory_proposals = normalize_memory_proposals(&state.memory_proposals);
    state.open_loops = normalize_open_loops(&state.open_loops);
}

pub fn remove_memory_proposal_from_summary_state(
    state: &mut CompactionSummaryState,
    title: &str,
) -> bool {
    let Some(normalized_title) = normalized_summary_title_key(title) else {
        return false;
    };
    let original_len = state.memory_proposals.len();
    state.memory_proposals.retain(|proposal| {
        normalized_summary_title_key(&proposal.title).as_deref() != Some(normalized_title.as_str())
    });
    let changed = state.memory_proposals.len() != original_len;
    normalize_compaction_summary_state(state);
    changed
}

pub fn remove_open_loop_from_summary_state(
    state: &mut CompactionSummaryState,
    title: &str,
) -> bool {
    let Some(normalized_title) = normalized_summary_title_key(title) else {
        return false;
    };
    let original_len = state.open_loops.len();
    state.open_loops.retain(|open_loop| {
        normalized_summary_title_key(&open_loop.title).as_deref() != Some(normalized_title.as_str())
    });
    let mut changed = state.open_loops.len() != original_len;
    let in_progress_len = state.progress_in_progress.len();
    state.progress_in_progress.retain(|entry| {
        normalized_summary_title_key(progress_entry_title(entry)).as_deref()
            != Some(normalized_title.as_str())
    });
    changed |= state.progress_in_progress.len() != in_progress_len;
    normalize_compaction_summary_state(state);
    changed
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

fn compact_line(value: &str, max_chars: usize) -> Option<String> {
    let single_line = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.is_empty() {
        return None;
    }
    if single_line.chars().count() <= max_chars {
        Some(single_line)
    } else {
        Some(
            single_line
                .chars()
                .take(max_chars.saturating_sub(1))
                .collect::<String>()
                + "…",
        )
    }
}

fn push_string_section(lines: &mut Vec<String>, title: &str, values: &[String]) {
    if values.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push(format!("### {}", title));
    for value in values {
        lines.push(format!("- {}", value.trim()));
    }
}

fn push_memory_proposal_section(lines: &mut Vec<String>, proposals: &[CompactionMemoryProposal]) {
    if proposals.is_empty() {
        return;
    }
    lines.push(String::new());
    lines.push("### Memory Proposals".to_string());
    for proposal in proposals {
        lines.push(format!(
            "- {}: {}",
            proposal.title,
            proposal.entries.join("; ")
        ));
    }
}

fn push_open_loop_section(lines: &mut Vec<String>, open_loops: &[CompactionOpenLoop]) {
    if open_loops.is_empty() {
        return;
    }
    lines.push(String::new());
    lines.push("### Open Loops".to_string());
    for open_loop in open_loops {
        lines.push(format!(
            "- {}: {} (next: {})",
            open_loop.title, open_loop.summary, open_loop.next_step
        ));
    }
}

fn collect_turn_signals(turns: &[SessionTurnRecord]) -> TurnSignals {
    let mut done = Vec::new();
    let mut failures = Vec::new();
    let mut files = BTreeSet::new();
    let mut decisions = Vec::new();
    let mut next_steps = Vec::new();
    let mut critical = Vec::new();
    let mut constraints = Vec::new();
    let mut open_loops = Vec::new();

    for turn in turns {
        collect_file_candidates(&turn.prompt_user_text, &mut files);
        collect_file_candidates(&turn.assistant_text, &mut files);
        constraints.extend(extract_preference_candidates(&turn.prompt_user_text));
        match turn.status {
            SessionTurnStatus::Completed => {
                if let Some(line) = compact_line(&turn.prompt_user_text, COMPACTION_MAX_ITEM_LEN) {
                    done.push(line);
                }
                if let Some(line) = compact_line(&turn.assistant_text, COMPACTION_MAX_ITEM_LEN)
                    .filter(|line| line.len() > 12 && !line.eq_ignore_ascii_case("captured"))
                {
                    decisions.push(line);
                }
            }
            SessionTurnStatus::TimedOut
            | SessionTurnStatus::Failed
            | SessionTurnStatus::Cancelled
            | SessionTurnStatus::Interrupted => {
                let reason = turn
                    .error_text
                    .as_deref()
                    .unwrap_or("no additional error text recorded");
                if let Some(line) = compact_line(
                    &format!("{}: {}", turn.prompt_user_text.trim(), reason.trim()),
                    COMPACTION_MAX_ITEM_LEN,
                ) {
                    failures.push(line);
                }
                if let Some(open_loop) = failure_open_loop(turn) {
                    open_loops.push(open_loop);
                }
            }
            SessionTurnStatus::Running => {}
        }
    }

    if let Some(last_turn) = turns.last() {
        if let Some(line) = compact_line(&last_turn.prompt_user_text, COMPACTION_MAX_ITEM_LEN) {
            next_steps.push(line);
        }
        if let Some(open_loop) = trailing_open_loop(last_turn) {
            open_loops.push(open_loop);
        }
        if matches!(
            last_turn.status,
            SessionTurnStatus::TimedOut
                | SessionTurnStatus::Failed
                | SessionTurnStatus::Cancelled
                | SessionTurnStatus::Interrupted
        ) {
            if let Some(line) = compact_line(
                &format_failure_note(last_turn.status, last_turn.error_text.as_deref()),
                COMPACTION_MAX_ITEM_LEN,
            ) {
                critical.push(line);
            }
        }
    }

    let goal = turns
        .iter()
        .rev()
        .find_map(|turn| compact_line(&turn.prompt_user_text, COMPACTION_MAX_ITEM_LEN));
    let constraints_preferences = normalize_lines(&constraints, COMPACTION_LIST_KEEP);
    let memory_proposals = if constraints_preferences.is_empty() {
        Vec::new()
    } else {
        vec![CompactionMemoryProposal {
            title: "Working Preferences".to_string(),
            rationale: "durable preferences and constraints captured during the session"
                .to_string(),
            entries: constraints_preferences.clone(),
        }]
    };

    TurnSignals {
        goal,
        constraints_preferences,
        progress_done: keep_tail(done, COMPACTION_LIST_KEEP),
        progress_blocked: keep_tail(failures, COMPACTION_LIST_KEEP),
        relevant_files: files.into_iter().take(COMPACTION_LIST_KEEP).collect(),
        key_decisions: keep_tail(decisions, COMPACTION_LIST_KEEP),
        next_steps: keep_tail(next_steps, COMPACTION_LIST_KEEP),
        critical_context: keep_tail(critical, COMPACTION_LIST_KEEP),
        memory_proposals,
        open_loops: merge_unique_open_loops(Vec::new(), open_loops),
    }
}

fn collect_file_candidates(text: &str, files: &mut BTreeSet<String>) {
    for token in text.split_whitespace() {
        let candidate = token
            .trim_matches(|ch: char| ['"', '\'', '(', ')', '[', ']', ',', ';', ':'].contains(&ch));
        if candidate.len() < 3 || !candidate.contains('.') {
            continue;
        }
        let looks_like_path = candidate.contains('/')
            || [
                ".rs", ".md", ".toml", ".json", ".yaml", ".yml", ".txt", ".py", ".ts", ".tsx",
                ".js", ".jsx", ".html", ".css",
            ]
            .iter()
            .any(|suffix| candidate.ends_with(suffix));
        if looks_like_path {
            files.insert(candidate.to_string());
        }
    }
}

fn extract_preference_candidates(text: &str) -> Vec<String> {
    const MARKERS: [&str; 10] = [
        "prefer ", "always ", "never ", "avoid ", "must ", "should ", "keep ", "do not ", "don't ",
        "no ",
    ];

    text.lines()
        .flat_map(|line| line.split(['.', ';']))
        .filter_map(|segment| {
            let normalized = segment.split_whitespace().collect::<Vec<_>>().join(" ");
            let lower = normalized.to_ascii_lowercase();
            if MARKERS.iter().any(|marker| lower.contains(marker)) {
                compact_line(&normalized, COMPACTION_MAX_ITEM_LEN)
            } else {
                None
            }
        })
        .collect()
}

fn failure_open_loop(turn: &SessionTurnRecord) -> Option<CompactionOpenLoop> {
    let title = compact_line(&turn.prompt_user_text, 80)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "Resolve blocked work".to_string());
    let summary = compact_line(
        &format_failure_note(turn.status, turn.error_text.as_deref()),
        COMPACTION_MAX_ITEM_LEN,
    )?;
    let next_step = compact_line(&turn.prompt_user_text, COMPACTION_MAX_ITEM_LEN)?;
    Some(CompactionOpenLoop {
        title,
        summary,
        next_step,
    })
}

fn trailing_open_loop(turn: &SessionTurnRecord) -> Option<CompactionOpenLoop> {
    let next_step = compact_line(&turn.prompt_user_text, COMPACTION_MAX_ITEM_LEN)?;
    if next_step.is_empty() {
        return None;
    }
    let title = compact_line(&turn.prompt_user_text, 80)?;
    let summary = if matches!(
        turn.status,
        SessionTurnStatus::TimedOut
            | SessionTurnStatus::Failed
            | SessionTurnStatus::Cancelled
            | SessionTurnStatus::Interrupted
    ) {
        compact_line(
            &format_failure_note(turn.status, turn.error_text.as_deref()),
            COMPACTION_MAX_ITEM_LEN,
        )?
    } else {
        "Recent compacted turns left follow-up work visible.".to_string()
    };
    Some(CompactionOpenLoop {
        title,
        summary,
        next_step,
    })
}

fn normalize_lines(values: &[String], limit: usize) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();
    for value in values {
        let Some(compacted) = compact_line(value, COMPACTION_MAX_ITEM_LEN) else {
            continue;
        };
        if !seen.insert(compacted.clone()) {
            continue;
        }
        normalized.push(compacted);
        if normalized.len() >= limit {
            break;
        }
    }
    normalized
}

fn normalize_memory_proposals(
    values: &[CompactionMemoryProposal],
) -> Vec<CompactionMemoryProposal> {
    merge_unique_keyed(
        Vec::new(),
        values
            .iter()
            .filter_map(normalize_memory_proposal)
            .collect(),
        COMPACTION_LIST_KEEP,
        |proposal| normalized_summary_title_key(&proposal.title),
    )
}

fn normalize_open_loops(values: &[CompactionOpenLoop]) -> Vec<CompactionOpenLoop> {
    merge_unique_keyed(
        Vec::new(),
        values.iter().filter_map(normalize_open_loop).collect(),
        COMPACTION_LIST_KEEP,
        |open_loop| normalized_summary_title_key(&open_loop.title),
    )
}

fn normalize_memory_proposal(
    proposal: &CompactionMemoryProposal,
) -> Option<CompactionMemoryProposal> {
    let title = compact_line(&proposal.title, COMPACTION_MAX_ITEM_LEN)?;
    let rationale = compact_line(&proposal.rationale, COMPACTION_MAX_ITEM_LEN)
        .unwrap_or_else(|| "durable context from transcript compaction".to_string());
    let entries = normalize_lines(&proposal.entries, COMPACTION_LIST_KEEP);
    if entries.is_empty() {
        return None;
    }
    Some(CompactionMemoryProposal {
        title,
        rationale,
        entries,
    })
}

fn normalize_open_loop(open_loop: &CompactionOpenLoop) -> Option<CompactionOpenLoop> {
    let title = compact_line(&open_loop.title, COMPACTION_MAX_ITEM_LEN)?;
    let summary = compact_line(&open_loop.summary, COMPACTION_MAX_ITEM_LEN).unwrap_or_default();
    let next_step = compact_line(&open_loop.next_step, COMPACTION_MAX_ITEM_LEN)?;
    Some(CompactionOpenLoop {
        title,
        summary,
        next_step,
    })
}

fn merge_unique_strings(previous: Vec<String>, current: Vec<String>, limit: usize) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut merged = Vec::new();
    for value in previous.into_iter().chain(current) {
        if !seen.insert(value.clone()) {
            continue;
        }
        merged.push(value);
    }
    keep_tail(merged, limit)
}

fn merge_unique_memory_proposals(
    previous: Vec<CompactionMemoryProposal>,
    current: Vec<CompactionMemoryProposal>,
) -> Vec<CompactionMemoryProposal> {
    merge_unique_keyed(previous, current, COMPACTION_LIST_KEEP, |proposal| {
        normalized_summary_title_key(&proposal.title)
    })
}

fn merge_unique_open_loops(
    previous: Vec<CompactionOpenLoop>,
    current: Vec<CompactionOpenLoop>,
) -> Vec<CompactionOpenLoop> {
    merge_unique_keyed(previous, current, COMPACTION_LIST_KEEP, |open_loop| {
        normalized_summary_title_key(&open_loop.title)
    })
}

fn merge_unique_keyed<T, F>(previous: Vec<T>, current: Vec<T>, limit: usize, key_of: F) -> Vec<T>
where
    F: Fn(&T) -> Option<String>,
{
    let mut positions = BTreeMap::new();
    let mut merged = Vec::new();
    for item in previous.into_iter().chain(current) {
        let Some(key) = key_of(&item) else {
            continue;
        };
        if let Some(index) = positions.get(&key).copied() {
            merged[index] = item;
        } else {
            positions.insert(key, merged.len());
            merged.push(item);
        }
    }
    keep_tail(merged, limit)
}

fn keep_tail<T>(mut items: Vec<T>, limit: usize) -> Vec<T> {
    if items.len() > limit {
        let keep_from = items.len() - limit;
        items.drain(0..keep_from);
    }
    items
}

fn normalized_summary_title_key(title: &str) -> Option<String> {
    let compacted = compact_line(title, COMPACTION_MAX_ITEM_LEN)?;
    let normalized = normalized_title_key(&compacted);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn progress_entry_title(entry: &str) -> &str {
    entry
        .split_once(" (next:")
        .map(|(title, _)| title)
        .unwrap_or(entry)
        .trim()
}

fn serialize_turn_for_compaction(turn: &SessionTurnRecord) -> String {
    let mut lines = vec![
        format!("Turn {}", turn.sequence_no),
        format!(
            "User: {}",
            compact_line(&turn.prompt_user_text, 400).unwrap_or_else(|| "<empty>".to_string())
        ),
        format!("Status: {}", turn.status.as_str()),
    ];
    if !turn.assistant_text.trim().is_empty() {
        lines.push(format!(
            "Assistant: {}",
            compact_line(&turn.assistant_text, 500).unwrap_or_default()
        ));
    }
    if let Some(error_text) = turn.error_text.as_deref() {
        lines.push(format!(
            "Error: {}",
            compact_line(error_text, 300).unwrap_or_default()
        ));
    }
    lines.join("\n")
}

fn extract_json_object(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Some(trimmed.to_string());
    }

    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    if end <= start {
        return None;
    }
    Some(trimmed[start..=end].to_string())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::contracts::{SessionHistoryPolicy, SessionTurnKind, SessionTurnStatus};

    use super::{
        build_compaction_prompt, merge_compaction_summary_state, merge_compaction_summary_updates,
        merge_unique_memory_proposals, merge_unique_open_loops, normalize_memory_proposals,
        normalize_open_loops, parse_compaction_summary_state,
        remove_memory_proposal_from_summary_state, remove_open_loop_from_summary_state,
        render_compaction_summary, repair_turns, CompactionSummaryState, TranscriptMode,
        COMPACTION_LIST_KEEP,
    };
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
    fn compaction_summary_render_is_structured_and_bounded() {
        let state = CompactionSummaryState {
            goal: Some("Ship continuity search and merge commands".to_string()),
            constraints_preferences: vec!["Keep the core small.".to_string()],
            progress_done: vec!["Added continuity status output.".to_string()],
            progress_in_progress: vec!["Finishing proposal merge flow.".to_string()],
            progress_blocked: vec!["Need final QA pass.".to_string()],
            key_decisions: vec!["Continuity stays file-backed.".to_string()],
            relevant_files: vec!["src/kernel/continuity.rs".to_string()],
            next_steps: vec!["Run cargo test.".to_string()],
            critical_context: vec!["Assistant home workspace is the continuity root.".to_string()],
            memory_proposals: vec![super::CompactionMemoryProposal {
                title: "Working Preferences".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Keep the core small.".to_string()],
            }],
            open_loops: vec![super::CompactionOpenLoop {
                title: "Run cargo test".to_string(),
                summary: "Recent compacted turns left follow-up work visible.".to_string(),
                next_step: "Run cargo test.".to_string(),
            }],
        };

        let rendered = render_compaction_summary(1, 12, &state);
        assert!(rendered.contains("## Compacted Prior Turns 1-12"));
        assert!(rendered.contains("### Goal"));
        assert!(rendered.contains("### Key Decisions"));
        assert!(rendered.contains("Continuity stays file-backed."));
        assert!(rendered.contains("### Memory Proposals"));
        assert!(rendered.contains("### Open Loops"));
    }

    #[test]
    fn parse_compaction_summary_state_extracts_json_only() {
        let state = parse_compaction_summary_state(
            "ok {\"goal\":\"ship\",\"constraints_preferences\":[],\"progress_done\":[\"done\"],\"progress_in_progress\":[],\"progress_blocked\":[],\"key_decisions\":[],\"relevant_files\":[],\"next_steps\":[\"test\"],\"critical_context\":[],\"memory_proposals\":[],\"open_loops\":[]}",
        )
        .expect("parse summary");
        assert_eq!(state.goal.as_deref(), Some("ship"));
        assert_eq!(state.progress_done, vec!["done"]);
        assert_eq!(state.next_steps, vec!["test"]);
    }

    #[test]
    fn fallback_merge_keeps_recent_structured_state() {
        let first = merge_compaction_summary_state(
            None,
            &[
                turn(
                    1,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "review src/kernel/continuity.rs",
                    "updated continuity module",
                    None,
                ),
                turn(
                    2,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "run tests",
                    "tests passed",
                    None,
                ),
            ],
        );
        let merged = merge_compaction_summary_state(
            Some(&first),
            &[
                turn(
                    3,
                    Uuid::new_v4(),
                    SessionTurnStatus::Failed,
                    "final qa review",
                    "",
                    Some("needs fix"),
                ),
                turn(
                    4,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "prepare docs",
                    "updated docs/CONTINUITY_MODEL.md",
                    None,
                ),
            ],
        );

        assert!(merged
            .relevant_files
            .iter()
            .any(|item| item.contains("src/kernel/continuity.rs")));
        assert!(merged
            .progress_blocked
            .iter()
            .any(|item| item.contains("needs fix")));
        assert!(merged
            .next_steps
            .iter()
            .any(|item| item.contains("prepare docs")));
    }

    #[test]
    fn fallback_merge_extracts_preferences_memory_and_open_loops() {
        let merged = merge_compaction_summary_state(
            None,
            &[
                turn(
                    1,
                    Uuid::new_v4(),
                    SessionTurnStatus::Completed,
                    "keep the core small and prefer visible continuity files",
                    "updated src/kernel/continuity.rs to stay explicit",
                    None,
                ),
                turn(
                    2,
                    Uuid::new_v4(),
                    SessionTurnStatus::Failed,
                    "review the continuity search path",
                    "",
                    Some("fts query failed"),
                ),
            ],
        );

        assert_eq!(
            merged.goal.as_deref(),
            Some("review the continuity search path")
        );
        assert!(merged
            .constraints_preferences
            .iter()
            .any(|item| item.contains("keep the core small")));
        assert!(!merged.memory_proposals.is_empty());
        assert!(merged.memory_proposals.iter().any(|proposal| proposal
            .entries
            .iter()
            .any(|entry| entry.contains("visible continuity files"))));
        assert!(merged.open_loops.iter().any(|open_loop| open_loop
            .title
            .contains("review the continuity search path")));
    }

    #[test]
    fn compaction_summary_updates_can_replace_stale_goal() {
        let previous = CompactionSummaryState {
            goal: Some("finish continuity substrate".to_string()),
            ..CompactionSummaryState::default()
        };
        let current = CompactionSummaryState {
            goal: Some("review the scheduler follow-up".to_string()),
            ..CompactionSummaryState::default()
        };

        let merged = merge_compaction_summary_updates(Some(&previous), current);

        assert_eq!(
            merged.goal.as_deref(),
            Some("review the scheduler follow-up")
        );
    }

    #[test]
    fn compaction_prompt_includes_schema_marker_and_turns() {
        let prompt = build_compaction_prompt(
            None,
            &[turn(
                1,
                Uuid::new_v4(),
                SessionTurnStatus::Completed,
                "inspect src/kernel/core.rs",
                "done",
                None,
            )],
        );
        assert!(prompt.contains("lionclaw_compaction_handoff_v1"));
        assert!(prompt.contains("\"goal\": string|null"));
        assert!(prompt.contains("Turn 1"));
        assert!(prompt.contains("inspect src/kernel/core.rs"));
    }

    #[test]
    fn removed_proposals_and_loops_can_reappear_in_later_current_snapshots() {
        let mut state = CompactionSummaryState {
            memory_proposals: vec![super::CompactionMemoryProposal {
                title: "Working Preferences".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Keep the core small.".to_string()],
            }],
            open_loops: vec![super::CompactionOpenLoop {
                title: "Review continuity search".to_string(),
                summary: "Need to verify the search path".to_string(),
                next_step: "Run continuity search".to_string(),
            }],
            ..CompactionSummaryState::default()
        };

        assert!(remove_memory_proposal_from_summary_state(
            &mut state,
            "Working Preferences"
        ));
        assert!(remove_open_loop_from_summary_state(
            &mut state,
            "Review continuity search"
        ));
        assert!(state.memory_proposals.is_empty());
        assert!(state.open_loops.is_empty());

        let merged = merge_compaction_summary_updates(
            Some(&state),
            CompactionSummaryState {
                memory_proposals: vec![super::CompactionMemoryProposal {
                    title: "Working Preferences".to_string(),
                    rationale: "durable preference".to_string(),
                    entries: vec!["Keep the core small.".to_string()],
                }],
                open_loops: vec![super::CompactionOpenLoop {
                    title: "Review continuity search".to_string(),
                    summary: "Need to verify the search path".to_string(),
                    next_step: "Run continuity search".to_string(),
                }],
                ..CompactionSummaryState::default()
            },
        );

        assert_eq!(merged.memory_proposals.len(), 1);
        assert_eq!(merged.open_loops.len(), 1);
    }

    #[test]
    fn removal_uses_normalized_title_identity() {
        let mut state = CompactionSummaryState {
            memory_proposals: vec![super::CompactionMemoryProposal {
                title: "Working Preferences".to_string(),
                rationale: "durable preference".to_string(),
                entries: vec!["Keep the core small.".to_string()],
            }],
            open_loops: vec![super::CompactionOpenLoop {
                title: "Follow Up On Continuity Surface".to_string(),
                summary: "Need to verify case-insensitive cleanup.".to_string(),
                next_step: "Run cleanup".to_string(),
            }],
            progress_in_progress: vec![
                "follow up on continuity surface (next: Run cleanup)".to_string()
            ],
            ..CompactionSummaryState::default()
        };

        assert!(remove_memory_proposal_from_summary_state(
            &mut state,
            "working preferences"
        ));
        assert!(remove_open_loop_from_summary_state(
            &mut state,
            "follow up on continuity surface"
        ));
        assert!(state.memory_proposals.is_empty());
        assert!(state.open_loops.is_empty());
        assert!(state.progress_in_progress.is_empty());
    }

    #[test]
    fn merge_keeps_newest_capped_memory_proposals_and_open_loops() {
        let previous_proposals = (0..COMPACTION_LIST_KEEP)
            .map(|index| super::CompactionMemoryProposal {
                title: format!("Proposal {}", index),
                rationale: "existing".to_string(),
                entries: vec![format!("entry {}", index)],
            })
            .collect::<Vec<_>>();
        let merged_proposals = merge_unique_memory_proposals(
            previous_proposals,
            vec![super::CompactionMemoryProposal {
                title: "Newest Proposal".to_string(),
                rationale: "new".to_string(),
                entries: vec!["latest entry".to_string()],
            }],
        );
        assert_eq!(merged_proposals.len(), COMPACTION_LIST_KEEP);
        assert!(merged_proposals
            .iter()
            .any(|proposal| proposal.title == "Newest Proposal"));
        assert!(!merged_proposals
            .iter()
            .any(|proposal| proposal.title == "Proposal 0"));

        let previous_loops = (0..COMPACTION_LIST_KEEP)
            .map(|index| super::CompactionOpenLoop {
                title: format!("Loop {}", index),
                summary: format!("summary {}", index),
                next_step: format!("next {}", index),
            })
            .collect::<Vec<_>>();
        let merged_loops = merge_unique_open_loops(
            previous_loops,
            vec![super::CompactionOpenLoop {
                title: "Newest Loop".to_string(),
                summary: "new summary".to_string(),
                next_step: "new next".to_string(),
            }],
        );
        assert_eq!(merged_loops.len(), COMPACTION_LIST_KEEP);
        assert!(merged_loops
            .iter()
            .any(|open_loop| open_loop.title == "Newest Loop"));
        assert!(!merged_loops
            .iter()
            .any(|open_loop| open_loop.title == "Loop 0"));
    }

    #[test]
    fn merge_replaces_stale_payload_when_titles_collide() {
        let merged_proposals = merge_unique_memory_proposals(
            vec![super::CompactionMemoryProposal {
                title: "Working Preferences".to_string(),
                rationale: "stale rationale".to_string(),
                entries: vec!["stale entry".to_string()],
            }],
            vec![super::CompactionMemoryProposal {
                title: " working  preferences ".to_string(),
                rationale: "fresh rationale".to_string(),
                entries: vec!["fresh entry".to_string()],
            }],
        );
        assert_eq!(merged_proposals.len(), 1);
        assert_eq!(merged_proposals[0].rationale, "fresh rationale");
        assert_eq!(merged_proposals[0].entries, vec!["fresh entry"]);

        let merged_loops = merge_unique_open_loops(
            vec![super::CompactionOpenLoop {
                title: "Release Checklist".to_string(),
                summary: "stale summary".to_string(),
                next_step: "stale next".to_string(),
            }],
            vec![super::CompactionOpenLoop {
                title: "release checklist".to_string(),
                summary: "fresh summary".to_string(),
                next_step: "fresh next".to_string(),
            }],
        );
        assert_eq!(merged_loops.len(), 1);
        assert_eq!(merged_loops[0].summary, "fresh summary");
        assert_eq!(merged_loops[0].next_step, "fresh next");
    }

    #[test]
    fn normalize_dedupes_duplicate_titles_and_keeps_latest_payload() {
        let normalized_proposals = normalize_memory_proposals(&[
            super::CompactionMemoryProposal {
                title: "Working Preferences".to_string(),
                rationale: "stale rationale".to_string(),
                entries: vec!["stale entry".to_string()],
            },
            super::CompactionMemoryProposal {
                title: " working preferences ".to_string(),
                rationale: "fresh rationale".to_string(),
                entries: vec!["fresh entry".to_string()],
            },
        ]);
        assert_eq!(normalized_proposals.len(), 1);
        assert_eq!(normalized_proposals[0].rationale, "fresh rationale");
        assert_eq!(normalized_proposals[0].entries, vec!["fresh entry"]);

        let normalized_loops = normalize_open_loops(&[
            super::CompactionOpenLoop {
                title: "Release Checklist".to_string(),
                summary: "stale summary".to_string(),
                next_step: "stale next".to_string(),
            },
            super::CompactionOpenLoop {
                title: "release checklist".to_string(),
                summary: "fresh summary".to_string(),
                next_step: "fresh next".to_string(),
            },
        ]);
        assert_eq!(normalized_loops.len(), 1);
        assert_eq!(normalized_loops[0].summary, "fresh summary");
        assert_eq!(normalized_loops[0].next_step, "fresh next");
    }
}
