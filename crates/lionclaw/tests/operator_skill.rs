//! The release skill is LionClaw's thin orchestration contract. Keep the
//! ratification scenarios explicit while command details remain in the CLI.

const SKILL: &str = include_str!("../../../skills/lionclaw/SKILL.md");

#[test]
fn ordinary_mission_requests_preserve_human_plan_ratification() {
    assert!(SKILL.contains(
        "If the initiating request did not explicitly delegate plan\n   ratification to you, show the proposal to the human and wait for their\n   decision; a general request to run a mission is not delegation."
    ));
    assert!(SKILL.contains("lionclaw mission plan show\n   --json"));
    assert!(SKILL.contains("LionClaw has no\n   `--yes` approval bypass; do not invent one."));
}

#[test]
fn delegated_ratification_uses_the_same_unbounded_revision_loop() {
    assert!(SKILL.contains(
        "If\n   ratification was explicitly delegated, review the complete proposal against\n   the objective and mission-type method yourself."
    ));
    assert!(SKILL.contains(
        "Revision is iterative and unbounded: review every new\n   complete proposal until the ratifier approves or aborts."
    ));
    assert!(SKILL.contains("revise --feedback-file <path>` or `--feedback-stdin"));
    assert!(SKILL.contains("Non-revise actions require `--justification`."));
}

#[test]
fn the_skill_keeps_one_blocking_advance_status_loop() {
    assert!(SKILL.contains(
        "Repeat this advance/status loop until the mission is terminal or needs a\n   decision."
    ));
    assert!(SKILL
        .contains("If the advance caller itself is\n   interrupted, run `mission advance` again"));
}
