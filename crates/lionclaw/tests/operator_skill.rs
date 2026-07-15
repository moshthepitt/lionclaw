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
fn the_skill_uses_wait_as_the_one_blocking_surface_and_documents_controls() {
    assert!(SKILL.contains(
        "Run `lionclaw mission advance --wait` as the lead's blocking orchestration\n   primitive."
    ));
    assert!(SKILL.contains(
        "Plain `mission advance` starts or observes the detached driver\n   and returns after its startup handshake."
    ));
    assert!(SKILL
        .contains("`mission extend`, `mission stop`, or, after it\n   parks, `mission continue`"));
    assert!(SKILL.contains("Ctrl-C detaches an observer; it does not stop\n   the driver."));
}
