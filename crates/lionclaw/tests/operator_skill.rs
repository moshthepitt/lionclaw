//! The release skill is LionClaw's thin everyday orchestration contract.

use clap::CommandFactory;
use lionclaw::cli::Cli;

const SKILL: &str = include_str!("../../../skills/lionclaw/SKILL.md");

#[test]
fn everyday_run_is_the_one_product_entrypoint() {
    assert!(SKILL.contains("lionclaw run [runtime]"));
    assert!(SKILL.contains("This session is the orchestrator."));
    assert!(SKILL
        .contains("Read `/runtime/AGENTS.generated.md` first. It contains neutral startup facts"));
    assert!(SKILL.contains("the repository, selected runtime"));
    assert!(SKILL.contains("lionclaw mission guide --json"));
    assert!(!SKILL.contains("## Run a mission"));
}

#[test]
fn no_mission_selects_one_of_exactly_five_generic_methods() {
    assert!(SKILL.contains("`software-dev`, `optimization`, `research`,\n   `review`, or `design`"));
    assert!(SKILL.contains("Inspect the repository and the human's objective before choosing one"));
    assert!(SKILL.contains(
        "author one complete mission-local\n   proposal containing `plan`, `team`, and any `oracles`"
    ));
    assert!(SKILL.contains("The everyday executable pins the repository, so omit\n   `--repo`."));
    assert!(SKILL.contains(
        "Use that runtime for proposed team roles unless a concrete need\n   justifies another configured runtime."
    ));
    assert!(SKILL.contains(
        "Never use shell text as an oracle or copy\nrepository commands into the generic method."
    ));
    assert!(!SKILL.contains("metric-driven"));
}

#[test]
fn current_next_is_the_only_action_authority() {
    assert!(SKILL.contains(
        "Select an operator action only\nwhen the exact target, parameters, and action appear in current\n`next.choices`"
    ));
    assert!(SKILL.contains(
        "Use `lionclaw mission advance --wait` only while current `next.effects` is\nnonempty."
    ));
    assert!(
        SKILL.contains("and after any process or session recovery, refresh authoritative\nstate")
    );
    assert!(SKILL.contains(
        "Never\nsynthesize a choice, silently approve one, or treat a runtime exit as mission\nsuccess."
    ));
}

#[test]
fn ordinary_requests_preserve_human_plan_ratification() {
    assert!(SKILL.contains(
        "If the initiating request did not explicitly delegate plan\nratification to you, show the proposal to the human and wait for their\ndecision; a general request to run a mission is not delegation."
    ));
    assert!(SKILL.contains("lionclaw mission plan show\n--json"));
    assert!(SKILL.contains("LionClaw has no `--yes` approval bypass."));
}

#[test]
fn delegated_ratification_uses_the_same_unbounded_revision_loop() {
    assert!(SKILL.contains(
        "If\nratification was explicitly delegated, review the complete proposal against\nthe objective and selected method yourself."
    ));
    assert!(SKILL.contains(
        "Revision is\niterative and unbounded: review every new complete proposal until the ratifier\napproves it or the mission is ended through an advertised abort choice."
    ));
    assert!(SKILL.contains("revise\n--feedback-file <path>` or `--feedback-stdin"));
    assert!(SKILL.contains("Aborting never\naccepts or verifies work."));
}

#[test]
fn required_proof_failure_is_never_taught_as_acceptance() {
    assert!(SKILL.contains(
        "Required proof failure can never be accepted: a failed-proof target never\nexposes `accept`."
    ));
    assert!(SKILL.contains(
        "A failed or blocking gap review is ordinary required proof\nand exposes only the recovery choices computed by `Next`."
    ));
    assert!(!SKILL.contains("terminal-review `accept`"));
    assert!(!SKILL.contains("waived at review"));
    assert!(!SKILL.contains("accepted below bar"));
}

#[test]
fn the_skill_documents_runtime_loss_controls_and_honest_completion() {
    assert!(SKILL.contains(
        "Plain `mission advance` starts or observes the detached driver and\nreturns after its startup handshake."
    ));
    assert!(SKILL.contains(
        "Use `mission extend`,\n`mission stop`, or `mission continue` only for the exact effect generation and\nmode advertised in current choices."
    ));
    assert!(SKILL.contains("Ctrl-C\ndetaches an observer; it does not stop the driver."));
    assert!(SKILL.contains("A green\nmission never finishes automatically."));
    assert!(SKILL.contains(
        "Run\n`lionclaw mission apply` only if the exact post-terminal apply choice is\nadvertised."
    ));
}

#[test]
fn generated_help_and_manual_include_the_everyday_exit_contract() {
    let mut command = Cli::command();
    let help = command.render_long_help().to_string();
    assert!(help.contains("run"));
    assert!(help.contains("Launch or resume the everyday orchestrator"));

    let mut manual = Vec::new();
    clap_mangen::Man::new(Cli::command())
        .render(&mut manual)
        .unwrap();
    let manual = String::from_utf8(manual).unwrap();
    assert!(manual.contains("lionclaw run"));
    assert!(manual.contains("work remains nonterminal"));
}
