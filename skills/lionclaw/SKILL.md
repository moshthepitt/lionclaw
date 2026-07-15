---
name: lionclaw
description: Run and supervise LionClaw missions through the bundled CLI.
---

# LionClaw

Use the `lionclaw` executable beside this file. Resolve it relative to this
`SKILL.md`; do not assume it is on `PATH`.

If the executable is absent, this is the source template rather than a release
skill. Download the latest matching `lionclaw-*-linux-x86_64.tar.gz` and
`SHA256SUMS` assets from `moshthepitt/lionclaw`, verify the archive with
`sha256sum -c`, and install the extracted `lionclaw/` directory through the
current harness's normal skill installation mechanism. Never download a bare
binary separately from its skill instructions.

## Run a mission

1. Run `lionclaw install` idempotently, then `lionclaw doctor`.
2. Resolve the requested mission type by installed name or explicit directory.
3. Read its method with `lionclaw mission type show <mission-type>`.
4. Start the mission with an explicit objective, repository, mission type, and
   runtime profile.
5. Run `lionclaw mission advance --wait` as the lead's blocking orchestration
   primitive. Plain `mission advance` starts or observes the detached driver
   and returns after its startup handshake. Read `mission status --json` at
   each checkpoint and repeat until the mission is terminal or needs a
   decision. Status is the source of current evidence and legal actions.
6. When a plan proposal parks, inspect it with `lionclaw mission plan show
   --json`. If the initiating request did not explicitly delegate plan
   ratification to you, show the proposal to the human and wait for their
   decision; a general request to run a mission is not delegation. If
   ratification was explicitly delegated, review the complete proposal against
   the objective and mission-type method yourself. Approve only when it is
   ready. Otherwise submit exact, actionable feedback with `mission decide ...
   revise --feedback-file <path>` or `--feedback-stdin`, then return to the
   advance/status loop. Revision is iterative and unbounded: review every new
   complete proposal until the ratifier approves or aborts. LionClaw has no
   `--yes` approval bypass; do not invent one.
7. Use `mission decide` only for an action listed on an open attention item.
   Non-revise actions require `--justification`. Preserve acceptance below the
   mission's proof bar for the human unless the initiating request explicitly
   delegates that decision. Routine retry, repair, and evidence-led replanning
   may be driven autonomously when the listed action is supported by the
   evidence.
8. A concurrent observer may use `mission status --watch` and can target the
   projected effect id with `mission extend`, `mission stop`, or, after it
   parks, `mission continue`. Every control needs a reason and applies only to
   that exact effect generation. Ctrl-C detaches an observer; it does not stop
   the driver. If the driver dies, the next `mission advance` attributes the
   interruption to each inherited effect while preserving task workspaces.
9. Finish by reading `lionclaw mission report` and state clearly what was
   verified, accepted below bar, or left unresolved.

Use `lionclaw --help`, subcommand `--help`, or the bundled `lionclaw(1)` manual
for the command contract. Mission-specific behavior belongs to the selected
mission type's `playbook.md`, not this skill.
