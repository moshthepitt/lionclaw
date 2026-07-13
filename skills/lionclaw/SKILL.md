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
5. Treat `lionclaw mission status --json` as the current playbook: it reports
   mission state, evidence, and legal actions for every open attention item.
   Use `mission advance` to drive automatic work and `mission decide` for an
   open item.
6. Preserve human plan approval or acceptance decisions for the human. Routine
   retries, repair, and replanning may be driven autonomously when the CLI
   presents those actions and the evidence supports them.
7. Finish by reading `lionclaw mission report` and state clearly what was
   verified, accepted below bar, or left unresolved.

Use `lionclaw --help`, subcommand `--help`, or the bundled `lionclaw(1)` manual
for the command contract. Mission-specific behavior belongs to the selected
mission type's `playbook.md`, not this skill.
