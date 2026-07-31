---
name: lionclaw
description: Orchestrate durable LionClaw missions from the real agent session.
---

# LionClaw

LionClaw runs real agents under a small trusted core and explicit local
boundary. This session is the orchestrator. LionClaw owns durable mission
truth, exact legal actions, confinement, proof receipts, and finish authority.

Use the `lionclaw` executable beside this file. Resolve it relative to this
`SKILL.md`; do not assume it is on `PATH`. In examples below, `lionclaw` means
that exact executable.

If the executable is absent, this is the source template rather than a release
skill. Download the latest matching `lionclaw-*-linux-x86_64.tar.gz` and
`SHA256SUMS` assets from `moshthepitt/lionclaw`, verify the archive with
`sha256sum -c`, and install the extracted `lionclaw/` directory through the
current harness's normal skill installation mechanism. Never download a bare
binary separately from its skill instructions.

## Everyday path

The human starts or resumes this session with:

```text
lionclaw run [runtime]
```

After a mission finishes, the completed mission remains current so its report
and apply choice stay obvious. To leave an unapplied result intact and begin or
select later work, the human explicitly runs `lionclaw run [runtime] --new`.
The old result remains available by mission id, including
`lionclaw mission apply <mission-id>` while its apply choice remains legal.

Read `/runtime/AGENTS.generated.md` first. It contains neutral startup facts:
the repository, selected runtime, an unambiguous mission when one exists,
terminal truth, and the exact folded `mission.next`. After every LionClaw
command, and after any process or session recovery, refresh authoritative
state with:

```text
lionclaw mission guide --json
```

Never infer legality from an earlier turn. `next.effects` is kernel-owned work;
drive it with `lionclaw mission advance --wait`. Select an operator action only
when the exact target, parameters, and action appear in current
`next.choices`, then invoke the corresponding validated command. Never
synthesize a choice, silently approve one, or treat a runtime exit as mission
success.

## Create the mission

When startup facts say no mission exists:

1. Run `lionclaw install` idempotently. Use `lionclaw doctor` when environment
   readiness is in doubt.
2. Inspect the repository and the human's objective before choosing one
   installed generic method: `software-dev`, `optimization`, `research`,
   `review`, or `design`.
3. Read the selected method with `lionclaw mission type show <method>`.
4. Start it with an explicit objective, method, and runtime profile from the
   startup facts. The everyday executable pins the repository, so omit
   `--repo`. Use that runtime for proposed team roles unless a concrete need
   justifies another configured runtime. Do not start a mission when selection
   is ambiguous; ask the human to identify the intended mission instead.
5. Refresh `mission guide --json`. If its exact choices permit a proposal,
   inspect `mission team show --json` and author one complete mission-local
   proposal containing `plan`, `team`, and any `oracles`.

The proposal is repository-specific. Its plan contains objective requirements,
falsifiable assertions, and coherent task outputs. Its next team revision
assigns every task to a read-only report producer or writable artifact
producer, as the outcome requires, and every reviewer-checkable assertion to
roles with the right output contract and least authority. An oracle is
structured data with
`type: "command"`, an argument vector, workspace-relative `cwd`, environment,
timeout, grants, and resources. Never use shell text as an oracle or copy
repository commands into the generic method.

At a `verified` stop bar, every proof-bearing requirement must be
`confined_provable`, and every named assertion must bind a command oracle whose
exit status decides the whole claim. At an `attested` stop bar, use command
proof for genuinely executable claims and `reviewer_checkable` assertions for
claims that require independent judgment. Record host-only acceptance and
known limitations explicitly.

Draft transient proposal files only under `/scratch`; the repository is
read-only to this orchestrator. Submit the complete JSON over bridge stdin only
if the current choice permits it:

```text
lionclaw mission plan propose --file - < /scratch/proposal.json
```

### Delegate a task to a child mission

A task assignment may be either a tagged `role` assignment or a tagged
`child_mission` assignment. Use a child mission only when the task benefits
from its own complete mission contract and proof loop on this same machine.
The assignment contains the child objective, report-or-artifact output
semantics, complete narrower `MissionConfig`, complete revision-zero
plan/team/oracle proposal, and a bounded deadline. It never contains lineage,
ancestry, credentials, tokens, passwords, API keys, or other secret values.

The child is an ordinary LionClaw mission. Do not invent a child command loop
or start it separately. Once the parent proposal is approved,
`mission guide --json` visibly projects the child request in `next.effects`;
drive it with the same `lionclaw mission advance --wait` command used for role
and oracle effects. The kernel durably records the parent request before it
creates and binds the deterministic child mission, reconnects that same child
after recovery, and exposes the folded result in `child_mission_receipts`.

Child success supplies only the assigned task output. Its proof summary is
audit evidence, not authoritative parent proof; the parent still runs every
required oracle and independent review. A pending child blocks finish. Child
failure enters the existing task failure choices, and parent stop or abort
propagates to the child before the parent effect is cleaned.

## Ratify and drive

When a plan proposal parks, inspect it with `lionclaw mission plan show
--json`. If the initiating request did not explicitly delegate plan
ratification to you, show the proposal to the human and wait for their
decision; a general request to run a mission is not delegation. If
ratification was explicitly delegated, review the complete proposal against
the objective and selected method yourself.

Approve only when that exact action is advertised. Otherwise, when advertised,
submit exact actionable feedback with `mission decide ... revise
--feedback-file <path>` or `--feedback-stdin`, then refresh state. Revision is
iterative and unbounded: review every new complete proposal until the ratifier
approves it or the mission is ended through an advertised abort choice.
LionClaw has no `--yes` approval bypass.

Use `lionclaw mission advance --wait` only while current `next.effects` is
nonempty. Plain `mission advance` starts or observes the detached driver and
returns after its startup handshake. Refresh the guide at every checkpoint.
Routine recovery and evidence-led replanning may be driven autonomously only
through choices supported by current evidence.

Required proof failure can never be accepted: a failed-proof target never
exposes `accept`. A failed or blocking gap review is ordinary required proof
and exposes only the recovery choices computed by `Next`. Aborting never
accepts or verifies work.

A concurrent observer may use `mission status --watch`. Use `mission extend`,
`mission stop`, or `mission continue` only for the exact effect generation and
mode advertised in current choices. Every control needs a reason. Ctrl-C
detaches an observer; it does not stop the driver. If the driver dies, refresh
the guide; the event log and current `Next`, not runtime memory, define what
survived.

When current choices expose `finish`, finish with a recorded reason. A green
mission never finishes automatically. Then read `lionclaw mission report` and
state clearly what was verified, attested, or left unresolved. Run
`lionclaw mission apply` only if the exact post-terminal apply choice is
advertised.

Use `lionclaw --help`, subcommand `--help`, or the bundled `lionclaw(1)` manual
for the command contract. Mission-specific behavior belongs to the selected
mission type's `playbook.md`, not this skill.
