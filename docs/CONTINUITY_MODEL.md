# Continuity Model

LionClaw continuity is visible, local, and file-backed.

The goal is not hidden "AI memory." The goal is durable assistant continuity that:

- lives under `LIONCLAW_HOME`
- is readable and editable without special tooling
- stays small in the prompt hot path
- keeps transcript compaction separate from long-term memory

## Roots

LionClaw has two different roots:

- `LIONCLAW_HOME/workspaces/<daemon.workspace>`: the assistant home workspace
- optional project/task workspace root: the filesystem root used for brokered `fs.read` / `fs.write`

Continuity always lives in the assistant home workspace.

That means:

- prompt identity and continuity are loaded from the assistant home workspace
- scheduler artifacts and daily notes are written to the assistant home workspace
- brokered filesystem actions may target a different project/task root

By default, when no separate project root is configured, both roles point at the same workspace.

## Layout

The assistant home workspace contains:

- `IDENTITY.md`
- `SOUL.md`
- `AGENTS.md`
- `USER.md`
- `MEMORY.md`
- `continuity/ACTIVE.md`
- `continuity/daily/...`
- `continuity/open-loops/...`
- `continuity/artifacts/...`
- `continuity/rollups/...`

## Current v1 behavior

Implemented now:

- `MEMORY.md` is bootstrapped and auto-loaded into prompts
- `continuity/ACTIVE.md` is kernel-generated from deterministic state
- deterministic daily continuity entries are written for:
  - pending channel pairing
  - scheduled job success/failure
  - failed session turns
- scheduled outputs are recorded as artifact files
- transcript compaction summaries are stored in SQLite and loaded separately from file memory
- transcript compaction uses one structured persisted handoff summary plus the recent raw turn tail
- compaction performs a typed continuity flush before summary persistence:
  - memory proposals are written under `continuity/proposals/memory/...`
  - open loops are upserted under `continuity/open-loops/...`
- continuity has first-class kernel/operator/API surfaces for:
  - status
  - search
  - get
  - list/merge/reject memory proposals
  - list/resolve open loops
- prompt history uses one bounded compaction handoff summary plus the recent raw turn tail

Not implemented yet:

- semantic/FTS continuity index
- inferred open loops outside deterministic/kernel-flushed paths
- continuity skill contract

## Hot prompt context

The prompt hot path stays intentionally small.

Auto-loaded:

- `IDENTITY.md`
- `SOUL.md`
- `AGENTS.md`
- `USER.md`
- `MEMORY.md`
- `continuity/ACTIVE.md`

Not auto-loaded:

- old daily files
- artifact history
- rollups
- archived loop material

## Compaction

Transcript compaction is a core lifecycle, not a memory skill.

LionClaw keeps:

- full raw turn history in SQLite
- compacted transcript summaries in SQLite
- visible continuity files in the assistant home workspace

Prompt rendering follows the proven assistant pattern used by Hermes, IronClaw, and OpenClaw:

- one bounded persisted structured handoff summary for compacted history
- recent raw turns kept intact
- no ever-growing literal digest of all older turns in the prompt

The handoff summary is core-owned. When the active runtime explicitly supports side-effect-free hidden summarization, LionClaw updates that handoff through a strict JSON schema; otherwise it falls back to deterministic kernel-side compaction. That keeps the core small without introducing hidden side effects outside LionClaw's normal policy boundary.

Before compaction is persisted, LionClaw performs a typed continuity flush so durable facts and active commitments can land in visible files rather than only in the transcript summary.

This follows the proven split used by the stronger assistant systems:

- bounded hot memory files
- separate transcript compression
- no hidden memory database as the canonical source of truth
