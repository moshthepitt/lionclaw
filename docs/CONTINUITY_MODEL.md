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

Not implemented yet:

- memory search/index
- `MEMORY.md` proposal/merge flow
- inferred open loops
- continuity skill contract
- operator-facing continuity commands

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

This follows the proven split used by the stronger assistant systems:

- bounded hot memory files
- separate transcript compression
- no hidden memory database as the canonical source of truth
