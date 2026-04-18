# Continuity Model

LionClaw continuity is visible, local, file-backed, and runtime-independent.

The goal is not hidden "AI memory." The goal is durable assistant continuity
that:

- lives under `LIONCLAW_HOME`,
- is readable and editable without special tooling,
- stays small in the prompt hot path,
- survives runtime changes,
- keeps transcript compaction separate from long-term memory,
- does not confuse runtime-private harness state with LionClaw memory.

The current continuity security boundary is Unix-only. Descriptor-rooted
assistant-home filesystem hardening assumes Linux/macOS-style Unix behavior.

## Roots

LionClaw has three important roots:

- `LIONCLAW_HOME`: machine-owned instance state.
- `LIONCLAW_HOME/workspaces/<daemon.workspace>`: assistant home workspace.
- project/task workspace root: the filesystem tree mounted into confined
  runtimes at `/workspace`.

Continuity always lives in the assistant home workspace.

That means:

- prompt identity and continuity are loaded from assistant home,
- scheduler artifacts and daily notes are written to assistant home,
- normal agent file work happens in the separate project/task root,
- runtime-private harness state lives under the confined `/runtime` mount,
- runtime-private state can help the harness resume but is not canonical
  LionClaw memory.

By default, local `lionclaw run` uses the current working directory as the
project root, while `LIONCLAW_HOME` remains the LionClaw instance home.

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
- `continuity/proposals/memory/...`
- `continuity/rollups/...`

These files are product continuity. They should stay understandable to the
operator and to future runtime prompts.

## Current Behavior

Implemented now:

- `MEMORY.md` is bootstrapped and auto-loaded into prompts.
- `continuity/ACTIVE.md` is kernel-generated from deterministic state.
- deterministic daily continuity entries are written for:
  - pending channel pairing,
  - scheduled job success/failure,
  - failed session turns.
- scheduled outputs are recorded as artifact files.
- transcript compaction summaries are stored in SQLite and loaded separately
  from file memory.
- transcript compaction uses one structured persisted handoff summary plus the
  recent raw turn tail.
- compaction performs a typed continuity flush before summary persistence:
  - memory proposals are written under `continuity/proposals/memory/...`,
  - open loops are upserted under `continuity/open-loops/...`.
- active proposals and open loops use deterministic title-keyed filenames:
  - `"{slug}--{uuid-v5}.md"`,
  - the same title maps back to the same active file,
  - different titles that normalize to the same slug still get distinct files,
  - the managed active filename is the stable identity for merge/reject/resolve cleanup,
  - content edits are supported, but active-file renames are outside the managed contract.
- continuity has first-class kernel/operator/API surfaces for:
  - status,
  - indexed search,
  - get,
  - list/merge/reject memory proposals,
  - list/resolve open loops,
  - list/promote/discard runtime drafts.
- prompt history uses one bounded compaction handoff summary plus the recent raw turn tail.
- deterministic kernel-side compaction extracts:
  - current goal,
  - constraints and preferences,
  - durable memory proposals,
  - open loops,
  - recent decisions, files, and next steps.
- continuity archives are historical records only:
  - merged/rejected proposals move under `continuity/proposals/memory/archive/...`,
  - resolved loops move under `continuity/open-loops/archive/...`,
  - archive presence does not suppress future active items with the same title,
  - repeated same-title archive actions keep distinct history entries.
- active continuity state has only two authorities:
  - canonical active Markdown files under assistant home,
  - each session's latest persisted compaction summary state.
- continuity file I/O is rooted in the assistant home workspace through
  descriptor-based Unix filesystem operations:
  - writes do not rely on check-then-open pathname validation,
  - symlinked roots or subtree components are rejected for traversal,
  - symlinked leaf files are replaced in place rather than followed outside assistant home.
- the rest of the hot assistant-home prompt surface (`IDENTITY.md`, `SOUL.md`,
  `AGENTS.md`, `USER.md`) is read and bootstrapped through the same rooted
  workspace boundary.
- continuity-adjacent API mutations backed by SQLite commit authoritative
  state and audit atomically in one transaction.
- `continuity/ACTIVE.md` refresh is derived state:
  - it runs after committed mutations,
  - snapshot rebuilds are serialized in the kernel,
  - global slices use bounded, purpose-specific store queries,
  - refresh failure is audited as `continuity.refresh_failed`,
  - refresh failure does not turn an already-committed mutation into an outward API error.

Not implemented yet:

- inferred open loops outside deterministic/kernel-flushed paths,
- richer memory review workflows,
- semantic hidden summarization for every production runtime,
- home-channel delivery as a first-class continuity destination.

## Hot Prompt Context

The prompt hot path stays intentionally small.

Auto-loaded:

- `IDENTITY.md`
- `SOUL.md`
- `AGENTS.md`
- `USER.md`
- `MEMORY.md`
- `continuity/ACTIVE.md`

Not auto-loaded:

- old daily files,
- artifact history,
- rollups,
- archived loop material,
- archived proposal material,
- arbitrary runtime-private state.

Runtime-private continuation is handled separately. When a program-backed
runtime can resume its own session from `/runtime`, LionClaw sends the new
input plus a continuation note instead of replaying the full transcript every
turn. LionClaw still owns the canonical transcript.

## Compaction

Transcript compaction is a core lifecycle, not a memory skill.

LionClaw keeps:

- full raw turn history in SQLite,
- compacted transcript summaries in SQLite,
- visible continuity files in the assistant home workspace,
- runtime-private harness state under `/runtime`.

The prompt uses:

- one bounded persisted structured handoff summary for compacted history,
- recent raw turns kept intact,
- hot assistant-home files.

The handoff summary is core-owned. When the active runtime explicitly supports
side-effect-free hidden summarization, LionClaw can update that handoff through
a strict schema. Otherwise it falls back to deterministic kernel-side
compaction. That keeps the core small without letting hidden runtime behavior
become the memory authority.

Continuity search is backed by a derived SQLite FTS index inside
`lionclaw.db`. The canonical source of truth remains the assistant-home
Markdown files; the index is rebuilt from and synchronized with those files so
search stays fast without introducing a second memory plane.

Read-only continuity enumeration paths tolerate per-file `ENOENT` churn. If
`MEMORY.md` or another continuity file disappears between listing and read,
LionClaw skips that file, continues from the remaining canonical files, and
still surfaces real boundary or permission failures.

Before compaction is persisted, LionClaw performs a typed continuity flush so
durable facts and active commitments can land in visible files rather than
only in the transcript summary.

## Design Rule

LionClaw continuity must be portable across runtimes.

Codex may keep its own private session state. OpenCode may keep its own
private session state. Future runtimes may do the same. LionClaw can use that
state to make turns efficient, but it must not depend on any one runtime's
private format as the assistant's durable memory.
