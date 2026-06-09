# LionClaw Private Context

`lionclaw-private-context` is LionClaw's bundled first-party private-context
skill. It stores explicit local assistant profile, user profile, and memory
records in host-only state, then projects selected context through LionClaw's
existing private-context projector boundary. It also records bounded committed
turns for deterministic episodic recall.

## Enable

```toml
[private_context]
projector_skill = "lionclaw-private-context"
```

The skill-owned operator command is `scripts/context`. It expects
`LIONCLAW_SKILL_STATE_DIR` to point at the selected skill state directory.

The projector entrypoint is declared in `lionclaw.toml`:

```toml
[private_context_projector]
command = "scripts/projector"

[private_context_recorder]
command = "scripts/recorder"
```

LionClaw starts the projector and recorder as host processes and passes the
skill state directory through `LIONCLAW_SKILL_STATE_DIR`. Runtimes never receive
direct access to the SQLite store.

## Operator Commands

Profile records are written explicitly by an operator. Assistant profile slots
are `identity`, `style`, `boundaries`, `workflow`, and `defaults`. User profile
slots are `identity`, `preferences`, `environment`, `working_style`, and
`standing_requests`.

```bash
export LIONCLAW_SKILL_STATE_DIR=/path/to/lionclaw/skill-state/lionclaw-private-context

skills/lionclaw-private-context/scripts/context profile assistant set style \
  "Prefer concise, concrete answers."
skills/lionclaw-private-context/scripts/context profile user set preferences \
  "The operator prefers direct status updates."
skills/lionclaw-private-context/scripts/context profile assistant list
skills/lionclaw-private-context/scripts/context profile assistant history style
```

Memory records are also explicit operator writes. They can be searched, listed,
updated, deleted, and inspected through history.

```bash
skills/lionclaw-private-context/scripts/context memory remember \
  "The project uses issue-136 for the private context skill." \
  --title "Issue marker" \
  --tag lionclaw \
  --priority 10 \
  --pinned
skills/lionclaw-private-context/scripts/context memory search "private context issue"
skills/lionclaw-private-context/scripts/context memory list --limit 20
skills/lionclaw-private-context/scripts/context operations
```

The recorder can also create durable records from explicit user directive lines
in committed turns:

```text
remember: <text>
remember that <text>
assistant style: <text>
assistant workflow: <text>
assistant default: <text>
user preferences: <text>
user standing requests: <text>
```

Directive parsing is exact, line-oriented, and case-insensitive at the prefix.
One leading Markdown bullet marker, `- ` or `* `, is allowed. Assistant text is
stored as part of the bounded episode but never creates durable profile or
memory records.

Use `--scope global` for global records or `--scope project:<scope-id>` for a
project-specific record. Project-scoped projections include global records and
exactly matching project records; profile projection orders global before
project-specific records, while memory projection ranks exact project matches
before global matches.

## Projection Behavior

The projector returns only the classes LionClaw asks for. Assistant and user
profiles do not need current user input. Memory projection requires non-empty
current input and retrieves only FTS matches from current memory records;
punctuation-only or unparseable input returns no memory rather than all memory.

Pinned memory ranks above unpinned memory only after retrieval has found
matching records. Priority, BM25 rank, update time, and stable id provide
deterministic ordering after scope and pinning.

The store keeps a WAL-mode SQLite database under the skill state directory with
current records, revision history, recorded turns, FTS indexes, metadata, and an
operation log. Operation log entries intentionally record metadata only; they do
not store profile bodies, memory bodies, titles, tags, transcript bodies, prompt
text, or runtime output.
