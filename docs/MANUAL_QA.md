# Manual QA

This is the live acceptance checklist for behavior-changing LionClaw work. It
uses product commands first and keeps platform commands only for environment
evidence.

The local runtime, project, instance, job, background-daemon, and doctor phases
are runnable without a configured credential-backed channel. Credential-backed
channel acceptance requires real transport credentials and a real conversation;
record those checks as skipped when credentials are unavailable.

Record each phase as:

```text
phase:
result: pass | fail | skipped
evidence:
notes:
```

Skip with the concrete blocker when a dependency is unavailable, such as host
Codex auth, Podman, Linux systemd user manager, or network access.

## Phase 0: Local Gates

Run from the repository root:

```bash
cargo fmt -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
bash ./scripts/ci.sh
```

Record environment evidence:

```bash
rustc -V
cargo clippy -V
podman --version
git rev-parse HEAD
```

Expected:

- all gates pass
- failures are reproduced locally or documented with the exact blocker

## Phase 1: Fresh Project

Use a fresh project root:

```bash
export QA_STAMP=$(date +%Y%m%d-%H%M%S)
export PROJ_A=/tmp/lionclaw-live-$QA_STAMP-project-a
export PROJ_B=/tmp/lionclaw-live-$QA_STAMP-project-b
export LIONCLAW_BIN="$PWD/target/debug/lionclaw"

cargo build --workspace
mkdir -p "$PROJ_A" "$PROJ_B"
printf 'project-a\n' > "$PROJ_A/project-marker.txt"
printf 'project-b\n' > "$PROJ_B/project-marker.txt"
git -C "$PROJ_A" init
git -C "$PROJ_B" init
```

Configure project A:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" project init
"$LIONCLAW_BIN" instance create reviewer
"$LIONCLAW_BIN" configure --runtime codex
"$LIONCLAW_BIN" status
"$LIONCLAW_BIN" doctor
```

Expected:

- project metadata exists under `.lionclaw/`
- `main` and `reviewer` instance homes exist and can be targeted independently
- `status` targets the selected project instance
- `doctor` reports no blocking setup issues and prints the scoped `run` command
- runtime profiles in config use the current `driver`/`command` shape; old
  `kind`/`executable` profiles intentionally fail to load with an explicit
  format-break error rather than being migrated

Check explicit runtime mounts:

```bash
mkdir -p "$PROJ_A/reference-docs"
"$LIONCLAW_BIN" runtime mount add codex docs --source "$PROJ_A/reference-docs"
"$LIONCLAW_BIN" runtime mount list codex
"$LIONCLAW_BIN" runtime mount remove codex docs
mkdir -p "$PROJ_A/.lionclaw/private-mount"
if "$LIONCLAW_BIN" runtime mount add codex private --source "$PROJ_A/.lionclaw/private-mount"; then
  echo "unexpected metadata mount accepted"
  exit 1
fi
```

Expected:

- `add` reports a read-only mount at `/mnt/docs`
- `list` shows target, source, and access
- `remove` deletes the mount and a subsequent `list` reports no configured mounts
- metadata under `.lionclaw/` is rejected as an explicit runtime mount source

## Phase 2: Direct Run

Start the everyday path:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" run
```

Expected on a TTY:

- a full-screen operator console opens
- the top ribbon shows the selected project instance, runtime, boundary flags,
  and timeout
- the Run surface shows durable user and assistant messages, prompt input,
  live runtime status, and recent file-change feedback without raw runtime
  event spam in the durable transcript
- on a wide terminal, Project, Run, Inspector, and Files panes are visible
  together
- `Ctrl+O` focuses controls; `Left`/`Right` cycle project, selected instance,
  runtime, boundary, activity, audit, and files
- the prompt accepts printable characters such as `?`; `Tab` visibly moves
  between Run and controls; `Ctrl+X` maximizes Run or the current control
  pane; `Ctrl+D` exits when idle
- after submitting a prompt, Run scrolls without tabbing away from prompt
  entry; printable input continues prompt editing in the same surface
- while a turn is active, `Ctrl+C` requests a turn stop instead of exiting the
  console; the status changes to stopping and the console returns to idle after
  the cancelled turn is recorded

For the plain line-oriented path:

```bash
printf '/lionclaw exit\n' | "$LIONCLAW_BIN" run
"$LIONCLAW_BIN" run --plain
```

Expected:

- piped non-TTY usage avoids the console
- `--plain` uses the line-oriented prompt even on a TTY
- the debug-only session-turn journal API can read the completed turn's
  canonical journal by `session_id` and `turn_id`; raw payloads are omitted
  unless the request opts in and the kernel is running with raw runtime payload
  retention enabled

Check the selected runtime's native terminal UI:

```bash
printf '\n- Native runtime TUI sentinel: LIONCLAW_NATIVE_TUI_CONTEXT_OK\n' >> \
  .lionclaw/instances/main/workspaces/main/AGENTS.md
"$LIONCLAW_BIN" run --runtime-tui
```

Prompt inside the native runtime UI:

```text
Reply with exactly the LionClaw native runtime TUI sentinel from your instructions.
```

Expected:

- the selected runtime's own terminal UI opens inside the LionClaw launch
  boundary
- Codex shows `/workspace` as the directory and no inner Codex sandbox or
  workspace-trust prompt
- the answer is `LIONCLAW_NATIVE_TUI_CONTEXT_OK`
- exiting the native UI records `runtime.tui.launch` and `runtime.tui.exit`
  audit events
- ACP profiles such as OpenCode launch the profile command as the native UI
  without ACP protocol args; when the profile declares `terminal.resume-args`,
  saved ready ACP sessions resume through those args
- native UI turns are not inserted into LionClaw session history and are not
  available to later `lionclaw run`, `run --plain`, or channel context
- opening and exiting the native UI does not prime, clear, or otherwise change
  later program-backed continuation state
- relaunching the native UI does not run prelaunch recovery work
- interactive `run` and `run --runtime-tui` resume the latest LionClaw session
  by default; `--new-session` starts fresh LionClaw control state while keeping
  runtime-native config and history under `/runtime/home`
- a second `run --runtime-tui` targeting the same active native UI reports a
  conflict instead of attaching to the same runtime state concurrently

Use these prompts:

```text
Reply with exactly RUN_OK_A_1 and nothing else.
/rename LionClaw QA
Read project-marker.txt from the current workspace and reply with exactly its contents.
Write a draft file named keep.txt under LIONCLAW_DRAFTS_DIR containing exactly KEEP_OK, and reply only with keep.txt. Do not explain.
/compact
```

Expected:

- the first answer is `RUN_OK_A_1`
- `/rename` and `/compact` are handled by the native runtime UI
- native interrupt keys such as Codex's Ctrl+C remain handled by the runtime UI
- the marker read returns `project-a`
- generated drafts stay outside the project checkout
- LionClaw slash controls such as `/lionclaw retry`, `/lionclaw reset`, and
  `/lionclaw exit` are not available inside `run --runtime-tui`; exit through
  the runtime's native exit gesture
- turn-scoped LionClaw bridges such as runtime `channel.send` are not exposed
  inside `run --runtime-tui`

## Phase 3: Instances And Work Roots

Create two more instances in project A:

```bash
cd "$PROJ_A"
mkdir -p shared-work reviewer-work
"$LIONCLAW_BIN" instance create reviewer --work-root "$PROJ_A/reviewer-work"
"$LIONCLAW_BIN" instance create shared --work-root "$PROJ_A/shared-work"
"$LIONCLAW_BIN" instance create shared-two --work-root "$PROJ_A/shared-work"
"$LIONCLAW_BIN" --instance reviewer configure --runtime codex
"$LIONCLAW_BIN" instance list
"$LIONCLAW_BIN" --instance reviewer status
"$LIONCLAW_BIN" --instance shared status
"$LIONCLAW_BIN" status --all
"$LIONCLAW_BIN" doctor --all
cat <<'EOF' | "$LIONCLAW_BIN" --instance reviewer run
Read LIONCLAW_PROJECT_INSTANCE and the JSON file named by LIONCLAW_PROJECT_INSTANCES_FILE. Reply with exactly INVENTORY_OK if the current instance is reviewer and the instances are main, reviewer, shared, and shared-two.
/lionclaw exit
EOF
```

Expected:

- `instance list` marks the default instance and shows both shared instances
  pointing at the same work root
- `--instance reviewer` targets only the reviewer instance
- `status --all` and `doctor --all` enumerate project instances explicitly
- shared work roots are reported as shared, not treated as corruption
- the reviewer runtime has `LIONCLAW_PROJECT_INSTANCE=reviewer` and can read a
  read-only `LIONCLAW_PROJECT_INSTANCES_FILE` JSON listing `main`, `reviewer`,
  `shared`, and `shared-two`
- channel-specific neighbor contacts and runtime helpers are covered by the
  relevant owning skill docs under `skills/`

## Phase 4: Project Isolation

Configure project B separately:

```bash
cd "$PROJ_B"
"$LIONCLAW_BIN" project init
"$LIONCLAW_BIN" configure --runtime codex
"$LIONCLAW_BIN" run
```

Prompt:

```text
Read project-marker.txt from the current workspace and reply with exactly its contents.
```

Expected:

- the answer is `project-b`
- project B does not reuse project A work-root state

## Phase 5: Private Context Skill Acceptance

Exercise the bundled first-party private-context skill through the regular
installer and LionClaw-managed skill state:

```bash
cd "$PROJ_A"
export LIONCLAW_REPO_ROOT=$(cd "$(dirname "$LIONCLAW_BIN")/../.." && pwd)
cargo build --manifest-path "$LIONCLAW_REPO_ROOT/Cargo.toml" --workspace --bins
"$LIONCLAW_BIN" skill install \
  "$LIONCLAW_REPO_ROOT/skills/lionclaw-private-context" \
  --alias lionclaw-private-context
"$LIONCLAW_BIN" configure --private-context-projector lionclaw-private-context
export QA_PRIVATE_CONTEXT_SKILL=.lionclaw/instances/main/skills/lionclaw-private-context
export QA_PRIVATE_CONTEXT_STATE=.lionclaw/instances/main/config/skill-state/lionclaw-private-context
export LIONCLAW_SKILL_STATE_DIR="$QA_PRIVATE_CONTEXT_STATE"
"$QA_PRIVATE_CONTEXT_SKILL/scripts/context" profile assistant set style \
  "For QA private context marker prompts, reply exactly PRIVATE_CONTEXT_PROFILE_OK."
"$QA_PRIVATE_CONTEXT_SKILL/scripts/context" profile assistant list
PRIVATE_CONTEXT_MEMORY_ID=$(
  "$QA_PRIVATE_CONTEXT_SKILL/scripts/context" memory remember \
    "For QA private context memory marker prompts, reply exactly PRIVATE_CONTEXT_MEMORY_OK." \
    --title "QA memory" \
    --tag qa \
    --priority 10 \
    --pinned |
  jq -r '.id'
)
"$QA_PRIVATE_CONTEXT_SKILL/scripts/context" memory search "private context memory marker"
"$QA_PRIVATE_CONTEXT_SKILL/scripts/context" memory history "$PRIVATE_CONTEXT_MEMORY_ID"
"$QA_PRIVATE_CONTEXT_SKILL/scripts/context" operations --item-id "$PRIVATE_CONTEXT_MEMORY_ID"
```

Check the installed skill snapshot and activation:

```bash
test -x "$QA_PRIVATE_CONTEXT_SKILL/bin/lionclaw-private-context"
test -x "$QA_PRIVATE_CONTEXT_SKILL/scripts/projector"
test -x "$QA_PRIVATE_CONTEXT_SKILL/scripts/recorder"
test -f "$QA_PRIVATE_CONTEXT_SKILL/runtime/lionclaw-private-context/SKILL.md"
"$LIONCLAW_BIN" doctor
```

Then run the everyday path:

```bash
cat <<'EOF' | "$LIONCLAW_BIN" run --plain
What is the QA private context profile marker?
What is the QA private context memory marker?
/lionclaw exit
EOF
```

Expected:

- the context script creates a host-only SQLite store under
  `.lionclaw/instances/main/config/skill-state/lionclaw-private-context`
- profile, memory, history, search, and operations commands return JSON
- memory search returns the explicit memory for matching input and does not
  return unrelated records
- the operation log omits profile bodies, memory bodies, titles, tags, prompt
  text, and runtime output
- `doctor` accepts the configured projector metadata without raw HTTP
- the runtime has no direct access to the SQLite store
- the profile marker answer is `PRIVATE_CONTEXT_PROFILE_OK`
- the memory marker answer is `PRIVATE_CONTEXT_MEMORY_OK`

Skip the final `run --plain` subcheck with the concrete runtime/auth blocker if
no local runtime is available.

## Phase 6: Channel Skill Acceptance

Top-level QA checks only the kernel contract shared by channel skills.
Transport-specific setup, commands, webhook behavior, attachment formats,
reactions, and delivery rendering belong in the owning channel skill directory.
Run the relevant owning skill checklist under `skills/` for channel-specific
behavior. For example, email acceptance lives in
`skills/channel-email/README.md`, including the basic auth, OAuth2/XOAUTH2
`lionclaw connect email <provider>` setup flow, held-mail digest, and provider
smoke checks. Telegram
acceptance lives in `skills/channel-telegram/README.md`.

When a real credential-backed channel is available, configure it through its
own skill docs, then record the generic kernel checks here:

```bash
cd "$PROJ_A"
export QA_CHANNEL_ID=your-channel-id
export QA_CHANNEL_ENV_FILE=/path/to/channel.env
"$LIONCLAW_BIN" connect "$QA_CHANNEL_ID" --env-file "$QA_CHANNEL_ENV_FILE"
```

Expected when credentials are available:

- the default runtime image reports the Codex version matching the
  `CODEX_VERSION` pin in
  `containers/runtime/Containerfile`, and can run common assistant probes such
  as `python3 --version`,
  `ffprobe -version`, `file --version`, `jq --version`, and `pdftotext -v`
- scoped grant approval is explicit
- accepted channel turns run through the configured runtime
- `lionclaw logs -f` can inspect the selected instance without raw HTTP
- `doctor` surfaces channel pairing/outbox state and worker health without
  contacting transport APIs
- required channel env is stored in selected-instance private channel env
- `doctor` does not print secret values and shows the latest worker health
  report after the worker has submitted one
- transport files are not downloaded before kernel admission approves them
- retryable delivery failure survives worker restart and is retried through the
  outbox lease/report flow

If credentials are not available, record this subphase as skipped with the
concrete missing transport credential or conversation.

## Phase 7: Background Operation

On Linux with a systemd user manager:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" up
"$LIONCLAW_BIN" status
"$LIONCLAW_BIN" logs --tail 200
"$LIONCLAW_BIN" logs -f
"$LIONCLAW_BIN" doctor
"$LIONCLAW_BIN" down
```

Expected:

- the managed daemon is active
- configured background workers are active when a credential-backed background
  channel was configured
- `doctor` reports stable `[LC-D...]` findings when there is drift
- `doctor` warns when a configured credential-backed background channel has no
  worker health report or its latest report is more than ten minutes old
- `doctor` does not repair, start, stop, allocate, or rewrite state
- `down` stops only units owned by the selected instance

For platform evidence only:

```bash
systemctl --user status lionclaw*.service
journalctl --user -u 'lionclaw*.service' -n 100 --no-pager
```

## Phase 8: Jobs

Create and run a local job:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" job add qa-brief \
  --runtime codex \
  --schedule "every 1d" \
  --prompt "Reply with exactly JOB_OK and nothing else."
"$LIONCLAW_BIN" job ls
"$LIONCLAW_BIN" job run qa-brief
"$LIONCLAW_BIN" job runs qa-brief
"$LIONCLAW_BIN" job rm qa-brief
```

Expected:

- manual job run succeeds
- job history is visible through the CLI
- if a credential-backed channel was configured in Phase 6, a separate delivery
  job can target that approved conversation and should reach the transport
  through the outbox lease/report flow

## Phase 9: Doctor Diagnostics

Every diagnostic finding must include `[LC-D...]`, severity, `target`,
`expected`, `observed`, and read-only `inspect`. `repair` appears only when the
command is concrete and safe.

### Missing Default Instance

Use a scratch copy or restore the file after the check:

```bash
cd "$PROJ_A"
cp .lionclaw/project.toml .lionclaw/project.toml.qa-bak
printf 'version = 1\ndefault_instance = "missing"\n' > .lionclaw/project.toml
"$LIONCLAW_BIN" doctor
mv .lionclaw/project.toml.qa-bak .lionclaw/project.toml
```

Expected:

- `doctor` exits 1
- the finding reports the missing configured default instance
- the repair is an explicit `lionclaw --project ... instance create missing`

### Missing Channel Env

Only run this after configuring a channel with required env:

```bash
cd "$PROJ_A"
export QA_CHANNEL_ID=your-channel-id
mv ".lionclaw/instances/main/config/channels/$QA_CHANNEL_ID.env" \
  ".lionclaw/instances/main/config/channels/$QA_CHANNEL_ID.env.qa-bak"
"$LIONCLAW_BIN" doctor
mv ".lionclaw/instances/main/config/channels/$QA_CHANNEL_ID.env.qa-bak" \
  ".lionclaw/instances/main/config/channels/$QA_CHANNEL_ID.env"
```

Expected:

- `doctor` exits 1
- the finding names the channel env problem without printing secret values
- `inspect` is read-only and `repair` points at `lionclaw connect <channel>`

Skip with `no credential-backed background channel configured` when this
precondition is not available.

### Bind Conflict

Reserve the selected configured bind with a non-LionClaw process, then run
doctor. Use the actual configured port from
`.lionclaw/instances/main/config/lionclaw.toml`.

```bash
cd "$PROJ_A"
PORT=$(sed -n 's/^bind = "127\.0\.0\.1:\([0-9][0-9]*\)"/\1/p' \
  .lionclaw/instances/main/config/lionclaw.toml | head -n 1)
test -n "$PORT"
python3 -m http.server "$PORT" --bind 127.0.0.1 --directory "$PROJ_A" \
  >/tmp/lionclaw-bind-conflict.log 2>&1 &
BIND_QA_PID=$!
trap 'kill "$BIND_QA_PID" 2>/dev/null || true' EXIT
"$LIONCLAW_BIN" doctor
kill "$BIND_QA_PID" 2>/dev/null || true
trap - EXIT
```

Expected:

- `doctor` exits 1
- the bind finding is `LC-D001`
- `inspect` uses `ss -ltnp '( sport = :PORT )'`
- `note` says to stop the process shown by inspect
- no `repair` is printed because stopping the occupying process is a manual decision
- no raw HTTP command is printed

### Stale Or Ghost Units

Create only one unit drift at a time. Examples:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" up
systemctl --user stop 'lionclaw*.service'
"$LIONCLAW_BIN" doctor
```

For ghost-unit coverage, preserve a copy of an owned LionClaw unit file, run
`down`, restore the copied unit file under `~/.config/systemd/user/`, then run:

```bash
"$LIONCLAW_BIN" doctor --all
```

Expected:

- stopped owned units are errors with explicit `lionclaw up` repair
- ghost or unowned LionClaw-looking units are warnings
- warnings never mutate or remove units

### Final Doctor Sweep

After restoring drift:

```bash
"$LIONCLAW_BIN" doctor
"$LIONCLAW_BIN" doctor --all
```

Expected:

- every finding has `[LC-D...]`, severity, target, expected, and observed
- every finding has a read-only `inspect` command
- channel observations may include latest worker health checks, but transport
  diagnostics remain worker-reported rather than doctor-performed
- optional `repair` commands are explicit LionClaw or platform commands
- info and warnings exit 0
- errors exit 1

## Cleanup

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" down --all || true
```

Remove temporary `/tmp/lionclaw-live-*` directories only after preserving any
evidence needed for the PR.
