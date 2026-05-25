# Manual QA

This is the live acceptance checklist for behavior-changing LionClaw work. It
uses product commands first and keeps platform commands only for environment
evidence.

The local runtime, project, instance, job, background-daemon, and doctor phases
are runnable without a configured external channel. Provider-channel acceptance
requires real provider credentials and a real provider conversation; record
those checks as skipped when credentials are unavailable.

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
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
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
"$LIONCLAW_BIN" configure --runtime codex
"$LIONCLAW_BIN" status
"$LIONCLAW_BIN" doctor
```

Expected:

- project metadata exists under `.lionclaw/`
- `status` targets the selected project instance
- `doctor` reports no blocking setup issues and prints the scoped `run` command

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

For the plain fallback:

```bash
printf '/lionclaw exit\n' | "$LIONCLAW_BIN" run
"$LIONCLAW_BIN" run --plain
```

Expected:

- piped non-TTY usage avoids the console
- `--plain` uses the line-oriented prompt even on a TTY

Use these prompts:

```text
Reply with exactly RUN_OK_A_1 and nothing else.
/rename LionClaw QA
Read project-marker.txt from the current workspace and reply with exactly its contents.
Write a draft file named keep.txt under LIONCLAW_DRAFTS_DIR containing exactly KEEP_OK, and reply only with keep.txt. Do not explain.
/compact
/lionclaw retry
/lionclaw reset
Reply with exactly RESET_OK and nothing else.
/lionclaw exit
```

Expected:

- the first answer is `RUN_OK_A_1`
- `/rename` and `/compact` are handled as native runtime controls,
  not sent as ordinary prompt text
- the marker read returns `project-a`
- `/lionclaw retry` reruns the previous prompt through LionClaw history
- `/lionclaw reset` starts a fresh interactive session
- generated drafts stay outside the project checkout

## Phase 3: Instances And Work Roots

Create two more instances in project A:

```bash
cd "$PROJ_A"
mkdir -p shared-work reviewer-work
"$LIONCLAW_BIN" instance create reviewer --work-root "$PROJ_A/reviewer-work"
"$LIONCLAW_BIN" instance create shared --work-root "$PROJ_A/shared-work"
"$LIONCLAW_BIN" instance create shared-two --work-root "$PROJ_A/shared-work"
"$LIONCLAW_BIN" instance list
"$LIONCLAW_BIN" --instance reviewer status
"$LIONCLAW_BIN" --instance shared status
"$LIONCLAW_BIN" status --all
"$LIONCLAW_BIN" doctor --all
```

Expected:

- `instance list` marks the default instance and shows both shared instances
  pointing at the same work root
- `--instance reviewer` targets only the reviewer instance
- `status --all` and `doctor --all` enumerate project instances explicitly
- shared work roots are reported as shared, not treated as corruption

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

## Phase 5: Provider Channels

Telegram is credential-gated manual QA. Run it only when a real bot token and
chat flow are available; do not fake Telegram with local shims in this
checklist.

```bash
cd "$PROJ_A"
printf 'TELEGRAM_BOT_TOKEN=...\n' > telegram.env
"$LIONCLAW_BIN" connect telegram --env-file ./telegram.env
```

Expected when credentials are available:

- the default runtime image can run common assistant probes such as
  `codex --version`, `opencode --version`, `python3 --version`,
  `ffprobe -version`, `file --version`, `jq --version`, and `pdftotext -v`
- scoped grant approval is explicit
- the channel response comes from the configured runtime
- `lionclaw logs -f` can inspect the selected instance without raw HTTP
- `doctor` surfaces channel pairing/outbox state and worker health without
  contacting provider APIs
- the token is stored in selected-instance private channel env
- `doctor` does not print the token and shows the latest Telegram worker health
  report after the worker has submitted one
- `connect telegram` prints a one-use direct connection link when the worker
  reports the bot identity, so the first host can click instead of typing
  `/start`
- a DM pairing link shaped like `https://t.me/<bot_username>?start=lc_<token>`
  claims through the kernel and does not start an agent turn
- a group invite shaped like
  `https://t.me/<bot_username>?startgroup=lc_<token>` claims where Telegram
  exposes the payload to the bot
- in DM, `/settings` is account/channel focused, while `/connections` shows a
  `Connect group` button that creates a short-lived one-use `startgroup` link;
  the link can be opened by the host or shared with a trusted group admin
- in connected groups, chat-scoped menu commands are installed so `/ask message`
  strips the Telegram envelope and submits only `message` to the runtime; empty
  `/ask` opens a Telegram reply prompt
- in connected groups, only Telegram accounts that are also connected as
  approved direct hosts can run `/ask`, runtime slash commands, or LionClaw
  group controls
- unknown targeted Telegram groups receive a clean "not connected" setup hint
  without exposing `pc_...` approval codes, and no provider files are downloaded
  before approval
- Telegram delivery works through the configured runtime after scoped grant
  approval
- `/help`, `/status`, `/stop`, `/settings`, and `/connections` behave as
  documented: Telegram-local commands stay local, `/lionclaw reset` and
  `/lionclaw retry` enter LionClaw as canonical controls, and `/compact` reaches
  the runtime
- inline buttons for status and stop acknowledge clicks without leaking
  controls across users, chats, or forum topics
- a forum topic with a thread grant keeps replies in the same Telegram topic
- a conversation grant used inside a topic follows the channel scoped-grant
  behavior from Channels v2
- a photo, document, voice, or video attachment reaches the runtime under
  `/attachments/...`
- a rapid burst of short text messages from the same chat is received as one
  coherent turn when Telegram delivers the updates together
- a Telegram album is received as one turn with all album attachments, and a
  runtime-generated two-photo/two-video batch is returned as a native Telegram
  media group
- a Telegram location and venue reach the runtime as readable text with
  structured provider metadata
- unsupported Telegram content such as a contact or poll gets a clear local
  reply in DMs and addressed groups, and stays silent in unaddressed groups
- a runtime-generated image is returned to Telegram as a native media
  attachment, even when the runtime final text is empty
- Markdown in runtime answers renders as Telegram formatting, and local
  workspace links are shown as labels rather than broken Telegram links
- a long-running turn first shows typing, then one provisional message that is
  edited in place, then deletes that provisional message when the durable final
  answer arrives
- the original inbound Telegram message receives best-effort reactions for
  accepted, completed, stopped, or failed lifecycle states when reactions are
  available in that chat
- webhook mode rejects requests without
  `X-Telegram-Bot-Api-Secret-Token: <TELEGRAM_WEBHOOK_SECRET_TOKEN>` and accepts
  the same update when the configured secret is present
- a retryable Telegram delivery failure survives worker restart and is retried
  through the outbox lease/report flow

If credentials are not available, record this subphase as skipped with
`missing Telegram bot token`.

## Phase 6: Background Operation

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
- configured background workers are active when a background provider channel
  was configured
- `doctor` reports stable `[LC-D...]` findings when there is drift
- `doctor` warns when a configured background provider channel has no worker
  health report or its latest report is more than ten minutes old
- `doctor` does not repair, start, stop, allocate, or rewrite state
- `down` stops only units owned by the selected instance

For platform evidence only:

```bash
systemctl --user status lionclaw*.service
journalctl --user -u 'lionclaw*.service' -n 100 --no-pager
```

## Phase 7: Jobs

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
- if a provider channel was configured in Phase 5, a separate delivery job can
  target that approved conversation and should reach the provider through the
  outbox lease/report flow

## Phase 8: Doctor Diagnostics

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

Only run this after configuring a channel with required env, such as Telegram:

```bash
cd "$PROJ_A"
mv .lionclaw/instances/main/config/channels/telegram.env \
  .lionclaw/instances/main/config/channels/telegram.env.qa-bak
"$LIONCLAW_BIN" doctor
mv .lionclaw/instances/main/config/channels/telegram.env.qa-bak \
  .lionclaw/instances/main/config/channels/telegram.env
```

Expected:

- `doctor` exits 1
- the finding names the channel env problem without printing secret values
- `inspect` is read-only and `repair` points at `lionclaw connect telegram`

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
- channel observations may include latest worker health checks, but provider
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
