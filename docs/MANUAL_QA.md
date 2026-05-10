# Manual QA

This is the live acceptance checklist for behavior-changing LionClaw work. It
uses product commands first and keeps platform commands only for environment
evidence.

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
- `doctor` prints no errors for a healthy setup

## Phase 2: Direct Run

Start the everyday path:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" run
```

Use these prompts:

```text
Reply with exactly RUN_OK_A_1 and nothing else.
Read project-marker.txt from the current workspace and reply with exactly its contents.
Write a draft file named keep.txt under LIONCLAW_DRAFTS_DIR containing exactly KEEP_OK, and reply only with keep.txt. Do not explain.
/retry
/reset
Reply with exactly RESET_OK and nothing else.
/exit
```

Expected:

- the first answer is `RUN_OK_A_1`
- the marker read returns `project-a`
- `/retry` reruns the previous prompt through LionClaw history
- `/reset` starts a fresh interactive session
- generated drafts stay outside the project checkout

## Phase 3: Project Isolation

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

## Phase 4: Channels

From project A, attach the terminal channel:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" connect terminal
```

Approve the peer with the command printed by the terminal worker, then send a
message through the terminal UI.

Expected:

- pairing approval is explicit
- the channel response comes from the configured runtime
- `lionclaw logs -f` can inspect the selected instance without raw HTTP

## Phase 5: Background Operation

On Linux with a systemd user manager:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" up
"$LIONCLAW_BIN" status
"$LIONCLAW_BIN" logs --tail 200
"$LIONCLAW_BIN" doctor
```

Expected:

- the managed daemon is active
- configured background workers are active
- `doctor` reports stable `[LC-D...]` findings when there is drift
- `doctor` does not repair, start, stop, allocate, or rewrite state

For platform evidence only:

```bash
systemctl --user status lionclaw*.service
journalctl --user -u 'lionclaw*.service' -n 100 --no-pager
```

## Phase 6: Jobs

With the terminal channel available:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" job add qa-brief \
  --runtime codex \
  --schedule "every 1d" \
  --prompt "Reply with exactly JOB_OK and nothing else." \
  --deliver-channel terminal \
  --deliver-peer "${USER:-local}"
"$LIONCLAW_BIN" job ls
"$LIONCLAW_BIN" job run qa-brief
"$LIONCLAW_BIN" job runs qa-brief
"$LIONCLAW_BIN" job rm qa-brief
```

Expected:

- manual job run succeeds
- delivery reaches the approved channel peer
- job history is visible through the CLI

## Phase 7: Doctor Drift

Create only the drift relevant to the change under test, then run:

```bash
"$LIONCLAW_BIN" doctor
"$LIONCLAW_BIN" doctor --all
```

Expected:

- every finding has `[LC-D...]`, severity, target, expected, and observed
- optional `inspect` commands are read-only
- optional `repair` commands are explicit LionClaw or platform commands
- warnings alone exit 0
- errors exit 1

## Cleanup

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" down --all || true
```

Remove temporary `/tmp/lionclaw-live-*` directories only after preserving any
evidence needed for the PR.
