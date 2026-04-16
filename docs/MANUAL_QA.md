# Manual QA Live Pass

This is the repeatable live manual QA process for LionClaw. It validates the
real Codex runtime path under real Podman confinement, real host Codex auth,
real daemon reuse, and real channel/job flows. It is not a unit-test
substitute.

Run this before merging behavior-changing work that touches:

- runtime execution or Podman confinement
- Codex auth/config staging
- runtime image identity or selected-runtime resolution
- daemon compatibility, service reuse, or project scoping
- `lionclaw run`, `service up`, `channel attach`, terminal channels, or jobs
- runtime secrets, drafts, continuity, or session recovery

Do not use production secrets. Use only dummy `runtime-secrets.env` values.
The host Codex auth store for the pass is `${CODEX_HOME:-$HOME/.codex}`. If
`CODEX_HOME` is set, keep it exported for every phase so direct runs and
managed daemon runs validate the same host Codex home.

## Pass/Fail Rule

Stop early for a major blocker in a core phase:

- no successful direct `lionclaw run codex`
- project B can reuse project A daemon/session state
- runtime secrets are printed or left newly orphaned in Podman
- selected runtime image resolution blocks an unrelated selected-good runtime
- terminal channel cannot complete pair/approve/send

Model prompt-following variance, such as an extra explanatory line before the
expected token, is not a blocker unless LionClaw mutated the prompt, replayed
wrong context, or used the wrong project root.

Record one line per phase while running the pass:

```text
phase:
result: pass | fail | skipped
evidence:
notes:
```

Skipped phases must include the exact blocker, such as missing host Codex auth,
Podman unavailable, systemd unavailable, or network unavailable.

## Scope

Default full pass:

- automated gates
- shared runtime image rebuild
- fresh `LIONCLAW_HOME`
- two git-backed project roots
- direct Codex run
- draft create/promote/discard
- `/retry`, `/reset`, and `--continue-last-session`
- managed daemon `service up/status/logs`
- selected runtime image identity checks
- terminal channel pair/approve/send/reattach/retry
- scheduler job run and channel delivery
- runtime-secret mounted turn
- same-home project isolation
- stale-config restart

Out of scope unless the PR specifically touches them:

- OpenCode runtime live pass
- Telegram channel live pass
- destructive host cleanup of old Podman resources

## Phase 0: Automated Gates

Run from the repository root:

```bash
cargo fmt -- --check
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
bash ./scripts/ci.sh
cargo build --bins
```

Build the local shared runtime image:

```bash
podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .
podman image inspect lionclaw-runtime:v1 --format 'IMAGE={{.Id}} OS={{.Os}} ARCH={{.Architecture}}'
```

Record:

```bash
rustc -V
cargo clippy -V
podman --version
git rev-parse HEAD
export HOST_CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
stat "$HOST_CODEX_HOME/auth.json"
```

Expected:

- all gates pass
- image exists locally and is for the host architecture
- host Codex auth exists as a regular owner-only file

## Phase 1: Fresh Home And Projects

Use a fresh home and two separate project roots:

```bash
export QA_STAMP=$(date +%Y%m%d-%H%M%S)
export LIONCLAW_HOME=/tmp/lionclaw-live-$QA_STAMP-home
export PROJ_A=/tmp/lionclaw-live-$QA_STAMP-project-a
export PROJ_B=/tmp/lionclaw-live-$QA_STAMP-project-b
export REPO_ROOT="$PWD"
export LIONCLAW_BIN="$PWD/target/debug/lionclaw"

mkdir -p "$LIONCLAW_HOME" "$PROJ_A" "$PROJ_B"
printf 'project-a\n' > "$PROJ_A/project-marker.txt"
printf 'project-b\n' > "$PROJ_B/project-marker.txt"
git -C "$PROJ_A" init
git -C "$PROJ_B" init
```

Configure LionClaw from project A:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" onboard --bind auto
"$LIONCLAW_BIN" runtime add codex --kind codex --bin codex --image lionclaw-runtime:v1
"$LIONCLAW_BIN" runtime set-default codex
"$LIONCLAW_BIN" skill add "$REPO_ROOT/skills/channel-terminal" --alias terminal
"$LIONCLAW_BIN" channel add terminal --skill terminal --launch interactive
"$LIONCLAW_BIN" runtime ls
"$LIONCLAW_BIN" continuity status
```

Optional selected-runtime regression fixture:

```bash
"$LIONCLAW_BIN" runtime add broken-codex \
  --kind codex \
  --bin codex \
  --image localhost/lionclaw-runtime:definitely-missing-manual-qa
```

Expected:

- `codex` is the default runtime
- terminal channel is registered as `interactive`
- adding the broken runtime does not affect subsequent selected-good `codex`
  launches

## Phase 2: Direct Codex Run

Start the interactive path from project A:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" run codex
```

Use these prompts:

```text
Reply with exactly RUN_OK_A_1 and nothing else.
Read project-marker.txt from the current workspace and reply with exactly its contents.
Write a draft file named keep.txt under LIONCLAW_DRAFTS_DIR containing exactly KEEP_OK, and reply only with keep.txt. Do not explain.
Write a second draft file named drop.txt under LIONCLAW_DRAFTS_DIR containing exactly DROP_OK, and reply only with drop.txt. Do not explain.
/retry
/reset
Reply with exactly RESET_OK and nothing else.
/exit
```

Expected:

- first answer is `RUN_OK_A_1`
- marker read uses project A and returns `project-a`
- drafts are created under the runtime draft area, not in the project checkout
- `/retry` reruns the prior prompt using LionClaw history
- `/reset` opens a fresh interactive session

Host checks:

```bash
"$LIONCLAW_BIN" continuity drafts ls --runtime codex
find "$PROJ_A" -maxdepth 2 -type f -print | sort
find "$LIONCLAW_HOME/runtime" -maxdepth 8 -print | sort
```

Expected:

- `keep.txt` and `drop.txt` are listed as drafts
- project A contains only expected project files, not draft outputs

## Phase 3: Draft Management

```bash
"$LIONCLAW_BIN" continuity drafts promote keep.txt --runtime codex
"$LIONCLAW_BIN" continuity drafts discard drop.txt --runtime codex
"$LIONCLAW_BIN" continuity drafts ls --runtime codex
"$LIONCLAW_BIN" continuity status
find "$LIONCLAW_HOME/workspaces" -maxdepth 6 -type f -print | sort
```

Expected:

- promoted draft appears as a continuity artifact
- discarded draft is gone
- `recent_artifacts` increments

## Phase 4: Local Session Reuse

From project A:

```bash
"$LIONCLAW_BIN" run --continue-last-session codex
```

Prompt:

```text
What exact token did you reply with in the most recent prior assistant turn? Reply with exactly that token and nothing else.
```

Expected:

- recent history shows the reset session
- answer is `RESET_OK`
- Codex resumes the same runtime thread for that project/session

Exit with `/exit`.

## Phase 5: Managed Daemon And Runtime Image Selection

With the optional broken runtime still configured:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" service up --runtime codex
"$LIONCLAW_BIN" service status
"$LIONCLAW_BIN" service logs

bind=$(sed -n 's/^bind = "\(.*\)"$/\1/p' "$LIONCLAW_HOME/config/lionclaw.toml" | head -n1)
daemon_info=""
for _ in $(seq 1 30); do
  if daemon_info=$(curl -fsS "http://$bind/v0/daemon/info" 2>/dev/null); then
    break
  fi
  sleep 1
done
printf '%s\n' "$daemon_info"
test -n "$daemon_info"
```

Expected:

- service starts or reconciles cleanly
- the broken unselected runtime does not block `service up --runtime codex`
- daemon info reports stable `home_id`, `project_scope`, and
  `config_fingerprint`

Negative selected-runtime check:

```bash
"$LIONCLAW_BIN" run broken-codex
```

Expected:

- fails before REPL startup with a clear missing image error for
  `broken-codex`

## Phase 6: Stale Config Restart And CODEX_HOME Persistence

Record current daemon identity, then make a harmless config change while the
managed daemon is running:

```bash
before_info=$(curl -sS "http://$bind/v0/daemon/info")
printf '%s\n' "$before_info"

"$LIONCLAW_BIN" runtime add codex-same-image \
  --kind codex \
  --bin codex \
  --image lionclaw-runtime:v1
"$LIONCLAW_BIN" service up --runtime codex

after_info=""
for _ in $(seq 1 30); do
  if after_info=$(curl -fsS "http://$bind/v0/daemon/info" 2>/dev/null); then
    break
  fi
  sleep 1
done
printf '%s\n' "$after_info"
test -n "$after_info"
grep -E '^(CODEX_HOME|LIONCLAW_DAEMON_CONFIG_FINGERPRINT)=' \
  "$LIONCLAW_HOME/services/env/lionclawd.env" || true
```

Expected:

- daemon remains reachable after `service up`
- `config_fingerprint` changes after the config update
- same `home_id` and `project_scope` are preserved
- if `CODEX_HOME` was exported at the start of the pass, the generated daemon
  env file contains that exact absolute path
- if `CODEX_HOME` was not exported, the generated daemon env file does not add
  a synthetic `CODEX_HOME`
- the added unselected `codex-same-image` runtime does not affect subsequent
  selected `codex` launches

## Phase 7: Terminal Channel

Attach from project A:

```bash
"$LIONCLAW_BIN" channel attach terminal --runtime codex --peer qa-terminal
```

In the TUI, type a message without pressing Tab first:

```text
hello terminal qa
```

Approve the displayed pairing code from another shell:

```bash
"$LIONCLAW_BIN" channel pairing list --channel-id terminal
"$LIONCLAW_BIN" channel pairing approve terminal qa-terminal <code> --trust-tier main
```

Back in the TUI, send:

```text
Reply with exactly TERMINAL_LIVE_OK and nothing else.
```

Exit and reattach:

```bash
"$LIONCLAW_BIN" channel attach terminal --runtime codex --peer qa-terminal
```

Then send:

```text
/retry
```

Expected:

- initial text creates pending pairing immediately without a focus workaround
- approval updates the running TUI
- first approved turn returns `TERMINAL_LIVE_OK`
- reattach restores the transcript
- `/retry` returns `TERMINAL_LIVE_OK` again

API-backed check:

```bash
curl -sS "http://$bind/v0/sessions/latest?channel_id=terminal&peer_id=qa-terminal&history_policy=interactive"
```

Expected:

- completed terminal turns have `runtime_id=codex`
- retry turn has `kind=retry`

## Phase 8: Scheduler Job And Channel Delivery

```bash
job_output=$("$LIONCLAW_BIN" job add live-qa-job \
  --runtime codex \
  --schedule "every 1d" \
  --prompt "Reply with exactly JOB_LIVE_OK and nothing else." \
  --deliver-channel terminal \
  --deliver-peer qa-terminal)
printf '%s\n' "$job_output"
job_id=$(printf '%s\n' "$job_output" | sed -n 's/^created job \([^ ]*\).*/\1/p')

"$LIONCLAW_BIN" job show "$job_id"
"$LIONCLAW_BIN" job run "$job_id"
"$LIONCLAW_BIN" job runs "$job_id"
"$LIONCLAW_BIN" continuity status
find "$LIONCLAW_HOME/workspaces/main/continuity/artifacts" -maxdepth 5 -type f -print -exec sed -n '1,80p' {} \;
```

Check channel delivery without relying on the interactive session:

```bash
curl -sS -X POST "http://$bind/v0/channels/stream/pull" \
  -H "Content-Type: application/json" \
  -d '{"channel_id":"terminal","consumer_id":"manual-delivery-check","start_mode":"resume","limit":50,"wait_ms":0}'
```

Expected:

- job run completes
- artifact body contains `JOB_LIVE_OK`
- channel stream contains an outbound `JOB_LIVE_OK`
- latest interactive terminal session is not replaced by the scheduler session

Cleanup the job:

```bash
"$LIONCLAW_BIN" job pause "$job_id"
"$LIONCLAW_BIN" job resume "$job_id"
"$LIONCLAW_BIN" job rm "$job_id"
"$LIONCLAW_BIN" job ls
```

## Phase 9: Runtime Secrets

First change the fresh home to use a secrets-enabled preset. Edit
`$LIONCLAW_HOME/config/lionclaw.toml` so it contains:

```toml
[defaults]
runtime = "codex"
preset = "secrets-on"

[presets.secrets-on]
workspace-access = "read-write"
network-mode = "on"
mount-runtime-secrets = true
```

Reconcile the daemon and record the new fingerprint:

```bash
"$LIONCLAW_BIN" service up --runtime codex
daemon_info=""
for _ in $(seq 1 30); do
  if daemon_info=$(curl -fsS "http://$bind/v0/daemon/info" 2>/dev/null); then
    break
  fi
  sleep 1
done
printf '%s\n' "$daemon_info"
test -n "$daemon_info"
```

Create dummy secrets after the daemon is already running:

```bash
mkdir -p "$LIONCLAW_HOME/config"
printf 'DUMMY_RUNTIME_SECRET=manual-live-only\n' > "$LIONCLAW_HOME/config/runtime-secrets.env"
chmod 0644 "$LIONCLAW_HOME/config/runtime-secrets.env"
stat "$LIONCLAW_HOME/config/runtime-secrets.env"
secrets_before=$(mktemp)
podman secret ls --format '{{.Name}}' | sort | tee "$secrets_before"
```

Run a daemon-backed turn through the approved terminal peer:

```bash
curl -sS -X POST "http://$bind/v0/channels/inbound" \
  -H "Content-Type: application/json" \
  -d '{"channel_id":"terminal","peer_id":"qa-terminal","text":"Check whether a LionClaw runtime secret file exists under /run/secrets with the expected prefix. Do not print contents. Reply only yes or no.","external_message_id":"manual-secret-check-1","runtime_id":"codex"}'
```

Poll:

```bash
for _ in $(seq 1 90); do
  latest=$(curl -sS "http://$bind/v0/sessions/latest?channel_id=terminal&peer_id=qa-terminal&history_policy=interactive")
  printf '%s\n' "$latest"
  if printf '%s' "$latest" | grep -q '"assistant_text":"yes"'; then
    break
  fi
  sleep 1
done

# Wait until the turn is fully complete before judging cleanup.
for _ in $(seq 1 30); do
  latest=$(curl -sS "http://$bind/v0/sessions/latest?channel_id=terminal&peer_id=qa-terminal&history_policy=interactive")
  printf '%s\n' "$latest"
  if printf '%s' "$latest" | grep -Eq '"assistant_text":"yes".*"finished_at":"[^"]+"'; then
    break
  fi
  sleep 1
done

stat "$LIONCLAW_HOME/config/runtime-secrets.env"
secrets_after=$(mktemp)
podman secret ls --format '{{.Name}}' | sort | tee "$secrets_after"
comm -13 "$secrets_before" "$secrets_after" | grep '^lionclaw-runtime-secrets-' || true
```

Expected:

- answer is `yes`
- source file is hardened to owner-only permissions
- after the turn is completed, the `comm` check prints no new
  `lionclaw-runtime-secrets-*` names
- secret contents never appear in logs or assistant output

If an old `lionclaw-runtime-secrets-*` already existed before this phase, do not
count it as a current regression unless a new one appears.

## Phase 10: Same-Home Project Isolation

Keep the project-A daemon running and switch to project B:

```bash
cd "$PROJ_B"
"$LIONCLAW_BIN" service up --runtime codex
"$LIONCLAW_BIN" channel attach terminal --runtime codex --peer qa-terminal
```

Expected:

- both refuse to reuse the project-A daemon
- error states that the bind is served by the same home for a different project

Then test local project-B session scope:

```bash
"$LIONCLAW_BIN" run --continue-last-session codex
```

Prompt:

```text
Read project-marker.txt and reply with exactly its contents. Do not explain.
```

Expected:

- no project-A recent history appears
- answer content is `project-b`
- no project-A local session is reopened

Exit with `/exit`.

## Phase 11: Cleanup

From project A:

```bash
cd "$PROJ_A"
"$LIONCLAW_BIN" service down
"$LIONCLAW_BIN" service status
```

Expected:

- daemon is inactive/dead
- no live containers remain for the completed turns
- no new `lionclaw-runtime-secrets-*` Podman secret remains

Check:

```bash
podman secret ls --format '{{.ID}} {{.Name}}' | sort
```

## Optional Fault-Injection Checks

Run these when touching timeout, recovery, or session-history code:

- induce a partial answer timeout and verify `/continue` is offered only when
  assistant text exists
- induce a reasoning-only failure and verify `/continue` is not offered
- verify `/retry` rebuilds from LionClaw canonical history instead of runtime
  hidden state
- restart the daemon during a running terminal turn and verify reattach resumes
  from the last durable stream checkpoint

## Failure Bundle

For each failure, record:

```text
commit:
rustc/clippy version:
podman version:
runtime id:
command path: run | attach | service | job | secrets
LIONCLAW_HOME:
project root:
bind addr:
daemon info JSON:
exact command:
exact prompt:
expected result:
actual result:
can reproduce from fresh home? yes/no
can reproduce without broken unselected runtime? yes/no
can reproduce on another runtime? yes/no
```

Attach:

```bash
"$LIONCLAW_BIN" service status
"$LIONCLAW_BIN" service logs
curl -sS "http://$bind/v0/daemon/info"
find "$LIONCLAW_HOME/runtime" -maxdepth 8 -print | sort
find "$LIONCLAW_HOME/workspaces" -maxdepth 8 -print | sort
ls -l "$LIONCLAW_HOME/config"
stat "$LIONCLAW_HOME/config/runtime-secrets.env" 2>/dev/null || true
podman secret ls --format '{{.ID}} {{.Name}}' | sort
```

Do not attach:

- provider tokens
- secret contents
- full `runtime-secrets.env`
