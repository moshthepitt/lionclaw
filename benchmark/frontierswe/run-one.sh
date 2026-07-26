#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_ROOT="${1:-$REPO_ROOT/.tmp/frontierswe-$(date -u +%Y%m%dT%H%M%SZ)}"
SOURCE_ROOT="$RUN_ROOT/source"
JOBS_ROOT="$RUN_ROOT/jobs"
JOB_NAME="slice10a-dependent-type-checker"
JOB_DIR="$JOBS_ROOT/$JOB_NAME"
LIONCLAW_BIN="${LIONCLAW_BIN:-$REPO_ROOT/target/debug/lionclaw}"
CODEX_BIN="${CODEX_BIN:-codex}"
MISSION_TYPE="$REPO_ROOT/mission-types/frontierswe-dependent-type-checker"
LEAD_PROMPT="$SCRIPT_DIR/lead-prompt.md"
export LIONCLAW_HOME="${LIONCLAW_HOME:-$RUN_ROOT/lionclaw-home}"

if [[ ! -x "$LIONCLAW_BIN" ]]; then
  echo "LionClaw binary is missing: $LIONCLAW_BIN" >&2
  echo "build it with: cargo build -p lionclaw" >&2
  exit 2
fi
harbor --version | grep -Fx '0.2.0'
"$LIONCLAW_BIN" install
"$LIONCLAW_BIN" doctor

python3 "$SCRIPT_DIR/fetch_task.py" --output "$SOURCE_ROOT"
IMAGE_REFERENCE="$(
  python3 - "$SCRIPT_DIR/bundle.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    print(tomllib.load(handle)["image"]["reference"])
PY
)"
podman image exists "$IMAGE_REFERENCE"

export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"
set +e
harbor run \
  --yes \
  --job-name "$JOB_NAME" \
  --jobs-dir "$JOBS_ROOT" \
  --n-attempts 1 \
  --n-concurrent 1 \
  --max-retries 0 \
  --path "$SOURCE_ROOT/task" \
  --agent-import-path lionclaw_frontierswe.agent:LionClawAgent \
  --environment-import-path lionclaw_frontierswe.podman_environment:PodmanEnvironment \
  --ek "allow_unenforced_resources=true" \
  --ak "lionclaw_bin=$LIONCLAW_BIN" \
  --ak "mission_type=$MISSION_TYPE" \
  --ak "image=$IMAGE_REFERENCE" \
  --ak "supervisor_module=lionclaw_frontierswe.supervisor" \
  --ak "lead_prompt=$LEAD_PROMPT" \
  --ak "codex_bin=$CODEX_BIN"
HARBOR_EXIT=$?
set -e

python3 "$SCRIPT_DIR/finalize_report.py" \
  --job-dir "$JOB_DIR" \
  --scorer "$SOURCE_ROOT/upstream/scripts/score_from_reward.py"
exit "$HARBOR_EXIT"
