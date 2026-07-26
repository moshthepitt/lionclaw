#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUN_ROOT="${1:-$REPO_ROOT/.tmp/frontierswe-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$RUN_ROOT"
RUN_ROOT="$(cd "$RUN_ROOT" && pwd)"
SOURCE_ROOT="$RUN_ROOT/source"
JOBS_ROOT="$RUN_ROOT/jobs"
JOB_NAME="slice10a-dependent-type-checker"
JOB_DIR="$JOBS_ROOT/$JOB_NAME"
LIONCLAW_BIN="${LIONCLAW_BIN:-$REPO_ROOT/target/debug/lionclaw}"
CODEX_BIN="${CODEX_BIN:-codex}"
MISSION_TYPE="$REPO_ROOT/mission-types/frontierswe-dependent-type-checker"
LEAD_PROMPT="$SCRIPT_DIR/lead-prompt.md"
export LIONCLAW_HOME="${LIONCLAW_HOME:-$RUN_ROOT/lionclaw-home}"
DEFAULT_FRONTIERSWE_MODEL_SOURCE="not_configured"
if [[ -n "${FRONTIERSWE_MODEL:-}" ]]; then
  DEFAULT_FRONTIERSWE_MODEL="$FRONTIERSWE_MODEL"
  DEFAULT_FRONTIERSWE_MODEL_SOURCE="FRONTIERSWE_MODEL"
elif [[ -n "${CODEX_MODEL:-}" ]]; then
  DEFAULT_FRONTIERSWE_MODEL="$CODEX_MODEL"
  DEFAULT_FRONTIERSWE_MODEL_SOURCE="CODEX_MODEL"
else
  DEFAULT_FRONTIERSWE_MODEL="$(python3 "$SCRIPT_DIR/runtime_config.py" resolve-model)"
  if [[ -n "$DEFAULT_FRONTIERSWE_MODEL" ]]; then
    DEFAULT_FRONTIERSWE_MODEL_SOURCE="codex_config"
  fi
fi
FRONTIERSWE_LEAD_MODEL="${FRONTIERSWE_LEAD_MODEL:-$DEFAULT_FRONTIERSWE_MODEL}"
FRONTIERSWE_ROLE_MODEL="${FRONTIERSWE_ROLE_MODEL:-}"

if [[ ! -x "$LIONCLAW_BIN" ]]; then
  echo "LionClaw binary is missing: $LIONCLAW_BIN" >&2
  echo "build it with: cargo build -p lionclaw" >&2
  exit 2
fi
harbor --version | grep -Fx '0.2.0'
"$LIONCLAW_BIN" install
if [[ -n "$FRONTIERSWE_ROLE_MODEL" ]]; then
  python3 "$SCRIPT_DIR/runtime_config.py" write-codex-profile \
    --lionclaw-home "$LIONCLAW_HOME" \
    --model "$FRONTIERSWE_ROLE_MODEL" >/dev/null
fi
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
AGENT_KWARGS=(
  --ak "lionclaw_bin=$LIONCLAW_BIN"
  --ak "mission_type=$MISSION_TYPE"
  --ak "image=$IMAGE_REFERENCE"
  --ak "supervisor_module=lionclaw_frontierswe.supervisor"
  --ak "lead_prompt=$LEAD_PROMPT"
  --ak "codex_bin=$CODEX_BIN"
  --ak "resolved_model=$DEFAULT_FRONTIERSWE_MODEL"
  --ak "resolved_model_source=$DEFAULT_FRONTIERSWE_MODEL_SOURCE"
  --ak "role_model_request=$FRONTIERSWE_ROLE_MODEL"
)
if [[ -n "$FRONTIERSWE_LEAD_MODEL" ]]; then
  AGENT_KWARGS+=(--ak "lead_model=$FRONTIERSWE_LEAD_MODEL")
fi
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
  "${AGENT_KWARGS[@]}"
HARBOR_EXIT=$?
set -e

python3 "$SCRIPT_DIR/finalize_report.py" \
  --job-dir "$JOB_DIR" \
  --scorer "$SOURCE_ROOT/upstream/scripts/score_from_reward.py"
exit "$HARBOR_EXIT"
