#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  install-channel-skill.sh [options]

Options:
  --base-url URL         LionClaw base URL (default: http://127.0.0.1:8979)
  --channel-id ID        Channel ID to bind (default: telegram)
  --skill-source PATH    Skill source path (default: skills/channel-telegram)
  --skill-alias ALIAS    Skill alias to register and bind (default: channel id)
  --skill-ref REF        Skill reference (default: local)
  --runtime-id ID        Optional runtime override exported to worker env (default: omitted)
  --lionclaw-bin PATH    LionClaw CLI to use (default: lionclaw)
  --start-worker         Start the canonical snapshot's worker after install+bind
  -h, --help             Show help

Environment pass-through for worker:
  LIONCLAW_BASE_URL, LIONCLAW_CHANNEL_ID, LIONCLAW_RUNTIME_ID
  (Telegram worker also needs TELEGRAM_BOT_TOKEN)
USAGE
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

BASE_URL="${BASE_URL:-http://127.0.0.1:8979}"
CHANNEL_ID="${CHANNEL_ID:-telegram}"
SKILL_SOURCE="${SKILL_SOURCE:-skills/channel-telegram}"
SKILL_ALIAS="${SKILL_ALIAS:-}"
SKILL_REF="${SKILL_REF:-local}"
RUNTIME_ID="${RUNTIME_ID:-}"
LIONCLAW_BIN="${LIONCLAW_BIN:-lionclaw}"
START_WORKER=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="$2"; shift 2 ;;
    --channel-id) CHANNEL_ID="$2"; shift 2 ;;
    --skill-source) SKILL_SOURCE="$2"; shift 2 ;;
    --skill-alias) SKILL_ALIAS="$2"; shift 2 ;;
    --skill-ref) SKILL_REF="$2"; shift 2 ;;
    --runtime-id) RUNTIME_ID="$2"; shift 2 ;;
    --lionclaw-bin) LIONCLAW_BIN="$2"; shift 2 ;;
    --start-worker) START_WORKER=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

resolve_snapshot_worker() {
  "$LIONCLAW_BIN" skill worker-path "$SKILL_ALIAS"
}

require_cmd "$LIONCLAW_BIN"

SKILL_MD_PATH="$SKILL_SOURCE/SKILL.md"
if [[ ! -f "$SKILL_MD_PATH" ]]; then
  echo "missing SKILL.md: $SKILL_MD_PATH" >&2
  exit 1
fi

if [[ -z "$SKILL_ALIAS" ]]; then
  SKILL_ALIAS="$CHANNEL_ID"
fi

"$LIONCLAW_BIN" skill add "$SKILL_SOURCE" --alias "$SKILL_ALIAS" --reference "$SKILL_REF" >/dev/null
"$LIONCLAW_BIN" channel add "$CHANNEL_ID" --skill "$SKILL_ALIAS" >/dev/null
"$LIONCLAW_BIN" apply >/dev/null

echo "Registered skill: $SKILL_ALIAS"
echo "Bound channel: $CHANNEL_ID -> $SKILL_ALIAS"
echo
echo "Pairing check command:"
echo "  $LIONCLAW_BIN channel pairing list --channel-id $CHANNEL_ID"

if [[ -z "$RUNTIME_ID" ]]; then
  echo
  echo "Runtime selection:"
  echo "  use '$LIONCLAW_BIN service up --runtime <id>' for the normal managed flow"
fi

if [[ "$START_WORKER" == true ]]; then
  require_cmd curl
  curl -fsS "$BASE_URL/health" >/dev/null
  WORKER="$(resolve_snapshot_worker)"
  if [[ ! -x "$WORKER" ]]; then
    echo "worker script is not executable: $WORKER" >&2
    exit 1
  fi

  export LIONCLAW_BASE_URL="$BASE_URL"
  export LIONCLAW_CHANNEL_ID="$CHANNEL_ID"
  if [[ -n "$RUNTIME_ID" ]]; then
    export LIONCLAW_RUNTIME_ID="$RUNTIME_ID"
  fi

  echo
  echo "Starting worker: $WORKER"
  exec "$WORKER"
fi
