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
  --skill-ref REF        Skill reference (default: local)
  --runtime-id ID        Runtime ID to set in bind config (default: codex, use "" to omit)
  --start-worker         Start <skill-source>/scripts/worker.sh after install+bind
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

post_json() {
  local url="$1"
  local body="$2"
  curl -fsS -X POST "$url" \
    -H 'content-type: application/json' \
    -d "$body"
}

BASE_URL="${BASE_URL:-http://127.0.0.1:8979}"
CHANNEL_ID="${CHANNEL_ID:-telegram}"
SKILL_SOURCE="${SKILL_SOURCE:-skills/channel-telegram}"
SKILL_REF="${SKILL_REF:-local}"
RUNTIME_ID="${RUNTIME_ID:-codex}"
START_WORKER=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="$2"; shift 2 ;;
    --channel-id) CHANNEL_ID="$2"; shift 2 ;;
    --skill-source) SKILL_SOURCE="$2"; shift 2 ;;
    --skill-ref) SKILL_REF="$2"; shift 2 ;;
    --runtime-id) RUNTIME_ID="$2"; shift 2 ;;
    --start-worker) START_WORKER=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd curl
require_cmd jq

SKILL_MD_PATH="$SKILL_SOURCE/SKILL.md"
if [[ ! -f "$SKILL_MD_PATH" ]]; then
  echo "missing SKILL.md: $SKILL_MD_PATH" >&2
  exit 1
fi

curl -fsS "$BASE_URL/health" >/dev/null

SKILL_MD_CONTENT="$(cat "$SKILL_MD_PATH")"

INSTALL_BODY="$(jq -nc \
  --arg source "$SKILL_SOURCE" \
  --arg reference "$SKILL_REF" \
  --arg skill_md "$SKILL_MD_CONTENT" \
  '{source:$source, reference:$reference, skill_md:$skill_md}')"

INSTALL_RESP="$(post_json "$BASE_URL/v0/skills/install" "$INSTALL_BODY")"
SKILL_ID="$(jq -r '.skill_id // empty' <<<"$INSTALL_RESP")"
if [[ -z "$SKILL_ID" ]]; then
  echo "failed to install skill: $INSTALL_RESP" >&2
  exit 1
fi

ENABLE_BODY="$(jq -nc --arg skill_id "$SKILL_ID" '{skill_id:$skill_id}')"
post_json "$BASE_URL/v0/skills/enable" "$ENABLE_BODY" >/dev/null

BIND_BODY="$(jq -nc \
  --arg channel_id "$CHANNEL_ID" \
  --arg skill_id "$SKILL_ID" \
  --arg runtime_id "$RUNTIME_ID" \
  '{
    channel_id:$channel_id,
    skill_id:$skill_id,
    enabled:true,
    config:(if $runtime_id == "" then {} else {runtime_id:$runtime_id} end)
  }')"

BIND_RESP="$(post_json "$BASE_URL/v0/channels/bind" "$BIND_BODY")"

echo "Installed skill: $SKILL_ID"
echo "$BIND_RESP" | jq
echo
echo "Pairing check command:"
echo "  curl -sS \"$BASE_URL/v0/channels/peers?channel_id=$CHANNEL_ID\" | jq"

if [[ "$START_WORKER" == true ]]; then
  WORKER="$SKILL_SOURCE/scripts/worker.sh"
  if [[ ! -x "$WORKER" ]]; then
    chmod +x "$WORKER" 2>/dev/null || true
  fi
  if [[ ! -x "$WORKER" ]]; then
    echo "worker script is not executable: $WORKER" >&2
    exit 1
  fi

  export LIONCLAW_BASE_URL="$BASE_URL"
  export LIONCLAW_CHANNEL_ID="$CHANNEL_ID"
  export LIONCLAW_RUNTIME_ID="$RUNTIME_ID"

  echo
  echo "Starting worker: $WORKER"
  exec "$WORKER"
fi
