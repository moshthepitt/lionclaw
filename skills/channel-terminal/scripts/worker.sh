#!/usr/bin/env bash
set -euo pipefail

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "missing required command: $name" >&2
    exit 1
  fi
}

require_command bash
require_command curl
require_command jq

default_peer_id() {
  local candidate

  candidate="${USER:-}"
  if [[ -n "${candidate// }" ]]; then
    printf '%s\n' "$candidate"
    return
  fi

  candidate="${USERNAME:-}"
  if [[ -n "${candidate// }" ]]; then
    printf '%s\n' "$candidate"
    return
  fi

  printf 'local-user\n'
}

LIONCLAW_HOME="${LIONCLAW_HOME:-$HOME/.lionclaw}"
LIONCLAW_BASE_URL="${LIONCLAW_BASE_URL:-http://127.0.0.1:8979}"
LIONCLAW_CHANNEL_ID="${LIONCLAW_CHANNEL_ID:-terminal}"
LIONCLAW_PEER_ID="${LIONCLAW_PEER_ID:-$(default_peer_id)}"
LIONCLAW_RUNTIME_ID="${LIONCLAW_RUNTIME_ID:-}"
LIONCLAW_OUTBOX_LIMIT="${LIONCLAW_OUTBOX_LIMIT:-20}"
LIONCLAW_OUTBOX_POLL_SECS="${LIONCLAW_OUTBOX_POLL_SECS:-1}"

WORKER_INSTANCE_ID="terminal-${LIONCLAW_CHANNEL_ID}-$$"
inbound_sequence=0
outbox_pid=""

lionclaw_get() {
  local path="$1"
  shift
  curl -fsS --get "$LIONCLAW_BASE_URL$path" "$@"
}

lionclaw_post() {
  local path="$1"
  local body="$2"
  curl -fsS -X POST "$LIONCLAW_BASE_URL$path" \
    -H 'content-type: application/json' \
    -d "$body"
}

fetch_peer_json() {
  local peers_json
  peers_json="$(lionclaw_get '/v0/channels/peers' \
    --data-urlencode "channel_id=$LIONCLAW_CHANNEL_ID")" || return 1

  jq -c \
    --arg peer_id "$LIONCLAW_PEER_ID" \
    '.peers[]? | select(.peer_id == $peer_id)' <<<"$peers_json"
}

print_pairing_status() {
  local peer_json
  if ! peer_json="$(fetch_peer_json)"; then
    echo "warning: failed to query channel peer state" >&2
    return
  fi

  if [[ -z "$peer_json" ]]; then
    echo "status: peer '$LIONCLAW_PEER_ID' is not known yet on channel '$LIONCLAW_CHANNEL_ID'"
    echo "status: send a message to create pairing state"
    return
  fi

  local status
  status="$(jq -r '.status' <<<"$peer_json")"
  case "$status" in
    approved)
      local trust_tier
      trust_tier="$(jq -r '.trust_tier' <<<"$peer_json")"
      echo "status: peer '$LIONCLAW_PEER_ID' is approved (trust_tier=$trust_tier)"
      ;;
    pending)
      local pairing_code
      pairing_code="$(jq -r '.pairing_code' <<<"$peer_json")"
      echo "status: peer '$LIONCLAW_PEER_ID' is pending approval"
      echo "approve: lionclaw channel pairing approve $LIONCLAW_CHANNEL_ID $LIONCLAW_PEER_ID $pairing_code --trust-tier main"
      ;;
    blocked)
      echo "status: peer '$LIONCLAW_PEER_ID' is blocked on channel '$LIONCLAW_CHANNEL_ID'"
      ;;
    *)
      echo "status: peer '$LIONCLAW_PEER_ID' is in unexpected state '$status'" >&2
      ;;
  esac
}

send_inbound() {
  local text="$1"
  local external_message_id
  local payload

  external_message_id="terminal-inbound:$WORKER_INSTANCE_ID:$inbound_sequence"
  payload="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg peer_id "$LIONCLAW_PEER_ID" \
    --arg text "$text" \
    --arg external_message_id "$external_message_id" \
    --arg runtime_id "$LIONCLAW_RUNTIME_ID" \
    '{
      channel_id: $channel_id,
      peer_id: $peer_id,
      text: $text,
      external_message_id: $external_message_id
    } + (if $runtime_id == "" then {} else { runtime_id: $runtime_id } end)'
  )"

  lionclaw_post '/v0/channels/inbound' "$payload" >/dev/null
  inbound_sequence="$((inbound_sequence + 1))"
}

pull_outbox() {
  local request
  request="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --argjson limit "$LIONCLAW_OUTBOX_LIMIT" \
    '{channel_id: $channel_id, limit: $limit}'
  )"

  lionclaw_post '/v0/channels/outbox/pull' "$request"
}

ack_outbox() {
  local message_id="$1"
  local external_message_id="$2"
  local request

  request="$(jq -nc \
    --arg message_id "$message_id" \
    --arg external_message_id "$external_message_id" \
    '{message_id: $message_id, external_message_id: $external_message_id}'
  )"

  lionclaw_post '/v0/channels/outbox/ack' "$request" >/dev/null
}

print_assistant_message() {
  local content="$1"
  local line

  while IFS= read -r line || [[ -n "$line" ]]; do
    printf 'lionclaw> %s\n' "$line"
  done <<<"$content"
}

flush_outbox_once() {
  local outbox_json
  outbox_json="$(pull_outbox)" || {
    echo "warning: failed to pull channel outbox" >&2
    return
  }

  if ! jq -e '.messages | type == "array"' >/dev/null <<<"$outbox_json"; then
    echo "warning: invalid outbox response: $(jq -c '.' <<<"$outbox_json")" >&2
    return
  fi

  while IFS= read -r message; do
    local message_id
    local peer_id
    local content
    local external_message_id

    message_id="$(jq -r '.message_id' <<<"$message")"
    peer_id="$(jq -r '.peer_id' <<<"$message")"
    content="$(jq -r '.content' <<<"$message")"

    if [[ "$peer_id" != "$LIONCLAW_PEER_ID" ]]; then
      echo "warning: channel '$LIONCLAW_CHANNEL_ID' has pending output for unexpected peer '$peer_id'; this worker only serves '$LIONCLAW_PEER_ID'" >&2
      continue
    fi

    print_assistant_message "$content"
    external_message_id="terminal-outbound:$WORKER_INSTANCE_ID:$message_id"
    ack_outbox "$message_id" "$external_message_id"
  done < <(jq -c '.messages[]?' <<<"$outbox_json")
}

outbox_loop() {
  while true; do
    flush_outbox_once
    sleep "$LIONCLAW_OUTBOX_POLL_SECS"
  done
}

cleanup() {
  if [[ -n "$outbox_pid" ]]; then
    kill "$outbox_pid" >/dev/null 2>&1 || true
    wait "$outbox_pid" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

echo "LionClaw terminal channel worker"
echo "channel: $LIONCLAW_CHANNEL_ID"
echo "peer: $LIONCLAW_PEER_ID"
echo "commands: /status, /quit"
print_pairing_status

outbox_loop &
outbox_pid="$!"

while true; do
  IFS= read -r -p "you> " line || {
    echo
    break
  }

  case "$line" in
    "")
      continue
      ;;
    /quit|/exit)
      break
      ;;
    /status)
      print_pairing_status
      continue
      ;;
  esac

  if ! send_inbound "$line"; then
    echo "warning: failed to submit inbound message" >&2
    continue
  fi
done
