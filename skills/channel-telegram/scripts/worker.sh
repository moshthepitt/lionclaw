#!/usr/bin/env bash
set -euo pipefail

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "missing required command: $name" >&2
    exit 1
  fi
}

require_command curl
require_command jq

: "${TELEGRAM_BOT_TOKEN:?TELEGRAM_BOT_TOKEN is required}"

LIONCLAW_HOME="${LIONCLAW_HOME:-$HOME/.lionclaw}"
LIONCLAW_BASE_URL="${LIONCLAW_BASE_URL:-http://127.0.0.1:8979}"
LIONCLAW_CHANNEL_ID="${LIONCLAW_CHANNEL_ID:-telegram}"
LIONCLAW_RUNTIME_ID="${LIONCLAW_RUNTIME_ID:-}"
LIONCLAW_OUTBOX_LIMIT="${LIONCLAW_OUTBOX_LIMIT:-20}"
TELEGRAM_POLL_TIMEOUT_SECS="${TELEGRAM_POLL_TIMEOUT_SECS:-25}"
TELEGRAM_LOOP_DELAY_SECS="${TELEGRAM_LOOP_DELAY_SECS:-1}"
LIONCLAW_CHANNEL_RUNTIME_DIR="${LIONCLAW_CHANNEL_RUNTIME_DIR:-$LIONCLAW_HOME/runtime/channels/$LIONCLAW_CHANNEL_ID}"
mkdir -p "$LIONCLAW_CHANNEL_RUNTIME_DIR"
TELEGRAM_OFFSET_FILE="${TELEGRAM_OFFSET_FILE:-$LIONCLAW_CHANNEL_RUNTIME_DIR/telegram.offset}"

telegram_api() {
  local method="$1"
  printf 'https://api.telegram.org/bot%s/%s' "$TELEGRAM_BOT_TOKEN" "$method"
}

lionclaw_post() {
  local path="$1"
  local body="$2"
  curl -fsS -X POST "$LIONCLAW_BASE_URL$path" \
    -H 'content-type: application/json' \
    -d "$body"
}

offset=0
if [[ -f "$TELEGRAM_OFFSET_FILE" ]]; then
  raw_offset="$(cat "$TELEGRAM_OFFSET_FILE")"
  if [[ "$raw_offset" =~ ^[0-9]+$ ]]; then
    offset="$raw_offset"
  fi
fi

save_offset() {
  printf '%s' "$offset" > "$TELEGRAM_OFFSET_FILE"
}

send_inbound() {
  local update_id="$1"
  local chat_id="$2"
  local text="$3"

  local payload
  payload="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg peer_id "$chat_id" \
    --arg text "$text" \
    --argjson update_id "$update_id" \
    --arg external_message_id "telegram-update:$update_id" \
    --arg runtime_id "$LIONCLAW_RUNTIME_ID" \
    '{
      channel_id: $channel_id,
      peer_id: $peer_id,
      text: $text,
      update_id: $update_id,
      external_message_id: $external_message_id
    } + (if $runtime_id == "" then {} else { runtime_id: $runtime_id } end)'
  )"

  lionclaw_post '/v0/channels/inbound' "$payload" >/dev/null
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

process_updates() {
  local updates_json
  if ! updates_json="$(curl -fsS --get "$(telegram_api getUpdates)" \
    --data-urlencode "timeout=$TELEGRAM_POLL_TIMEOUT_SECS" \
    --data-urlencode "offset=$offset")"; then
    echo "telegram getUpdates request failed" >&2
    return
  fi

  if ! jq -e '.ok == true' >/dev/null <<<"$updates_json"; then
    echo "telegram getUpdates failed: $(jq -c '.' <<<"$updates_json")" >&2
    return
  fi

  while IFS= read -r update; do
    local update_id
    update_id="$(jq -r '.update_id // 0' <<<"$update")"

    local chat_id
    chat_id="$(jq -r '(.message.chat.id // .edited_message.chat.id // .channel_post.chat.id // empty) | tostring' <<<"$update")"

    local text
    text="$(jq -r '.message.text // .edited_message.text // .channel_post.text // empty' <<<"$update")"

    if [[ -n "$chat_id" && -n "$text" ]]; then
      if ! send_inbound "$update_id" "$chat_id" "$text"; then
        echo "lionclaw inbound submit failed for update_id=$update_id" >&2
        return
      fi
    fi

    offset="$((update_id + 1))"
    save_offset
  done < <(jq -c '.result[]?' <<<"$updates_json")
}

flush_outbox() {
  local outbox
  if ! outbox="$(pull_outbox)"; then
    echo "lionclaw outbox pull request failed" >&2
    return
  fi

  if ! jq -e '.messages | type == "array"' >/dev/null <<<"$outbox"; then
    echo "lionclaw outbox pull failed: $(jq -c '.' <<<"$outbox")" >&2
    return
  fi

  while IFS= read -r message; do
    local message_id
    message_id="$(jq -r '.message_id' <<<"$message")"

    local peer_id
    peer_id="$(jq -r '.peer_id' <<<"$message")"

    local content
    content="$(jq -r '.content' <<<"$message")"

    local send_result
    if ! send_result="$(curl -fsS -X POST "$(telegram_api sendMessage)" \
      --data-urlencode "chat_id=$peer_id" \
      --data-urlencode "text=$content")"; then
      echo "telegram sendMessage request failed for message_id=$message_id" >&2
      continue
    fi

    if ! jq -e '.ok == true' >/dev/null <<<"$send_result"; then
      echo "telegram sendMessage failed: $(jq -c '.' <<<"$send_result")" >&2
      continue
    fi

    local telegram_message_id
    telegram_message_id="$(jq -r '.result.message_id | tostring' <<<"$send_result")"
    ack_outbox "$message_id" "telegram-message:$peer_id:$telegram_message_id"
  done < <(jq -c '.messages[]?' <<<"$outbox")
}

while true; do
  process_updates
  flush_outbox
  sleep "$TELEGRAM_LOOP_DELAY_SECS"
done
