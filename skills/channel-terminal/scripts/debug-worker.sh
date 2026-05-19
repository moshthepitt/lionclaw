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

process_nonce() {
  if [[ -r /proc/sys/kernel/random/uuid ]]; then
    local uuid
    uuid="$(</proc/sys/kernel/random/uuid)"
    printf '%s\n' "${uuid//-/}"
    return
  fi

  od -An -N16 -tx1 /dev/urandom | tr -d ' \n'
  printf '\n'
}

LIONCLAW_HOME="${LIONCLAW_HOME:-$HOME/.lionclaw}"
LIONCLAW_BASE_URL="${LIONCLAW_BASE_URL:-http://127.0.0.1:8979}"
LIONCLAW_CHANNEL_ID="${LIONCLAW_CHANNEL_ID:-terminal}"
LIONCLAW_PEER_ID="${LIONCLAW_PEER_ID:-$(default_peer_id)}"
LIONCLAW_STREAM_LIMIT="${LIONCLAW_STREAM_LIMIT:-50}"
LIONCLAW_STREAM_WAIT_MS="${LIONCLAW_STREAM_WAIT_MS:-30000}"
LIONCLAW_STREAM_START_MODE="${LIONCLAW_STREAM_START_MODE:-tail}"
LIONCLAW_CONSUMER_ID="${LIONCLAW_CONSUMER_ID:-terminal:$LIONCLAW_CHANNEL_ID:$LIONCLAW_PEER_ID}"
LIONCLAW_STREAM_RETRY_SECS="${LIONCLAW_STREAM_RETRY_SECS:-1}"
LIONCLAW_SESSION_ID="${LIONCLAW_SESSION_ID:-}"

WORKER_INSTANCE_ID="terminal-${LIONCLAW_CHANNEL_ID}-$(process_nonce)"
inbound_sequence=0
stream_pid=""

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
  local pairings_json
  pairings_json="$(lionclaw_get '/v0/channels/pairing' \
    --data-urlencode "channel_id=$LIONCLAW_CHANNEL_ID")" || return 1

  jq -c \
    --arg sender_ref "$LIONCLAW_PEER_ID" \
    '(
      [
        .grants[]?
        | select(
            .sender_ref == $sender_ref
            and .routing_profile == "direct"
            and (.status == "approved" or .status == "blocked")
          )
      ][0]
    ) // (
      [
        .pairings[]?
        | select(
            .sender_ref == $sender_ref
            and .requested_profile == "direct"
            and .status == "pending"
          )
      ][0]
    ) // empty' <<<"$pairings_json"
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
      local pairing_id
      pairing_id="$(jq -r '.pairing_id' <<<"$peer_json")"
      echo "status: peer '$LIONCLAW_PEER_ID' is pending approval"
      echo "approve: lionclaw channel pairing approve $LIONCLAW_CHANNEL_ID $pairing_id --trust-tier main"
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
  local event_id
  local payload
  local response
  local outcome
  local turn_id

  event_id="terminal-inbound:$WORKER_INSTANCE_ID:$inbound_sequence"
  payload="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg event_id "$event_id" \
    --arg sender_ref "$LIONCLAW_PEER_ID" \
    --arg conversation_ref "$LIONCLAW_PEER_ID" \
    --arg text "$text" \
    '{
      channel_id: $channel_id,
      event_id: $event_id,
      sender_ref: $sender_ref,
      conversation_ref: $conversation_ref,
      text: $text,
      attachments: [],
      trigger: "dm",
      provider_metadata: {source: "debug-worker"}
    }'
  )"

  response="$(lionclaw_post '/v0/channels/inbound' "$payload")" || return 1
  outcome="$(jq -r '.outcome // empty' <<<"$response")"
  turn_id="$(jq -r '.turn_id // empty' <<<"$response")"
  inbound_sequence="$((inbound_sequence + 1))"

  case "$outcome" in
    queued)
      if [[ -n "$turn_id" ]]; then
        print_status_message "queued turn $turn_id" "queue.queued"
      else
        print_status_message "queued" "queue.queued"
      fi
      ;;
    pending_approval)
      print_status_message "pairing pending" "pairing.pending"
      ;;
    duplicate)
      print_status_message "duplicate inbound ignored" "queue.duplicate"
      ;;
    blocked)
      print_error_message "peer is blocked" "blocked"
      ;;
    trigger_ignored)
      print_status_message "trigger ignored" "trigger_ignored"
      ;;
    waiting_for_attachments)
      print_status_message "waiting for attachments" "waiting_for_attachments"
      ;;
    *)
      print_error_message "unexpected inbound outcome '$outcome'"
      return 1
      ;;
  esac
}

pull_stream() {
  local request
  request="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg consumer_id "$LIONCLAW_CONSUMER_ID" \
    --arg start_mode "$LIONCLAW_STREAM_START_MODE" \
    --argjson limit "$LIONCLAW_STREAM_LIMIT" \
    --argjson wait_ms "$LIONCLAW_STREAM_WAIT_MS" \
    '{
      channel_id: $channel_id,
      consumer_id: $consumer_id,
      start_mode: $start_mode,
      limit: $limit,
      wait_ms: $wait_ms
    }'
  )"

  lionclaw_post '/v0/channels/stream/pull' "$request"
}

ack_stream() {
  local through_sequence="$1"
  local request

  request="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg consumer_id "$LIONCLAW_CONSUMER_ID" \
    --argjson through_sequence "$through_sequence" \
    '{
      channel_id: $channel_id,
      consumer_id: $consumer_id,
      through_sequence: $through_sequence
    }'
  )"

  lionclaw_post '/v0/channels/stream/ack' "$request" >/dev/null
}

pull_outbox() {
  local request
  request="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg worker_id "$LIONCLAW_CONSUMER_ID" \
    --arg conversation_ref "$LIONCLAW_PEER_ID" \
    '{
      channel_id: $channel_id,
      worker_id: $worker_id,
      conversation_ref: $conversation_ref,
      limit: 10,
      lease_ms: 120000
    }'
  )"

  lionclaw_post '/v0/channels/outbox/pull' "$request"
}

report_outbox_delivered() {
  local delivery_id="$1"
  local attempt_id="$2"
  local request

  request="$(jq -nc \
    --arg channel_id "$LIONCLAW_CHANNEL_ID" \
    --arg worker_id "$LIONCLAW_CONSUMER_ID" \
    --arg delivery_id "$delivery_id" \
    --arg attempt_id "$attempt_id" \
    '{
      channel_id: $channel_id,
      worker_id: $worker_id,
      delivery_id: $delivery_id,
      attempt_id: $attempt_id,
      outcome: "delivered",
      provider_receipt: {
        provider: "terminal-debug",
        rendered: true
      }
    }'
  )"

  lionclaw_post '/v0/channels/outbox/report' "$request" >/dev/null
}

print_answer_delta() {
  local content="$1"
  local line

  while IFS= read -r line || [[ -n "$line" ]]; do
    printf 'lionclaw> %s\n' "$line"
  done <<<"$content"
}

print_reasoning_delta() {
  local content="$1"
  local line

  while IFS= read -r line || [[ -n "$line" ]]; do
    printf 'thinking> %s\n' "$line"
  done <<<"$content"
}

print_status_message() {
  local content="$1"
  local code="${2:-}"
  if [[ -n "$code" ]]; then
    printf '[status] %s: %s\n' "$code" "$content"
  else
    printf '[status] %s\n' "$content"
  fi
}

print_error_message() {
  local content="$1"
  local code="${2:-}"
  if [[ -n "$code" ]]; then
    printf '[error] %s: %s\n' "$code" "$content" >&2
  else
    printf '[error] %s\n' "$content" >&2
  fi
}

flush_stream_once() {
  local stream_json
  local last_sequence=""

  stream_json="$(pull_stream)" || {
    echo "warning: failed to pull channel stream" >&2
    return 1
  }

  if ! jq -e '.events | type == "array"' >/dev/null <<<"$stream_json"; then
    echo "warning: invalid channel stream response: $(jq -c '.' <<<"$stream_json")" >&2
    return 1
  fi

  while IFS= read -r event; do
    local sequence
    local peer_id
    local kind
    local lane
    local code
    local text

    sequence="$(jq -r '.sequence' <<<"$event")"
    peer_id="$(jq -r '.peer_id' <<<"$event")"
    kind="$(jq -r '.kind' <<<"$event")"
    lane="$(jq -r '.lane // empty' <<<"$event")"
    code="$(jq -r '.code // empty' <<<"$event")"
    text="$(jq -r '.text // empty' <<<"$event")"

    if [[ "$peer_id" != "$LIONCLAW_PEER_ID" ]]; then
      echo "warning: channel '$LIONCLAW_CHANNEL_ID' stream delivered peer '$peer_id' to terminal worker for '$LIONCLAW_PEER_ID'" >&2
      return 1
    fi

    case "$kind" in
      message_delta)
        case "$lane" in
          answer)
            print_answer_delta "$text"
            ;;
          reasoning)
            print_reasoning_delta "$text"
            ;;
          *)
            print_status_message "$text" "$code"
            ;;
        esac
        ;;
      status)
        print_status_message "$text" "$code"
        ;;
      error)
        print_error_message "$text" "$code"
        ;;
      turn_completed)
        # The debug worker prints live answer deltas as they arrive. The Python
        # TUI uses this event to reconcile its rendered answer pane.
        ;;
      done)
        ;;
      *)
        echo "warning: unknown stream event kind '$kind'" >&2
        ;;
    esac

    last_sequence="$sequence"
  done < <(jq -c '.events[]?' <<<"$stream_json")

  if [[ -n "$last_sequence" ]]; then
    ack_stream "$last_sequence" || {
      echo "warning: failed to acknowledge channel stream through sequence $last_sequence" >&2
      return 1
    }
  fi

  return 0
}

flush_outbox_once() {
  local outbox_json

  outbox_json="$(pull_outbox)" || {
    echo "warning: failed to pull channel outbox" >&2
    return 1
  }

  if ! jq -e '.deliveries | type == "array"' >/dev/null <<<"$outbox_json"; then
    echo "warning: invalid channel outbox response: $(jq -c '.' <<<"$outbox_json")" >&2
    return 1
  fi

  while IFS= read -r delivery; do
    local delivery_id
    local attempt_id
    local conversation_ref
    local text

    delivery_id="$(jq -r '.delivery_id' <<<"$delivery")"
    attempt_id="$(jq -r '.attempt_id' <<<"$delivery")"
    conversation_ref="$(jq -r '.conversation_ref' <<<"$delivery")"
    text="$(jq -r '.content.text // empty' <<<"$delivery")"

    if [[ "$conversation_ref" != "$LIONCLAW_PEER_ID" ]]; then
      echo "warning: terminal outbox delivery for peer '$conversation_ref' reached '$LIONCLAW_PEER_ID'" >&2
      continue
    fi

    if [[ -n "$text" ]]; then
      print_answer_delta "$text"
    fi
    report_outbox_delivered "$delivery_id" "$attempt_id" || {
      echo "warning: failed to report channel outbox delivery $delivery_id" >&2
      return 1
    }
  done < <(jq -c '.deliveries[]?' <<<"$outbox_json")
}

stream_loop() {
  while true; do
    if ! flush_stream_once; then
      sleep "$LIONCLAW_STREAM_RETRY_SECS"
    fi
    flush_outbox_once || sleep "$LIONCLAW_STREAM_RETRY_SECS"
  done
}

cleanup() {
  if [[ -n "$stream_pid" ]]; then
    kill "$stream_pid" >/dev/null 2>&1 || true
    wait "$stream_pid" 2>/dev/null || true
  fi
}

interrupt() {
  echo
  exit 130
}

trap cleanup EXIT
trap interrupt INT TERM

echo "LionClaw terminal channel worker"
echo "channel: $LIONCLAW_CHANNEL_ID"
echo "peer: $LIONCLAW_PEER_ID"
echo "consumer: $LIONCLAW_CONSUMER_ID (start_mode=$LIONCLAW_STREAM_START_MODE)"
echo "commands: /status, /quit"
print_pairing_status

stream_loop &
stream_pid="$!"

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
