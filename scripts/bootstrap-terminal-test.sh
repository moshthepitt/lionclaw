#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/bootstrap-terminal-test.sh <lionclaw-home> [runtime-id] [runtime-command] [channel-id]

Create or refresh a LionClaw home for manual terminal-channel testing, then
attach the interactive terminal channel in the current TTY.

Arguments:
  lionclaw-home   Path to the LionClaw home to create or reuse
  runtime-id      Optional runtime id to configure (default: codex)
  runtime-command Optional runtime command or executable (default: runtime-id)
  channel-id      Optional channel id to attach (default: terminal)
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

resolve_home() {
  local raw="$1"
  if [[ "$raw" == "~"* ]]; then
    printf '%s\n' "${raw/#\~/$HOME}"
  else
    printf '%s\n' "$raw"
  fi
}

configured_bind() {
  sed -n 's/^bind = "\(.*\)"$/\1/p' "$LIONCLAW_HOME/config/lionclaw.toml" | head -n1
}

if [[ $# -lt 1 || $# -gt 4 ]]; then
  usage >&2
  exit 64
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lionclaw_home="$(resolve_home "$1")"
runtime_id="${2:-codex}"
runtime_command="${3:-$runtime_id}"
channel_id="${4:-terminal}"

cd "$repo_root"
export LIONCLAW_HOME="$lionclaw_home"

mkdir -p "$LIONCLAW_HOME"
cargo build --bins

if [[ ! -f "$LIONCLAW_HOME/config/lionclaw.toml" ]]; then
  "./target/debug/lionclaw" onboard --bind auto
fi

bind_addr="$(configured_bind)"

printf 'Using LIONCLAW_HOME=%s\n' "$LIONCLAW_HOME"
printf 'Using bind=%s\n' "${bind_addr:-unknown}"
printf 'Ensuring runtime=%s command=%s channel=%s\n' "$runtime_id" "$runtime_command" "$channel_id"

"./target/debug/lionclaw" runtime add "$runtime_id" --kind codex --bin "$runtime_command"
"./target/debug/lionclaw" runtime set-default "$runtime_id"
"./target/debug/lionclaw" skill add skills/channel-terminal --alias terminal
"./target/debug/lionclaw" channel add "$channel_id" --skill terminal --launch interactive

export LIONCLAW_SKIP_BUILD=1
exec "./scripts/attach-terminal-test.sh" "$LIONCLAW_HOME" "$runtime_id" "$channel_id"
