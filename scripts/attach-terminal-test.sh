#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/attach-terminal-test.sh <lionclaw-home> [runtime-id] [channel-id]

Rebuild LionClaw, stop any managed background services for the given home,
and attach the interactive terminal channel in the current TTY.

Arguments:
  lionclaw-home  Path to the LionClaw home to test
  runtime-id     Optional runtime id override
  channel-id     Optional channel id override (default: terminal)
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

resolve_default_runtime() {
  local runtime_id
  runtime_id="$("./target/debug/lionclaw" runtime ls \
    | sed -n 's/^\* \([^ ]\+\) .*/\1/p' \
    | head -n1)"

  [[ -n "$runtime_id" ]] || die "no default runtime configured for $LIONCLAW_HOME; pass a runtime id explicitly"
  printf '%s\n' "$runtime_id"
}

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage >&2
  exit 64
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lionclaw_home="$(resolve_home "$1")"
channel_id="${3:-terminal}"

cd "$repo_root"
export LIONCLAW_HOME="$lionclaw_home"

[[ -d "$LIONCLAW_HOME" ]] || die "LionClaw home does not exist: $LIONCLAW_HOME"
[[ -f "$LIONCLAW_HOME/config/lionclaw.toml" ]] || die "missing config at $LIONCLAW_HOME/config/lionclaw.toml; run lionclaw onboard first"

runtime_id="${2:-$(resolve_default_runtime)}"

printf 'Using LIONCLAW_HOME=%s\n' "$LIONCLAW_HOME"
printf 'Using runtime=%s channel=%s\n' "$runtime_id" "$channel_id"

if [[ "${LIONCLAW_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build --bins
fi
"./target/debug/lionclaw" service down
exec "./target/debug/lionclaw" channel attach "$channel_id" --runtime "$runtime_id"
