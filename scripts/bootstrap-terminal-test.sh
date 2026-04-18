#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/bootstrap-terminal-test.sh <home> [runtime-id] [runtime-command] [channel-id]

Create or refresh a LionClaw home for manual terminal-channel testing, then
attach the interactive terminal channel in the current TTY.

Arguments:
  lionclaw-home   Path to the LionClaw home to create or reuse
  runtime-id      Optional runtime id to configure (default: codex)
  runtime-command Optional runtime command or executable (default: runtime-id)
  channel-id      Optional channel id to attach (default: terminal)

Environment:
  LIONCLAW_RUNTIME_IMAGE  Runtime image to configure/build (default: lionclaw-runtime:v1)
  LIONCLAW_RUNTIME_KIND   Runtime kind to configure (default: inferred from runtime-id)
  LIONCLAW_SKIP_IMAGE_BUILD=1 skips building the runtime image when it is missing
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

runtime_kind_for() {
  local runtime_id="$1"
  local explicit="${LIONCLAW_RUNTIME_KIND:-}"

  if [[ -n "$explicit" ]]; then
    printf '%s\n' "$explicit"
    return
  fi

  case "$runtime_id" in
    opencode)
      printf '%s\n' "opencode"
      ;;
    *)
      printf '%s\n' "codex"
      ;;
  esac
}

ensure_runtime_image() {
  local image="$1"

  command -v podman >/dev/null 2>&1 || die "podman is required to configure the runtime image"

  if podman image exists "$image"; then
    return
  fi

  if [[ "${LIONCLAW_SKIP_IMAGE_BUILD:-0}" == "1" ]]; then
    die "runtime image '$image' is missing; unset LIONCLAW_SKIP_IMAGE_BUILD or build it first"
  fi

  printf 'Building runtime image=%s\n' "$image"
  podman build -t "$image" -f containers/runtime/Containerfile .
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
runtime_kind="$(runtime_kind_for "$runtime_id")"
runtime_image="${LIONCLAW_RUNTIME_IMAGE:-lionclaw-runtime:v1}"

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
printf 'Ensuring runtime=%s kind=%s command=%s image=%s channel=%s\n' \
  "$runtime_id" "$runtime_kind" "$runtime_command" "$runtime_image" "$channel_id"

ensure_runtime_image "$runtime_image"
"./target/debug/lionclaw" runtime add "$runtime_id" \
  --kind "$runtime_kind" \
  --bin "$runtime_command" \
  --image "$runtime_image"
"./target/debug/lionclaw" runtime set-default "$runtime_id"
"./target/debug/lionclaw" skill add skills/channel-terminal --alias terminal
"./target/debug/lionclaw" channel add "$channel_id" --skill terminal --launch interactive

export LIONCLAW_SKIP_BUILD=1
exec "./scripts/attach-terminal-test.sh" "$LIONCLAW_HOME" "$runtime_id" "$channel_id"
