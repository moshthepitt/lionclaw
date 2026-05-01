#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./scripts/lionclaw-project.sh <command> [args]

Project helper for making one repository easy to use with LionClaw.

Commands:
  doctor           Check prerequisites and project-managed state
  configure        Onboard the project home and register runtime, skills, and channel
  build-image      Build the configured runtime image if missing or stale
  rebuild-image    Force rebuild the configured runtime image
  run              Configure the project, then run the runtime in the workspace
  attach           Configure the project, then attach the interactive terminal channel
  up               Configure the project and start the managed LionClaw daemon
  down             Stop managed LionClaw services for this project home
  status           Show managed service status for this project home
  logs             Show managed service logs for this project home
  pairing-list     List channel pairing requests
  pairing-approve  Approve a pairing request: pairing-approve <peer-id> <code> [trust-tier]
  help             Show this message

Environment overrides:
  LIONCLAW_PROJECT_ROOT           Default: git root for $PWD, else $PWD
  LIONCLAW_WORKSPACE_ROOT         Default: single project */Containerfile dir, else project root
  LIONCLAW_HOME                   Default: existing <project>/lionclaw-home, else <project>/.lionclaw/home
  LIONCLAW_REPO                   Default: this script's repo, or sibling ../lionclaw
  LIONCLAW_BIN                    Default: <lionclaw-repo>/target/debug/lionclaw, else PATH
  LIONCLAW_FORCE_BUILD=1          Force cargo build --bins before LionClaw commands
  LIONCLAW_RUNTIME_ID             Default: codex
  LIONCLAW_RUNTIME_KIND           Default: codex
  LIONCLAW_RUNTIME_BIN            Default: runtime id
  LIONCLAW_RUNTIME_IMAGE          Default: <workspace>-runtime:v1 for project images, else lionclaw-runtime:v1
  LIONCLAW_RUNTIME_CONTAINERFILE  Default: project Containerfile, single */Containerfile, else LionClaw runtime Containerfile
  LIONCLAW_IMAGE_CONTEXT          Default: Containerfile directory, or LionClaw repo for built-in image
  LIONCLAW_PODMAN_BIN             Optional explicit Podman path
  LIONCLAW_FORCE_IMAGE_BUILD=1    Force runtime image rebuild
  LIONCLAW_CHANNEL_ID             Default: terminal
  LIONCLAW_TERMINAL_ALIAS         Default: terminal
  LIONCLAW_TERMINAL_SKILL_SOURCE  Default: <lionclaw-repo>/skills/channel-terminal
  LIONCLAW_PROJECT_SKILLS         Default: auto-detect <project>/skills/*/SKILL.md; comma-separated alias=path, or empty to disable
  LIONCLAW_DRY_RUN=1              Print mutating actions without changing state
  LIONCLAW_ALLOW_REWRITE=1        Allow configure/run/attach/up to overwrite mismatched managed entries

Examples:
  ./scripts/lionclaw-project.sh doctor
  ./scripts/lionclaw-project.sh run --timeout 4h
  LIONCLAW_PROJECT_SKILLS='pdf-to-markdown=skills/pdf-to-markdown' ./scripts/lionclaw-project.sh configure
USAGE
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

warn() {
  printf 'warning: %s\n' "$*" >&2
}

is_dry_run() {
  [[ "${LIONCLAW_DRY_RUN:-0}" == "1" ]]
}

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "$value"
}

abs_dir() {
  local path="$1"
  [[ -d "$path" ]] || return 1
  (cd "$path" && pwd -P)
}

abs_file() {
  local path="$1"
  local parent base abs_parent
  parent="$(dirname "$path")"
  base="$(basename "$path")"
  abs_parent="$(abs_dir "$parent")" || return
  printf '%s/%s\n' "$abs_parent" "$base"
}

resolve_path() {
  local base="$1"
  local raw="$2"
  if [[ "$raw" == /* ]]; then
    printf '%s\n' "$raw"
  else
    printf '%s/%s\n' "$base" "$raw"
  fi
}

abs_path() {
  local base="$1"
  local raw="$2"
  realpath -m -- "$(resolve_path "$base" "$raw")"
}

resolve_project_root() {
  if [[ -n "${LIONCLAW_PROJECT_ROOT:-}" ]]; then
    abs_dir "$LIONCLAW_PROJECT_ROOT"
    return
  fi

  if has_cmd git && git -C "$PWD" rev-parse --show-toplevel >/dev/null 2>&1; then
    git -C "$PWD" rev-parse --show-toplevel
    return
  fi

  pwd -P
}

find_project_containerfile() {
  local project_root="$1"
  local candidates=()
  local candidate

  if [[ -f "$project_root/Containerfile" ]]; then
    printf '%s/Containerfile\n' "$project_root"
    return
  fi

  while IFS= read -r candidate; do
    candidates+=("$candidate")
  done < <(find "$project_root" -mindepth 2 -maxdepth 2 -type f -name Containerfile | sort)

  if [[ "${#candidates[@]}" -eq 1 ]]; then
    printf '%s\n' "${candidates[0]}"
  fi
}

resolve_default_workspace_root() {
  local project_root="$1"
  local project_containerfile="$2"

  if [[ -n "${LIONCLAW_WORKSPACE_ROOT:-}" ]]; then
    abs_dir "$(resolve_path "$project_root" "$LIONCLAW_WORKSPACE_ROOT")"
  elif [[ -n "$project_containerfile" ]]; then
    abs_dir "$(dirname "$project_containerfile")"
  else
    printf '%s\n' "$project_root"
  fi
}

resolve_default_home() {
  local project_root="$1"

  if [[ -n "${LIONCLAW_HOME:-}" ]]; then
    abs_path "$project_root" "$LIONCLAW_HOME"
  elif [[ -d "$project_root/lionclaw-home" || -f "$project_root/lionclaw-home/config/lionclaw.toml" ]]; then
    printf '%s/lionclaw-home\n' "$project_root"
  else
    printf '%s/.lionclaw/home\n' "$project_root"
  fi
}

runtime_image_name_for_workspace() {
  local workspace_root="$1"
  local raw
  raw="$(basename "$workspace_root" | tr '[:upper:]' '[:lower:]' | tr -cs '[:alnum:]_.-' '-')"
  raw="${raw#-}"
  raw="${raw%-}"
  printf '%s-runtime:v1\n' "${raw:-project}"
}

looks_like_lionclaw_repo() {
  local path="$1"
  [[ -f "$path/Cargo.toml" && -d "$path/skills/channel-terminal" && -f "$path/containers/runtime/Containerfile" ]]
}

resolve_lionclaw_repo() {
  local script_repo="$1"
  local project_root="$2"

  if [[ -n "${LIONCLAW_REPO:-}" ]]; then
    abs_dir "$LIONCLAW_REPO"
    return
  fi

  if looks_like_lionclaw_repo "$script_repo"; then
    printf '%s\n' "$script_repo"
    return
  fi

  if looks_like_lionclaw_repo "$project_root/../lionclaw"; then
    abs_dir "$project_root/../lionclaw"
    return
  fi

  printf '\n'
}

resolve_lionclaw_bin() {
  local repo="$1"

  if [[ -n "${LIONCLAW_BIN:-}" ]]; then
    printf '%s\n' "$LIONCLAW_BIN"
    return
  fi

  if [[ -n "$repo" ]]; then
    printf '%s/target/debug/lionclaw\n' "$repo"
    return
  fi

  command -v lionclaw || true
}

discover_project_skills() {
  local project_root="$1"
  local skills_root="$project_root/skills"
  local specs=()
  local skill_dir alias

  [[ -d "$skills_root" ]] || return 0

  while IFS= read -r skill_dir; do
    alias="$(basename "$skill_dir")"
    [[ "$alias" != channel-* ]] || continue
    specs+=("$alias=skills/$alias")
  done < <(find "$skills_root" -mindepth 1 -maxdepth 1 -type d -exec test -f '{}/SKILL.md' ';' -print | sort)

  local IFS=,
  printf '%s\n' "${specs[*]}"
}

configured_bind() {
  local config_file="$LIONCLAW_HOME/config/lionclaw.toml"
  [[ -f "$config_file" ]] || return 0
  sed -n 's/^bind = "\(.*\)"$/\1/p' "$config_file" | head -n1
}

config_path() {
  printf '%s/config/lionclaw.toml\n' "$LIONCLAW_HOME"
}

ensure_python3() {
  has_cmd python3 || die "python3 is required for project config validation"
}

ensure_podman() {
  [[ -n "$PODMAN_BIN" && -x "$PODMAN_BIN" ]] || die "podman is required; set LIONCLAW_PODMAN_BIN if it is not on PATH"
}

ensure_systemd_user() {
  has_cmd systemctl || die "systemd user services are required for this command"
  systemctl --user show-environment >/dev/null 2>&1 \
    || die "systemd user services are unavailable for this shell"
}

ensure_lionclaw_bin() {
  if [[ "${LIONCLAW_FORCE_BUILD:-0}" == "1" || ! -x "$LIONCLAW_BIN" ]]; then
    [[ -n "$LIONCLAW_REPO" ]] || die "missing LionClaw binary and LIONCLAW_REPO is not set"
    has_cmd cargo || die "cargo is required to build LionClaw"
    if is_dry_run; then
      printf '[dry-run] (%s) cargo build --bins\n' "$LIONCLAW_REPO"
    else
      (cd "$LIONCLAW_REPO" && cargo build --bins)
    fi
  fi

  [[ -x "$LIONCLAW_BIN" ]] || die "missing LionClaw binary: $LIONCLAW_BIN"
}

containerfile_base_image_ref() {
  [[ -f "$RUNTIME_IMAGE_CONTAINERFILE" ]] || return 0
  sed -n 's/^ARG BASE_IMAGE=\(.*\)$/\1/p' "$RUNTIME_IMAGE_CONTAINERFILE" | head -n1
}

podman_image_exists() {
  "$PODMAN_BIN" image exists "$1" >/dev/null 2>&1
}

podman_image_id() {
  "$PODMAN_BIN" image inspect --format '{{.Id}}' "$1" 2>/dev/null | head -n1
}

podman_image_label() {
  local image="$1"
  local label="$2"
  "$PODMAN_BIN" image inspect --format "{{ index .Labels \"$label\" }}" "$image" 2>/dev/null \
    | sed 's/^<no value>$//' \
    | head -n1
}

runtime_image_status() {
  local base_ref base_id recorded_ref recorded_id
  base_ref="$(containerfile_base_image_ref)"

  if ! podman_image_exists "$RUNTIME_IMAGE"; then
    if [[ -n "$base_ref" ]] && ! podman_image_exists "$base_ref"; then
      printf 'missing-base|%s|||\n' "$base_ref"
    else
      printf 'missing-runtime|%s|||\n' "$base_ref"
    fi
    return
  fi

  if [[ -z "$base_ref" ]]; then
    printf 'present|%s|||\n' "$base_ref"
    return
  fi

  recorded_ref="$(podman_image_label "$RUNTIME_IMAGE" "$RUNTIME_IMAGE_BASE_REF_LABEL")"
  recorded_id="$(podman_image_label "$RUNTIME_IMAGE" "$RUNTIME_IMAGE_BASE_ID_LABEL")"

  if [[ -z "$recorded_ref" || -z "$recorded_id" ]]; then
    printf 'untracked|%s|%s||%s\n' "$base_ref" "$recorded_ref" "$recorded_id"
    return
  fi

  if [[ "$recorded_ref" != "$base_ref" ]]; then
    printf 'stale-ref|%s|%s||%s\n' "$base_ref" "$recorded_ref" "$recorded_id"
    return
  fi

  if ! podman_image_exists "$base_ref"; then
    printf 'missing-base|%s|%s||%s\n' "$base_ref" "$recorded_ref" "$recorded_id"
    return
  fi

  base_id="$(podman_image_id "$base_ref")"
  if [[ -n "$base_id" && "$recorded_id" != "$base_id" ]]; then
    printf 'stale-id|%s|%s|%s|%s\n' "$base_ref" "$recorded_ref" "$base_id" "$recorded_id"
    return
  fi

  printf 'fresh|%s|%s|%s|%s\n' "$base_ref" "$recorded_ref" "$base_id" "$recorded_id"
}

ensure_runtime_image() {
  ensure_podman
  [[ -f "$RUNTIME_IMAGE_CONTAINERFILE" ]] || die "missing runtime Containerfile: $RUNTIME_IMAGE_CONTAINERFILE"
  [[ -d "$RUNTIME_IMAGE_CONTEXT" ]] || die "missing runtime image context: $RUNTIME_IMAGE_CONTEXT"

  local force_build="${1:-${LIONCLAW_FORCE_IMAGE_BUILD:-0}}"
  local status base_ref recorded_ref current_base_id recorded_base_id
  local reason=''

  IFS='|' read -r status base_ref recorded_ref current_base_id recorded_base_id < <(runtime_image_status)

  if [[ "$force_build" != "1" ]]; then
    case "$status" in
      fresh)
        printf 'Runtime image check: %s is current\n' "$RUNTIME_IMAGE"
        return
        ;;
      present)
        printf 'Runtime image check: %s is present\n' "$RUNTIME_IMAGE"
        return
        ;;
      missing-runtime)
        reason="runtime image is missing"
        ;;
      missing-base)
        reason="base image ${base_ref:-unknown} is missing"
        ;;
      stale-ref)
        reason="runtime image was built from ${recorded_ref:-unknown}, but Containerfile expects $base_ref"
        ;;
      stale-id)
        reason="runtime image was built from an older $base_ref image id"
        ;;
      untracked)
        reason="runtime image does not record base image metadata"
        ;;
      *)
        reason="runtime image needs reconciliation (state=$status)"
        ;;
    esac
  else
    reason="force rebuild requested"
  fi

  if is_dry_run; then
    printf '[dry-run] would build runtime image=%s from %s context=%s (%s)\n' \
      "$RUNTIME_IMAGE" "$RUNTIME_IMAGE_CONTAINERFILE" "$RUNTIME_IMAGE_CONTEXT" "$reason"
    return
  fi

  local build_args=()
  if [[ -n "$base_ref" ]]; then
    build_args+=(--build-arg "BASE_IMAGE=$base_ref")
    if podman_image_exists "$base_ref"; then
      build_args+=(--build-arg "BASE_IMAGE_ID=$(podman_image_id "$base_ref")")
    fi
  fi

  printf 'Building runtime image=%s from %s context=%s (%s)\n' \
    "$RUNTIME_IMAGE" "$RUNTIME_IMAGE_CONTAINERFILE" "$RUNTIME_IMAGE_CONTEXT" "$reason"
  "$PODMAN_BIN" build "${build_args[@]}" -t "$RUNTIME_IMAGE" -f "$RUNTIME_IMAGE_CONTAINERFILE" "$RUNTIME_IMAGE_CONTEXT"
}

run_lionclaw() {
  (cd "$WORKSPACE_ROOT" && env LIONCLAW_HOME="$LIONCLAW_HOME" "$LIONCLAW_BIN" "$@")
}

run_lionclaw_action() {
  if is_dry_run; then
    printf '[dry-run] (%s) LIONCLAW_HOME=%q %q' "$WORKSPACE_ROOT" "$LIONCLAW_HOME" "$LIONCLAW_BIN"
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
    return
  fi
  run_lionclaw "$@"
}

print_context() {
  printf 'Project root:      %s\n' "$PROJECT_ROOT"
  printf 'Workspace root:    %s\n' "$WORKSPACE_ROOT"
  printf 'LIONCLAW_HOME:     %s\n' "$LIONCLAW_HOME"
  printf 'LionClaw repo:     %s\n' "${LIONCLAW_REPO:-not set}"
  printf 'LionClaw binary:   %s\n' "${LIONCLAW_BIN:-missing}"
  printf 'Runtime:           %s (%s via %s)\n' "$RUNTIME_ID" "$RUNTIME_KIND" "$RUNTIME_IMAGE"
  printf 'Containerfile:     %s\n' "$RUNTIME_IMAGE_CONTAINERFILE"
  printf 'Image context:     %s\n' "$RUNTIME_IMAGE_CONTEXT"
  printf 'Podman:            %s\n' "${PODMAN_BIN:-missing}"
  printf 'Channel:           %s (skill alias %s)\n' "$CHANNEL_ID" "$TERMINAL_ALIAS"
  printf 'Project skills:    %s\n' "${PROJECT_SKILLS:-none}"
  printf 'Dry run:           %s\n' "${LIONCLAW_DRY_RUN:-0}"
}

ensure_home_onboarded() {
  if is_dry_run; then
    printf '[dry-run] would mkdir -p %q\n' "$LIONCLAW_HOME"
  else
    mkdir -p "$LIONCLAW_HOME"
  fi

  if [[ ! -f "$(config_path)" ]]; then
    run_lionclaw_action onboard --bind auto
  fi
}

project_skill_specs_for_python() {
  local spec alias source resolved
  [[ -n "$PROJECT_SKILLS" ]] || return 0
  IFS=',' read -r -a skill_specs <<<"$PROJECT_SKILLS"
  for spec in "${skill_specs[@]}"; do
    spec="$(trim "$spec")"
    [[ -n "$spec" ]] || continue
    alias="${spec%%=*}"
    source="${spec#*=}"
    [[ "$alias" != "$spec" && -n "$alias" && -n "$source" ]] \
      || die "invalid LIONCLAW_PROJECT_SKILLS entry '$spec'; expected alias=path"
    resolved="$(resolve_path "$PROJECT_ROOT" "$source")"
    printf '%s=%s\n' "$alias" "$(abs_dir "$resolved")"
  done
}

validate_managed_config() {
  local config_file
  config_file="$(config_path)"
  [[ -f "$config_file" ]] || return 0

  ensure_python3

  local project_skill_specs validation_output
  project_skill_specs="$(project_skill_specs_for_python)"

  if ! validation_output="$(
    CONFIG_FILE="$config_file" \
    LIONCLAW_HOME="$LIONCLAW_HOME" \
    EXPECT_RUNTIME_ID="$RUNTIME_ID" \
    EXPECT_RUNTIME_KIND="$RUNTIME_KIND" \
    EXPECT_RUNTIME_BIN="$RUNTIME_BIN" \
    EXPECT_PODMAN_BIN="$PODMAN_BIN" \
    EXPECT_RUNTIME_IMAGE="$RUNTIME_IMAGE" \
    EXPECT_TERMINAL_ALIAS="$TERMINAL_ALIAS" \
    EXPECT_TERMINAL_SOURCE="local:$TERMINAL_SKILL_SOURCE" \
    EXPECT_CHANNEL_ID="$CHANNEL_ID" \
    EXPECT_LAUNCH_MODE="interactive" \
    EXPECT_PROJECT_SKILLS="$project_skill_specs" \
    python3 - <<'PY'
import os
import pathlib
import sys
import tomllib

config_path = os.environ["CONFIG_FILE"]
with open(config_path, "rb") as handle:
    config = tomllib.load(handle)

issues = []

runtime_id = os.environ["EXPECT_RUNTIME_ID"]
runtime = (config.get("runtimes") or {}).get(runtime_id)
if runtime is not None:
    for key, expected in {
        "kind": os.environ["EXPECT_RUNTIME_KIND"],
        "executable": os.environ["EXPECT_RUNTIME_BIN"],
    }.items():
        actual = runtime.get(key)
        if actual != expected:
            issues.append(f"runtime {runtime_id!r} has {key}={actual!r}; expected {expected!r}")
    confinement = runtime.get("confinement") or {}
    for key, expected in {
        "backend": "podman",
        "image": os.environ["EXPECT_RUNTIME_IMAGE"],
    }.items():
        actual = confinement.get(key)
        if actual != expected:
            issues.append(
                f"runtime {runtime_id!r} confinement.{key}={actual!r}; expected {expected!r}"
            )
    expected_engine = os.environ["EXPECT_PODMAN_BIN"]
    if expected_engine:
        actual_engine = confinement.get("engine")
        if actual_engine != expected_engine:
            issues.append(
                f"runtime {runtime_id!r} confinement.engine={actual_engine!r}; expected {expected_engine!r}"
            )

actual_default = (config.get("defaults") or {}).get("runtime")
if actual_default is not None and actual_default != runtime_id:
    issues.append(f"default runtime is {actual_default!r}; expected {runtime_id!r}")

channels = {
    entry.get("id"): entry
    for entry in config.get("channels") or []
    if entry.get("id")
}
channel_id = os.environ["EXPECT_CHANNEL_ID"]
channel = channels.get(channel_id)
if channel is not None:
    expected_skill = os.environ["EXPECT_TERMINAL_ALIAS"]
    if channel.get("skill") != expected_skill:
        issues.append(
            f"channel {channel_id!r} has skill={channel.get('skill')!r}; expected {expected_skill!r}"
        )
    expected_launch = os.environ["EXPECT_LAUNCH_MODE"]
    if channel.get("launch_mode") != expected_launch:
        issues.append(
            f"channel {channel_id!r} has launch_mode={channel.get('launch_mode')!r}; expected {expected_launch!r}"
        )

home = pathlib.Path(os.environ["LIONCLAW_HOME"])
expected_skills = {
    os.environ["EXPECT_TERMINAL_ALIAS"]: os.environ["EXPECT_TERMINAL_SOURCE"],
}
for line in os.environ["EXPECT_PROJECT_SKILLS"].splitlines():
    if not line.strip():
        continue
    alias, source = line.split("=", 1)
    expected_skills[alias] = f"local:{source}"

for alias, expected_source in expected_skills.items():
    metadata = home / "skills" / alias / ".lionclaw-skill.toml"
    if not metadata.exists():
        continue
    with metadata.open("rb") as handle:
        installed = tomllib.load(handle)
    actual_source = installed.get("source")
    if actual_source != expected_source:
        issues.append(
            f"skill {alias!r} has source={actual_source!r}; expected {expected_source!r}"
        )

if issues:
    for issue in issues:
        print(issue)
    sys.exit(1)
PY
  )"; then
    if [[ "${LIONCLAW_ALLOW_REWRITE:-0}" == "1" ]]; then
      warn "continuing despite managed config mismatches because LIONCLAW_ALLOW_REWRITE=1"
      printf '%s\n' "$validation_output" >&2
      return 0
    fi
    printf 'managed project state mismatch in %s\n' "$config_file" >&2
    printf '%s\n' "$validation_output" >&2
    printf 'Set LIONCLAW_ALLOW_REWRITE=1 to let this script overwrite managed entries.\n' >&2
    return 1
  fi
}

install_project_skills() {
  local spec alias source resolved
  [[ -n "$PROJECT_SKILLS" ]] || return 0

  IFS=',' read -r -a skill_specs <<<"$PROJECT_SKILLS"
  for spec in "${skill_specs[@]}"; do
    spec="$(trim "$spec")"
    [[ -n "$spec" ]] || continue
    alias="${spec%%=*}"
    source="${spec#*=}"
    [[ "$alias" != "$spec" && -n "$alias" && -n "$source" ]] \
      || die "invalid LIONCLAW_PROJECT_SKILLS entry '$spec'; expected alias=path"
    resolved="$(resolve_path "$PROJECT_ROOT" "$source")"
    [[ -d "$resolved" ]] || die "missing project skill source: $resolved"
    run_lionclaw_action skill add "$(abs_dir "$resolved")" --alias "$alias"
  done
}

ensure_project_config() {
  ensure_lionclaw_bin
  ensure_home_onboarded
  validate_managed_config
  ensure_podman

  [[ -d "$TERMINAL_SKILL_SOURCE" ]] || die "missing terminal skill source: $TERMINAL_SKILL_SOURCE"

  run_lionclaw_action runtime add "$RUNTIME_ID" \
    --kind "$RUNTIME_KIND" \
    --bin "$RUNTIME_BIN" \
    --engine "$PODMAN_BIN" \
    --image "$RUNTIME_IMAGE"
  run_lionclaw_action runtime set-default "$RUNTIME_ID"
  run_lionclaw_action skill add "$TERMINAL_SKILL_SOURCE" --alias "$TERMINAL_ALIAS"
  install_project_skills
  run_lionclaw_action channel add "$CHANNEL_ID" --skill "$TERMINAL_ALIAS" --launch interactive
}

doctor() {
  local status=0 bind_addr config_file image_state image_status base_ref recorded_ref current_base_id recorded_base_id
  config_file="$(config_path)"

  print_context
  printf 'Workspace exists:  %s\n' "$( [[ -d "$WORKSPACE_ROOT" ]] && printf yes || printf no )"
  printf 'Config file:       %s\n' "$( [[ -f "$config_file" ]] && printf '%s' "$config_file" || printf 'missing (%s)' "$config_file" )"
  printf 'LionClaw binary:   %s\n' "$( [[ -x "$LIONCLAW_BIN" ]] && printf yes || printf no )"
  printf 'Python3:           %s\n' "$( has_cmd python3 && printf yes || printf no )"
  printf 'Podman:            %s\n' "$( [[ -n "$PODMAN_BIN" && -x "$PODMAN_BIN" ]] && printf yes || printf no )"
  printf 'Terminal skill:    %s\n' "$( [[ -d "$TERMINAL_SKILL_SOURCE" ]] && printf yes || printf no )"

  [[ -d "$WORKSPACE_ROOT" ]] || status=1
  [[ -x "$LIONCLAW_BIN" ]] || status=1
  [[ -n "$PODMAN_BIN" && -x "$PODMAN_BIN" ]] || status=1
  [[ -d "$TERMINAL_SKILL_SOURCE" ]] || status=1

  if [[ -f "$config_file" ]]; then
    bind_addr="$(configured_bind)"
    printf 'Bind:              %s\n' "${bind_addr:-unknown}"
    if validate_managed_config >/dev/null; then
      printf 'Managed entries:   match expected project values\n'
    else
      printf 'Managed entries:   mismatch expected project values\n'
      status=1
    fi
  else
    printf 'Bind:              not onboarded yet\n'
    printf 'Managed entries:   not configured yet\n'
    status=1
  fi

  if [[ -n "$PODMAN_BIN" && -x "$PODMAN_BIN" ]]; then
    image_state="$(runtime_image_status)"
    IFS='|' read -r image_status base_ref recorded_ref current_base_id recorded_base_id <<<"$image_state"
    case "$image_status" in
      fresh)
        printf 'Runtime image:     present and fresh (%s, base=%s)\n' "$RUNTIME_IMAGE" "$base_ref"
        ;;
      present)
        printf 'Runtime image:     present (%s)\n' "$RUNTIME_IMAGE"
        ;;
      missing-runtime)
        printf 'Runtime image:     missing (%s)\n' "$RUNTIME_IMAGE"
        status=1
        ;;
      missing-base)
        printf 'Runtime image:     base image missing (%s)\n' "${base_ref:-unknown}"
        status=1
        ;;
      untracked)
        printf 'Runtime image:     present but untracked (%s)\n' "$RUNTIME_IMAGE"
        status=1
        ;;
      stale-ref|stale-id)
        printf 'Runtime image:     stale (%s)\n' "$image_status"
        status=1
        ;;
      *)
        printf 'Runtime image:     unknown state (%s)\n' "$image_status"
        status=1
        ;;
    esac
  else
    printf 'Runtime image:     skipped (podman unavailable)\n'
  fi

  return "$status"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
script_repo="$(abs_dir "$script_dir/..")"

PROJECT_ROOT="$(resolve_project_root)"
PROJECT_CONTAINERFILE="$(find_project_containerfile "$PROJECT_ROOT")"
WORKSPACE_ROOT="$(resolve_default_workspace_root "$PROJECT_ROOT" "$PROJECT_CONTAINERFILE")"
export LIONCLAW_HOME="$(resolve_default_home "$PROJECT_ROOT")"
LIONCLAW_REPO="$(resolve_lionclaw_repo "$script_repo" "$PROJECT_ROOT")"
LIONCLAW_BIN="$(resolve_lionclaw_bin "$LIONCLAW_REPO")"
RUNTIME_ID="${LIONCLAW_RUNTIME_ID:-codex}"
RUNTIME_KIND="${LIONCLAW_RUNTIME_KIND:-codex}"
RUNTIME_BIN="${LIONCLAW_RUNTIME_BIN:-$RUNTIME_ID}"
if [[ -n "${LIONCLAW_RUNTIME_IMAGE:-}" ]]; then
  RUNTIME_IMAGE="$LIONCLAW_RUNTIME_IMAGE"
elif [[ -n "$PROJECT_CONTAINERFILE" ]]; then
  RUNTIME_IMAGE="$(runtime_image_name_for_workspace "$WORKSPACE_ROOT")"
else
  RUNTIME_IMAGE="lionclaw-runtime:v1"
fi
PODMAN_BIN="${LIONCLAW_PODMAN_BIN:-$(command -v podman || true)}"
CHANNEL_ID="${LIONCLAW_CHANNEL_ID:-terminal}"
TERMINAL_ALIAS="${LIONCLAW_TERMINAL_ALIAS:-terminal}"
PROJECT_SKILLS="${LIONCLAW_PROJECT_SKILLS:-$(discover_project_skills "$PROJECT_ROOT")}"
RUNTIME_IMAGE_BASE_REF_LABEL="io.lionclaw.base-image-ref"
RUNTIME_IMAGE_BASE_ID_LABEL="io.lionclaw.base-image-id"

if [[ -n "${LIONCLAW_TERMINAL_SKILL_SOURCE:-}" ]]; then
  TERMINAL_SKILL_SOURCE="$(abs_dir "$LIONCLAW_TERMINAL_SKILL_SOURCE")"
elif [[ -n "$LIONCLAW_REPO" ]]; then
  TERMINAL_SKILL_SOURCE="$LIONCLAW_REPO/skills/channel-terminal"
else
  TERMINAL_SKILL_SOURCE=""
fi

if [[ -n "${LIONCLAW_RUNTIME_CONTAINERFILE:-}" ]]; then
  runtime_containerfile_path="$(resolve_path "$PROJECT_ROOT" "$LIONCLAW_RUNTIME_CONTAINERFILE")"
  if ! RUNTIME_IMAGE_CONTAINERFILE="$(abs_file "$runtime_containerfile_path")"; then
    die "invalid LIONCLAW_RUNTIME_CONTAINERFILE parent: $(dirname "$runtime_containerfile_path")"
  fi
  [[ -f "$RUNTIME_IMAGE_CONTAINERFILE" ]] \
    || die "missing runtime Containerfile: $RUNTIME_IMAGE_CONTAINERFILE"
elif [[ -n "$PROJECT_CONTAINERFILE" ]]; then
  RUNTIME_IMAGE_CONTAINERFILE="$PROJECT_CONTAINERFILE"
elif [[ -n "$LIONCLAW_REPO" ]]; then
  RUNTIME_IMAGE_CONTAINERFILE="$LIONCLAW_REPO/containers/runtime/Containerfile"
else
  RUNTIME_IMAGE_CONTAINERFILE="$PROJECT_ROOT/Containerfile"
fi

if [[ -n "${LIONCLAW_IMAGE_CONTEXT:-}" ]]; then
  RUNTIME_IMAGE_CONTEXT="$(abs_dir "$(resolve_path "$PROJECT_ROOT" "$LIONCLAW_IMAGE_CONTEXT")")"
elif [[ -n "$LIONCLAW_REPO" && "$RUNTIME_IMAGE_CONTAINERFILE" == "$LIONCLAW_REPO/containers/runtime/Containerfile" ]]; then
  RUNTIME_IMAGE_CONTEXT="$LIONCLAW_REPO"
else
  RUNTIME_IMAGE_CONTEXT="$(dirname "$RUNTIME_IMAGE_CONTAINERFILE")"
fi

cmd="${1:-help}"
shift $(( $# > 0 ? 1 : 0 ))

case "$cmd" in
  doctor)
    doctor
    ;;
  configure)
    print_context
    ensure_project_config
    printf 'Bind:              %s\n' "$(configured_bind || true)"
    ;;
  build-image)
    [[ $# -eq 0 ]] || die "usage: ./scripts/lionclaw-project.sh build-image"
    print_context
    ensure_runtime_image 0
    ;;
  rebuild-image)
    [[ $# -eq 0 ]] || die "usage: ./scripts/lionclaw-project.sh rebuild-image"
    print_context
    ensure_runtime_image 1
    ;;
  run)
    print_context
    ensure_runtime_image
    ensure_project_config
    if is_dry_run; then
      printf '[dry-run] would run runtime=%s from %s\n' "$RUNTIME_ID" "$WORKSPACE_ROOT"
      exit 0
    fi
    cd "$WORKSPACE_ROOT"
    exec env LIONCLAW_HOME="$LIONCLAW_HOME" "$LIONCLAW_BIN" run "$@" "$RUNTIME_ID"
    ;;
  attach)
    print_context
    ensure_runtime_image
    ensure_project_config
    if is_dry_run; then
      printf '[dry-run] would attach channel=%s runtime=%s from %s\n' "$CHANNEL_ID" "$RUNTIME_ID" "$WORKSPACE_ROOT"
      exit 0
    fi
    cd "$WORKSPACE_ROOT"
    exec env LIONCLAW_HOME="$LIONCLAW_HOME" "$LIONCLAW_BIN" channel attach "$CHANNEL_ID" "$@" --runtime "$RUNTIME_ID"
    ;;
  up)
    ensure_systemd_user
    print_context
    ensure_runtime_image
    ensure_project_config
    run_lionclaw_action service up --runtime "$RUNTIME_ID"
    ;;
  down)
    ensure_systemd_user
    print_context
    ensure_lionclaw_bin
    if [[ ! -f "$(config_path)" ]]; then
      printf 'Project home is not onboarded yet; nothing to stop.\n'
      exit 0
    fi
    run_lionclaw_action service down
    ;;
  status)
    ensure_systemd_user
    print_context
    ensure_lionclaw_bin
    if [[ ! -f "$(config_path)" ]]; then
      printf 'Project home is not onboarded yet.\n'
      exit 0
    fi
    printf 'Bind:              %s\n' "$(configured_bind || true)"
    run_lionclaw service status
    ;;
  logs)
    ensure_systemd_user
    print_context
    ensure_lionclaw_bin
    if [[ ! -f "$(config_path)" ]]; then
      printf 'Project home is not onboarded yet.\n'
      exit 0
    fi
    run_lionclaw service logs "$@"
    ;;
  pairing-list)
    print_context
    ensure_lionclaw_bin
    if [[ ! -f "$(config_path)" ]]; then
      printf 'Project home is not onboarded yet.\n'
      exit 0
    fi
    run_lionclaw channel pairing list --channel-id "$CHANNEL_ID"
    ;;
  pairing-approve)
    print_context
    ensure_lionclaw_bin
    if [[ ! -f "$(config_path)" ]]; then
      printf 'Project home is not onboarded yet.\n'
      exit 0
    fi
    peer_id="${1:-}"
    pairing_code="${2:-}"
    trust_tier="${3:-main}"
    [[ -n "$peer_id" && -n "$pairing_code" ]] || die "usage: ./scripts/lionclaw-project.sh pairing-approve <peer-id> <code> [trust-tier]"
    run_lionclaw_action channel pairing approve "$CHANNEL_ID" "$peer_id" "$pairing_code" --trust-tier "$trust_tier"
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 64
    ;;
esac
