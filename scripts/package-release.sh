#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <lionclaw-binary> <version> <target> <dist-dir>" >&2
  exit 2
fi

BINARY=$1
VERSION=$2
TARGET=$3
DIST=$4
ASSET="lionclaw-v${VERSION}-${TARGET}.tar.gz"
STAGE=$(mktemp -d)
TEST_HOME=$(mktemp -d)
trap 'rm -rf "$STAGE" "$TEST_HOME"' EXIT

ROOT="$STAGE/lionclaw"
mkdir -p "$ROOT/share/man/man1" "$DIST"
cp skills/lionclaw/SKILL.md "$ROOT/SKILL.md"
cp README.md "$ROOT/README.md"
cp "$BINARY" "$ROOT/lionclaw"
cp LICENSE crates/lionclaw/LICENSE-zenith "$ROOT/"
"$ROOT/lionclaw" man > "$ROOT/share/man/man1/lionclaw.1"

test -x "$ROOT/lionclaw"
test -s "$ROOT/README.md"
test "$(find "$ROOT" -name SKILL.md -type f | wc -l)" -eq 1
test -s "$ROOT/share/man/man1/lionclaw.1"
test "$("$ROOT/lionclaw" --version)" = "lionclaw $VERSION"
"$ROOT/lionclaw" --help >/dev/null
"$ROOT/lionclaw" mission plan show --help >/dev/null
"$ROOT/lionclaw" man mission plan show >/dev/null
if DOCTOR_JSON=$(cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$ROOT/lionclaw" doctor --json); then
  echo "doctor unexpectedly passed with a clean home" >&2
  exit 1
fi
if [[ "$DOCTOR_JSON" != *'"schema":"lionclaw.doctor.v1"'* || "$DOCTOR_JSON" != *'"ok":false'* ]]; then
  echo "doctor did not emit truthful JSON with a clean home" >&2
  exit 1
fi
(cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$ROOT/lionclaw" install)
EXPECTED_TYPES=$'design\noptimization\nresearch\nreview\nsoftware-dev'
ACTUAL_TYPES=$(cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$ROOT/lionclaw" mission type list)
if [[ "$ACTUAL_TYPES" != "$EXPECTED_TYPES" ]]; then
  echo "clean install has unexpected mission types:" >&2
  printf '%s\n' "$ACTUAL_TYPES" >&2
  exit 1
fi
for mission_type in design optimization research review software-dev; do
  (cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$ROOT/lionclaw" mission type check "$mission_type")
done

tar -C "$STAGE" -czf "$DIST/$ASSET" lionclaw
(
  cd "$DIST"
  sha256sum "$ASSET" > SHA256SUMS
)
