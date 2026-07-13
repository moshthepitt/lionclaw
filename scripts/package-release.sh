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
cp "$BINARY" "$ROOT/lionclaw"
cp LICENSE crates/lionclaw/LICENSE-zenith "$ROOT/"
"$ROOT/lionclaw" man > "$ROOT/share/man/man1/lionclaw.1"

test -x "$ROOT/lionclaw"
test "$(find "$ROOT" -name SKILL.md -type f | wc -l)" -eq 1
test -s "$ROOT/share/man/man1/lionclaw.1"
LIONCLAW_HOME="$TEST_HOME" "$ROOT/lionclaw" install
LIONCLAW_HOME="$TEST_HOME" "$ROOT/lionclaw" mission type check software-dev

tar -C "$STAGE" -czf "$DIST/$ASSET" lionclaw
(
  cd "$DIST"
  sha256sum "$ASSET" > SHA256SUMS
)
