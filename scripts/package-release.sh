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
VERIFY=$(mktemp -d)
trap 'rm -rf "$STAGE" "$TEST_HOME" "$VERIFY"' EXIT

ROOT="$STAGE/lionclaw"
mkdir -p "$ROOT/share/man/man1" "$DIST"
cp skills/lionclaw/SKILL.md "$ROOT/SKILL.md"
cp README.md "$ROOT/README.md"
cp "$BINARY" "$ROOT/lionclaw"
cp LICENSE crates/lionclaw/LICENSE-zenith "$ROOT/"
"$ROOT/lionclaw" man > "$ROOT/share/man/man1/lionclaw.1"

tar -C "$STAGE" -czf "$DIST/$ASSET" lionclaw
tar -C "$VERIFY" -xzf "$DIST/$ASSET"
PACKAGE_ROOT="$VERIFY/lionclaw"

test -x "$PACKAGE_ROOT/lionclaw"
test -s "$PACKAGE_ROOT/README.md"
test "$(find "$PACKAGE_ROOT" -name SKILL.md -type f | wc -l)" -eq 1
test -s "$PACKAGE_ROOT/share/man/man1/lionclaw.1"
test "$("$PACKAGE_ROOT/lionclaw" --version)" = "lionclaw $VERSION"
(cd "$PACKAGE_ROOT" && ./lionclaw --help >/dev/null)
(cd "$PACKAGE_ROOT" && ./lionclaw run codex --help >/dev/null)
(cd "$PACKAGE_ROOT" && ./lionclaw mission plan show --help >/dev/null)
(cd "$PACKAGE_ROOT" && ./lionclaw man mission plan show >/dev/null)
if DOCTOR_JSON=$(cd "$PACKAGE_ROOT" && env -u CODEX_HOME HOME="$TEST_HOME" LIONCLAW_HOME="$TEST_HOME" ./lionclaw doctor --json); then
  echo "doctor unexpectedly passed with a clean home" >&2
  exit 1
fi
if ! jq -e '
  type == "object" and
  (keys | sort) == ["checks", "ok", "schema"] and
  .schema == "lionclaw.doctor.v1" and
  (.ok | type) == "boolean" and
  (.checks | type) == "array" and
  (.checks | length) > 0 and
  all(.checks[];
    type == "object" and
    (has("name") and has("status")) and
    ([keys[] | select(. != "name" and . != "status" and . != "detail" and . != "retryable" and . != "repair")] | length) == 0 and
    (.name | type == "string" and length > 0) and
    (.status == "pass" or .status == "fail") and
    (.retryable | type == "boolean") and
    ((has("detail") | not) or (.detail | type == "string" and length > 0)) and
    (if .status == "fail" then (.repair | type == "string" and length > 0) else (has("repair") | not) end)
  ) and
  (.ok == (.checks | all(.status == "pass"))) and
  (.ok == false)
' >/dev/null <<<"$DOCTOR_JSON"; then
  echo "doctor did not emit a truthful typed JSON report with a clean home" >&2
  exit 1
fi
(cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$PACKAGE_ROOT/lionclaw" install)
EXPECTED_TYPES=$'design\noptimization\nresearch\nreview\nsoftware-dev'
ACTUAL_TYPES=$(cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$PACKAGE_ROOT/lionclaw" mission type list)
if [[ "$ACTUAL_TYPES" != "$EXPECTED_TYPES" ]]; then
  echo "clean install has unexpected mission types:" >&2
  printf '%s\n' "$ACTUAL_TYPES" >&2
  exit 1
fi
for mission_type in design optimization research review software-dev; do
  (cd "$TEST_HOME" && LIONCLAW_HOME="$TEST_HOME" "$PACKAGE_ROOT/lionclaw" mission type check "$mission_type")
done

(
  cd "$DIST"
  sha256sum "$ASSET" > SHA256SUMS
  sha256sum -c SHA256SUMS
)
