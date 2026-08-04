#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

# Exercise the release artifact from an unrelated working directory and a
# clean LionClaw home through the same packaging path used by releases.
PACKAGE_DIST=$(mktemp -d)
trap 'rm -rf "$PACKAGE_DIST"' EXIT

cargo build -p lionclaw --bin lionclaw
METADATA=$(cargo metadata --no-deps --format-version 1)
VERSION=$(jq -r '.packages[] | select(.name == "lionclaw") | .version' <<<"$METADATA")
TARGET_DIR=$(jq -r '.target_directory' <<<"$METADATA")

bash ./scripts/package-release.sh \
    "$TARGET_DIR/debug/lionclaw" \
    "$VERSION" \
    linux-x86_64 \
    "$PACKAGE_DIST"
