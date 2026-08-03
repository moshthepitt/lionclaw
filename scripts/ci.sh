#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt -- --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace

# Exercise the release artifact from an unrelated working directory and a
# clean LionClaw home through the same packaging path used by releases.
PACKAGE_DIST=$(mktemp -d)
trap 'rm -rf "$PACKAGE_DIST"' EXIT
cargo build -p lionclaw --bin lionclaw
VERSION=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name == "lionclaw") | .version')
bash ./scripts/package-release.sh \
    target/debug/lionclaw \
    "$VERSION" \
    linux-x86_64 \
    "$PACKAGE_DIST"

# Runtime invariant guards need Podman and the runtime image. The external
# driver additionally needs delegated CPU and memory controllers because its
# kernel-owned limits fail closed when the host cannot enforce them.
if command -v podman >/dev/null 2>&1 &&
    podman image exists localhost/lionclaw-runtime-dev:v1 2>/dev/null; then
    controllers=" $(podman info --format '{{range .Host.CgroupControllers}}{{.}} {{end}}' 2>/dev/null || true)"
    if [[ "$controllers" == *" cpu "* && "$controllers" == *" memory "* ]]; then
        bash ./scripts/external-oracle-oci.sh
    else
        echo "skipping external oracle OCI test (CPU/memory cgroup controllers unavailable)"
    fi
    cargo run -q -p lionclaw -- mission self-test
else
    echo "skipping Podman tests (podman or runtime image unavailable)"
fi
