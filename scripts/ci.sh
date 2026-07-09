#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt -- --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace

# Runtime invariant guard: the mission self-test drives the real stack (real
# podman confinement + real engine-run oracle). Model-auth-free, but it needs
# podman and the runtime image. Skip cleanly where either is absent.
if command -v podman >/dev/null 2>&1 \
    && podman image exists localhost/lionclaw-runtime-dev:v1 2>/dev/null; then
    cargo run -q -p lionclaw-cli -- mission self-test
else
    echo "skipping mission self-test (podman or runtime image unavailable)"
fi
