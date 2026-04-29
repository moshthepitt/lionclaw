#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt -- --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo test
uv run --project skills/channel-terminal python -m unittest discover -s skills/channel-terminal/tests
bash -n skills/channel-terminal/scripts/worker
bash -n skills/channel-terminal/scripts/debug-worker.sh
bash -n skills/channel-telegram/scripts/worker
