#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt -- --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo test
uv run --project skills/channel-telegram black --check skills/channel-telegram/lionclaw_channel_telegram skills/channel-telegram/tests
uv run --project skills/channel-telegram ruff check skills/channel-telegram/lionclaw_channel_telegram skills/channel-telegram/tests
uv run --project skills/channel-telegram python -m unittest discover -s skills/channel-telegram/tests
bash -n skills/channel-telegram/scripts/worker
