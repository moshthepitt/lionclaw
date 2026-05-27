#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt -- --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace
uv run --project skills/channel-telegram black --check skills/channel-telegram/lionclaw_channel_telegram skills/channel-telegram/tests
uv run --project skills/channel-telegram ruff check skills/channel-telegram/lionclaw_channel_telegram skills/channel-telegram/tests
uv run --project skills/channel-telegram python -m unittest discover -s skills/channel-telegram/tests
bash -n skills/channel-telegram/scripts/worker
bash -n skills/channel-email/scripts/worker
bash -n skills/channel-team-local/scripts/worker
bash -n skills/channel-team-local/runtime/team-local/scripts/list
bash -n skills/channel-team-local/runtime/team-local/scripts/resolve
bash -n skills/channel-team-local/runtime/team-local/scripts/send
test -f skills/email-work-inbox/SKILL.md
test -f skills/email-work-inbox/EMAIL_WORKFLOW_GUIDE.md
