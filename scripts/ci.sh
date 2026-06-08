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
bash -n skills/lionclaw-private-context/scripts/context
bash -n skills/lionclaw-private-context/scripts/projector
bash -n skills/channel-team-local/runtime/team-local/scripts/list
bash -n skills/channel-team-local/runtime/team-local/scripts/resolve
bash -n skills/channel-team-local/runtime/team-local/scripts/send
test -f skills/channel-email/README.md
test -f skills/channel-email/runtime/email/SKILL.md
test -f skills/channel-email/runtime/email/EMAIL_WORKFLOW_GUIDE.md
test -f skills/channel-team-local/README.md
test -f skills/channel-telegram/README.md
test -f skills/lionclaw-private-context/README.md
test -f skills/lionclaw-private-context/SKILL.md
test -f skills/lionclaw-private-context/lionclaw.toml
grep -q '^name: email$' skills/channel-email/runtime/email/SKILL.md
grep -q '^name: lionclaw-private-context$' skills/lionclaw-private-context/SKILL.md
