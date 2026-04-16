#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

cargo fmt -- --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo test
