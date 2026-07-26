#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARBOR_VERSION="$(
  python3 - "$SCRIPT_DIR/bundle.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    print(tomllib.load(handle)["harbor"]["version"])
PY
)"

uv tool install --force "harbor==$HARBOR_VERSION"
harbor --version | grep -Fx "$HARBOR_VERSION"
