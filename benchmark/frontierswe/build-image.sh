#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUNDLE_ROOT="${1:?usage: build-image.sh FETCHED_BUNDLE_ROOT}"
TASK_ROOT="$BUNDLE_ROOT/task"

if [[ ! -f "$TASK_ROOT/environment/workspace/instruction.md" ]]; then
  echo "missing fetched dependent-type-checker workspace under $TASK_ROOT" >&2
  exit 2
fi

readarray -t IMAGE_CONFIG < <(
  python3 - "$SCRIPT_DIR/bundle.toml" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as handle:
    image = tomllib.load(handle)["image"]
print(image["base"])
print(image["tag"])
PY
)
BASE_IMAGE="${IMAGE_CONFIG[0]}"
IMAGE_TAG="${IMAGE_CONFIG[1]}"

podman image exists "$BASE_IMAGE"
podman build \
  --timestamp 0 \
  --build-arg "LIONCLAW_DEV_IMAGE=$BASE_IMAGE" \
  --tag "$IMAGE_TAG" \
  --file "$REPO_ROOT/containers/frontierswe-dependent-type-checker/Containerfile" \
  "$TASK_ROOT"

IMAGE_REFERENCE="$(
  podman image inspect "$IMAGE_TAG" \
    --format '{{index .RepoDigests 0}}'
)"
if [[ "$IMAGE_REFERENCE" != *@sha256:* ]]; then
  echo "build did not produce an immutable repository digest: $IMAGE_REFERENCE" >&2
  exit 1
fi
printf '%s\n' "$IMAGE_REFERENCE"
