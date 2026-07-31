#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

image=localhost/lionclaw-runtime-dev:v1

command -v podman >/dev/null 2>&1 || {
    echo "external oracle OCI test requires podman" >&2
    exit 1
}
podman image exists "$image" || {
    echo "external oracle OCI test requires image $image" >&2
    exit 1
}

controllers=" $(podman info --format '{{range .Host.CgroupControllers}}{{.}} {{end}}')"
for required in cpu memory; do
    [[ "$controllers" == *" $required "* ]] || {
        echo "external oracle OCI test requires delegated $required cgroup controller" >&2
        exit 1
    }
done

cargo test -p lionclaw \
    --test external_oracles \
    production_external_driver_uses_kernel_broker_without_container_credentials \
    -- --ignored --exact
