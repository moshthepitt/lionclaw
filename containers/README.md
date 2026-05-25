# Containers

LionClaw keeps the default runtime image separate from project development
images.

## Runtime Image

`containers/runtime/Containerfile` is the product runtime image. It contains the
agent CLIs and common assistant tools used by `lionclaw run`.

```bash
podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .
```

## LionClaw Dev Image

`containers/dev/Containerfile` layers Rust tooling on top of the runtime image
for working on LionClaw itself. It includes the pinned Rust toolchain,
`rustfmt`, `clippy`, `rust-analyzer`, `rust-src`, native build dependencies,
SQLite development headers, and basic debugging tools.

```bash
podman build \
  --build-arg RUNTIME_IMAGE=lionclaw-runtime:v1 \
  -t lionclaw-runtime-dev:v1 \
  -f containers/dev/Containerfile .
```

Use the dev image for this checkout with:

```bash
lionclaw runtime add codex --kind codex --bin codex --image lionclaw-runtime-dev:v1
lionclaw runtime set-default codex
```
