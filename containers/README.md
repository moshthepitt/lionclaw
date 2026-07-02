# Containers

LionClaw keeps the default runtime image separate from project development
images.

## Runtime Image

`containers/runtime/Containerfile` is the product runtime image. It contains the
agent CLIs and common assistant tools used by `lionclaw run`.

The image pins the tested `@openai/codex` and `opencode-ai` package versions in
the Containerfile and fails the build if the installed global packages do not
match those pins.

```bash
podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .
```

## LionClaw Dev Image

`containers/dev/Containerfile` layers LionClaw development tooling on top of
the runtime image. It includes the pinned Rust toolchain from
`rust-toolchain.toml`, `rustfmt`, `clippy`, `rust-analyzer`, `rust-src`, `uv`,
Python 3.12 for the Python skill checks, native build dependencies, SQLite
development headers, and basic debugging tools.

```bash
podman build \
  --build-arg RUNTIME_IMAGE=lionclaw-runtime:v1 \
  -t lionclaw-runtime-dev:v1 \
  -f containers/dev/Containerfile .
```

Run the repository gate in the dev image with:

```bash
podman run --rm -it \
  -v "$PWD:/workspace:Z" \
  -w /workspace \
  lionclaw-runtime-dev:v1 \
  bash ./scripts/ci.sh
```

Use the dev image for this checkout with:

```bash
lionclaw runtime add codex --kind codex --bin codex --image lionclaw-runtime-dev:v1
lionclaw runtime set-default codex
```
