# Containers

LionClaw keeps the default runtime image separate from project development
images.

## Runtime Image

`containers/runtime/Containerfile` is the product runtime image. It contains the
agent CLIs and common assistant tools a mission's roles and oracles run under.

The image pins the tested `@openai/codex` and `opencode-ai` package versions in
the Containerfile and fails the build if the installed global packages do not
match those pins.

```bash
podman build -t lionclaw-runtime:v1 -f containers/runtime/Containerfile .
```

## LionClaw Dev Image

`containers/dev/Containerfile` layers LionClaw development tooling on top of
the runtime image. It includes the pinned Rust toolchain from
`rust-toolchain.toml`, `rustfmt`, `clippy`, `rust-analyzer`, `rust-src`, native build dependencies,
SQLite development headers, and basic debugging tools. Cargo caches live under
`/runtime` for writable runtime state.

```bash
podman build \
  --build-arg RUNTIME_IMAGE=lionclaw-runtime:v1 \
  -t lionclaw-runtime-dev:v1 \
  -f containers/dev/Containerfile .
```

Run the repository gate in the dev image with:

```bash
podman run --rm -it \
  --userns=keep-id:uid=1001,gid=1001 \
  -v "$PWD:/workspace:Z" \
  -w /workspace \
  lionclaw-runtime-dev:v1 \
  bash ./scripts/ci.sh
```

The `keep-id` mapping targets the image's `lionclaw` user so rootless Podman
can write build outputs into the host-owned checkout while keeping the
container-owned cargo and uv caches under `/runtime` writable.

A mission type declares the image it runs under in its `mission.toml`
(`image = "localhost/lionclaw-runtime-dev:v1"`); build the image under that tag,
or override per mission with `lionclaw mission start --image <ref>`. The tag is
resolved to a content id once at `start` and pinned for the mission's life.
