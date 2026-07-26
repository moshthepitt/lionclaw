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

## FrontierSWE Dependent Type Checker Image

`containers/frontierswe-dependent-type-checker/Containerfile` composes the
pinned dependent-type-checker toolchain and prepared public workspace onto the
digest-pinned LionClaw dev image. It deliberately contains no Harbor tests,
reference implementation, or scoring data. Build it only on the host; image
building from a confined mission is outside LionClaw's authority model.

Fetch the pinned task and build the image with:

```bash
python3 benchmark/frontierswe/fetch_task.py \
  --output .tmp/frontierswe-dependent-type-checker
benchmark/frontierswe/build-image.sh \
  .tmp/frontierswe-dependent-type-checker
```

The build script prints the immutable repository digest. Record that exact
`name@sha256:...` reference in the mission type and benchmark bundle before a
run. The build context comes from the pinned FrontierSWE commit in
`benchmark/frontierswe/bundle.toml`; mutable upstream image tags are never used
as a build base.

The bundle uses a native Podman Harbor environment adapter. It accepts only a
prebuilt digest, preserves the image during Harbor cleanup, and enforces the
task container's `network=none`. On a host without delegated CPU, memory, or
storage quota controls, the adapter fails closed unless a bring-up run
explicitly enables `allow_unenforced_resources=true`. That exception is
recorded as a non-publishable invalidation in the mission report.
