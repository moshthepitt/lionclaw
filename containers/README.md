# Containers

LionClaw keeps the default runtime image separate from project development
images.

## Runtime Image

`containers/runtime/Containerfile` is the product runtime image. It contains the
agent CLIs, common assistant tools, and the image-compatible `lionclaw` binary
used for destination-scoped network proxying.

The image pins the tested `@openai/codex` and `opencode-ai` package versions in
the Containerfile and fails the build if the installed global packages do not
match those pins. The build also verifies `lionclaw __network-proxy` and
`lionclaw __network-proxy-health` so role containers can stay on an internal
effect network while the proxy container owns the only egress attachment. At
runtime LionClaw probes the health command inside the proxy container before
launching the workload, so a running proxy process is not treated as ready until
its HTTP and SOCKS listeners are bound.

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

## External Oracle Drivers

External oracle drivers are operator-installed programs in the pinned runtime
image. The fixed launcher name is `lionclaw-external-oracle-driver`; missions
may name only a configured driver id and bounded typed request data, never an
executable path, shell command, adapter name, or credential bytes.

Declare installed drivers in the runtime profile and scope their own network
authority there. The selected runtime image digest, driver id, network grant,
and non-secret auth configuration identity are resolved into mission state when
the plan is admitted, so later runtime profile edits cannot silently change an
inflight oracle's authority.

```toml
[runtimes.codex.external-oracle-drivers.local-ci]
network = { mode = "allow", destinations = [
  { host = "ci.example.com", ports = [443] }
] }
auth = { kind = "native-home", source = "/home/operator/.ci-token" }
```

LionClaw invokes the launcher with JSON submit/poll requests on stdin. Driver
credentials stay in kernel-owned auth staging, scoped to the effect, and are
never placed in mission requests, argv, environment variables, events, blobs,
reports, or logs. External drivers currently accept `native-home` auth
configuration only.
