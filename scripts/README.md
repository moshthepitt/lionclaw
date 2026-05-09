# Scripts

This directory contains the sharp tools.

## Why this exists
Most users should stay on the main `lionclaw` path: register a real agent
runtime, run it through LionClaw, and add channels or jobs through the CLI.

These scripts are here for local CI and manual terminal-channel smoke testing.
Manual setup. Debugging. Tight feedback loops.

Use them when you mean it.

## Available scripts
- `ci.sh`: runs the local CI gate, mirrors the GitHub Actions `ci` workflow, and is the preferred pre-push verification entrypoint.
- `bootstrap-terminal-test.sh`: bootstraps or refreshes a manual terminal-channel
  test home, gives a fresh home its own loopback bind, configures the runtime
  image and terminal channel, then attaches it in the current TTY.
- `attach-terminal-test.sh`: rebuilds LionClaw, stops the managed daemon for a specific `LIONCLAW_HOME`, and attaches the interactive terminal channel in the current TTY.

## Usage
Run the same checks as GitHub CI:
```bash
./scripts/ci.sh
```

Fresh terminal-channel test home in one command on Linux with the systemd user
manager:
```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

That command initializes a fresh project instance and lets `lionclaw up`
allocate the managed bind, so manual test projects do not collide with another
LionClaw daemon already using the default bind. It configures the runtime with
`lionclaw-runtime:v1` and builds that shared local image first when it is
missing. The attach step uses
LionClaw's managed daemon path, which currently needs the systemd user manager.

Override the runtime id, command, or channel:
```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e work codex terminal
```

Override the runtime image or kind with environment variables:
```bash
LIONCLAW_RUNTIME_IMAGE=lionclaw-runtime:v2 \
LIONCLAW_RUNTIME_KIND=opencode \
  ./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e opencode opencode terminal
```

Rebuild + restart + attach the terminal test channel:
```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

Override the runtime or channel:
```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e codex terminal
```
