# Scripts

These scripts are developer and CI smoke helpers. They are not the normal
LionClaw setup path.

Use the product CLI for everyday work:

```bash
lionclaw project init
lionclaw configure --runtime codex
lionclaw run
```

## Available Scripts

- `ci.sh`: local CI gate that mirrors the GitHub Actions `ci` workflow.
- `bootstrap-terminal-test.sh`: creates or refreshes a manual terminal-channel
  smoke home, configures a runtime image and terminal channel, then attaches in
  the current TTY.
- `attach-terminal-test.sh`: rebuilds LionClaw, restarts the managed daemon for
  a specific smoke-test home, and attaches the interactive terminal channel in
  the current TTY.

## Usage

Run the same checks as GitHub CI:

```bash
./scripts/ci.sh
```

Create a fresh terminal-channel smoke home on Linux with a systemd user manager:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e
```

The bootstrap script is for local development feedback. It creates disposable
state, builds the shared local runtime image when missing, and lets
`lionclaw up` allocate a managed bind for the smoke home.

Override the runtime id, command, or channel:

```bash
./scripts/bootstrap-terminal-test.sh /tmp/lionclaw-terminal-e2e work codex terminal
```

Rebuild, restart, and attach the terminal smoke channel:

```bash
./scripts/attach-terminal-test.sh /tmp/lionclaw-terminal-e2e
```
