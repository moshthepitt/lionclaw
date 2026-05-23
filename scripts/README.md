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

## Usage

Run the same checks as GitHub CI:

```bash
./scripts/ci.sh
```
