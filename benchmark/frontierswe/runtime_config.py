"""Runtime-profile helpers for FrontierSWE benchmark bring-up."""

from __future__ import annotations

import argparse
import json
import os
import tomllib
from pathlib import Path


def resolve_codex_config_model(codex_home: Path | None = None) -> str | None:
    root = codex_home or Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()
    config = root / "config.toml"
    try:
        data = tomllib.loads(config.read_text())
    except (FileNotFoundError, tomllib.TOMLDecodeError):
        return None
    model = data.get("model")
    return model.strip() if isinstance(model, str) and model.strip() else None


def codex_runtime_profile(model: str) -> str:
    quoted_model = json.dumps(model)
    return (
        "[runtimes.codex]\n"
        'driver = "codex"\n'
        'command = "codex"\n'
        "native-resume = true\n"
        'auth = "codex"\n'
        'skills-dir = ".agents/skills"\n'
        f"model = {quoted_model}\n"
        'confinement = { backend = "podman", read-only-rootfs = true, '
        'tmpfs = ["/tmp:rw,size=512m"] }\n'
    )


def write_codex_runtime_profile(lionclaw_home: Path, model: str) -> Path:
    path = lionclaw_home / "runtimes.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(codex_runtime_profile(model))
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)

    resolve = subcommands.add_parser("resolve-model")
    resolve.add_argument("--codex-home", type=Path)

    write = subcommands.add_parser("write-codex-profile")
    write.add_argument("--lionclaw-home", type=Path, required=True)
    write.add_argument("--model", required=True)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "resolve-model":
        model = resolve_codex_config_model(args.codex_home)
        if model:
            print(model)
        return
    if args.command == "write-codex-profile":
        print(write_codex_runtime_profile(args.lionclaw_home, args.model))
        return
    raise AssertionError(f"unhandled command {args.command}")


if __name__ == "__main__":
    main()
