#!/usr/bin/env python3
"""Fetch the pinned FrontierSWE task without cloning unrelated large assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tomllib
import urllib.request
from pathlib import Path
from typing import Any


BUNDLE_PATH = Path(__file__).with_name("bundle.toml")
ROOT_FILES = ("SCORING.md", "pyproject.toml", "scripts/score_from_reward.py", "uv.lock")
IMAGE_LINE = re.compile(r'^docker_image = "[^"]+"$', re.MULTILINE)


def read_bundle() -> dict[str, Any]:
    with BUNDLE_PATH.open("rb") as handle:
        return tomllib.load(handle)


def request_bytes(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "lionclaw-slice10a"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def fetch_tree(repository: str, commit: str) -> list[dict[str, Any]]:
    slug = repository.removeprefix("https://github.com/")
    url = f"https://api.github.com/repos/{slug}/git/trees/{commit}?recursive=1"
    payload = json.loads(request_bytes(url))
    if payload.get("truncated"):
        raise RuntimeError("GitHub returned a truncated FrontierSWE tree")
    return payload["tree"]


def write_blob(repository: str, commit: str, path: str, destination: Path) -> str:
    slug = repository.removeprefix("https://github.com/")
    data = request_bytes(f"https://raw.githubusercontent.com/{slug}/{commit}/{path}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def pin_task_image(task_toml: Path, image: str) -> str:
    original = task_toml.read_text()
    matches = IMAGE_LINE.findall(original)
    if len(matches) != 1:
        raise RuntimeError(f"expected one docker_image entry in {task_toml}, found {len(matches)}")
    upstream = matches[0].split('"', 2)[1]
    task_toml.write_text(IMAGE_LINE.sub(f'docker_image = "{image}"', original))
    return upstream


def fetch(output: Path) -> dict[str, Any]:
    if output.exists() and any(output.iterdir()):
        raise RuntimeError(f"refusing to overwrite non-empty output directory: {output}")

    config = read_bundle()
    frontier = config["frontierswe"]
    repository = frontier["repository"]
    commit = frontier["commit"]
    task_name = frontier["task"]
    task_prefix = f"tasks/{task_name}/"
    output.mkdir(parents=True, exist_ok=True)

    selected: list[tuple[str, Path, str]] = []
    for entry in fetch_tree(repository, commit):
        path = entry["path"]
        if entry["type"] != "blob" or not path.startswith(task_prefix):
            continue
        relative = path.removeprefix(task_prefix)
        selected.append((path, output / "task" / relative, entry["mode"]))

    if not selected:
        raise RuntimeError(f"task {task_name!r} was absent from FrontierSWE commit {commit}")

    digests: dict[str, str] = {}
    for source, destination, mode in selected:
        digests[source] = write_blob(repository, commit, source, destination)
        if mode == "100755":
            destination.chmod(destination.stat().st_mode | 0o111)

    for source in ROOT_FILES:
        digests[source] = write_blob(repository, commit, source, output / "upstream" / source)

    upstream_image = pin_task_image(output / "task" / "task.toml", config["image"]["reference"])
    source_record = {
        "schema_version": 1,
        "repository": repository,
        "commit": commit,
        "task": task_name,
        "task_path": task_prefix.rstrip("/"),
        "upstream_image": upstream_image,
        "local_image": config["image"]["reference"],
        "files": dict(sorted(digests.items())),
    }
    (output / "source.json").write_text(json.dumps(source_record, indent=2, sort_keys=True) + "\n")
    return source_record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    record = fetch(args.output.resolve())
    print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
