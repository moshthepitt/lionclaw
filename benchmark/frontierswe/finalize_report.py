#!/usr/bin/env python3
"""Merge Harbor's post-agent verifier result into the mission report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lionclaw_frontierswe.report import finalize_report, read_json, write_json


def exactly_one(paths: list[Path], label: str) -> Path:
    if len(paths) != 1:
        raise RuntimeError(f"expected exactly one {label}, found {len(paths)}")
    return paths[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-dir", type=Path, required=True)
    parser.add_argument("--scorer", type=Path, required=True)
    args = parser.parse_args()

    job_dir = args.job_dir.resolve()
    report_path = exactly_one(
        list(job_dir.glob("*/agent/lionclaw-mission-report.json")),
        "LionClaw mission report",
    )
    result_path = exactly_one(list(job_dir.glob("*/result.json")), "Harbor trial result")
    reward_path = exactly_one(list(job_dir.glob("*/verifier/reward.json")), "Harbor reward")
    environment_path = exactly_one(
        list(job_dir.glob("*/agent/environment.json")),
        "environment evidence",
    )

    report = finalize_report(
        read_json(report_path),
        read_json(result_path),
        read_json(reward_path),
        args.scorer.resolve(),
        read_json(environment_path),
    )
    write_json(report_path, report)
    output = job_dir / "mission-report.json"
    write_json(output, report)
    print(json.dumps({"mission_report": str(output), **report["benchmark_outcome"]}))


if __name__ == "__main__":
    main()
