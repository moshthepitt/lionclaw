#!/usr/bin/env python3
"""Restart-safe external benchmark lead for one LionClaw mission."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

from .report import build_pending_report, parse_codex_usage, write_json


def run_checked(command: list[str], *, stdin: str | None = None) -> str:
    completed = subprocess.run(
        command,
        input=stdin,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        rendered = " ".join(command)
        raise RuntimeError(
            f"command failed ({completed.returncode}): {rendered}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed.stdout


def lionclaw_json(binary: Path, repo: Path, *args: str) -> dict[str, Any]:
    output = run_checked([str(binary), "mission", *args, "--repo", str(repo), "--json"])
    return json.loads(output)


def lead_prompt(
    template: str,
    binary: Path,
    repo: Path,
    mission_id: str,
    guide: dict[str, Any],
    status: dict[str, Any],
) -> str:
    return (
        template.replace("{{LIONCLAW_BIN}}", str(binary))
        .replace("{{REPO}}", str(repo))
        .replace("{{MISSION_ID}}", mission_id)
        .replace("{{GUIDE_JSON}}", json.dumps(guide, indent=2, sort_keys=True))
        .replace("{{STATUS_JSON}}", json.dumps(status, indent=2, sort_keys=True))
    )


def state_fingerprint(status: dict[str, Any]) -> str:
    stable = {
        "phase": status.get("phase"),
        "disposition": status.get("disposition"),
        "revision": status.get("revision"),
        "team_revision": status.get("team_revision"),
        "current_sha": status.get("current_sha"),
        "finish": status.get("finish"),
        "attention": status.get("attention"),
        "parked_effects": status.get("parked_effects"),
        "next_actions": status.get("next_actions"),
        "conversations": status.get("conversations"),
    }
    return json.dumps(stable, sort_keys=True)


def drive(args: argparse.Namespace) -> dict[str, Any]:
    repo = args.repo.resolve()
    objective = args.objective_file.read_text().strip()
    start = lionclaw_json(
        args.lionclaw_bin,
        repo,
        "start",
        "--type",
        str(args.mission_type.resolve()),
        "--objective",
        objective,
        "--runtime",
        "codex",
        "--image",
        args.image,
    )
    mission_id = start["mission_id"]
    template = args.lead_prompt.read_text()
    attempts: list[dict[str, Any]] = []
    unchanged = 0

    try:
        for attempt_no in range(1, args.max_lead_attempts + 1):
            guide = lionclaw_json(args.lionclaw_bin, repo, "guide", mission_id)
            status = lionclaw_json(args.lionclaw_bin, repo, "status", mission_id)
            if status.get("disposition") == "terminal":
                break
            if (
                status.get("phase") == "planning"
                and status.get("disposition") == "awaiting_plan"
                and "mission advance" not in status.get("next_actions", [])
            ):
                raise RuntimeError(
                    "kernel does not advertise planner dispatch from awaiting_plan; "
                    "external benchmark lead must not author or inject the plan"
                )

            before = state_fingerprint(status)
            prompt = lead_prompt(
                template, args.lionclaw_bin, repo, mission_id, guide, status
            )
            jsonl_path = args.logs_dir / f"lead-attempt-{attempt_no:03d}.jsonl"
            stderr_path = args.logs_dir / f"lead-attempt-{attempt_no:03d}.stderr"
            completed = subprocess.run(
                [
                    str(args.codex_bin),
                    "exec",
                    "--ephemeral",
                    "--sandbox",
                    "workspace-write",
                    "-c",
                    'approval_policy="never"',
                    "-C",
                    str(repo),
                    "--json",
                    "-",
                ],
                input=prompt,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=args.lead_timeout_secs,
            )
            jsonl_path.write_text(completed.stdout)
            stderr_path.write_text(completed.stderr)
            usage = parse_codex_usage(jsonl_path)
            attempts.append(
                {
                    "attempt": attempt_no,
                    "exit_code": completed.returncode,
                    "usage": usage,
                    "jsonl": str(jsonl_path),
                    "stderr": str(stderr_path),
                }
            )

            after_status = lionclaw_json(
                args.lionclaw_bin, repo, "status", mission_id
            )
            after = state_fingerprint(after_status)
            unchanged = unchanged + 1 if after == before else 0
            write_json(
                args.logs_dir / "supervisor-state.json",
                {
                    "schema_version": 1,
                    "mission_id": mission_id,
                    "lead_attempts": attempts,
                    "unchanged_attempts": unchanged,
                    "status": after_status,
                },
            )
            if unchanged >= 3:
                raise RuntimeError(
                    "benchmark lead made no canonical mission progress in three attempts"
                )
            if after_status.get("disposition") == "terminal":
                break
            time.sleep(args.restart_delay_secs)
        else:
            raise RuntimeError(
                f"mission did not terminate after {args.max_lead_attempts} lead attempts"
            )

        status = lionclaw_json(args.lionclaw_bin, repo, "status", mission_id)
        canonical = lionclaw_json(args.lionclaw_bin, repo, "report", mission_id)
        if canonical.get("finish") and canonical.get("current_sha") != canonical.get(
            "base_sha"
        ):
            run_checked(
                [
                    str(args.lionclaw_bin),
                    "mission",
                    "apply",
                    mission_id,
                    "--repo",
                    str(repo),
                    "--force",
                ]
            )
            canonical = lionclaw_json(args.lionclaw_bin, repo, "report", mission_id)

        report = build_pending_report(canonical, status, attempts)
        write_json(args.report, report)
        return report
    except Exception as error:
        status = lionclaw_json(args.lionclaw_bin, repo, "status", mission_id)
        canonical = lionclaw_json(args.lionclaw_bin, repo, "report", mission_id)
        report = build_pending_report(canonical, status, attempts)
        report["lionclaw_outcome"]["supervisor"] = {
            "status": "failed",
            "error": str(error),
        }
        report["benchmark_outcome"] = {
            "status": "pending_harbor_verifier",
            "task": "dependent-type-checker",
            "lionclaw_supervisor_failed": True,
        }
        write_json(args.report, report)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--mission-type", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--objective-file", type=Path, required=True)
    parser.add_argument("--lead-prompt", type=Path, required=True)
    parser.add_argument("--logs-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--lionclaw-bin", type=Path, required=True)
    parser.add_argument("--codex-bin", type=Path, default=Path("codex"))
    parser.add_argument("--max-lead-attempts", type=int, default=200)
    parser.add_argument("--lead-timeout-secs", type=int, default=1800)
    parser.add_argument("--restart-delay-secs", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.logs_dir.mkdir(parents=True, exist_ok=True)
    report = drive(args)
    print(json.dumps(report["lionclaw_outcome"], sort_keys=True))


if __name__ == "__main__":
    main()
