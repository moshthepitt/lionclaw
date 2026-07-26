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

PASSIVE_ACTIONS = frozenset({"mission guide", "mission status", "mission log", "mission report"})
ABORT_ACTION = "mission abort"
ACTION_SCHEMA = "lionclaw.frontierswe.lead-action.v1"


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
    objective: str,
    action_file: Path,
    supervisor_feedback: dict[str, Any] | None,
    guide: dict[str, Any],
    status: dict[str, Any],
    actions: list[str],
) -> str:
    return (
        template.replace("{{LIONCLAW_BIN}}", str(binary))
        .replace("{{REPO}}", str(repo))
        .replace("{{MISSION_ID}}", mission_id)
        .replace("{{OBJECTIVE}}", objective)
        .replace("{{ACTION_FILE}}", str(action_file))
        .replace(
            "{{SUPERVISOR_FEEDBACK_JSON}}",
            json.dumps(supervisor_feedback or {}, indent=2, sort_keys=True),
        )
        .replace("{{LEGAL_ACTIONS_JSON}}", json.dumps(actions, indent=2))
        .replace("{{GUIDE_JSON}}", json.dumps(guide, indent=2, sort_keys=True))
        .replace("{{STATUS_JSON}}", json.dumps(status, indent=2, sort_keys=True))
    )


def advertised_actions(status: dict[str, Any], guide: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    for source in (status.get("next_actions"), guide.get("next_actions")):
        if not isinstance(source, list):
            continue
        for action in source:
            if isinstance(action, str) and action not in actions:
                actions.append(action)
    return actions


def needs_lead(actions: list[str]) -> bool:
    return any(action not in PASSIVE_ACTIONS and action != ABORT_ACTION for action in actions)


def read_action(path: Path) -> dict[str, Any]:
    try:
        action = json.loads(path.read_text())
    except FileNotFoundError as error:
        raise RuntimeError(f"lead did not write action handoff {path}") from error
    except json.JSONDecodeError as error:
        raise RuntimeError(f"lead action handoff is invalid JSON: {path}") from error
    if not isinstance(action, dict):
        raise RuntimeError("lead action handoff must be a JSON object")
    if action.get("schema") != ACTION_SCHEMA:
        raise RuntimeError(f"lead action handoff has wrong schema: {action.get('schema')!r}")
    return action


def _require_text(action: dict[str, Any], field: str) -> str:
    value = action.get(field)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"lead action field '{field}' must be a non-empty string")
    return value


def _optional_text_list(action: dict[str, Any], field: str) -> list[str]:
    value = action.get(field, [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RuntimeError(f"lead action field '{field}' must be a list of strings")
    return value


def _attention_actions(status: dict[str, Any], item_id: str) -> list[str]:
    for item in status.get("attention", []):
        if isinstance(item, dict) and item.get("id") == item_id:
            actions = item.get("actions", [])
            if isinstance(actions, list):
                return [action for action in actions if isinstance(action, str)]
    return []


def compact_status(status: dict[str, Any]) -> dict[str, Any]:
    attention = []
    for item in status.get("attention", []):
        if not isinstance(item, dict):
            continue
        attention.append(
            {
                "id": item.get("id"),
                "kind": item.get("kind"),
                "report": item.get("report"),
                "actions": item.get("actions"),
            }
        )
    return {
        "phase": status.get("phase"),
        "disposition": status.get("disposition"),
        "next_actions": status.get("next_actions"),
        "attention": attention,
        "finish": status.get("finish"),
    }


def no_progress_feedback(
    attempt_no: int,
    actions: list[str],
    execution: dict[str, Any],
    status: dict[str, Any],
    unchanged_attempts: int,
) -> dict[str, Any]:
    return {
        "attempt": attempt_no,
        "advertised_actions": actions,
        "error": "executed action left canonical mission status unchanged",
        "unchanged_attempts": unchanged_attempts,
        "executed_action": {
            key: execution.get(key)
            for key in ("command", "decision", "item", "executed")
            if key in execution
        },
        "status": compact_status(status),
        "instruction": (
            "Choose a different legal action, or choose none/abort if no legal "
            "action can advance the mission."
        ),
    }


def execute_action(
    binary: Path,
    repo: Path,
    mission_id: str,
    action: dict[str, Any],
    legal_actions: list[str],
    status: dict[str, Any],
    logs_dir: Path,
    attempt_no: int,
) -> dict[str, Any]:
    command = _require_text(action, "command")
    if command == "none":
        return {"executed": False, "command": command, "reason": action.get("reason")}
    if command not in legal_actions:
        raise RuntimeError(
            f"lead requested '{command}', but legal actions are {legal_actions}"
        )

    if command == "mission plan propose":
        proposal = action.get("proposal")
        if not isinstance(proposal, dict):
            raise RuntimeError("mission plan propose action requires object field 'proposal'")
        proposal_path = logs_dir / f"lead-proposal-{attempt_no:03d}.json"
        write_json(proposal_path, proposal)
        output = run_checked(
            [
                str(binary),
                "mission",
                "plan",
                "propose",
                mission_id,
                "--repo",
                str(repo),
                "--file",
                str(proposal_path),
            ]
        )
        return {"executed": True, "command": command, "proposal": str(proposal_path), "output": output}

    if command == "mission decide":
        item = _require_text(action, "item")
        decision = _require_text(action, "decision")
        if decision not in _attention_actions(status, item):
            raise RuntimeError(
                f"lead requested decision '{decision}' for '{item}', "
                "but status does not advertise it"
            )
        decide_command = [
            str(binary),
            "mission",
            "decide",
            mission_id,
            item,
            decision,
            "--repo",
            str(repo),
        ]
        if decision == "revise":
            feedback = _require_text(action, "feedback")
            feedback_path = logs_dir / f"lead-feedback-{attempt_no:03d}.txt"
            feedback_path.write_text(feedback)
            decide_command.extend(["--feedback-file", str(feedback_path)])
        else:
            decide_command.extend(["--justification", _require_text(action, "justification")])
        output = run_checked(decide_command)
        return {"executed": True, "command": command, "item": item, "decision": decision, "output": output}

    if command == "mission advance":
        output = run_checked(
            [
                str(binary),
                "mission",
                "advance",
                mission_id,
                "--repo",
                str(repo),
                "--wait",
            ]
        )
        return {"executed": True, "command": command, "output": output}

    if command == "mission send":
        message = _require_text(action, "message")
        send_command = [
            str(binary),
            "mission",
            "send",
            "--mission-id",
            mission_id,
            "--repo",
            str(repo),
        ]
        recipients = _optional_text_list(action, "to")
        if action.get("all") is True:
            send_command.append("--all")
        else:
            if not recipients:
                raise RuntimeError("mission send action requires 'to' or all=true")
            for recipient in recipients:
                send_command.extend(["--to", recipient])
        for field, option in (("receipts", "--receipt"), ("parks", "--park"), ("commits", "--commit")):
            for value in _optional_text_list(action, field):
                send_command.extend([option, value])
        send_command.append(message)
        output = run_checked(send_command)
        return {"executed": True, "command": command, "output": output}

    if command in {"mission continue", "mission continue --recreate"}:
        continue_command = [
            str(binary),
            "mission",
            "continue",
            mission_id,
            _require_text(action, "effect_id"),
            "--repo",
            str(repo),
            "--reason",
            _require_text(action, "reason"),
        ]
        if command == "mission continue --recreate":
            continue_command.append("--recreate")
        output = run_checked(continue_command)
        return {"executed": True, "command": command, "output": output}

    if command == "mission abort":
        output = run_checked(
            [
                str(binary),
                "mission",
                "abort",
                mission_id,
                "--repo",
                str(repo),
                "--reason",
                _require_text(action, "reason"),
            ]
        )
        return {"executed": True, "command": command, "output": output}

    raise RuntimeError(f"lead requested unsupported command '{command}'")


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
    benchmark_context = {
        "resolved_model": args.resolved_model,
        "resolved_model_source": args.resolved_model_source,
        "role_model_request": args.role_model_request,
        "role_model_policy": (
            "explicit FRONTIERSWE_ROLE_MODEL requested through the LionClaw runtime profile"
            if args.role_model_request
            else "not requested by default because this Codex app-server build does not confirm the applied model in turn/start"
        ),
    }
    unchanged = 0
    idle_polls = 0
    supervisor_feedback: dict[str, Any] | None = None

    try:
        while len(attempts) < args.max_lead_attempts:
            guide = lionclaw_json(args.lionclaw_bin, repo, "guide", mission_id)
            status = lionclaw_json(args.lionclaw_bin, repo, "status", mission_id)
            if status.get("disposition") == "terminal":
                break
            actions = advertised_actions(status, guide)
            if not needs_lead(actions):
                idle_polls += 1
                if idle_polls > args.max_idle_polls:
                    raise RuntimeError(
                        "mission did not advertise a lead-actionable command "
                        f"after {args.max_idle_polls} status polls; last actions: {actions}"
                    )
                time.sleep(args.restart_delay_secs)
                continue
            idle_polls = 0

            before = state_fingerprint(status)
            attempt_no = len(attempts) + 1
            action_file = repo / ".tmp" / "lead" / f"lead-action-{attempt_no:03d}.json"
            action_file.parent.mkdir(parents=True, exist_ok=True)
            action_file.unlink(missing_ok=True)
            prompt = lead_prompt(
                template,
                args.lionclaw_bin,
                repo,
                mission_id,
                objective,
                action_file,
                supervisor_feedback,
                guide,
                status,
                actions,
            )
            jsonl_path = args.logs_dir / f"lead-attempt-{attempt_no:03d}.jsonl"
            stderr_path = args.logs_dir / f"lead-attempt-{attempt_no:03d}.stderr"
            codex_command = [
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
            ]
            if args.lead_model:
                codex_command.extend(["--model", args.lead_model])
            codex_command.append("-")
            completed = subprocess.run(
                codex_command,
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
            direct_status = lionclaw_json(
                args.lionclaw_bin, repo, "status", mission_id
            )
            if state_fingerprint(direct_status) != before:
                raise RuntimeError(
                    "benchmark lead changed canonical mission state directly; "
                    "state-changing work must be requested through the action handoff"
                )
            try:
                action = read_action(action_file)
                execution = execute_action(
                    args.lionclaw_bin,
                    repo,
                    mission_id,
                    action,
                    actions,
                    status,
                    args.logs_dir,
                    attempt_no,
                )
                supervisor_feedback = None
            except Exception as action_error:
                execution = {
                    "executed": False,
                    "error": str(action_error),
                    "action_file": str(action_file),
                }
                supervisor_feedback = {
                    "attempt": attempt_no,
                    "advertised_actions": actions,
                    "error": str(action_error),
                    "action_file": str(action_file),
                }
            attempts.append(
                {
                    "attempt": attempt_no,
                    "exit_code": completed.returncode,
                    "usage": usage,
                    "runtime": "codex_exec",
                    "model": args.lead_model,
                    "advertised_actions": actions,
                    "action_file": str(action_file),
                    "executed_action": execution,
                    "jsonl": str(jsonl_path),
                    "stderr": str(stderr_path),
                }
            )

            after_status = lionclaw_json(
                args.lionclaw_bin, repo, "status", mission_id
            )
            after = state_fingerprint(after_status)
            if after == before:
                unchanged += 1
                if execution.get("executed") is True:
                    supervisor_feedback = no_progress_feedback(
                        attempt_no,
                        actions,
                        execution,
                        after_status,
                        unchanged,
                    )
            else:
                unchanged = 0
                if execution.get("executed") is True:
                    supervisor_feedback = None
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

        report = build_pending_report(canonical, status, attempts, benchmark_context)
        write_json(args.report, report)
        return report
    except Exception as error:
        status = lionclaw_json(args.lionclaw_bin, repo, "status", mission_id)
        canonical = lionclaw_json(args.lionclaw_bin, repo, "report", mission_id)
        report = build_pending_report(canonical, status, attempts, benchmark_context)
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
    parser.add_argument("--lead-model")
    parser.add_argument("--resolved-model")
    parser.add_argument("--resolved-model-source")
    parser.add_argument("--role-model-request")
    parser.add_argument("--max-lead-attempts", type=int, default=200)
    parser.add_argument("--max-idle-polls", type=int, default=600)
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
