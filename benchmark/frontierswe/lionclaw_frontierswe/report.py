"""Machine-readable LionClaw and Harbor usage/outcome reporting."""

from __future__ import annotations

import importlib.util
import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any


TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_tokens",
    "total_tokens",
)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def parse_codex_usage(log_path: Path) -> dict[str, int]:
    """Return the last, most complete usage snapshot from one Codex exec."""

    candidates: list[dict[str, int]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            candidate = {
                field: item
                for field in TOKEN_FIELDS
                if isinstance((item := value.get(field)), int)
            }
            if "input_tokens" in candidate or "output_tokens" in candidate:
                candidates.append(candidate)
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    if not log_path.exists():
        return {}
    for line in log_path.read_text(errors="replace").splitlines():
        try:
            visit(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not candidates:
        return {}
    return max(candidates, key=lambda item: sum(item.values()))


def _role_instance(receipt: dict[str, Any]) -> str:
    return (
        receipt.get("source", {}).get("request", {}).get("role_instance")
        or "unknown-role"
    )


def aggregate_usage(
    canonical_report: dict[str, Any], lead_attempts: list[dict[str, Any]]
) -> dict[str, Any]:
    role_tokens: Counter[str] = Counter()
    role_rounds: Counter[str] = Counter()
    turn_costs: defaultdict[str, Decimal] = defaultdict(Decimal)
    cumulative_costs: dict[tuple[str, str], Decimal] = {}
    reported_cost_receipts = 0

    receipts = canonical_report.get("role_attempt_receipts", [])
    for receipt in receipts:
        role = _role_instance(receipt)
        role_rounds[role] += 1
        runtime_usage = receipt.get("runtime_usage", {})
        if runtime_usage.get("status") != "reported":
            continue
        usage = runtime_usage.get("usage", {})
        for field in TOKEN_FIELDS:
            value = usage.get(field)
            if isinstance(value, int):
                role_tokens[field] += value
        cost = usage.get("cost")
        if not isinstance(cost, dict):
            continue
        try:
            amount = Decimal(cost["amount"])
            currency = str(cost["currency"])
            scope = cost["scope"]
        except (KeyError, TypeError, ArithmeticError):
            continue
        reported_cost_receipts += 1
        if scope == "turn":
            turn_costs[currency] += amount
        elif scope == "session_cumulative":
            key = (role, currency)
            cumulative_costs[key] = max(cumulative_costs.get(key, Decimal()), amount)

    lead_tokens: Counter[str] = Counter()
    for attempt in lead_attempts:
        lead_tokens.update(attempt.get("usage", {}))

    currencies: defaultdict[str, Decimal] = defaultdict(Decimal)
    for currency, amount in turn_costs.items():
        currencies[currency] += amount
    for (_, currency), amount in cumulative_costs.items():
        currencies[currency] += amount

    combined_tokens = role_tokens + lead_tokens
    cost_status = "reported" if reported_cost_receipts else "not_reported"
    cost = {
        "status": cost_status,
        "currencies": {
            currency: format(amount, "f") for currency, amount in sorted(currencies.items())
        },
        "method": (
            "turn costs are summed; session_cumulative costs use the maximum "
            "reported amount per role conversation"
        ),
    }
    if not currencies:
        cost["currencies"] = {}

    return {
        "rounds": {
            "role_attempts": len(receipts),
            "lead_attempts": len(lead_attempts),
            "by_role": dict(sorted(role_rounds.items())),
        },
        "tokens": {
            "role_attempts": dict(role_tokens),
            "benchmark_lead": dict(lead_tokens),
            "combined": dict(combined_tokens),
            "method": "sum of counters reported by each completed runtime turn",
        },
        "cost": cost,
    }


def build_pending_report(
    canonical_report: dict[str, Any],
    status: dict[str, Any],
    lead_attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    usage = aggregate_usage(canonical_report, lead_attempts)
    usd = usage["cost"]["currencies"].get("USD")
    combined = usage["tokens"]["combined"]
    return {
        "schema_version": 1,
        "mission": canonical_report,
        "lionclaw_outcome": {
            "mission_id": canonical_report["mission_id"],
            "disposition": canonical_report.get("disposition"),
            "phase": status.get("phase"),
            "finish": canonical_report.get("finish"),
            "deliverable_head": canonical_report.get("deliverable_head"),
            "stop_bar": canonical_report.get("stop_bar"),
        },
        "usage": usage,
        "harbor_context": {
            "n_input_tokens": combined.get("input_tokens"),
            "n_cache_tokens": combined.get("cached_input_tokens"),
            "n_output_tokens": combined.get("output_tokens"),
            "cost_usd": float(usd) if usd is not None else None,
        },
        "benchmark_outcome": {
            "status": "pending_harbor_verifier",
            "task": "dependent-type-checker",
        },
    }


def _load_scorer(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location("frontierswe_score", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load pinned scorer: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def finalize_report(
    report: dict[str, Any],
    trial_result: dict[str, Any],
    reward: dict[str, Any],
    scorer_path: Path,
    environment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task = "dependent-type-checker"
    exception = trial_result.get("exception_info")
    scorer = _load_scorer(scorer_path)
    correctness, speedup = scorer.extract_score(reward, task)
    gated_score = scorer.compute_gated_score(correctness, speedup, task)
    validity_reasons = list((environment or {}).get("invalidation_reasons", []))
    validity_reasons.append(
        "lionclaw_worker_network_broader_than_official_agent_domain_allowlist"
    )
    if report.get("lionclaw_outcome", {}).get("supervisor", {}).get("status") == "failed":
        validity_reasons.append("lionclaw_supervisor_failed")
    task_outcome = (
        "correctness_gate_passed"
        if correctness >= 1.0
        else "correctness_gate_failed"
    )
    if exception or validity_reasons:
        outcome = "invalid"
    else:
        outcome = task_outcome

    report["benchmark_outcome"] = {
        "status": "invalid" if exception or validity_reasons else "scored",
        "outcome": outcome,
        "task_outcome": task_outcome,
        "task": task,
        "correctness": correctness,
        "speedup": speedup,
        "gated_score": gated_score,
        "harbor_reward": trial_result.get("verifier_result", {}).get("rewards"),
        "reward": reward,
        "trial_exception": exception,
        "trial_name": trial_result.get("trial_name"),
        "trial_uri": trial_result.get("trial_uri"),
    }
    report["validity"] = {
        "publishable": False,
        "invalidation_reasons": validity_reasons,
        "environment": environment,
    }
    return report
