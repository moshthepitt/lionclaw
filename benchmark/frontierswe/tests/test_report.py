from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lionclaw_frontierswe.report import (  # noqa: E402
    aggregate_usage,
    finalize_report,
    model_identity,
    parse_codex_usage,
)
from runtime_config import (  # noqa: E402
    codex_runtime_profile,
    resolve_codex_config_model,
)


def receipt(
    role: str,
    *,
    input_tokens: int,
    output_tokens: int,
    cost: dict[str, str] | None = None,
    configuration: dict | None = None,
    instrument_runtime: dict | None = None,
) -> dict:
    usage: dict = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }
    if cost is not None:
        usage["cost"] = cost
    request = {"role_instance": role}
    if instrument_runtime is not None:
        request["instrument_identity"] = {"runtime": instrument_runtime}
    return {
        "source": {"request": request},
        "effective_runtime_configuration": configuration or {},
        "runtime_usage": {"status": "reported", "usage": usage},
    }


class UsageTests(unittest.TestCase):
    def test_aggregates_turn_cost_and_latest_session_cost(self) -> None:
        report = {
            "role_attempt_receipts": [
                receipt(
                    "worker",
                    input_tokens=10,
                    output_tokens=2,
                    cost={"amount": "0.40", "currency": "USD", "scope": "turn"},
                ),
                receipt(
                    "judge",
                    input_tokens=20,
                    output_tokens=3,
                    cost={
                        "amount": "1.00",
                        "currency": "USD",
                        "scope": "session_cumulative",
                    },
                ),
                receipt(
                    "judge",
                    input_tokens=30,
                    output_tokens=4,
                    cost={
                        "amount": "1.25",
                        "currency": "USD",
                        "scope": "session_cumulative",
                    },
                ),
            ]
        }
        usage = aggregate_usage(
            report,
            [{"usage": {"input_tokens": 5, "output_tokens": 1}}],
        )

        self.assertEqual(usage["rounds"]["role_attempts"], 3)
        self.assertEqual(usage["rounds"]["by_role"], {"judge": 2, "worker": 1})
        self.assertEqual(usage["tokens"]["combined"]["input_tokens"], 65)
        self.assertEqual(usage["tokens"]["combined"]["output_tokens"], 10)
        self.assertEqual(usage["cost"]["currencies"]["USD"], "1.65")

    def test_cost_is_explicitly_not_reported(self) -> None:
        usage = aggregate_usage(
            {
                "role_attempt_receipts": [
                    receipt("worker", input_tokens=1, output_tokens=1)
                ]
            },
            [],
        )
        self.assertEqual(usage["cost"]["status"], "not_reported")
        self.assertEqual(usage["cost"]["currencies"], {})
        self.assertIn("published pricing", usage["cost"]["explanation"])

    def test_codex_usage_uses_most_complete_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "lead.jsonl"
            path.write_text(
                "\n".join(
                    (
                        json.dumps({"usage": {"input_tokens": 3, "output_tokens": 1}}),
                        json.dumps(
                            {
                                "type": "turn.completed",
                                "usage": {
                                    "input_tokens": 30,
                                    "cached_input_tokens": 10,
                                    "cache_write_input_tokens": 2,
                                    "output_tokens": 7,
                                    "reasoning_output_tokens": 3,
                                },
                            }
                        ),
                    )
                )
            )
            self.assertEqual(
                parse_codex_usage(path),
                {
                    "input_tokens": 30,
                    "cached_input_tokens": 10,
                    "cache_write_input_tokens": 2,
                    "output_tokens": 7,
                    "reasoning_tokens": 3,
                },
            )

    def test_model_identity_records_roles_and_lead_model(self) -> None:
        identity = model_identity(
            {
                "team": {"roles": {"worker": {"runtime": "codex"}}},
                "role_attempt_receipts": [
                    receipt(
                        "worker",
                        input_tokens=1,
                        output_tokens=1,
                        configuration={
                            "requested_model": "gpt-x",
                            "applied_model": "gpt-x",
                            "model_confirmation": "observed",
                        },
                        instrument_runtime={"runtime": "codex", "model": "gpt-x"},
                    )
                ],
            },
            [{"model": "gpt-x"}],
            {
                "resolved_model": "gpt-x",
                "resolved_model_source": "codex_config",
                "role_model_policy": "not requested",
            },
        )

        self.assertEqual(
            identity["role_attempts"]["worker"]["requested_model"], "gpt-x"
        )
        self.assertEqual(identity["role_attempts"]["worker"]["runtime"], "codex")
        self.assertEqual(
            identity["role_attempts"]["worker"]["instrument_model"], "gpt-x"
        )
        self.assertEqual(identity["benchmark_lead"]["models"], ["gpt-x"])
        self.assertEqual(identity["benchmark_context"]["resolved_model"], "gpt-x")


class RuntimeConfigTests(unittest.TestCase):
    def test_resolves_codex_model_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config.toml").write_text('model = "gpt-test"\n')
            self.assertEqual(resolve_codex_config_model(root), "gpt-test")

    def test_codex_runtime_profile_contains_explicit_model(self) -> None:
        profile = codex_runtime_profile('gpt-"quoted"')
        self.assertIn('driver = "codex"', profile)
        self.assertIn('model = "gpt-\\"quoted\\""', profile)


class FinalizeTests(unittest.TestCase):
    def test_uses_pinned_scorer_for_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            scorer = Path(directory) / "scorer.py"
            scorer.write_text(
                "def extract_score(reward, task):\n"
                "    return reward['correctness'], reward['speedup']\n"
                "def compute_gated_score(correctness, speedup, task):\n"
                "    return 0.5 + 0.5 * speedup if correctness == 1 else 0.5 * correctness\n"
            )
            report = {"benchmark_outcome": {"status": "pending_harbor_verifier"}}
            result = {
                "trial_name": "trial",
                "trial_uri": "file:///trial",
                "verifier_result": {"rewards": {"reward": 2}},
                "exception_info": None,
            }
            finalized = finalize_report(
                report,
                result,
                {"correctness": 1.0, "speedup": 2.0},
                scorer,
                {
                    "publishable": True,
                    "invalidation_reasons": [],
                },
            )

        outcome = finalized["benchmark_outcome"]
        self.assertEqual(outcome["outcome"], "invalid")
        self.assertEqual(outcome["task_outcome"], "correctness_gate_passed")
        self.assertEqual(outcome["gated_score"], 1.5)
        self.assertEqual(outcome["status"], "invalid")
        self.assertEqual(
            finalized["validity"]["invalidation_reasons"],
            [
                "lionclaw_worker_network_broader_than_official_agent_domain_allowlist"
            ],
        )


if __name__ == "__main__":
    unittest.main()
