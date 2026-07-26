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
    parse_codex_usage,
)


def receipt(
    role: str,
    *,
    input_tokens: int,
    output_tokens: int,
    cost: dict[str, str] | None = None,
) -> dict:
    usage: dict = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }
    if cost is not None:
        usage["cost"] = cost
    return {
        "source": {"request": {"role_instance": role}},
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
                                    "output_tokens": 7,
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
                    "output_tokens": 7,
                },
            )


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
