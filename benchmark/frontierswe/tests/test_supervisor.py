from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lionclaw_frontierswe.supervisor import (  # noqa: E402
    ACTION_SCHEMA,
    advertised_actions,
    execute_action,
    lead_prompt,
    needs_lead,
    no_progress_feedback,
)


class SupervisorActionTests(unittest.TestCase):
    def test_advertised_actions_deduplicate_status_and_guide(self) -> None:
        self.assertEqual(
            advertised_actions(
                {"next_actions": ["mission plan propose", "mission abort"]},
                {"next_actions": ["mission plan propose", "mission status"]},
            ),
            ["mission plan propose", "mission abort", "mission status"],
        )

    def test_passive_and_abort_actions_do_not_spawn_lead(self) -> None:
        self.assertFalse(needs_lead(["mission status", "mission abort"]))
        self.assertFalse(needs_lead(["mission report"]))
        self.assertTrue(needs_lead(["mission advance", "mission abort"]))
        self.assertTrue(needs_lead(["mission plan propose"]))

    def test_lead_prompt_includes_objective_and_actions(self) -> None:
        prompt = lead_prompt(
            "{{OBJECTIVE}}\n{{ACTION_FILE}}\n{{SUPERVISOR_FEEDBACK_JSON}}\n{{LEGAL_ACTIONS_JSON}}\n{{GUIDE_JSON}}\n{{STATUS_JSON}}",
            Path("/bin/lionclaw"),
            Path("/repo"),
            "m1",
            "Do the task",
            Path("/repo/.tmp/lead/action.json"),
            {"error": "invalid requirement kind"},
            {"next_actions": ["mission plan propose"]},
            {"disposition": "awaiting_plan"},
            ["mission plan propose"],
        )

        self.assertIn("Do the task", prompt)
        self.assertIn("/repo/.tmp/lead/action.json", prompt)
        self.assertIn("invalid requirement kind", prompt)
        self.assertIn('"mission plan propose"', prompt)
        self.assertIn('"disposition": "awaiting_plan"', prompt)

    def test_executor_rejects_unadvertised_action(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "legal actions"):
            execute_action(
                Path("/bin/lionclaw"),
                Path("/repo"),
                "m1",
                {"schema": ACTION_SCHEMA, "command": "mission advance"},
                ["mission plan propose"],
                {},
                Path("/logs"),
                1,
            )

    def test_no_progress_feedback_is_compact_and_actionable(self) -> None:
        feedback = no_progress_feedback(
            3,
            ["mission decide", "mission abort"],
            {
                "command": "mission decide",
                "decision": "revise",
                "item": "gap_review_gaps:mission",
                "executed": True,
                "output": "large output omitted",
            },
            {
                "phase": "attention_needed",
                "disposition": "parked",
                "next_actions": ["mission decide", "mission abort"],
                "attention": [
                    {
                        "id": "gap_review_gaps:mission",
                        "kind": "gap_review_gaps",
                        "report": "Gap review found blocking gaps.",
                        "actions": ["retry", "revise", "accept"],
                        "evidence": {"large": "omitted"},
                    }
                ],
            },
            1,
        )

        self.assertIn("unchanged", feedback["error"])
        self.assertEqual(feedback["executed_action"]["decision"], "revise")
        self.assertNotIn("output", feedback["executed_action"])
        self.assertEqual(
            feedback["status"]["attention"][0]["actions"],
            ["retry", "revise", "accept"],
        )
        self.assertNotIn("evidence", feedback["status"]["attention"][0])


if __name__ == "__main__":
    unittest.main()
