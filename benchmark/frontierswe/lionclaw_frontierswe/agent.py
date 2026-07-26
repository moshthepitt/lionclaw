"""Harbor agent that delegates task work to a confined LionClaw mission."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from typing import Any

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext

from .report import read_json


class LionClawAgent(BaseAgent):
    def __init__(
        self,
        *args: Any,
        lionclaw_bin: str,
        mission_type: str,
        image: str,
        supervisor_module: str,
        lead_prompt: str,
        codex_bin: str = "codex",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.lionclaw_bin = Path(lionclaw_bin).resolve()
        self.mission_type = Path(mission_type).resolve()
        self.image = image
        self.supervisor_module = supervisor_module
        self.lead_prompt = Path(lead_prompt).resolve()
        self.codex_bin = codex_bin

    @staticmethod
    def name() -> str:
        return "lionclaw"

    def version(self) -> str:
        return "slice10a-v1"

    async def setup(self, environment: BaseEnvironment) -> None:
        del environment

    @staticmethod
    def _git(repo: Path, *args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(repo), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                f"git {' '.join(args)} failed:\n{completed.stdout}\n{completed.stderr}"
            )
        return completed.stdout.strip()

    def _initialize_repo(self, repo: Path) -> None:
        self._git(repo, "init", "--initial-branch=benchmark-base")
        self._git(repo, "config", "user.name", "LionClaw Benchmark")
        self._git(repo, "config", "user.email", "benchmark@lionclaw.invalid")
        self._git(repo, "add", "instruction.md", "examples", "type-checker")
        self._git(
            repo,
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "Initialize pinned FrontierSWE task workspace",
        )

    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        workspace = self.logs_dir / "workspace"
        workspace.mkdir(parents=True, exist_ok=False)
        await environment.download_dir("/app/type-checker", workspace / "type-checker")
        await environment.download_dir("/app/examples", workspace / "examples")
        await environment.download_file("/app/instruction.md", workspace / "instruction.md")
        self._initialize_repo(workspace)

        objective_path = self.logs_dir / "objective.txt"
        objective_path.write_text(
            "Implement and optimize the pinned FrontierSWE dependent type checker. "
            "Satisfy every requirement in the repository's instruction.md; /app in "
            "that instruction maps to this repository root. Produce type-checker as "
            "the candidate, preserve the CLI and exit-code contract, and close "
            "honestly without claiming access to Harbor's hidden verifier. The "
            "pinned Harbor task instruction passed to this agent is byte-identical "
            "to instruction.md.\n"
        )
        report_path = self.logs_dir / "lionclaw-mission-report.json"
        process = await asyncio.create_subprocess_exec(
            "python3",
            "-m",
            self.supervisor_module,
            "--repo",
            str(workspace),
            "--mission-type",
            str(self.mission_type),
            "--image",
            self.image,
            "--objective-file",
            str(objective_path),
            "--lead-prompt",
            str(self.lead_prompt),
            "--logs-dir",
            str(self.logs_dir / "lead"),
            "--report",
            str(report_path),
            "--lionclaw-bin",
            str(self.lionclaw_bin),
            "--codex-bin",
            self.codex_bin,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        (self.logs_dir / "supervisor.stdout").write_bytes(stdout)
        (self.logs_dir / "supervisor.stderr").write_bytes(stderr)
        if not report_path.exists():
            raise RuntimeError(
                f"LionClaw supervisor failed before reporting (exit {process.returncode}): "
                f"{stderr.decode(errors='replace')}"
            )

        report = read_json(report_path)
        deliverable_sha = report["lionclaw_outcome"].get("deliverable_head")
        base_sha = report["mission"].get("base_sha")
        candidate_root = workspace
        if report["lionclaw_outcome"].get("finish") and deliverable_sha != base_sha:
            candidate_root = self.logs_dir / "deliverable"
            self._git(
                workspace,
                "worktree",
                "add",
                "--detach",
                str(candidate_root),
                deliverable_sha,
            )

        candidate = candidate_root / "type-checker"
        if not candidate.is_dir():
            raise RuntimeError(f"LionClaw produced no type-checker candidate at {candidate}")
        await environment.upload_dir(candidate, "/app/type-checker")

        harbor_context = report["harbor_context"]
        context.n_input_tokens = harbor_context.get("n_input_tokens")
        context.n_cache_tokens = harbor_context.get("n_cache_tokens")
        context.n_output_tokens = harbor_context.get("n_output_tokens")
        context.cost_usd = harbor_context.get("cost_usd")
        context.metadata = {
            "lionclaw_mission_id": report["lionclaw_outcome"]["mission_id"],
            "lionclaw_finish": report["lionclaw_outcome"].get("finish"),
            "lionclaw_deliverable_head": deliverable_sha,
            "lionclaw_report": str(report_path),
            "supervisor_exit_code": process.returncode,
        }
