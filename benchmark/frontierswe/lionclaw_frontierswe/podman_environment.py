"""Harbor environment adapter for one immutable, prebuilt Podman image."""

from __future__ import annotations

import asyncio
import asyncio.subprocess
import json
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.models.environment_type import EnvironmentType
from harbor.models.trial.paths import EnvironmentPaths


def _container_name(session_id: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "-", session_id)
    if not sanitized or not sanitized[0].isalnum():
        sanitized = f"0-{sanitized}"
    return f"lionclaw-frontierswe-{sanitized}"[:128]


class PodmanEnvironment(BaseEnvironment):
    """Run Harbor's environment contract through the native Podman CLI.

    The adapter deliberately supports prebuilt images only. Image composition is a
    host-side bundle operation, never an action available to a benchmark worker.
    """

    @classmethod
    def preflight(cls) -> None:
        if not shutil.which("podman"):
            raise SystemExit("Podman is not installed or not on PATH")
        completed = subprocess.run(
            ["podman", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise SystemExit(f"Podman is unavailable: {completed.stderr.strip()}")

    def __init__(
        self,
        *args: Any,
        allow_unenforced_resources: bool = False,
        **kwargs: Any,
    ) -> None:
        self._allow_unenforced_resources = allow_unenforced_resources
        self._name = ""
        super().__init__(*args, **kwargs)
        self._name = _container_name(self.session_id)

    @staticmethod
    def type() -> EnvironmentType:
        # Harbor 0.2.0 has no PODMAN enum; custom import paths do not use this value
        # for dispatch.
        return EnvironmentType.DOCKER

    @property
    def is_mounted(self) -> bool:
        return True

    @property
    def supports_gpus(self) -> bool:
        return False

    @property
    def can_disable_internet(self) -> bool:
        return True

    def _validate_definition(self) -> None:
        image = self.task_env_config.docker_image
        if not image or "@sha256:" not in image:
            raise ValueError(
                "PodmanEnvironment requires an immutable docker_image digest"
            )

    @staticmethod
    def _delegated_controllers() -> set[str]:
        path = (
            Path("/sys/fs/cgroup/user.slice")
            / f"user-{os.getuid()}.slice"
            / f"user@{os.getuid()}.service"
            / "cgroup.controllers"
        )
        try:
            return set(path.read_text().split())
        except OSError:
            return set()

    def _resource_evidence(self) -> dict[str, Any]:
        controllers = self._delegated_controllers()
        requested = {
            "cpus": self.task_env_config.cpus,
            "memory_mb": self.task_env_config.memory_mb,
            "storage_mb": self.task_env_config.storage_mb,
        }
        enforced = {
            "cpus": "cpu" in controllers,
            "memory_mb": "memory" in controllers,
            # Rootless overlay storage quotas require backing filesystem project
            # quotas, which are unavailable on this host.
            "storage_mb": False,
        }
        reasons = [
            f"{resource}_limit_unenforced"
            for resource, is_enforced in enforced.items()
            if not is_enforced
        ]
        return {
            "schema_version": 1,
            "provider": "podman",
            "image": self.task_env_config.docker_image,
            "network": {
                "requested": (
                    "enabled"
                    if self.task_env_config.allow_internet
                    else "disabled"
                ),
                "enforced": True,
            },
            "resources": {
                "requested": requested,
                "enforced": enforced,
                "delegated_cgroup_controllers": sorted(controllers),
            },
            "publishable": not reasons,
            "invalidation_reasons": reasons,
        }

    async def _podman(
        self,
        command: list[str],
        *,
        check: bool = True,
        timeout_sec: int | None = None,
    ) -> ExecResult:
        process = await asyncio.create_subprocess_exec(
            "podman",
            *command,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(), timeout=timeout_sec
            )
        except TimeoutError:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                await process.wait()
            raise RuntimeError(
                f"Podman command timed out after {timeout_sec} seconds"
            )

        result = ExecResult(
            stdout=stdout_bytes.decode(errors="replace") or None,
            stderr=stderr_bytes.decode(errors="replace") or None,
            return_code=process.returncode or 0,
        )
        trace_path = self.trial_paths.agent_dir / "podman-events.jsonl"
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        with trace_path.open("a") as trace:
            trace.write(
                json.dumps(
                    {
                        "operation": command[0],
                        "return_code": result.return_code,
                        "stderr": result.stderr,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        if check and result.return_code != 0:
            raise RuntimeError(
                f"Podman command failed ({result.return_code}): "
                f"{shlex.join(['podman', *command])}\n"
                f"stdout: {result.stdout or ''}\n"
                f"stderr: {result.stderr or ''}"
            )
        return result

    async def start(self, force_build: bool) -> None:
        if force_build:
            raise ValueError(
                "PodmanEnvironment does not build images; use build-image.sh on the host"
            )

        evidence = self._resource_evidence()
        if evidence["invalidation_reasons"] and not self._allow_unenforced_resources:
            joined = ", ".join(evidence["invalidation_reasons"])
            raise RuntimeError(
                f"host cannot enforce declared task resources ({joined}); "
                "bring-up runs must explicitly set allow_unenforced_resources=true"
            )

        self.trial_paths.mkdir()
        evidence_path = self.trial_paths.agent_dir / "environment.json"
        evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")

        image = self.task_env_config.docker_image
        assert image is not None
        await self._podman(["image", "exists", image])
        await self._podman(["rm", "--force", self._name], check=False)

        command = ["create", "--name", self._name, "--pull", "never"]
        if not self.task_env_config.allow_internet:
            command.extend(["--network", "none"])
        for host, container in (
            (self.trial_paths.verifier_dir, EnvironmentPaths.verifier_dir),
            (self.trial_paths.agent_dir, EnvironmentPaths.agent_dir),
            (self.trial_paths.artifacts_dir, EnvironmentPaths.artifacts_dir),
        ):
            command.extend(
                ["--volume", f"{host.resolve()}:{container.as_posix()}:rw,Z"]
            )
        command.extend([image, "tail", "-f", "/dev/null"])

        await self._podman(command)
        await self._podman(["start", self._name])
        await self.exec(
            f"chmod 777 {EnvironmentPaths.agent_dir} "
            f"{EnvironmentPaths.verifier_dir} {EnvironmentPaths.artifacts_dir}",
            user="root",
        )

    async def stop(self, delete: bool) -> None:
        del delete
        await self._podman(["rm", "--force", self._name], check=False)

    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        target = Path(target_path)
        await self.exec(f"mkdir -p {shlex.quote(str(target.parent))}", user="root")
        await self._podman(
            ["cp", str(Path(source_path)), f"{self._name}:{target_path}"]
        )

    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        await self.exec(f"mkdir -p {shlex.quote(target_dir)}", user="root")
        await self._podman(
            ["cp", f"{Path(source_dir)}/.", f"{self._name}:{target_dir}"]
        )

    async def download_file(
        self, source_path: str, target_path: Path | str
    ) -> None:
        target = Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        await self._podman(["cp", f"{self._name}:{source_path}", str(target)])

    async def download_dir(
        self, source_dir: str, target_dir: Path | str
    ) -> None:
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        await self._podman(["cp", f"{self._name}:{source_dir}/.", str(target)])

    async def exec(
        self,
        command: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_sec: int | None = None,
        user: str | int | None = None,
    ) -> ExecResult:
        exec_command = ["exec"]
        if cwd:
            exec_command.extend(["--workdir", cwd])
        for key, value in (self._merge_env(env) or {}).items():
            exec_command.extend(["--env", f"{key}={value}"])
        resolved_user = self._resolve_user(user)
        if resolved_user is not None:
            exec_command.extend(["--user", str(resolved_user)])
        exec_command.extend([self._name, "bash", "-c", command])
        return await self._podman(
            exec_command, check=False, timeout_sec=timeout_sec
        )
