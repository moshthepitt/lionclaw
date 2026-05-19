from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True, frozen=True)
class WorkerConfig:
    telegram_bot_token: str
    lionclaw_base_url: str
    channel_id: str
    stream_limit: int
    stream_wait_ms: int
    stream_start_mode: str
    consumer_id: str
    telegram_poll_timeout_secs: int
    telegram_loop_delay_secs: float
    health_report_interval_secs: float
    runtime_dir: Path
    telegram_offset_file: Path

    @classmethod
    def from_env(cls) -> WorkerConfig:
        telegram_bot_token = _required_env("TELEGRAM_BOT_TOKEN")
        lionclaw_home = _path_env("LIONCLAW_HOME", Path.home() / ".lionclaw")
        channel_id = _string_env("LIONCLAW_CHANNEL_ID", "telegram")
        runtime_dir = _path_env(
            "LIONCLAW_CHANNEL_RUNTIME_DIR",
            lionclaw_home / "runtime" / "channels" / channel_id,
        )
        return cls(
            telegram_bot_token=telegram_bot_token,
            lionclaw_base_url=_string_env("LIONCLAW_BASE_URL", "http://127.0.0.1:8979"),
            channel_id=channel_id,
            stream_limit=_int_env("LIONCLAW_STREAM_LIMIT", 100, min_value=1),
            stream_wait_ms=_int_env("LIONCLAW_STREAM_WAIT_MS", 30000, min_value=0),
            stream_start_mode=_string_env("LIONCLAW_STREAM_START_MODE", "resume"),
            consumer_id=_string_env(
                "LIONCLAW_CONSUMER_ID",
                f"telegram:{channel_id}",
            ),
            telegram_poll_timeout_secs=_int_env(
                "TELEGRAM_POLL_TIMEOUT_SECS",
                25,
                min_value=0,
            ),
            telegram_loop_delay_secs=_float_env(
                "TELEGRAM_LOOP_DELAY_SECS",
                1.0,
                min_value=0.0,
            ),
            health_report_interval_secs=_float_env(
                "LIONCLAW_HEALTH_REPORT_INTERVAL_SECS",
                60.0,
                min_value=0.0,
            ),
            runtime_dir=runtime_dir,
            telegram_offset_file=_path_env(
                "TELEGRAM_OFFSET_FILE",
                runtime_dir / "telegram.offset",
            ),
        )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"{name} is required")
    return value


def _string_env(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None:
        return default
    if not value.strip():
        raise RuntimeError(f"{name} must not be empty")
    return value


def _path_env(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    if value is None:
        return default.expanduser()
    if not value.strip():
        raise RuntimeError(f"{name} must not be empty")
    return Path(value).expanduser()


def _int_env(name: str, default: int, *, min_value: int) -> int:
    value = os.environ.get(name)
    if value is None:
        parsed = default
    else:
        try:
            parsed = int(value)
        except ValueError as err:
            raise RuntimeError(f"{name} must be an integer") from err
    if parsed < min_value:
        raise RuntimeError(f"{name} must be >= {min_value}")
    return parsed


def _float_env(name: str, default: float, *, min_value: float) -> float:
    value = os.environ.get(name)
    if value is None:
        parsed = default
    else:
        try:
            parsed = float(value)
        except ValueError as err:
            raise RuntimeError(f"{name} must be a number") from err
    if not math.isfinite(parsed):
        raise RuntimeError(f"{name} must be finite")
    if parsed < min_value:
        raise RuntimeError(f"{name} must be >= {min_value:g}")
    return parsed
