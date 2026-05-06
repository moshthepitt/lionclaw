from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True, frozen=True)
class WorkerConfig:
    telegram_bot_token: str
    lionclaw_base_url: str
    channel_id: str
    runtime_id: str | None
    stream_limit: int
    stream_wait_ms: int
    stream_start_mode: str
    consumer_id: str
    telegram_poll_timeout_secs: int
    telegram_loop_delay_secs: float
    runtime_dir: Path
    telegram_offset_file: Path

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        telegram_bot_token = _required_env("TELEGRAM_BOT_TOKEN")
        lionclaw_home = Path(
            os.environ.get("LIONCLAW_HOME", str(Path.home() / ".lionclaw"))
        ).expanduser()
        channel_id = os.environ.get("LIONCLAW_CHANNEL_ID", "telegram")
        runtime_dir = Path(
            os.environ.get(
                "LIONCLAW_CHANNEL_RUNTIME_DIR",
                str(lionclaw_home / "runtime" / "channels" / channel_id),
            )
        ).expanduser()
        return cls(
            telegram_bot_token=telegram_bot_token,
            lionclaw_base_url=os.environ.get(
                "LIONCLAW_BASE_URL", "http://127.0.0.1:8979"
            ),
            channel_id=channel_id,
            runtime_id=_optional_env("LIONCLAW_RUNTIME_ID"),
            stream_limit=_int_env("LIONCLAW_STREAM_LIMIT", 100),
            stream_wait_ms=_int_env("LIONCLAW_STREAM_WAIT_MS", 30000),
            stream_start_mode=os.environ.get("LIONCLAW_STREAM_START_MODE", "resume"),
            consumer_id=os.environ.get(
                "LIONCLAW_CONSUMER_ID", f"telegram:{channel_id}"
            ),
            telegram_poll_timeout_secs=_int_env("TELEGRAM_POLL_TIMEOUT_SECS", 25),
            telegram_loop_delay_secs=_float_env("TELEGRAM_LOOP_DELAY_SECS", 1.0),
            runtime_dir=runtime_dir,
            telegram_offset_file=Path(
                os.environ.get(
                    "TELEGRAM_OFFSET_FILE",
                    str(runtime_dir / "telegram.offset"),
                )
            ).expanduser(),
        )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def _optional_env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _int_env(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    return int(value)


def _float_env(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    return float(value)
