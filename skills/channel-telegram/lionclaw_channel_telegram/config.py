from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

STREAM_START_MODES = frozenset({"resume", "tail"})
TELEGRAM_UPDATE_MODES = frozenset({"polling", "webhook"})


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
    telegram_update_mode: str
    telegram_webhook_host: str
    telegram_webhook_port: int
    telegram_webhook_path: str
    telegram_webhook_secret_token: str | None
    telegram_webhook_max_body_bytes: int
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
        config = cls(
            telegram_bot_token=telegram_bot_token,
            lionclaw_base_url=_string_env("LIONCLAW_BASE_URL", "http://127.0.0.1:8979"),
            channel_id=channel_id,
            stream_limit=_int_env("LIONCLAW_STREAM_LIMIT", 100, min_value=1),
            stream_wait_ms=_int_env("LIONCLAW_STREAM_WAIT_MS", 30000, min_value=0),
            stream_start_mode=_choice_env(
                "LIONCLAW_STREAM_START_MODE",
                "resume",
                allowed=STREAM_START_MODES,
            ),
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
            telegram_update_mode=_choice_env(
                "TELEGRAM_UPDATE_MODE",
                "polling",
                allowed=TELEGRAM_UPDATE_MODES,
            ),
            telegram_webhook_host=_string_env("TELEGRAM_WEBHOOK_HOST", "127.0.0.1"),
            telegram_webhook_port=_port_env("TELEGRAM_WEBHOOK_PORT", 8080),
            telegram_webhook_path=_webhook_path_env(
                "TELEGRAM_WEBHOOK_PATH",
                "/telegram/webhook",
            ),
            telegram_webhook_secret_token=_optional_string_env(
                "TELEGRAM_WEBHOOK_SECRET_TOKEN"
            ),
            telegram_webhook_max_body_bytes=_int_env(
                "TELEGRAM_WEBHOOK_MAX_BODY_BYTES",
                1024 * 1024,
                min_value=1,
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
        if (
            config.telegram_update_mode == "webhook"
            and config.telegram_webhook_secret_token is None
        ):
            raise RuntimeError(
                "TELEGRAM_WEBHOOK_SECRET_TOKEN is required when "
                "TELEGRAM_UPDATE_MODE=webhook"
            )
        return config


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


def _optional_string_env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
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


def _choice_env(name: str, default: str, *, allowed: frozenset[str]) -> str:
    value = _string_env(name, default).casefold()
    if value not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise RuntimeError(f"{name} must be one of: {allowed_values}")
    return value


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


def _port_env(name: str, default: int) -> int:
    parsed = _int_env(name, default, min_value=0)
    if parsed > 65535:
        raise RuntimeError(f"{name} must be <= 65535")
    return parsed


def _webhook_path_env(name: str, default: str) -> str:
    value = _string_env(name, default)
    if not value.startswith("/"):
        raise RuntimeError(f"{name} must start with /")
    if any(char.isspace() for char in value):
        raise RuntimeError(f"{name} must not contain whitespace")
    return value


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
