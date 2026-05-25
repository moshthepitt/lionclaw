from __future__ import annotations

import hmac
import json
import logging
from collections.abc import Awaitable, Callable

from aiogram.types import Update
from aiohttp import web

from lionclaw_channel_telegram.config import WorkerConfig

logger = logging.getLogger(__name__)

TELEGRAM_SECRET_TOKEN_HEADER = "X-Telegram-Bot-Api-Secret-Token"


class TelegramWebhookServer:
    def __init__(
        self,
        config: WorkerConfig,
        handle_update: Callable[[Update], Awaitable[bool]],
    ) -> None:
        if config.telegram_webhook_secret_token is None:
            raise RuntimeError("telegram webhook secret token is required")
        self._config = config
        self._handle_update = handle_update
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.bound_host: str | None = None
        self.bound_port: int | None = None

    async def start(self) -> None:
        app = web.Application(
            client_max_size=self._config.telegram_webhook_max_body_bytes
        )
        app.router.add_post(self._config.telegram_webhook_path, self._handle)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            site = web.TCPSite(
                runner,
                host=self._config.telegram_webhook_host,
                port=self._config.telegram_webhook_port,
            )
            await site.start()
            self._runner = runner
            self._site = site
            self._record_bound_address(site)
        except Exception:
            await runner.cleanup()
            self._runner = None
            self._site = None
            self.bound_host = None
            self.bound_port = None
            raise
        logger.info(
            "telegram webhook server listening on %s:%s%s",
            self.bound_host or self._config.telegram_webhook_host,
            self.bound_port or self._config.telegram_webhook_port,
            self._config.telegram_webhook_path,
        )

    async def close(self) -> None:
        runner = self._runner
        try:
            if runner is not None:
                await runner.cleanup()
        finally:
            self._runner = None
            self._site = None
            self.bound_host = None
            self.bound_port = None

    async def _handle(self, request: web.Request) -> web.Response:
        if not self._secret_token_matches(request):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        try:
            body = await request.read()
            payload = json.loads(body.decode("utf-8"))
            update = Update.model_validate(payload)
        except json.JSONDecodeError:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)
        except UnicodeDecodeError:
            return web.json_response({"ok": False, "error": "invalid_utf8"}, status=400)
        except Exception:
            logger.exception("telegram webhook payload validation failed")
            return web.json_response(
                {"ok": False, "error": "invalid_update"},
                status=400,
            )

        try:
            handled = await self._handle_update(update)
        except Exception:
            logger.exception("telegram webhook update handler failed")
            handled = False

        if not handled:
            return web.json_response(
                {"ok": False, "error": "temporary_failure"},
                status=503,
            )
        return web.json_response({"ok": True})

    def _secret_token_matches(self, request: web.Request) -> bool:
        expected = self._config.telegram_webhook_secret_token
        actual = _single_header_value(request.headers, TELEGRAM_SECRET_TOKEN_HEADER)
        if expected is None or actual is None:
            return False
        try:
            actual_bytes = actual.encode("ascii")
            expected_bytes = expected.encode("ascii")
        except UnicodeEncodeError:
            return False
        return hmac.compare_digest(actual_bytes, expected_bytes)

    def _record_bound_address(self, site: web.TCPSite) -> None:
        server = getattr(site, "_server", None)
        sockets = getattr(server, "sockets", None)
        if not sockets:
            return
        host, port, *_ = sockets[0].getsockname()
        self.bound_host = str(host)
        self.bound_port = int(port)


def _single_header_value(headers: object, name: str) -> str | None:
    getall = getattr(headers, "getall", None)
    if callable(getall):
        raw_values = getall(name, [])
        if raw_values is None:
            return None
        values = list(raw_values)
        if len(values) != 1:
            return None
        value = values[0]
    else:
        get = getattr(headers, "get", None)
        if not callable(get):
            return None
        value = get(name)
    return value if isinstance(value, str) else None
