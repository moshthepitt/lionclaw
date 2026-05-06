from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

from lionclaw_channel_telegram.api import LionClawApi, StreamEvent
from lionclaw_channel_telegram.config import WorkerConfig
from lionclaw_channel_telegram.telegram import (
    AiogramTelegramTransport,
    TelegramTextUpdate,
    TelegramTransport,
    extract_text_update,
)

logger = logging.getLogger(__name__)

EXPECTED_INBOUND_OUTCOMES = {"queued", "pairing_pending", "duplicate", "peer_blocked"}
TYPING_STATUS_CODES = {"queue.started", "runtime.started"}


@dataclass(slots=True)
class OffsetStore:
    path: Path

    def load(self) -> int:
        if not self.path.exists():
            return 0
        raw_value = self.path.read_text(encoding="utf-8").strip()
        if raw_value.isdigit():
            return int(raw_value)
        return 0

    def save(self, offset: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(offset), encoding="utf-8")


@dataclass(slots=True)
class PendingTurns:
    answer_buffers: dict[tuple[str, str], str] = field(default_factory=dict)
    active_turns: set[tuple[str, str]] = field(default_factory=set)

    def append_answer(self, peer_id: str, turn_id: str, text: str) -> None:
        key = (peer_id, turn_id)
        self.answer_buffers[key] = self.answer_buffers.get(key, "") + text
        self.active_turns.add(key)

    def replace_answer(self, peer_id: str, turn_id: str, text: str) -> None:
        key = (peer_id, turn_id)
        self.answer_buffers[key] = text
        self.active_turns.add(key)

    def has_turn(self, peer_id: str, turn_id: str) -> bool:
        return (peer_id, turn_id) in self.active_turns

    def final_text(self, peer_id: str, turn_id: str) -> str | None:
        key = (peer_id, turn_id)
        if key not in self.active_turns:
            return None
        return self.answer_buffers.get(key, "") or None

    def clear(self, peer_id: str, turn_id: str) -> None:
        key = (peer_id, turn_id)
        self.active_turns.discard(key)
        self.answer_buffers.pop(key, None)

    def has_pending(self) -> bool:
        return bool(self.active_turns)


class TelegramWorker:
    def __init__(
        self,
        config: WorkerConfig,
        lionclaw_api: LionClawApi,
        telegram: TelegramTransport,
        offset_store: OffsetStore,
    ) -> None:
        self.config = config
        self.lionclaw_api = lionclaw_api
        self.telegram = telegram
        self.offset_store = offset_store
        self.offset = offset_store.load()
        self.pending_turns = PendingTurns()

    async def process_updates(self) -> None:
        try:
            updates = await self.telegram.get_updates(
                offset=self.offset,
                timeout_seconds=self.config.telegram_poll_timeout_secs,
            )
        except Exception:
            logger.exception("telegram getUpdates request failed")
            return

        for update in updates:
            text_update = extract_text_update(update)
            if text_update is not None:
                if not await self._submit_inbound(text_update):
                    return
            self.offset = update.update_id + 1
            self.offset_store.save(self.offset)

    async def flush_stream(self) -> None:
        try:
            events = await self.lionclaw_api.pull_stream()
        except Exception:
            logger.exception("lionclaw stream pull request failed")
            return

        last_safe_sequence: int | None = None
        stop_processing = False

        for event in events:
            if not await self._process_stream_event(event):
                stop_processing = True
                break
            if not self.pending_turns.has_pending():
                last_safe_sequence = event.sequence

        if not stop_processing and last_safe_sequence is not None:
            try:
                await self.lionclaw_api.ack_stream(last_safe_sequence)
            except Exception:
                logger.exception(
                    "lionclaw stream ack failed through sequence %s",
                    last_safe_sequence,
                )

    async def run_forever(self) -> None:
        while True:
            await self.process_updates()
            await self.flush_stream()
            await asyncio.sleep(self.config.telegram_loop_delay_secs)

    async def _submit_inbound(self, update: TelegramTextUpdate) -> bool:
        try:
            response = await self.lionclaw_api.send_inbound(update)
        except Exception:
            logger.exception(
                "lionclaw inbound submit failed for update_id=%s",
                update.update_id,
            )
            return False
        if response.outcome not in EXPECTED_INBOUND_OUTCOMES:
            logger.error(
                "lionclaw inbound returned unknown outcome '%s' for update_id=%s",
                response.outcome,
                update.update_id,
            )
            return False
        return True

    async def _process_stream_event(self, event: StreamEvent) -> bool:
        if event.kind == "message_delta":
            if event.lane == "answer":
                self.pending_turns.append_answer(event.peer_id, event.turn_id, event.text)
            return True

        if event.kind == "status":
            if event.code in TYPING_STATUS_CODES:
                try:
                    await self.telegram.send_typing(event.peer_id)
                except Exception:
                    logger.exception(
                        "telegram sendChatAction request failed for peer_id=%s",
                        event.peer_id,
                    )
                    return False
            return True

        if event.kind == "error":
            logger.error(
                "lionclaw stream error for peer_id=%s turn_id=%s: %s",
                event.peer_id,
                event.turn_id,
                event.text,
            )
            return True

        if event.kind == "turn_completed":
            self.pending_turns.replace_answer(event.peer_id, event.turn_id, event.text)
            return True

        if event.kind == "done":
            if not self.pending_turns.has_turn(event.peer_id, event.turn_id):
                return True
            final_text = self.pending_turns.final_text(event.peer_id, event.turn_id)
            if final_text is None:
                self.pending_turns.clear(event.peer_id, event.turn_id)
                return True
            try:
                await self.telegram.send_message(event.peer_id, final_text)
            except Exception:
                logger.exception(
                    "telegram sendMessage request failed for peer_id=%s",
                    event.peer_id,
                )
                return False
            self.pending_turns.clear(event.peer_id, event.turn_id)
            return True

        logger.error("lionclaw stream contained unknown event kind '%s'", event.kind)
        return True


async def run() -> None:
    config = WorkerConfig.from_env()
    config.runtime_dir.mkdir(parents=True, exist_ok=True)
    config.telegram_offset_file.parent.mkdir(parents=True, exist_ok=True)

    lionclaw_api = LionClawApi(
        base_url=config.lionclaw_base_url,
        channel_id=config.channel_id,
        consumer_id=config.consumer_id,
        start_mode=config.stream_start_mode,
        runtime_id=config.runtime_id,
        stream_limit=config.stream_limit,
        stream_wait_ms=config.stream_wait_ms,
    )
    telegram = AiogramTelegramTransport(config.telegram_bot_token)
    worker = TelegramWorker(
        config=config,
        lionclaw_api=lionclaw_api,
        telegram=telegram,
        offset_store=OffsetStore(config.telegram_offset_file),
    )

    try:
        await worker.run_forever()
    finally:
        await telegram.close()
        await lionclaw_api.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(run())
