from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import httpx
from aiogram.types import Update

from lionclaw_channel_telegram.api import LionClawApi, StreamEvent
from lionclaw_channel_telegram.config import WorkerConfig
from lionclaw_channel_telegram.telegram import TelegramTextUpdate, extract_text_update
from lionclaw_channel_telegram.worker import OffsetStore, TelegramWorker


class ExtractTextUpdateTests(unittest.TestCase):
    def test_supported_text_sources_map_to_peer_and_text(self) -> None:
        cases = {
            "message": {
                "update_id": 11,
                "message": {"message_id": 1, "date": 0, "chat": {"id": 42, "type": "private"}, "text": "hello"},
            },
            "edited_message": {
                "update_id": 12,
                "edited_message": {
                    "message_id": 2,
                    "date": 0,
                    "chat": {"id": 43, "type": "private"},
                    "text": "edited",
                },
            },
            "channel_post": {
                "update_id": 13,
                "channel_post": {
                    "message_id": 3,
                    "date": 0,
                    "chat": {"id": -10044, "type": "channel"},
                    "text": "broadcast",
                },
            },
        }

        for name, payload in cases.items():
            with self.subTest(source=name):
                update = Update.model_validate(payload)
                mapped = extract_text_update(update)
                self.assertIsNotNone(mapped)
                assert mapped is not None
                self.assertEqual(mapped.update_id, payload["update_id"])
                self.assertEqual(mapped.peer_id, str(next(value for key, value in payload.items() if key != "update_id")["chat"]["id"]))

    def test_non_text_updates_are_ignored(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 21,
                "message": {
                    "message_id": 4,
                    "date": 0,
                    "chat": {"id": 99, "type": "private"},
                },
            }
        )
        self.assertIsNone(extract_text_update(update))


class LionClawApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_inbound_uses_telegram_external_message_id(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(200, json={"outcome": "queued"})

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = LionClawApi(
            base_url="http://127.0.0.1:8979",
            channel_id="telegram",
            consumer_id="telegram:telegram",
            start_mode="resume",
            stream_limit=100,
            stream_wait_ms=30000,
            client=client,
        )

        response = await api.send_inbound(TelegramTextUpdate(update_id=55, peer_id="123", text="hello"))

        self.assertEqual(response.outcome, "queued")
        self.assertEqual(captured["path"], "/v0/channels/inbound")
        self.assertEqual(
            captured["payload"],
            {
                "channel_id": "telegram",
                "event_id": "telegram-update:55",
                "sender_ref": "123",
                "conversation_ref": "123",
                "text": "hello",
                "attachments": [],
                "trigger": "dm",
                "provider_metadata": {"update_id": 55},
            },
        )
        await api.close()

    async def test_pull_stream_uses_resume_start_mode(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(200, json={"events": []})

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = LionClawApi(
            base_url="http://127.0.0.1:8979",
            channel_id="telegram",
            consumer_id="telegram:telegram",
            start_mode="resume",
            stream_limit=100,
            stream_wait_ms=30000,
            client=client,
        )

        events = await api.pull_stream()

        self.assertEqual(events, [])
        self.assertEqual(captured["path"], "/v0/channels/stream/pull")
        self.assertEqual(
            captured["payload"],
            {
                "channel_id": "telegram",
                "consumer_id": "telegram:telegram",
                "start_mode": "resume",
                "limit": 100,
                "wait_ms": 30000,
            },
        )
        await api.close()


class TelegramWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_updates_persists_offset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=FakeTelegramTransport(
                    updates=[
                        Update.model_validate(
                            {
                                "update_id": 7,
                                "message": {
                                    "message_id": 1,
                                    "date": 0,
                                    "chat": {"id": 77, "type": "private"},
                                    "text": "hi",
                                },
                            }
                        ),
                        Update.model_validate(
                            {
                                "update_id": 8,
                                "channel_post": {
                                    "message_id": 2,
                                    "date": 0,
                                    "chat": {"id": -1009, "type": "channel"},
                                    "text": "news",
                                },
                            }
                        ),
                    ]
                ),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

            self.assertEqual(worker.offset, 9)
            self.assertEqual(worker.offset_store.path.read_text(encoding="utf-8"), "9")
            self.assertEqual(
                worker.lionclaw_api.sent_inbound,
                [
                    TelegramTextUpdate(update_id=7, peer_id="77", text="hi"),
                    TelegramTextUpdate(update_id=8, peer_id="-1009", text="news"),
                ],
            )

    async def test_flush_stream_sends_one_final_message_and_acks_done_sequence(self) -> None:
        api = FakeLionClawApi(
            stream_events=[
                StreamEvent(sequence=1, peer_id="peer-1", turn_id="turn-1", kind="status", code="queue.started"),
                StreamEvent(
                    sequence=2,
                    peer_id="peer-1",
                    turn_id="turn-1",
                    kind="message_delta",
                    lane="reasoning",
                    text="hidden",
                ),
                StreamEvent(
                    sequence=3,
                    peer_id="peer-1",
                    turn_id="turn-1",
                    kind="message_delta",
                    lane="answer",
                    text="partial",
                ),
                StreamEvent(
                    sequence=4,
                    peer_id="peer-1",
                    turn_id="turn-1",
                    kind="turn_completed",
                    text="final answer",
                ),
                StreamEvent(sequence=5, peer_id="peer-1", turn_id="turn-1", kind="done"),
            ]
        )
        telegram = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.flush_stream()

        self.assertEqual(telegram.typing_peers, ["peer-1"])
        self.assertEqual(telegram.sent_messages, [("peer-1", "final answer")])
        self.assertEqual(api.acked_sequences, [5])

    async def test_failed_final_send_leaves_turn_pending_and_skips_ack(self) -> None:
        api = FakeLionClawApi(
            stream_events=[
                StreamEvent(
                    sequence=1,
                    peer_id="peer-1",
                    turn_id="turn-1",
                    kind="message_delta",
                    lane="answer",
                    text="partial",
                ),
                StreamEvent(sequence=2, peer_id="peer-1", turn_id="turn-1", kind="done"),
            ]
        )
        telegram = FakeTelegramTransport(fail_send_message=True)
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.flush_stream()

        self.assertEqual(api.acked_sequences, [])
        self.assertTrue(worker.pending_turns.has_turn("peer-1", "turn-1"))


def build_config(runtime_dir: Path) -> WorkerConfig:
    return WorkerConfig(
        telegram_bot_token="token",
        lionclaw_base_url="http://127.0.0.1:8979",
        channel_id="telegram",
        stream_limit=100,
        stream_wait_ms=30000,
        stream_start_mode="resume",
        consumer_id="telegram:telegram",
        telegram_poll_timeout_secs=25,
        telegram_loop_delay_secs=0.0,
        runtime_dir=runtime_dir,
        telegram_offset_file=runtime_dir / "telegram.offset",
    )


class FakeLionClawApi:
    def __init__(self, stream_events: list[StreamEvent] | None = None) -> None:
        self.stream_events = list(stream_events or [])
        self.sent_inbound: list[TelegramTextUpdate] = []
        self.acked_sequences: list[int] = []

    async def send_inbound(self, update: TelegramTextUpdate):
        self.sent_inbound.append(update)
        return type("InboundResponse", (), {"outcome": "queued"})()

    async def pull_stream(self) -> list[StreamEvent]:
        return list(self.stream_events)

    async def ack_stream(self, through_sequence: int) -> None:
        self.acked_sequences.append(through_sequence)


class FakeTelegramTransport:
    def __init__(
        self,
        updates: list[Update] | None = None,
        fail_send_message: bool = False,
    ) -> None:
        self.updates = list(updates or [])
        self.sent_messages: list[tuple[str, str]] = []
        self.typing_peers: list[str] = []
        self.fail_send_message = fail_send_message

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]:
        return list(self.updates)

    async def send_message(self, peer_id: str, text: str) -> None:
        if self.fail_send_message:
            raise RuntimeError("send failed")
        self.sent_messages.append((peer_id, text))

    async def send_typing(self, peer_id: str) -> None:
        self.typing_peers.append(peer_id)
