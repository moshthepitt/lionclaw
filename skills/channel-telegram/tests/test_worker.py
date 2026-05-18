from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import httpx
from aiogram.types import Update

from lionclaw_channel_telegram.api import LionClawApi, OutboxContent, OutboxDelivery, StreamEvent
from lionclaw_channel_telegram.config import WorkerConfig
from lionclaw_channel_telegram.telegram import TelegramTextUpdate, extract_text_update
from lionclaw_channel_telegram.worker import OffsetStore, TelegramWorker


class ExtractTextUpdateTests(unittest.TestCase):
    def test_private_text_sources_map_to_peer_and_text(self) -> None:
        cases = {
            "message": {
                "update_id": 11,
                "message": {
                    "message_id": 1,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "text": "hello",
                },
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
        }

        for name, payload in cases.items():
            with self.subTest(source=name):
                update = Update.model_validate(payload)
                mapped = extract_text_update(update)
                self.assertIsNotNone(mapped)
                assert mapped is not None
                message = next(value for key, value in payload.items() if key != "update_id")
                self.assertEqual(mapped.update_id, payload["update_id"])
                self.assertEqual(mapped.peer_id, str(message["chat"]["id"]))
                self.assertEqual(mapped.message_ref, str(message["message_id"]))

    def test_non_private_text_updates_are_ignored(self) -> None:
        cases = {
            "group_message": {
                "update_id": 13,
                "message": {
                    "message_id": 3,
                    "date": 0,
                    "chat": {"id": -44, "type": "group"},
                    "text": "group",
                },
            },
            "channel_post": {
                "update_id": 14,
                "channel_post": {
                    "message_id": 4,
                    "date": 0,
                    "chat": {"id": -10044, "type": "channel"},
                    "text": "broadcast",
                },
            },
        }

        for name, payload in cases.items():
            with self.subTest(source=name):
                self.assertIsNone(extract_text_update(Update.model_validate(payload)))

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

        response = await api.send_inbound(
            TelegramTextUpdate(
                update_id=55,
                peer_id="123",
                message_ref="99",
                text="hello",
            )
        )

        self.assertEqual(response.outcome, "queued")
        self.assertEqual(captured["path"], "/v0/channels/inbound")
        self.assertEqual(
            captured["payload"],
            {
                "channel_id": "telegram",
                "event_id": "telegram-update:55",
                "sender_ref": "123",
                "conversation_ref": "123",
                "message_ref": "99",
                "text": "hello",
                "attachments": [],
                "trigger": "dm",
                "provider_metadata": {"update_id": 55, "message_id": "99"},
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

    async def test_pull_outbox_uses_worker_identity(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "deliveries": [
                        {
                            "delivery_id": "delivery-1",
                            "attempt_id": "attempt-1",
                            "conversation_ref": "123",
                            "reply_to_ref": "55",
                            "content": {"text": "hello"},
                        }
                    ]
                },
            )

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

        deliveries = await api.pull_outbox(limit=5, lease_ms=60000)

        self.assertEqual(captured["path"], "/v0/channels/outbox/pull")
        self.assertEqual(
            captured["payload"],
            {
                "channel_id": "telegram",
                "worker_id": "telegram:telegram",
                "limit": 5,
                "lease_ms": 60000,
            },
        )
        self.assertEqual(deliveries[0].delivery_id, "delivery-1")
        self.assertEqual(deliveries[0].content.text, "hello")
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
                    TelegramTextUpdate(
                        update_id=7,
                        peer_id="77",
                        message_ref="1",
                        text="hi",
                    ),
                ],
            )

    async def test_flush_stream_sends_typing_and_acks_done_sequence(self) -> None:
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
        self.assertEqual(telegram.sent_messages, [])
        self.assertEqual(api.acked_sequences, [5])

    async def test_flush_outbox_sends_delivery_and_reports_receipt(self) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="peer-1",
                    reply_to_ref="42",
                    content=OutboxContent(text="final answer"),
                )
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

            await worker.flush_outbox()

        self.assertEqual(telegram.sent_messages, [("peer-1", "final answer", "42")])
        self.assertEqual(
            api.outbox_reports,
            [
                (
                    "delivery-1",
                    "attempt-1",
                    "delivered",
                    {"message_id": 101, "chat_id": "peer-1"},
                    None,
                    None,
                )
            ],
        )

    async def test_failed_outbox_send_reports_terminal_failure(self) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="peer-1",
                    content=OutboxContent(text="final answer"),
                )
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
                await worker.flush_outbox()

        self.assertEqual(api.outbox_reports[0][2], "terminal_failed")
        self.assertEqual(api.outbox_reports[0][4], "telegram.send_failed")


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
    def __init__(
        self,
        stream_events: list[StreamEvent] | None = None,
        outbox_deliveries: list[OutboxDelivery] | None = None,
    ) -> None:
        self.stream_events = list(stream_events or [])
        self.outbox_deliveries = list(outbox_deliveries or [])
        self.sent_inbound: list[TelegramTextUpdate] = []
        self.acked_sequences: list[int] = []
        self.outbox_reports: list[
            tuple[
                str,
                str,
                str,
                dict[str, object] | None,
                str | None,
                str | None,
            ]
        ] = []

    async def send_inbound(self, update: TelegramTextUpdate):
        self.sent_inbound.append(update)
        return type("InboundResponse", (), {"outcome": "queued"})()

    async def pull_stream(self) -> list[StreamEvent]:
        return list(self.stream_events)

    async def ack_stream(self, through_sequence: int) -> None:
        self.acked_sequences.append(through_sequence)

    async def pull_outbox(self) -> list[OutboxDelivery]:
        return list(self.outbox_deliveries)

    async def report_outbox(
        self,
        delivery: OutboxDelivery,
        outcome: str,
        *,
        provider_receipt: dict[str, object] | None = None,
        error_code: str | None = None,
        error_text: str | None = None,
    ):
        self.outbox_reports.append(
            (
                delivery.delivery_id,
                delivery.attempt_id,
                outcome,
                provider_receipt,
                error_code,
                error_text,
            )
        )
        return type(
            "OutboxReportResponse",
            (),
            {"accepted": True, "status": "delivered", "attempt_status": outcome},
        )()


class FakeTelegramTransport:
    def __init__(
        self,
        updates: list[Update] | None = None,
        fail_send_message: bool = False,
    ) -> None:
        self.updates = list(updates or [])
        self.sent_messages: list[tuple[str, str, str | None]] = []
        self.typing_peers: list[str] = []
        self.fail_send_message = fail_send_message

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]:
        return list(self.updates)

    async def send_message(
        self, conversation_ref: str, text: str, reply_to_ref: str | None = None
    ) -> dict[str, object]:
        if self.fail_send_message:
            raise RuntimeError("send failed")
        self.sent_messages.append((conversation_ref, text, reply_to_ref))
        return {"message_id": 101, "chat_id": conversation_ref}

    async def send_typing(self, peer_id: str) -> None:
        self.typing_peers.append(peer_id)
