from __future__ import annotations

import asyncio
import contextlib
import json
import tempfile
import unittest
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import httpx
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Update

from lionclaw_channel_telegram.api import (
    AttachmentFinalizeResponse,
    AttachmentMissingReport,
    AttachmentStageResponse,
    HealthCheck,
    HealthReportResponse,
    InboundResponse,
    LionClawApi,
    OutboxAttachment,
    OutboxContent,
    OutboxDelivery,
    PairingClaimResponse,
    SessionActionResult,
    StreamEvent,
)
from lionclaw_channel_telegram.config import WorkerConfig
from lionclaw_channel_telegram.telegram import (
    TelegramBotIdentity,
    TelegramDownloadedAttachment,
    TelegramInboundAttachment,
    TelegramInboundUpdate,
    TelegramOutboundAttachment,
    TelegramPairingClaim,
    TelegramReferenceError,
    _coerce_thread_id,
    _format_telegram_text_chunks,
    _markdown_to_telegram_html,
    _split_telegram_text,
    extract_inbound_event,
)
from lionclaw_channel_telegram.worker import (
    OffsetStore,
    TelegramWorker,
    _classify_send_failure,
    _overall_health_status,
)

BOT = TelegramBotIdentity(user_id=99, username="lionclaw_bot")


class ExtractInboundEventTests(unittest.TestCase):
    def test_private_text_maps_to_channels_v2_refs(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 11,
                "message": {
                    "message_id": 1,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "text": "hello",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.event_id, "telegram:update:11")
        self.assertEqual(mapped.sender_ref, "telegram:user:42")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:42")
        self.assertEqual(mapped.message_ref, "telegram:message:1")
        self.assertIsNone(mapped.thread_ref)
        self.assertEqual(mapped.text, "hello")
        self.assertEqual(mapped.trigger, "dm")
        self.assertEqual(mapped.attachments, [])
        self.assertEqual(mapped.provider_metadata["chat_type"], "private")

    def test_group_mention_uses_entity_and_topic_refs(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 12,
                "message": {
                    "message_id": 2,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "message_thread_id": 77,
                    "is_topic_message": True,
                    "text": "@lionclaw_bot status",
                    "entities": [{"type": "mention", "offset": 0, "length": 13}],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.sender_ref, "telegram:user:9")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:-10042")
        self.assertEqual(mapped.thread_ref, "telegram:topic:77")
        self.assertEqual(mapped.trigger, "mention")
        self.assertTrue(mapped.provider_metadata["bot_mentioned"])

    def test_group_command_targets_bot_by_entity(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 13,
                "message": {
                    "message_id": 3,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "text": "/status@lionclaw_bot",
                    "entities": [{"type": "bot_command", "offset": 0, "length": 20}],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.trigger, "mention")

    def test_edited_message_sets_metadata_without_changing_message_ref(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 131,
                "edited_message": {
                    "message_id": 31,
                    "date": 0,
                    "edit_date": 1,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "text": "edited",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.event_id, "telegram:update:131")
        self.assertEqual(mapped.message_ref, "telegram:message:31")
        self.assertTrue(mapped.provider_metadata["edited"])
        self.assertEqual(mapped.provider_metadata["source"], "edited_message")

    def test_reply_to_bot_uses_reply_trigger_and_ref(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 14,
                "message": {
                    "message_id": 4,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "text": "yes",
                    "reply_to_message": {
                        "message_id": 3,
                        "date": 0,
                        "chat": {"id": -10042, "type": "supergroup"},
                        "from": {
                            "id": 99,
                            "is_bot": True,
                            "first_name": "LionClaw",
                            "username": "lionclaw_bot",
                        },
                        "text": "question",
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.trigger, "reply_to_bot")
        self.assertEqual(mapped.reply_to_ref, "telegram:message:3")

    def test_mention_takes_precedence_over_reply_to_bot(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 141,
                "message": {
                    "message_id": 41,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "text": "@lionclaw_bot yes",
                    "entities": [{"type": "mention", "offset": 0, "length": 13}],
                    "reply_to_message": {
                        "message_id": 40,
                        "date": 0,
                        "chat": {"id": -10042, "type": "supergroup"},
                        "from": {
                            "id": 99,
                            "is_bot": True,
                            "first_name": "LionClaw",
                            "username": "lionclaw_bot",
                        },
                        "text": "question",
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.trigger, "mention")
        self.assertEqual(mapped.reply_to_ref, "telegram:message:40")

    def test_plain_group_message_is_untargeted(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 15,
                "message": {
                    "message_id": 5,
                    "date": 0,
                    "chat": {"id": -44, "type": "group"},
                    "from": {"id": 12, "is_bot": False, "first_name": "Bo"},
                    "text": "ordinary",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.trigger, "none")

    def test_private_reply_message_thread_id_is_not_a_topic(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 16,
                "message": {
                    "message_id": 6,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "message_thread_id": 55,
                    "is_topic_message": False,
                    "text": "reply",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertIsNone(mapped.thread_ref)
        self.assertEqual(mapped.trigger, "dm")

    def test_topic_message_maps_to_thread_continuation(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 17,
                "message": {
                    "message_id": 7,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "message_thread_id": 1,
                    "is_topic_message": True,
                    "text": "topic continuation",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.thread_ref, "telegram:topic:1")
        self.assertEqual(mapped.trigger, "thread_continuation")

    def test_forum_general_topic_without_thread_id_maps_to_topic_one(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 171,
                "message": {
                    "message_id": 71,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup", "is_forum": True},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "text": "general topic",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.thread_ref, "telegram:topic:1")
        self.assertEqual(mapped.trigger, "thread_continuation")

    def test_channel_post_uses_sender_chat_identity(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 18,
                "channel_post": {
                    "message_id": 8,
                    "date": 0,
                    "chat": {"id": -1007, "type": "channel", "title": "News"},
                    "sender_chat": {"id": -1007, "type": "channel", "title": "News"},
                    "text": "broadcast",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.sender_ref, "telegram:sender_chat:-1007")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:-1007")
        self.assertEqual(mapped.trigger, "none")

    def test_pairing_token_claim_is_not_inbound_turn(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 19,
                "message": {
                    "message_id": 9,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "text": "/start lc_0123456789abcdef",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramPairingClaim)
        assert isinstance(mapped, TelegramPairingClaim)
        self.assertEqual(mapped.token, "lc_0123456789abcdef")
        self.assertEqual(mapped.sender_ref, "telegram:user:42")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:42")
        self.assertEqual(mapped.message_ref, "telegram:message:9")

    def test_startgroup_pairing_token_claim_is_not_inbound_turn(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 191,
                "message": {
                    "message_id": 91,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "text": "/startgroup@lionclaw_bot lc_0123456789abcdef",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramPairingClaim)
        assert isinstance(mapped, TelegramPairingClaim)
        self.assertEqual(mapped.token, "lc_0123456789abcdef")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:-10042")

    def test_plain_text_mentioning_pairing_token_remains_inbound(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 192,
                "message": {
                    "message_id": 92,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "text": "Please explain lc_0123456789abcdef",
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.text, "Please explain lc_0123456789abcdef")
        self.assertEqual(mapped.trigger, "dm")

    def test_document_caption_becomes_text_and_attachment_descriptor(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 20,
                "message": {
                    "message_id": 10,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "caption": "please read",
                    "document": {
                        "file_id": "doc-file-id",
                        "file_unique_id": "doc-unique",
                        "file_name": "brief.txt",
                        "mime_type": "text/plain",
                        "file_size": 123,
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.text, "please read")
        self.assertEqual(len(mapped.attachments), 1)
        attachment = mapped.attachments[0]
        self.assertEqual(
            attachment.attachment_id,
            "telegram:update:20:attachment:document:doc-unique",
        )
        self.assertEqual(attachment.provider_file_ref, "doc-file-id")
        self.assertEqual(attachment.filename, "brief.txt")
        self.assertEqual(attachment.mime_type, "text/plain")
        self.assertEqual(attachment.size_bytes, 123)
        self.assertEqual(attachment.caption, "please read")

    def test_supported_media_types_create_attachment_descriptors(self) -> None:
        cases = {
            "photo": (
                {
                    "photo": [
                        {
                            "file_id": "photo-small",
                            "file_unique_id": "photo-small-unique",
                            "width": 32,
                            "height": 32,
                            "file_size": 10,
                        },
                        {
                            "file_id": "photo-large",
                            "file_unique_id": "photo-large-unique",
                            "width": 640,
                            "height": 480,
                            "file_size": 100,
                        },
                    ]
                },
                "photo",
                "image/jpeg",
                "photo-large",
            ),
            "voice": (
                {
                    "voice": {
                        "file_id": "voice-file",
                        "file_unique_id": "voice-unique",
                        "duration": 2,
                        "mime_type": "audio/ogg",
                        "file_size": 20,
                    }
                },
                "voice",
                "audio/ogg",
                "voice-file",
            ),
            "video": (
                {
                    "video": {
                        "file_id": "video-file",
                        "file_unique_id": "video-unique",
                        "width": 640,
                        "height": 480,
                        "duration": 2,
                        "mime_type": "video/mp4",
                        "file_size": 30,
                    }
                },
                "video",
                "video/mp4",
                "video-file",
            ),
            "sticker": (
                {
                    "sticker": {
                        "file_id": "sticker-file",
                        "file_unique_id": "sticker-unique",
                        "type": "regular",
                        "width": 512,
                        "height": 512,
                        "is_animated": False,
                        "is_video": False,
                        "file_size": 40,
                    }
                },
                "sticker",
                "image/webp",
                "sticker-file",
            ),
        }

        for update_id, (name, (media, kind, mime_type, file_ref)) in enumerate(
            cases.items(),
            start=30,
        ):
            with self.subTest(media=name):
                update = Update.model_validate(
                    {
                        "update_id": update_id,
                        "message": {
                            "message_id": update_id,
                            "date": 0,
                            "chat": {"id": 42, "type": "private"},
                            "from": {
                                "id": 42,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            **media,
                        },
                    }
                )

                mapped = extract_inbound_event(update, bot_identity=BOT)

                self.assertIsInstance(mapped, TelegramInboundUpdate)
                assert isinstance(mapped, TelegramInboundUpdate)
                self.assertEqual(len(mapped.attachments), 1)
                self.assertEqual(mapped.attachments[0].kind, kind)
                self.assertEqual(mapped.attachments[0].mime_type, mime_type)
                self.assertEqual(mapped.attachments[0].provider_file_ref, file_ref)

    def test_bot_sender_is_ignored(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 21,
                "message": {
                    "message_id": 11,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 99, "is_bot": True, "first_name": "Bot"},
                    "text": "loop",
                },
            }
        )

        self.assertIsNone(extract_inbound_event(update, bot_identity=BOT))


class LionClawApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_inbound_uses_channels_v2_payload(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "outcome": "queued",
                    "turn_id": "turn-1",
                    "session_id": "session-1",
                    "session_key": "channel:telegram:direct:123",
                },
            )

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = build_api(client)

        response = await api.send_inbound(
            TelegramInboundUpdate(
                update_id=55,
                event_id="telegram:update:55",
                sender_ref="telegram:user:123",
                conversation_ref="telegram:chat:123",
                thread_ref=None,
                message_ref="telegram:message:99",
                reply_to_ref=None,
                text="hello",
                trigger="dm",
                attachments=[
                    TelegramInboundAttachment(
                        attachment_id="telegram:update:55:attachment:document:file",
                        kind="document",
                        provider_file_ref="file-id",
                        mime_type="text/plain",
                        filename="note.txt",
                        size_bytes=4,
                        caption="hello",
                    )
                ],
                provider_metadata={"update_id": 55},
            )
        )

        self.assertEqual(response.outcome, "queued")
        self.assertEqual(response.turn_id, "turn-1")
        self.assertEqual(response.session_id, "session-1")
        self.assertEqual(response.session_key, "channel:telegram:direct:123")
        self.assertEqual(captured["path"], "/v0/channels/inbound")
        payload = captured["payload"]
        assert isinstance(payload, dict)
        self.assertEqual(payload["channel_id"], "telegram")
        self.assertEqual(payload["event_id"], "telegram:update:55")
        self.assertEqual(payload["sender_ref"], "telegram:user:123")
        self.assertEqual(payload["conversation_ref"], "telegram:chat:123")
        self.assertEqual(payload["message_ref"], "telegram:message:99")
        self.assertEqual(payload["trigger"], "dm")
        self.assertEqual(payload["provider_metadata"], {"update_id": 55})
        self.assertNotIn("runtime_id", payload)
        self.assertNotIn("peer_id", payload)
        self.assertNotIn("external_message_id", payload)
        self.assertEqual(
            payload["attachments"],
            [
                {
                    "attachment_id": "telegram:update:55:attachment:document:file",
                    "kind": "document",
                    "mime_type": "text/plain",
                    "filename": "note.txt",
                    "size_bytes": 4,
                    "provider_file_ref": "file-id",
                    "caption": "hello",
                }
            ],
        )
        await api.close()

    async def test_cancel_active_turn_uses_tagged_session_action(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={"session_id": "session-1", "turn_id": "turn-1"},
            )

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = build_api(client)

        response = await api.cancel_active_turn(
            session_id="session-1",
            session_key="channel:telegram:direct:123",
            expected_turn_id="turn-1",
            reason="telegram stop command",
        )

        self.assertEqual(response.turn_id, "turn-1")
        self.assertEqual(captured["path"], "/v0/sessions/action")
        self.assertEqual(
            captured["payload"],
            {
                "action": "cancel_active_turn",
                "session_id": "session-1",
                "channel_id": "telegram",
                "session_key": "channel:telegram:direct:123",
                "expected_turn_id": "turn-1",
                "reason": "telegram stop command",
            },
        )
        await api.close()

    async def test_claim_pairing_uses_claim_endpoint(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={"outcome": "approved", "grant_id": "grant-1"},
            )

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = build_api(client)

        response = await api.claim_pairing(
            TelegramPairingClaim(
                token="lc_0123456789abcdef",
                sender_ref="telegram:user:42",
                conversation_ref="telegram:chat:42",
                update_id=1,
                message_ref="telegram:message:1",
                provider_metadata={"source": "message"},
            )
        )

        self.assertEqual(response.outcome, "approved")
        self.assertEqual(captured["path"], "/v0/channels/pairing/claim")
        self.assertEqual(
            captured["payload"],
            {
                "channel_id": "telegram",
                "token": "lc_0123456789abcdef",
                "sender_ref": "telegram:user:42",
                "conversation_ref": "telegram:chat:42",
                "thread_ref": None,
                "provider_metadata": {"source": "message"},
            },
        )
        await api.close()

    async def test_pull_outbox_parses_attachments_and_scoped_refs(self) -> None:
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
                            "conversation_ref": "telegram:chat:-1001",
                            "thread_ref": "telegram:topic:77",
                            "reply_to_ref": "telegram:message:55",
                            "session_id": "session-1",
                            "turn_id": "turn-1",
                            "content": {
                                "text": "hello",
                                "format_hint": "markdown",
                                "attachments": [
                                    {
                                        "attachment_id": "att-1",
                                        "path": "/tmp/a.txt",
                                        "filename": "a.txt",
                                        "mime_type": "text/plain",
                                    }
                                ],
                            },
                        }
                    ]
                },
            )

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = build_api(client)

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
        self.assertEqual(deliveries[0].conversation_ref, "telegram:chat:-1001")
        self.assertEqual(deliveries[0].thread_ref, "telegram:topic:77")
        self.assertEqual(deliveries[0].reply_to_ref, "telegram:message:55")
        self.assertEqual(deliveries[0].session_id, "session-1")
        self.assertEqual(deliveries[0].turn_id, "turn-1")
        self.assertEqual(deliveries[0].content.attachments[0].path, "/tmp/a.txt")
        await api.close()

    async def test_report_health_uses_channel_health_endpoint(self) -> None:
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(
                200,
                json={
                    "accepted": True,
                    "channel_id": "telegram",
                    "observed_at": "2026-05-18T17:00:00Z",
                },
            )

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = build_api(client)

        response = await api.report_health(
            "warning",
            [
                HealthCheck(
                    code="telegram.delivery_errors",
                    status="warning",
                    message="1 delivery failure observed",
                    details={"retryable_failed": 1, "terminal_failed": 0},
                )
            ],
            observed_at=datetime(2026, 5, 18, 17, 0, tzinfo=UTC),
        )

        self.assertTrue(response.accepted)
        self.assertEqual(captured["path"], "/v0/channels/health/report")
        self.assertEqual(
            captured["payload"],
            {
                "channel_id": "telegram",
                "reporter_id": "telegram:telegram",
                "status": "warning",
                "checks": [
                    {
                        "code": "telegram.delivery_errors",
                        "status": "warning",
                        "message": "1 delivery failure observed",
                        "details": {
                            "retryable_failed": 1,
                            "terminal_failed": 0,
                        },
                    }
                ],
                "observed_at": "2026-05-18T17:00:00+00:00",
            },
        )
        await api.close()


class OffsetStoreTests(unittest.TestCase):
    def test_save_replaces_offset_file_without_leaving_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram.offset"
            store = OffsetStore(path)

            store.save(42)
            store.save(43)

            self.assertEqual(store.load(), 43)
            self.assertEqual(path.read_text(encoding="utf-8"), "43")
            self.assertFalse((path.parent / ".telegram.offset.tmp").exists())


class TelegramWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_updates_routes_pairing_and_inbound_and_persists_offset(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 7,
                            "message": {
                                "message_id": 1,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/start lc_0123456789abcdef",
                            },
                        }
                    ),
                    Update.model_validate(
                        {
                            "update_id": 8,
                            "message": {
                                "message_id": 2,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "hi",
                            },
                        }
                    ),
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

            self.assertEqual(worker.offset, 9)
            self.assertEqual(worker.offset_store.path.read_text(encoding="utf-8"), "9")
            self.assertEqual(len(api.claims), 1)
            self.assertEqual(api.claims[0].token, "lc_0123456789abcdef")
            self.assertEqual(len(api.sent_inbound), 1)
            self.assertEqual(api.sent_inbound[0].event_id, "telegram:update:8")
            self.assertEqual(api.finalized, [])
            self.assertEqual(telegram.typing_peers, ["telegram:chat:77"])
            self.assertEqual(
                telegram.sent_messages[0],
                (
                    "telegram:chat:77",
                    "Pairing approved. You can send a message now.",
                    "telegram:message:1",
                    None,
                    [],
                ),
            )

    async def test_offset_save_failure_keeps_unpersisted_update_retryable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 7,
                            "message": {
                                "message_id": 1,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "first",
                            },
                        }
                    ),
                    Update.model_validate(
                        {
                            "update_id": 8,
                            "message": {
                                "message_id": 2,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "second",
                            },
                        }
                    ),
                ]
            )
            offset_path = Path(temp_dir) / "telegram.offset"
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=FailingOffsetStore(offset_path),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.process_updates()

            self.assertEqual(worker.offset, 0)
            self.assertFalse(offset_path.exists())
            self.assertEqual([update.text for update in api.sent_inbound], ["first"])

    async def test_malformed_update_is_quarantined_and_offset_advances(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    MalformedTelegramUpdate(update_id=7),
                    Update.model_validate(
                        {
                            "update_id": 8,
                            "message": {
                                "message_id": 2,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "after malformed",
                            },
                        }
                    ),
                ]
            )
            offset_path = Path(temp_dir) / "telegram.offset"
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(offset_path),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.process_updates()

            self.assertEqual(worker.offset, 9)
            self.assertEqual(offset_path.read_text(encoding="utf-8"), "9")
            self.assertEqual(
                [update.text for update in api.sent_inbound],
                ["after malformed"],
            )

            await worker.report_health()

            self.assertEqual(api.health_reports[0][0], "warning")
            checks_by_code = {check.code: check for check in api.health_reports[0][1]}
            update_extraction = checks_by_code["telegram.update_extraction"]
            self.assertEqual(update_extraction.status, "warning")
            self.assertEqual(update_extraction.details["malformed_updates"], 1)

    async def test_waiting_for_attachments_downloads_stages_and_finalizes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="waiting_for_attachments")
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 9,
                            "message": {
                                "message_id": 3,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "caption": "read this",
                                "document": {
                                    "file_id": "doc-file",
                                    "file_unique_id": "doc-unique",
                                    "file_name": "brief.txt",
                                    "mime_type": "text/plain",
                                    "file_size": 12,
                                },
                            },
                        }
                    )
                ],
                downloaded_content=b"hello",
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

            self.assertEqual(
                telegram.downloaded_attachment_ids,
                ["telegram:update:9:attachment:document:doc-unique"],
            )
            self.assertEqual(len(api.staged_attachments), 1)
            self.assertEqual(api.finalized[0][0].event_id, "telegram:update:9")
            self.assertEqual(api.finalized[0][1], [])
            self.assertEqual(telegram.typing_peers, ["telegram:chat:77"])

    async def test_help_is_channel_local_and_does_not_submit_inbound(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 91,
                            "message": {
                                "message_id": 1,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/help",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(api.sent_inbound, [])
        self.assertIn("LionClaw controls", telegram.sent_messages[0][1])

    async def test_retry_alias_submits_canonical_lionclaw_control(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 92,
                            "message": {
                                "message_id": 2,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/retry",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/lionclaw retry")

    async def test_retry_alias_accepts_newline_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 921,
                            "message": {
                                "message_id": 21,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/retry\nplease",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/lionclaw retry")

    async def test_retry_alias_deletes_superseded_progress_message(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-2",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 922,
                            "message": {
                                "message_id": 22,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/retry",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await worker._remember_active_turn(
                TelegramInboundUpdate(
                    update_id=921,
                    event_id="telegram:update:921",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:21",
                    text="slow",
                    trigger="dm",
                    provider_metadata={"chat_type": "private"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()

            await worker.process_updates()

        self.assertEqual(api.sent_inbound[0].text, "/lionclaw retry")
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertNotIn("turn-1", worker._active_turns)
        self.assertIn("turn-2", worker._active_turns)

    async def test_model_command_passes_through_to_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 93,
                            "message": {
                                "message_id": 3,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/model gpt-5.2",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/model gpt-5.2")

    async def test_unaddressed_group_local_command_passes_through(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 931,
                            "message": {
                                "message_id": 31,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/stop",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/stop")
        self.assertEqual(api.cancel_calls, [])

    async def test_stop_cancels_active_turn_with_expected_turn_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 94,
                            "message": {
                                "message_id": 4,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "long task",
                            },
                        }
                    ),
                    Update.model_validate(
                        {
                            "update_id": 95,
                            "message": {
                                "message_id": 5,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/stop",
                            },
                        }
                    ),
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(
            api.cancel_calls,
            [
                (
                    "session-1",
                    "channel:telegram:direct:77",
                    "turn-1",
                    "telegram stop command",
                )
            ],
        )
        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(telegram.sent_messages[-1][1], "Stopping...")

    async def test_stop_failure_removes_new_stopping_progress_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
                cancel_error=RuntimeError("kernel unavailable"),
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 941,
                            "message": {
                                "message_id": 41,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "long task",
                            },
                        }
                    ),
                    Update.model_validate(
                        {
                            "update_id": 942,
                            "message": {
                                "message_id": 42,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/stop",
                            },
                        }
                    ),
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.process_updates()

        self.assertEqual(telegram.sent_messages[0][1], "Stopping...")
        self.assertEqual(telegram.sent_messages[-1][1], "I could not stop that turn.")
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )

    async def test_stop_with_stale_local_turn_deletes_progress_and_forgets_turn(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(cancel_turn_id=None)
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await worker._remember_active_turn(
                TelegramInboundUpdate(
                    update_id=943,
                    event_id="telegram:update:943",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:43",
                    text="slow",
                    trigger="dm",
                    provider_metadata={"chat_type": "private"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()

            await worker._stop_active_turn(
                TelegramInboundUpdate(
                    update_id=944,
                    event_id="telegram:update:944",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:44",
                    text="/stop",
                    trigger="dm",
                    provider_metadata={"chat_type": "private"},
                )
            )

        self.assertEqual(
            api.cancel_calls,
            [
                (
                    "session-1",
                    "channel:telegram:direct:77",
                    "turn-1",
                    "telegram stop command",
                )
            ],
        )
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertEqual(
            telegram.sent_messages[-1][1],
            "No active LionClaw turn is running here.",
        )
        self.assertEqual(worker._active_turns, {})
        self.assertEqual(worker._route_turns, {})

    async def test_stage_api_failure_keeps_update_retryable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_outcome="waiting_for_attachments",
                stage_error=RuntimeError("kernel unavailable"),
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 13,
                            "message": {
                                "message_id": 7,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "document": {
                                    "file_id": "doc-file",
                                    "file_unique_id": "doc-unique",
                                    "file_name": "brief.txt",
                                    "mime_type": "text/plain",
                                    "file_size": 12,
                                },
                            },
                        }
                    )
                ],
                downloaded_content=b"hello",
            )
            offset_store = OffsetStore(Path(temp_dir) / "telegram.offset")
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=offset_store,
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.process_updates()

            self.assertEqual(
                telegram.downloaded_attachment_ids,
                ["telegram:update:13:attachment:document:doc-unique"],
            )
            self.assertEqual(len(api.staged_attachments), 1)
            self.assertEqual(api.finalized, [])
            self.assertEqual(worker.offset, 0)
            self.assertFalse(offset_store.path.exists())

    async def test_run_forever_flushes_outbox_while_update_poll_is_waiting(
        self,
    ) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:user:77",
                    thread_ref=None,
                    reply_to_ref=None,
                    content=OutboxContent(text="final answer"),
                )
            ]
        )
        telegram = BlockingUpdatesTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            task = asyncio.create_task(worker.run_forever())

            await asyncio.wait_for(telegram.poll_started.wait(), timeout=1.0)
            await asyncio.wait_for(
                _wait_until(lambda: len(telegram.sent_messages) >= 1),
                timeout=1.0,
            )

            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_typing_preserves_topic_context_across_stream_events(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 15,
                "message": {
                    "message_id": 7,
                    "date": 0,
                    "chat": {"id": -1001, "type": "supergroup", "is_forum": True},
                    "from": {"id": 77, "is_bot": False, "first_name": "Alice"},
                    "message_thread_id": 1,
                    "text": "@lionclaw_bot inspect",
                    "entities": [
                        {"type": "mention", "offset": 0, "length": len("@lionclaw_bot")}
                    ],
                },
            }
        )
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport(updates=[update])
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            await worker._process_stream_event(
                StreamEvent(
                    sequence=16,
                    peer_id="telegram:chat:-1001",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.started",
                )
            )

        self.assertEqual(
            telegram.typing_calls,
            [
                ("telegram:chat:-1001", "telegram:topic:1"),
                ("telegram:chat:-1001", "telegram:topic:1"),
            ],
        )

    async def test_stream_typing_uses_turn_topic_not_latest_chat_route(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport()
        first_target = ("telegram:chat:-1001", "telegram:topic:1")
        second_target = ("telegram:chat:-1001", "telegram:topic:2")
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            first_update = TelegramInboundUpdate(
                update_id=151,
                event_id="telegram:update:151",
                sender_ref="telegram:user:77",
                conversation_ref="telegram:chat:-1001",
                thread_ref="telegram:topic:1",
                message_ref="telegram:message:151",
                text="first topic",
                trigger="thread_continuation",
                provider_metadata={"chat_type": "supergroup"},
            )
            second_update = replace(
                first_update,
                update_id=152,
                event_id="telegram:update:152",
                thread_ref="telegram:topic:2",
                message_ref="telegram:message:152",
                text="second topic",
            )
            worker._remember_typing_route(first_update)
            await worker._remember_active_turn(
                first_update,
                InboundResponse(outcome="queued", turn_id="turn-1"),
            )
            worker._remember_typing_route(second_update)
            await worker._remember_active_turn(
                second_update,
                InboundResponse(outcome="queued", turn_id="turn-2"),
            )

            await worker._process_stream_event(
                StreamEvent(
                    sequence=151,
                    peer_id="telegram:chat:-1001",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.started",
                )
            )

            self.assertEqual(
                telegram.typing_calls[-1],
                first_target,
            )
            self.assertIn(first_target, worker._typing_deadlines)
            self.assertNotIn(second_target, worker._typing_deadlines)

            await worker._process_stream_event(
                StreamEvent(
                    sequence=152,
                    peer_id="telegram:chat:-1001",
                    turn_id="turn-1",
                    kind="turn_completed",
                )
            )

            self.assertNotIn(
                first_target,
                worker._typing_deadlines,
            )

    async def test_blocked_attachment_inbound_does_not_download(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="blocked")
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 10,
                            "message": {
                                "message_id": 4,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "document": {
                                    "file_id": "doc-file",
                                    "file_unique_id": "doc-unique",
                                    "file_name": "brief.txt",
                                    "mime_type": "text/plain",
                                    "file_size": 12,
                                },
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

            self.assertEqual(telegram.downloaded_attachment_ids, [])
            self.assertEqual(api.staged_attachments, [])
            self.assertEqual(api.finalized, [])

    async def test_pending_approval_notifies_with_pairing_code_without_download(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_outcome="pending_approval",
                inbound_pairing_code="pc_abcdef12",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 11,
                            "message": {
                                "message_id": 5,
                                "date": 0,
                                "chat": {"id": -44, "type": "group"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "@lionclaw_bot hi",
                                "entities": [
                                    {"type": "mention", "offset": 0, "length": 13}
                                ],
                                "document": {
                                    "file_id": "doc-file",
                                    "file_unique_id": "doc-unique",
                                    "file_name": "brief.txt",
                                    "mime_type": "text/plain",
                                    "file_size": 12,
                                },
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

            self.assertEqual(telegram.downloaded_attachment_ids, [])
            self.assertEqual(
                telegram.sent_messages,
                [
                    (
                        "telegram:chat:-44",
                        "This Telegram scope needs approval. Pairing code: pc_abcdef12",
                        "telegram:message:5",
                        None,
                        [],
                    )
                ],
            )

    async def test_inaccessible_attachment_finalizes_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="waiting_for_attachments")
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 12,
                            "message": {
                                "message_id": 6,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "document": {
                                    "file_id": "doc-file",
                                    "file_unique_id": "doc-unique",
                                    "file_name": "brief.txt",
                                    "mime_type": "text/plain",
                                    "file_size": 12,
                                },
                            },
                        }
                    )
                ],
                fail_download=True,
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.process_updates()

            self.assertEqual(api.staged_attachments, [])
            missing = api.finalized[0][1]
            self.assertEqual(len(missing), 1)
            self.assertEqual(missing[0].reason_code, "telegram.download_failed")

    async def test_flush_stream_sends_typing_and_acks_done_sequence(self) -> None:
        api = FakeLionClawApi(
            stream_events=[
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:user:77",
                    turn_id="turn-1",
                    kind="status",
                    code="queue.started",
                ),
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:user:77",
                    turn_id="turn-1",
                    kind="message_delta",
                    lane="answer",
                    text="partial",
                ),
                StreamEvent(
                    sequence=3,
                    peer_id="telegram:user:77",
                    turn_id="turn-1",
                    kind="turn_completed",
                    text="final answer",
                ),
                StreamEvent(
                    sequence=4,
                    peer_id="telegram:user:77",
                    turn_id="turn-1",
                    kind="done",
                ),
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

        self.assertEqual(telegram.typing_peers, ["telegram:user:77"])
        self.assertEqual(telegram.sent_messages, [])
        self.assertEqual(api.acked_sequences, [4])

    async def test_fast_turn_does_not_create_progress_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 96,
                            "message": {
                                "message_id": 6,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "quick",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="turn_completed",
                )
            )
            await worker.refresh_progress_messages()

        self.assertEqual(telegram.sent_messages, [])

    async def test_long_turn_creates_edits_and_deletes_one_progress_message(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 97,
                            "message": {
                                "message_id": 7,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "slow",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()
            worker._active_turns["turn-1"].last_edit_at = 0.0
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.started",
                )
            )
            await worker.refresh_progress_messages()
            await worker._process_stream_event(
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="turn_completed",
                )
            )
            self.assertEqual(telegram.deleted_messages, [])
            await worker._process_outbox_delivery(
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:chat:77",
                    turn_id="turn-1",
                    content=OutboxContent(text="final answer"),
                )
            )

        self.assertEqual(
            telegram.sent_messages,
            [
                (
                    "telegram:chat:77",
                    "Queued...",
                    "telegram:message:7",
                    None,
                    [],
                ),
                (
                    "telegram:chat:77",
                    "final answer",
                    None,
                    None,
                    [],
                ),
            ],
        )
        self.assertEqual(
            telegram.edited_messages,
            [
                ("telegram:chat:77", "telegram:message:101", "Working..."),
                ("telegram:chat:77", "telegram:message:101", "Finishing..."),
            ],
        )
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )

    async def test_unscoped_outbox_delivery_does_not_delete_active_progress(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await worker._remember_active_turn(
                TelegramInboundUpdate(
                    update_id=971,
                    event_id="telegram:update:971",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:71",
                    text="slow",
                    trigger="dm",
                    provider_metadata={"chat_type": "private"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()

            await worker._process_outbox_delivery(
                OutboxDelivery(
                    delivery_id="delivery-unscoped",
                    attempt_id="attempt-unscoped",
                    conversation_ref="telegram:chat:77",
                    content=OutboxContent(text="scheduled summary"),
                )
            )

        self.assertEqual(telegram.deleted_messages, [])
        self.assertIn("turn-1", worker._active_turns)
        self.assertEqual(telegram.sent_messages[-1][1], "scheduled summary")

    async def test_old_outbox_delivery_does_not_delete_new_active_progress(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            update = TelegramInboundUpdate(
                update_id=972,
                event_id="telegram:update:972",
                sender_ref="telegram:user:77",
                conversation_ref="telegram:chat:77",
                message_ref="telegram:message:72",
                text="slow",
                trigger="dm",
                provider_metadata={"chat_type": "private"},
            )
            await worker._remember_active_turn(
                update,
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            await worker._remember_active_turn(
                replace(
                    update,
                    update_id=973,
                    event_id="telegram:update:973",
                    message_ref="telegram:message:73",
                    text="new slow",
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-2",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            worker._active_turns["turn-2"].visible_after = 0.0
            await worker.refresh_progress_messages()

            await worker._process_outbox_delivery(
                OutboxDelivery(
                    delivery_id="delivery-old",
                    attempt_id="attempt-old",
                    conversation_ref="telegram:chat:77",
                    turn_id="turn-1",
                    content=OutboxContent(text="old final answer"),
                )
            )

        self.assertEqual(telegram.deleted_messages, [])
        self.assertNotIn("turn-1", worker._active_turns)
        self.assertIn("turn-2", worker._active_turns)
        self.assertEqual(telegram.sent_messages[-1][1], "old final answer")

    async def test_duplicate_turn_refresh_preserves_progress_message_ref(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            update = TelegramInboundUpdate(
                update_id=974,
                event_id="telegram:update:974",
                sender_ref="telegram:user:77",
                conversation_ref="telegram:chat:77",
                message_ref="telegram:message:74",
                text="slow",
                trigger="dm",
                provider_metadata={"chat_type": "private"},
            )
            initial = InboundResponse(
                outcome="queued",
                turn_id="turn-1",
                session_id=None,
                session_key=None,
            )
            await worker._remember_active_turn(update, initial)
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()

            await worker._remember_active_turn(
                replace(update, update_id=975, event_id="telegram:update:975"),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            await worker.refresh_progress_messages()

        active = worker._active_turns["turn-1"]
        self.assertEqual(len(telegram.sent_messages), 1)
        self.assertEqual(active.provisional_message_ref, "telegram:message:101")
        self.assertEqual(active.session_id, "session-1")
        self.assertEqual(active.session_key, "channel:telegram:direct:77")

    async def test_new_turn_supersedes_and_deletes_old_progress_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            update = TelegramInboundUpdate(
                update_id=101,
                event_id="telegram:update:101",
                sender_ref="telegram:user:77",
                conversation_ref="telegram:chat:77",
                message_ref="telegram:message:11",
                text="first",
                trigger="dm",
                provider_metadata={"chat_type": "private"},
            )
            await worker._remember_active_turn(
                update,
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()

            await worker._remember_active_turn(
                replace(
                    update,
                    update_id=102,
                    event_id="telegram:update:102",
                    message_ref="telegram:message:12",
                    text="second",
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-2",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.started",
                )
            )

        self.assertNotIn("turn-1", worker._active_turns)
        self.assertIn("turn-2", worker._active_turns)
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertEqual(telegram.edited_messages, [])

    async def test_cancelled_turn_edits_progress_to_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 98,
                            "message": {
                                "message_id": 8,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "slow",
                            },
                        }
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.cancelled",
                )
            )
            await worker._process_stream_event(
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="done",
                )
            )

        self.assertEqual(telegram.sent_messages[0][1], "Stopped.")
        self.assertEqual(telegram.deleted_messages, [])

    async def test_transient_progress_edit_failure_does_not_disable_edits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 99,
                            "message": {
                                "message_id": 9,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "slow",
                            },
                        }
                    )
                ],
                edit_errors=[RuntimeError("temporary network failure")],
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            turn = worker._active_turns["turn-1"]
            turn.visible_after = 0.0
            await worker.refresh_progress_messages()
            turn.last_edit_at = 0.0
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.started",
                )
            )
            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.refresh_progress_messages()
            self.assertTrue(turn.can_edit)
            turn.last_edit_at = 0.0
            await worker._process_stream_event(
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.artifact",
                )
            )
            await worker.refresh_progress_messages()

        self.assertEqual(
            telegram.edited_messages,
            [("telegram:chat:77", "telegram:message:101", "Preparing artifact...")],
        )

    async def test_permanent_progress_edit_failure_sends_terminal_fallback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 100,
                            "message": {
                                "message_id": 10,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "slow",
                            },
                        }
                    )
                ],
                edit_errors=[
                    TelegramBadRequest(
                        method=object(),
                        message="message to edit not found",
                    )
                ],
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()
            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker._process_stream_event(
                    StreamEvent(
                        sequence=1,
                        peer_id="telegram:chat:77",
                        turn_id="turn-1",
                        kind="error",
                        code="runtime.error",
                        text="bad provider response",
                    )
                )

        self.assertEqual(telegram.edited_messages, [])
        self.assertEqual(
            telegram.sent_messages[-1][1], "Turn failed: bad provider response"
        )

    async def test_flush_outbox_sends_delivery_and_reports_receipt(self) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:chat:-1001",
                    thread_ref="telegram:topic:77",
                    reply_to_ref="telegram:message:42",
                    content=OutboxContent(
                        text="final answer",
                        attachments=[
                            OutboxAttachment(
                                attachment_id="att-1",
                                path="/tmp/a.txt",
                                filename="a.txt",
                                mime_type="text/plain",
                            )
                        ],
                    ),
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

        self.assertEqual(
            telegram.sent_messages,
            [
                (
                    "telegram:chat:-1001",
                    "final answer",
                    "telegram:message:42",
                    "telegram:topic:77",
                    ["/tmp/a.txt"],
                )
            ],
        )
        self.assertEqual(telegram.sent_format_hints, ["plain"])
        self.assertEqual(
            api.outbox_reports,
            [
                (
                    "delivery-1",
                    "attempt-1",
                    "delivered",
                    {"message_id": 101, "chat_id": "telegram:chat:-1001"},
                    None,
                    None,
                )
            ],
        )

    async def test_failed_outbox_send_reports_retryable_failure(self) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:user:77",
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

        self.assertEqual(api.outbox_reports[0][2], "retryable_failed")
        self.assertEqual(api.outbox_reports[0][4], "telegram.send_failed")

    async def test_report_health_submits_provider_checks(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            await worker.report_health()

        self.assertEqual(len(api.health_reports), 1)
        status, checks = api.health_reports[0]
        self.assertEqual(status, "ok")
        self.assertEqual(
            [check.code for check in checks],
            [
                "telegram.bot_identity",
                "telegram.polling",
                "telegram.update_lag",
                "telegram.update_extraction",
                "telegram.delivery_errors",
            ],
        )
        self.assertTrue(all(check.status == "ok" for check in checks))
        self.assertEqual(checks[0].details["bot_id"], BOT.user_id)

    async def test_delivery_failure_is_reported_in_worker_health(self) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:user:77",
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
            await worker.report_health()

        self.assertEqual(api.health_reports[0][0], "warning")
        checks_by_code = {check.code: check for check in api.health_reports[0][1]}
        delivery_errors = checks_by_code["telegram.delivery_errors"]
        self.assertEqual(delivery_errors.status, "warning")
        self.assertEqual(delivery_errors.details["retryable_failed"], 1)
        self.assertEqual(delivery_errors.details["terminal_failed"], 0)

    async def test_get_updates_failure_is_reported_in_worker_health(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport(get_updates_error=RuntimeError("webhook set"))
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.process_updates()
            await worker.report_health()

        self.assertEqual(api.health_reports[0][0], "error")
        checks_by_code = {check.code: check for check in api.health_reports[0][1]}
        polling = checks_by_code["telegram.polling"]
        self.assertEqual(polling.status, "error")
        self.assertEqual(polling.message, "getUpdates failed")
        self.assertIn("webhook set", polling.details["error"])

    async def test_health_uses_fresh_bot_identity_check(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport(
            bot_identity_refresh_error=RuntimeError("token revoked")
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.report_health()

        checks_by_code = {check.code: check for check in api.health_reports[0][1]}
        self.assertEqual(checks_by_code["telegram.bot_identity"].status, "error")
        self.assertIn(
            "token revoked", checks_by_code["telegram.bot_identity"].details["error"]
        )

    async def test_stale_outbox_report_is_not_retried_as_success(self) -> None:
        api = FakeLionClawApi(
            outbox_deliveries=[
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:user:77",
                    content=OutboxContent(text="final answer"),
                )
            ],
            report_accepted=False,
        )
        telegram = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="WARNING"):
                await worker.flush_outbox()

        self.assertEqual(len(api.outbox_reports), 1)
        self.assertEqual(api.outbox_reports[0][2], "delivered")

    async def test_telegram_rejections_and_invalid_refs_are_terminal_outbox_failures(
        self,
    ) -> None:
        outcome, error_code = _classify_send_failure(
            TelegramBadRequest(method=object(), message="chat not found")
        )

        self.assertEqual(outcome, "terminal_failed")
        self.assertEqual(error_code, "telegram.send_rejected")

        outcome, error_code = _classify_send_failure(TelegramReferenceError("bad ref"))

        self.assertEqual(outcome, "terminal_failed")
        self.assertEqual(error_code, "telegram.invalid_ref")

    async def test_bot_identity_failure_makes_worker_health_error(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport(
            bot_identity_error=RuntimeError("unauthorized")
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.report_health()

        self.assertEqual(api.health_reports[0][0], "error")
        checks_by_code = {check.code: check for check in api.health_reports[0][1]}
        self.assertEqual(checks_by_code["telegram.bot_identity"].status, "error")


class TelegramDeliveryHelperTests(unittest.TestCase):
    def test_long_answer_chunks_by_telegram_utf16_limit(self) -> None:
        chunks = _split_telegram_text("x" * 4001)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], "x" * 4000)
        self.assertEqual(chunks[1], "x")
        self.assertTrue(all(_utf16_len(chunk) <= 4000 for chunk in chunks))

    def test_long_whitespace_does_not_create_empty_telegram_chunks(self) -> None:
        self.assertEqual(_split_telegram_text((" " * 4001) + "answer"), ["answer"])
        self.assertEqual(_split_telegram_text(" " * 4001), [])

    def test_topic_thread_ref_omits_general_topic_on_send(self) -> None:
        self.assertEqual(_coerce_thread_id("telegram:topic:77", omit_general=True), 77)
        self.assertIsNone(_coerce_thread_id("telegram:topic:1", omit_general=True))

    def test_topic_thread_ref_preserves_general_topic_on_typing(self) -> None:
        self.assertEqual(_coerce_thread_id("telegram:topic:1", omit_general=False), 1)

    def test_markdown_rendering_uses_telegram_html_and_suppresses_local_links(
        self,
    ) -> None:
        rendered = _markdown_to_telegram_html(
            "**Sauti Scribe Edge**\n"
            "- [CONTEXT.md](/workspace/CONTEXT.md)\n"
            "- [ADR 0001](docs/adr/0001.md)\n"
            "- [OpenAI](https://openai.com)\n"
        )

        self.assertIn("<b>Sauti Scribe Edge</b>", rendered)
        self.assertIn("<code>CONTEXT.md</code>", rendered)
        self.assertNotIn("/workspace/CONTEXT.md", rendered)
        self.assertNotIn("docs/adr/0001.md", rendered)
        self.assertIn('<a href="https://openai.com">OpenAI</a>', rendered)

    def test_plain_rendering_does_not_enable_telegram_html_parse_mode(self) -> None:
        chunks = _format_telegram_text_chunks("<b>literal</b>", "plain")

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].plain_text, "<b>literal</b>")
        self.assertIsNone(chunks[0].html_text)

    def test_markdown_rendering_falls_back_to_plain_when_html_would_exceed_limit(
        self,
    ) -> None:
        chunks = _format_telegram_text_chunks("<" * 3990, "markdown")

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].plain_text, "<" * 3990)
        self.assertIsNone(chunks[0].html_text)
        self.assertTrue(_utf16_len(chunks[0].plain_text) <= 4000)

    def test_markdown_rendering_escapes_code_blocks(self) -> None:
        rendered = _markdown_to_telegram_html("```text\n<b>literal</b>\n```")

        self.assertEqual(
            rendered, "<pre><code>&lt;b&gt;literal&lt;/b&gt;\n</code></pre>"
        )

    def test_overall_health_status_uses_most_severe_check(self) -> None:
        self.assertEqual(
            _overall_health_status(
                [
                    HealthCheck("a", "ok", "ok"),
                    HealthCheck("b", "warning", "warn"),
                ]
            ),
            "warning",
        )
        self.assertEqual(
            _overall_health_status(
                [
                    HealthCheck("a", "warning", "warn"),
                    HealthCheck("b", "error", "error"),
                ]
            ),
            "error",
        )


def build_api(client: httpx.AsyncClient) -> LionClawApi:
    return LionClawApi(
        base_url="http://127.0.0.1:8979",
        channel_id="telegram",
        consumer_id="telegram:telegram",
        start_mode="resume",
        stream_limit=100,
        stream_wait_ms=30000,
        client=client,
    )


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
        health_report_interval_secs=60.0,
        runtime_dir=runtime_dir,
        telegram_offset_file=runtime_dir / "telegram.offset",
    )


_USE_EXPECTED_TURN_ID = object()


class FakeLionClawApi:
    def __init__(
        self,
        stream_events: list[StreamEvent] | None = None,
        outbox_deliveries: list[OutboxDelivery] | None = None,
        inbound_outcome: str = "queued",
        inbound_pairing_code: str | None = None,
        claim_outcome: str = "approved",
        report_accepted: bool = True,
        stage_error: Exception | None = None,
        inbound_turn_id: str | None = None,
        inbound_session_id: str | None = None,
        inbound_session_key: str | None = None,
        cancel_error: Exception | None = None,
        cancel_turn_id: str | None | object = _USE_EXPECTED_TURN_ID,
    ) -> None:
        self.stream_events = list(stream_events or [])
        self.outbox_deliveries = list(outbox_deliveries or [])
        self.inbound_outcome = inbound_outcome
        self.inbound_pairing_code = inbound_pairing_code
        self.claim_outcome = claim_outcome
        self.report_accepted = report_accepted
        self.stage_error = stage_error
        self.inbound_turn_id = inbound_turn_id
        self.inbound_session_id = inbound_session_id
        self.inbound_session_key = inbound_session_key
        self.cancel_error = cancel_error
        self.cancel_turn_id = cancel_turn_id
        self.sent_inbound: list[TelegramInboundUpdate] = []
        self.cancel_calls: list[tuple[str, str, str | None, str]] = []
        self.claims: list[TelegramPairingClaim] = []
        self.staged_attachments: list[
            tuple[TelegramInboundUpdate, TelegramInboundAttachment]
        ] = []
        self.finalized: list[
            tuple[TelegramInboundUpdate, list[AttachmentMissingReport]]
        ] = []
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
        self.health_reports: list[tuple[str, list[HealthCheck]]] = []

    async def send_inbound(self, update: TelegramInboundUpdate) -> InboundResponse:
        self.sent_inbound.append(update)
        return InboundResponse(
            outcome=self.inbound_outcome,
            pairing_code=self.inbound_pairing_code,
            turn_id=self.inbound_turn_id,
            session_id=self.inbound_session_id,
            session_key=self.inbound_session_key,
        )

    async def cancel_active_turn(
        self,
        *,
        session_id: str,
        session_key: str,
        expected_turn_id: str | None,
        reason: str,
    ):
        self.cancel_calls.append((session_id, session_key, expected_turn_id, reason))
        if self.cancel_error is not None:
            raise self.cancel_error
        turn_id = (
            expected_turn_id
            if self.cancel_turn_id is _USE_EXPECTED_TURN_ID
            else self.cancel_turn_id
        )
        return SessionActionResult(session_id=session_id, turn_id=turn_id)

    async def claim_pairing(self, claim: TelegramPairingClaim) -> PairingClaimResponse:
        self.claims.append(claim)
        return PairingClaimResponse(outcome=self.claim_outcome)

    async def stage_attachment(
        self,
        update: TelegramInboundUpdate,
        attachment: TelegramInboundAttachment,
        downloaded: TelegramDownloadedAttachment,
    ) -> AttachmentStageResponse:
        self.staged_attachments.append((update, attachment))
        if self.stage_error is not None:
            raise self.stage_error
        return AttachmentStageResponse(
            status="staged", size_bytes=len(downloaded.content), sha256="00"
        )

    async def finalize_attachments(
        self,
        update: TelegramInboundUpdate,
        missing: list[AttachmentMissingReport],
    ) -> AttachmentFinalizeResponse:
        self.finalized.append((update, list(missing)))
        return AttachmentFinalizeResponse(outcome="queued")

    async def pull_stream(self) -> list[StreamEvent]:
        return list(self.stream_events)

    async def ack_stream(self, through_sequence: int) -> None:
        self.acked_sequences.append(through_sequence)

    async def pull_outbox(self) -> list[OutboxDelivery]:
        return list(self.outbox_deliveries)

    async def report_health(
        self,
        status: str,
        checks: list[HealthCheck],
    ) -> HealthReportResponse:
        self.health_reports.append((status, list(checks)))
        return HealthReportResponse(
            accepted=True,
            channel_id="telegram",
            observed_at="2026-05-18T17:00:00Z",
        )

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
            {
                "accepted": self.report_accepted,
                "status": "delivered",
                "attempt_status": outcome,
            },
        )()


class FailingOffsetStore(OffsetStore):
    def save(self, offset: int) -> None:
        raise RuntimeError(f"cannot persist offset {offset}")


class MalformedTelegramUpdate:
    def __init__(self, update_id: int) -> None:
        self.update_id = update_id

    @property
    def message(self):
        raise RuntimeError("malformed telegram update")


class FakeTelegramTransport:
    def __init__(
        self,
        updates: list[Update] | None = None,
        fail_send_message: bool = False,
        fail_download: bool = False,
        get_updates_error: Exception | None = None,
        bot_identity_error: Exception | None = None,
        bot_identity_refresh_error: Exception | None = None,
        downloaded_content: bytes = b"file",
        edit_errors: list[Exception] | None = None,
    ) -> None:
        self.updates = list(updates or [])
        self.sent_messages: list[tuple[str, str, str | None, str | None, list[str]]] = (
            []
        )
        self.edited_messages: list[tuple[str, str, str]] = []
        self.deleted_messages: list[tuple[str, str]] = []
        self.commands_configured = False
        self.sent_format_hints: list[str] = []
        self.typing_peers: list[str] = []
        self.typing_calls: list[tuple[str, str | None]] = []
        self.downloaded_attachment_ids: list[str] = []
        self.fail_send_message = fail_send_message
        self.fail_download = fail_download
        self.get_updates_error = get_updates_error
        self.bot_identity_error = bot_identity_error
        self.bot_identity_refresh_error = bot_identity_refresh_error
        self.downloaded_content = downloaded_content
        self.edit_errors = list(edit_errors or [])

    async def close(self) -> None:
        pass

    async def bot_identity(self, *, refresh: bool = False) -> TelegramBotIdentity:
        if refresh and self.bot_identity_refresh_error is not None:
            raise self.bot_identity_refresh_error
        if self.bot_identity_error is not None:
            raise self.bot_identity_error
        return BOT

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]:
        if self.get_updates_error is not None:
            raise self.get_updates_error
        return list(self.updates)

    async def configure_commands(self) -> None:
        self.commands_configured = True

    async def download_attachment(
        self,
        attachment: TelegramInboundAttachment,
        max_bytes: int,
    ) -> TelegramDownloadedAttachment:
        self.downloaded_attachment_ids.append(attachment.attachment_id)
        if self.fail_download:
            raise RuntimeError("download failed")
        return TelegramDownloadedAttachment(
            attachment=attachment,
            content=self.downloaded_content,
            filename=attachment.filename,
            mime_type=attachment.mime_type,
        )

    async def send_message(
        self,
        conversation_ref: str,
        text: str,
        reply_to_ref: str | None = None,
        thread_ref: str | None = None,
        format_hint: str = "plain",
        attachments: Sequence[TelegramOutboundAttachment] = (),
    ) -> dict[str, object]:
        if self.fail_send_message:
            raise RuntimeError("send failed")
        self.sent_format_hints.append(format_hint)
        self.sent_messages.append(
            (
                conversation_ref,
                text,
                reply_to_ref,
                thread_ref,
                [attachment.path for attachment in attachments],
            )
        )
        return {"message_id": 101, "chat_id": conversation_ref}

    async def send_typing(
        self,
        conversation_ref: str,
        thread_ref: str | None = None,
    ) -> None:
        self.typing_peers.append(conversation_ref)
        self.typing_calls.append((conversation_ref, thread_ref))

    async def edit_message(
        self,
        conversation_ref: str,
        message_ref: str,
        text: str,
        *,
        format_hint: str = "plain",
    ) -> None:
        if self.edit_errors:
            raise self.edit_errors.pop(0)
        self.edited_messages.append((conversation_ref, message_ref, text))

    async def delete_message(
        self,
        conversation_ref: str,
        message_ref: str,
    ) -> None:
        self.deleted_messages.append((conversation_ref, message_ref))


class BlockingUpdatesTelegramTransport(FakeTelegramTransport):
    def __init__(self) -> None:
        super().__init__()
        self.poll_started = asyncio.Event()
        self._release = asyncio.Event()

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]:
        self.poll_started.set()
        await self._release.wait()
        return []


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


async def _wait_until(predicate, *, interval_seconds: float = 0.01) -> None:
    while not predicate():
        await asyncio.sleep(interval_seconds)
