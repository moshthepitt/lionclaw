from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
import unittest
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

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
    TELEGRAM_TEXT_LIMIT,
    AiogramTelegramTransport,
    TelegramActionButton,
    TelegramBotIdentity,
    TelegramCallbackAction,
    TelegramDownloadedAttachment,
    TelegramInboundAttachment,
    TelegramInboundUpdate,
    TelegramOutboundAttachment,
    TelegramPairingClaim,
    TelegramPartialSendError,
    TelegramReferenceError,
    TelegramUnsupportedContent,
    _coerce_chat_id,
    _coerce_message_id,
    _coerce_thread_id,
    _format_telegram_text_chunks,
    _markdown_to_telegram_html,
    _split_telegram_text,
    extract_inbound_event,
)
from lionclaw_channel_telegram.webhook import (
    TELEGRAM_SECRET_TOKEN_HEADER,
    TelegramWebhookServer,
)
from lionclaw_channel_telegram.worker import (
    MAX_OUTBOX_RECEIPTS,
    ActiveTurnStore,
    OffsetStore,
    OutboxReceiptRecord,
    OutboxReceiptStore,
    PendingProgressDelete,
    ProgressDeleteStore,
    ProviderWorkItem,
    TelegramWorker,
    _classify_send_failure,
    _overall_health_status,
    _webhook_batch_key,
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
        self.assertEqual(mapped.provider_metadata["message_date_epoch"], 0)

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
        self.assertEqual(mapped.text, "status")
        self.assertEqual(mapped.trigger, "mention")
        self.assertTrue(mapped.provider_metadata["bot_mentioned"])
        self.assertEqual(
            mapped.provider_metadata["leading_mention_text"],
            "@lionclaw_bot",
        )

    def test_group_text_mention_records_leading_bot_mention(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 121,
                "message": {
                    "message_id": 21,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "text": "LionClaw /status",
                    "entities": [
                        {
                            "type": "text_mention",
                            "offset": 0,
                            "length": len("LionClaw"),
                            "user": {
                                "id": 99,
                                "is_bot": True,
                                "first_name": "LionClaw",
                            },
                        }
                    ],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.text, "/status")
        self.assertEqual(mapped.trigger, "mention")
        self.assertTrue(mapped.provider_metadata["bot_mentioned"])
        self.assertTrue(mapped.provider_metadata["leading_mention_targets_bot"])
        self.assertEqual(mapped.provider_metadata["leading_mention_text"], "LionClaw")

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
        self.assertEqual(mapped.provider_metadata["command_target"], "lionclaw_bot")
        self.assertTrue(mapped.provider_metadata["command_targets_bot"])

    def test_group_command_targeting_other_bot_is_not_a_bot_mention(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 131,
                "message": {
                    "message_id": 31,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "message_thread_id": 77,
                    "is_topic_message": True,
                    "text": "/stop@other_bot",
                    "entities": [
                        {
                            "type": "bot_command",
                            "offset": 0,
                            "length": len("/stop@other_bot"),
                        }
                    ],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.trigger, "thread_continuation")
        self.assertEqual(mapped.provider_metadata["command_target"], "other_bot")
        self.assertFalse(mapped.provider_metadata["command_targets_bot"])
        self.assertFalse(mapped.provider_metadata["bot_mentioned"])

    def test_callback_query_maps_to_callback_action(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 132,
                "callback_query": {
                    "id": "callback-1",
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "chat_instance": "chat-instance",
                    "data": "lc1:s:turn-1:mac",
                    "message": {
                        "message_id": 32,
                        "date": 0,
                        "chat": {
                            "id": -10042,
                            "type": "supergroup",
                            "is_forum": True,
                        },
                        "message_thread_id": 77,
                        "is_topic_message": True,
                        "from": {
                            "id": 99,
                            "is_bot": True,
                            "first_name": "LionClaw",
                            "username": "lionclaw_bot",
                        },
                        "text": "Working...",
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramCallbackAction)
        assert isinstance(mapped, TelegramCallbackAction)
        self.assertEqual(mapped.callback_query_id, "callback-1")
        self.assertEqual(mapped.action, "lc1:s:turn-1:mac")
        self.assertEqual(mapped.sender_ref, "telegram:user:9")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:-10042")
        self.assertEqual(mapped.thread_ref, "telegram:topic:77")
        self.assertEqual(mapped.message_ref, "telegram:message:32")

    def test_inaccessible_callback_query_still_maps_to_callback_action(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 133,
                "callback_query": {
                    "id": "callback-old",
                    "from": {"id": 9, "is_bot": False, "first_name": "Nia"},
                    "chat_instance": "chat-instance",
                    "data": "lc1:s:turn-1:mac",
                    "message": {
                        "message_id": 33,
                        "date": 0,
                        "chat": {
                            "id": -10042,
                            "type": "supergroup",
                            "is_forum": True,
                        },
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramCallbackAction)
        assert isinstance(mapped, TelegramCallbackAction)
        self.assertEqual(mapped.callback_query_id, "callback-old")
        self.assertEqual(mapped.sender_ref, "telegram:user:9")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:-10042")
        self.assertIsNone(mapped.thread_ref)
        self.assertEqual(mapped.message_ref, "telegram:message:33")
        self.assertTrue(mapped.provider_metadata["message_inaccessible"])

    def test_edited_message_sets_metadata_without_changing_message_ref(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 132,
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
        self.assertEqual(mapped.event_id, "telegram:update:132")
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

    def test_startgroup_targeting_other_bot_is_not_pairing_claim(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 192,
                "message": {
                    "message_id": 92,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "text": "/startgroup@other_bot lc_0123456789abcdef",
                    "entities": [
                        {
                            "type": "bot_command",
                            "offset": 0,
                            "length": len("/startgroup@other_bot"),
                        }
                    ],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.text, "/startgroup@other_bot lc_0123456789abcdef")
        self.assertEqual(mapped.provider_metadata["command_target"], "other_bot")
        self.assertFalse(mapped.provider_metadata["command_targets_bot"])

    def test_plain_text_mentioning_pairing_token_remains_inbound(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 193,
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

    def test_unsupported_contact_maps_to_local_feedback_event(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 194,
                "message": {
                    "message_id": 94,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "contact": {
                        "phone_number": "+15551234567",
                        "first_name": "Alice",
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramUnsupportedContent)
        assert isinstance(mapped, TelegramUnsupportedContent)
        self.assertEqual(mapped.event_id, "telegram:unsupported:194")
        self.assertEqual(mapped.kind, "contact")
        self.assertEqual(mapped.sender_ref, "telegram:user:42")
        self.assertEqual(mapped.conversation_ref, "telegram:chat:42")
        self.assertEqual(mapped.message_ref, "telegram:message:94")
        self.assertEqual(mapped.trigger, "dm")
        self.assertEqual(
            mapped.provider_metadata["unsupported_content_kind"],
            "contact",
        )

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

    def test_caption_leading_bot_mention_is_stripped_from_text_and_attachment(
        self,
    ) -> None:
        update = Update.model_validate(
            {
                "update_id": 201,
                "message": {
                    "message_id": 101,
                    "date": 0,
                    "chat": {"id": -10042, "type": "supergroup"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "caption": "@lionclaw_bot inspect this",
                    "caption_entities": [
                        {"type": "mention", "offset": 0, "length": 13}
                    ],
                    "photo": [
                        {
                            "file_id": "photo-large",
                            "file_unique_id": "photo-large-unique",
                            "width": 640,
                            "height": 480,
                            "file_size": 100,
                        }
                    ],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(mapped.text, "inspect this")
        self.assertEqual(len(mapped.attachments), 1)
        self.assertEqual(mapped.attachments[0].caption, "inspect this")
        self.assertEqual(mapped.trigger, "mention")

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

    def test_photo_descriptor_prefers_largest_dimensions_over_optional_file_size(
        self,
    ) -> None:
        update = Update.model_validate(
            {
                "update_id": 301,
                "message": {
                    "message_id": 301,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "photo": [
                        {
                            "file_id": "photo-small",
                            "file_unique_id": "photo-small-unique",
                            "width": 64,
                            "height": 64,
                            "file_size": 999999,
                        },
                        {
                            "file_id": "photo-large",
                            "file_unique_id": "photo-large-unique",
                            "width": 1280,
                            "height": 720,
                        },
                    ],
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(len(mapped.attachments), 1)
        self.assertEqual(mapped.attachments[0].provider_file_ref, "photo-large")
        self.assertEqual(
            mapped.attachments[0].attachment_id,
            "telegram:update:301:attachment:photo:photo-large-unique",
        )

    def test_location_maps_to_text_and_metadata(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 302,
                "message": {
                    "message_id": 302,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "location": {
                        "latitude": -1.292066,
                        "longitude": 36.821945,
                        "horizontal_accuracy": 12.5,
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(
            mapped.text,
            "Shared location: latitude -1.292066, "
            "longitude 36.821945 (geo:-1.292066,36.821945)",
        )
        self.assertEqual(mapped.attachments, [])
        self.assertEqual(
            mapped.provider_metadata["shared_location"],
            {
                "kind": "location",
                "latitude": -1.292066,
                "longitude": 36.821945,
                "horizontal_accuracy": 12.5,
            },
        )

    def test_venue_maps_to_text_and_metadata(self) -> None:
        update = Update.model_validate(
            {
                "update_id": 303,
                "message": {
                    "message_id": 303,
                    "date": 0,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 42, "is_bot": False, "first_name": "Alice"},
                    "venue": {
                        "location": {
                            "latitude": -1.2841,
                            "longitude": 36.8155,
                        },
                        "title": "Nairobi Garage",
                        "address": "Kilimani, Nairobi",
                        "google_place_id": "place-123",
                        "google_place_type": "coworking_space",
                    },
                },
            }
        )

        mapped = extract_inbound_event(update, bot_identity=BOT)

        self.assertIsInstance(mapped, TelegramInboundUpdate)
        assert isinstance(mapped, TelegramInboundUpdate)
        self.assertEqual(
            mapped.text,
            "Shared venue: Nairobi Garage\n"
            "Kilimani, Nairobi\n"
            "Location: latitude -1.2841, longitude 36.8155 "
            "(geo:-1.2841,36.8155)",
        )
        self.assertEqual(
            mapped.provider_metadata["shared_location"],
            {
                "kind": "venue",
                "title": "Nairobi Garage",
                "address": "Kilimani, Nairobi",
                "latitude": -1.2841,
                "longitude": 36.8155,
                "google_place_id": "place-123",
                "google_place_type": "coworking_space",
            },
        )

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

    async def test_pull_stream_normalizes_nullable_text(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "events": [
                        {
                            "sequence": 1,
                            "peer_id": "telegram:chat:77",
                            "turn_id": "turn-1",
                            "kind": "message_boundary",
                            "lane": "answer",
                            "text": None,
                        },
                        {
                            "sequence": 2,
                            "peer_id": "telegram:chat:77",
                            "turn_id": "turn-1",
                            "kind": "done",
                        },
                    ]
                },
            )

        client = httpx.AsyncClient(
            base_url="http://127.0.0.1:8979",
            transport=httpx.MockTransport(handler),
        )
        api = build_api(client)

        events = await api.pull_stream()

        self.assertEqual([event.text for event in events], ["", ""])
        self.assertEqual(events[0].kind, "message_boundary")
        self.assertEqual(events[1].kind, "done")
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


class OutboxReceiptStoreTests(unittest.TestCase):
    def test_save_replaces_receipt_file_without_leaving_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram.outbox-receipts.json"
            store = OutboxReceiptStore(path)

            store.save(
                {
                    "delivery-1": OutboxReceiptRecord(
                        status="delivered",
                        provider_receipt={"message_id": 101, "chat_id": "77"},
                    )
                }
            )
            store.save(
                {
                    "delivery-2": OutboxReceiptRecord(
                        status="partial",
                        provider_receipt={"message_id": 102, "chat_id": "77"},
                    )
                }
            )

            self.assertEqual(
                store.load(),
                {
                    "delivery-2": OutboxReceiptRecord(
                        status="partial",
                        provider_receipt={
                            "message_id": 102,
                            "chat_id": "77",
                            "messages": [{"message_id": 102, "chat_id": "77"}],
                        },
                    )
                },
            )
            self.assertFalse(
                (path.parent / ".telegram.outbox-receipts.json.tmp").exists()
            )

    def test_load_ignores_malformed_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram.outbox-receipts.json"
            path.write_text(
                json.dumps(
                    {
                        "delivery-1": {
                            "status": "delivered",
                            "provider_receipt": {
                                "message_id": 101,
                                "chat_id": "77",
                            },
                        },
                        "delivery-2": "bad",
                        "delivery-3": {
                            "status": "unknown",
                            "provider_receipt": {"message_id": 103},
                        },
                        "delivery-4": {"message_id": 104, "chat_id": "77"},
                        "delivery-5": {
                            "status": "delivered",
                            "provider_receipt": {"message_id": 105},
                        },
                        "delivery-6": {
                            "status": "partial",
                            "provider_receipt": {
                                "message_id": 107,
                                "chat_id": "88",
                                "messages": [
                                    {"message_id": 106, "chat_id": "77"},
                                    {"message_id": 107, "chat_id": "88"},
                                ],
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                OutboxReceiptStore(path).load(),
                {
                    "delivery-1": OutboxReceiptRecord(
                        status="delivered",
                        provider_receipt={
                            "message_id": 101,
                            "chat_id": "77",
                            "messages": [{"message_id": 101, "chat_id": "77"}],
                        },
                    ),
                    "delivery-4": OutboxReceiptRecord(
                        status="delivered",
                        provider_receipt={
                            "message_id": 104,
                            "chat_id": "77",
                            "messages": [{"message_id": 104, "chat_id": "77"}],
                        },
                    ),
                },
            )

    def test_load_caps_oversized_receipt_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram.outbox-receipts.json"
            path.write_text(
                json.dumps(
                    {
                        f"delivery-{index}": {
                            "status": "delivered",
                            "provider_receipt": {
                                "message_id": index,
                                "chat_id": "77",
                            },
                        }
                        for index in range(MAX_OUTBOX_RECEIPTS + 1)
                    }
                ),
                encoding="utf-8",
            )

            receipts = OutboxReceiptStore(path).load()

            self.assertEqual(len(receipts), MAX_OUTBOX_RECEIPTS)
            self.assertNotIn("delivery-0", receipts)
            self.assertIn(f"delivery-{MAX_OUTBOX_RECEIPTS}", receipts)


class ProgressDeleteStoreTests(unittest.TestCase):
    def test_save_replaces_progress_deletes_without_leaving_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram.progress-deletes.json"
            store = ProgressDeleteStore(path)
            pending = PendingProgressDelete(
                turn_id="turn-1",
                conversation_ref="telegram:chat:77",
                message_ref="telegram:message:101",
                attempts=3,
                next_attempt_at=123.0,
            )

            store.save({pending.key: pending})

            self.assertEqual(
                store.load(),
                {
                    pending.key: PendingProgressDelete(
                        turn_id="turn-1",
                        conversation_ref="telegram:chat:77",
                        message_ref="telegram:message:101",
                        attempts=3,
                        next_attempt_at=0.0,
                    )
                },
            )
            self.assertFalse(
                (path.parent / ".telegram.progress-deletes.json.tmp").exists()
            )

    def test_load_ignores_malformed_progress_deletes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "telegram.progress-deletes.json"
            path.write_text(
                json.dumps(
                    {
                        "pending": [
                            {
                                "turn_id": "turn-1",
                                "conversation_ref": "telegram:chat:77",
                                "message_ref": "telegram:message:101",
                                "attempts": 1,
                            },
                            {
                                "turn_id": "",
                                "conversation_ref": "telegram:chat:77",
                                "message_ref": "telegram:message:102",
                                "attempts": 1,
                            },
                            {
                                "turn_id": "turn-3",
                                "conversation_ref": "telegram:chat:77",
                                "message_ref": "telegram:message:103",
                                "attempts": True,
                            },
                            "bad",
                        ]
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                ProgressDeleteStore(path).load(),
                {
                    ("telegram:chat:77", "telegram:message:101"): PendingProgressDelete(
                        turn_id="turn-1",
                        conversation_ref="telegram:chat:77",
                        message_ref="telegram:message:101",
                        attempts=1,
                        next_attempt_at=0.0,
                    )
                },
            )


class WorkerConfigTests(unittest.TestCase):
    def test_from_env_derives_defaults_from_lionclaw_home(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.dict(
                os.environ,
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "LIONCLAW_HOME": temp_dir,
                },
                clear=True,
            ),
        ):
            config = WorkerConfig.from_env()

        self.assertEqual(config.telegram_bot_token, "token")
        self.assertEqual(config.channel_id, "telegram")
        self.assertEqual(config.consumer_id, "telegram:telegram")
        self.assertEqual(config.stream_limit, 100)
        self.assertEqual(config.stream_wait_ms, 30000)
        self.assertEqual(config.telegram_poll_timeout_secs, 25)
        self.assertEqual(config.telegram_loop_delay_secs, 1.0)
        self.assertEqual(config.telegram_update_mode, "polling")
        self.assertEqual(config.telegram_webhook_host, "127.0.0.1")
        self.assertEqual(config.telegram_webhook_port, 8080)
        self.assertEqual(config.telegram_webhook_path, "/telegram/webhook")
        self.assertIsNone(config.telegram_webhook_secret_token)
        self.assertEqual(config.telegram_webhook_max_body_bytes, 1024 * 1024)
        self.assertEqual(config.health_report_interval_secs, 60.0)
        self.assertEqual(
            config.runtime_dir,
            Path(temp_dir) / "runtime" / "channels" / "telegram",
        )
        self.assertEqual(
            config.telegram_offset_file,
            Path(temp_dir) / "runtime" / "channels" / "telegram" / "telegram.offset",
        )

    def test_from_env_rejects_empty_required_values(self) -> None:
        cases = {
            "TELEGRAM_BOT_TOKEN": "required",
            "LIONCLAW_HOME": "must not be empty",
            "LIONCLAW_BASE_URL": "must not be empty",
            "LIONCLAW_CHANNEL_ID": "must not be empty",
            "LIONCLAW_CONSUMER_ID": "must not be empty",
            "LIONCLAW_STREAM_START_MODE": "must not be empty",
            "LIONCLAW_CHANNEL_RUNTIME_DIR": "must not be empty",
            "TELEGRAM_OFFSET_FILE": "must not be empty",
            "TELEGRAM_UPDATE_MODE": "must not be empty",
            "TELEGRAM_WEBHOOK_HOST": "must not be empty",
            "TELEGRAM_WEBHOOK_PATH": "must not be empty",
            "TELEGRAM_WEBHOOK_SECRET_TOKEN": "must not be empty",
        }
        for env_name, error in cases.items():
            with self.subTest(env_name=env_name):
                env = {"TELEGRAM_BOT_TOKEN": "token", env_name: " "}
                with (
                    patch.dict(os.environ, env, clear=True),
                    self.assertRaises(RuntimeError) as raised,
                ):
                    WorkerConfig.from_env()
                message = str(raised.exception)
                self.assertIn(env_name, message)
                self.assertIn(error, message)

    def test_from_env_rejects_invalid_numeric_values(self) -> None:
        cases = (
            ("LIONCLAW_STREAM_LIMIT", "0", "must be >= 1"),
            ("LIONCLAW_STREAM_WAIT_MS", "-1", "must be >= 0"),
            ("TELEGRAM_POLL_TIMEOUT_SECS", "-1", "must be >= 0"),
            ("TELEGRAM_LOOP_DELAY_SECS", "-0.01", "must be >= 0"),
            ("TELEGRAM_WEBHOOK_PORT", "-1", "must be >= 0"),
            ("TELEGRAM_WEBHOOK_PORT", "65536", "must be <= 65535"),
            ("TELEGRAM_WEBHOOK_MAX_BODY_BYTES", "0", "must be >= 1"),
            ("LIONCLAW_HEALTH_REPORT_INTERVAL_SECS", "-1", "must be >= 0"),
            (
                "LIONCLAW_STREAM_WAIT_MS_INVALID",
                "abc",
                "must be an integer",
            ),
            ("TELEGRAM_LOOP_DELAY_SECS_INVALID", "later", "must be a number"),
            ("TELEGRAM_LOOP_DELAY_SECS_INVALID", "nan", "must be finite"),
            (
                "LIONCLAW_HEALTH_REPORT_INTERVAL_SECS_INVALID",
                "inf",
                "must be finite",
            ),
        )
        for env_name, value, error in cases:
            with self.subTest(env_name=env_name, value=value):
                real_env_name = env_name.removesuffix("_INVALID")
                env = {"TELEGRAM_BOT_TOKEN": "token", real_env_name: value}
                with (
                    patch.dict(os.environ, env, clear=True),
                    self.assertRaises(RuntimeError) as raised,
                ):
                    WorkerConfig.from_env()
                message = str(raised.exception)
                self.assertIn(real_env_name, message)
                self.assertIn(error, message)

    def test_from_env_validates_stream_start_mode(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "LIONCLAW_STREAM_START_MODE": "TAIL",
            },
            clear=True,
        ):
            self.assertEqual(WorkerConfig.from_env().stream_start_mode, "tail")

        with (
            patch.dict(
                os.environ,
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "LIONCLAW_STREAM_START_MODE": "from_the_middle",
                },
                clear=True,
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
            WorkerConfig.from_env()

        message = str(raised.exception)
        self.assertIn("LIONCLAW_STREAM_START_MODE", message)
        self.assertIn("resume, tail", message)

    def test_from_env_validates_telegram_update_mode(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_UPDATE_MODE": "WEBHOOK",
                "TELEGRAM_WEBHOOK_SECRET_TOKEN": "secret",
            },
            clear=True,
        ):
            config = WorkerConfig.from_env()

        self.assertEqual(config.telegram_update_mode, "webhook")
        self.assertEqual(config.telegram_webhook_secret_token, "secret")

        with (
            patch.dict(
                os.environ,
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "TELEGRAM_UPDATE_MODE": "side_channel",
                },
                clear=True,
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
            WorkerConfig.from_env()

        message = str(raised.exception)
        self.assertIn("TELEGRAM_UPDATE_MODE", message)
        self.assertIn("polling, webhook", message)

    def test_from_env_requires_secret_token_for_webhook_mode(self) -> None:
        with (
            patch.dict(
                os.environ,
                {"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_UPDATE_MODE": "webhook"},
                clear=True,
            ),
            self.assertRaises(RuntimeError) as raised,
        ):
            WorkerConfig.from_env()

        self.assertIn("TELEGRAM_WEBHOOK_SECRET_TOKEN", str(raised.exception))

    def test_from_env_validates_webhook_secret_token(self) -> None:
        cases = (
            ("secrét", "ASCII"),
            ("secret!", "letters, numbers, underscore, and hyphen"),
            ("s" * 257, "at most 256"),
        )
        for value, error in cases:
            with self.subTest(value=value):
                with (
                    patch.dict(
                        os.environ,
                        {
                            "TELEGRAM_BOT_TOKEN": "token",
                            "TELEGRAM_UPDATE_MODE": "webhook",
                            "TELEGRAM_WEBHOOK_SECRET_TOKEN": value,
                        },
                        clear=True,
                    ),
                    self.assertRaises(RuntimeError) as raised,
                ):
                    WorkerConfig.from_env()
                message = str(raised.exception)
                self.assertIn("TELEGRAM_WEBHOOK_SECRET_TOKEN", message)
                self.assertIn(error, message)

    def test_from_env_validates_webhook_path(self) -> None:
        cases = (
            ("telegram/webhook", "must start with /"),
            ("/telegram hook", "must not contain whitespace"),
        )
        for value, error in cases:
            with self.subTest(value=value):
                with (
                    patch.dict(
                        os.environ,
                        {"TELEGRAM_BOT_TOKEN": "token", "TELEGRAM_WEBHOOK_PATH": value},
                        clear=True,
                    ),
                    self.assertRaises(RuntimeError) as raised,
                ):
                    WorkerConfig.from_env()
                self.assertIn("TELEGRAM_WEBHOOK_PATH", str(raised.exception))
                self.assertIn(error, str(raised.exception))


class TelegramWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def _record_active_turn(
        self,
        worker: TelegramWorker,
        *,
        turn_id: str = "turn-1",
        update_id: int = 970,
        message_ref: str = "telegram:message:70",
    ) -> None:
        await worker._remember_active_turn(
            TelegramInboundUpdate(
                update_id=update_id,
                event_id=f"telegram:update:{update_id}",
                sender_ref="telegram:user:77",
                conversation_ref="telegram:chat:77",
                message_ref=message_ref,
                text="slow",
                trigger="dm",
                provider_metadata={"chat_type": "private"},
            ),
            InboundResponse(
                outcome="queued",
                turn_id=turn_id,
                session_id="session-1",
                session_key="channel:telegram:direct:77",
            ),
        )

    async def _remember_visible_progress(
        self,
        worker: TelegramWorker,
        *,
        turn_id: str = "turn-1",
        update_id: int = 970,
        message_ref: str = "telegram:message:70",
    ) -> str:
        await self._record_active_turn(
            worker,
            turn_id=turn_id,
            update_id=update_id,
            message_ref=message_ref,
        )
        worker._active_turns[turn_id].visible_after = 0.0
        await worker.refresh_progress_messages()
        progress_ref = worker._active_turns[turn_id].provisional_message_ref
        self.assertIsNotNone(progress_ref)
        assert progress_ref is not None
        return progress_ref

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

    async def test_process_updates_batches_back_to_back_text_messages(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 81,
                            "message": {
                                "message_id": 11,
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
                            "update_id": 82,
                            "message": {
                                "message_id": 12,
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
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "first\n\nsecond")
        self.assertEqual(api.sent_inbound[0].event_id, "telegram:update:81:batch:82")
        self.assertEqual(
            api.sent_inbound[0].provider_metadata["batched_update_ids"],
            [81, 82],
        )
        self.assertEqual(api.sent_inbound[0].provider_metadata["update_id"], 82)
        self.assertEqual(worker.offset, 83)

    async def test_process_updates_keeps_stale_backlog_text_messages_separate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 821,
                            "message": {
                                "message_id": 121,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "old backlog",
                            },
                        }
                    ),
                    Update.model_validate(
                        {
                            "update_id": 822,
                            "message": {
                                "message_id": 122,
                                "date": 60,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "newer backlog",
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
            [update.text for update in api.sent_inbound],
            ["old backlog", "newer backlog"],
        )
        self.assertEqual(worker.offset, 823)

    async def test_process_updates_does_not_batch_commands_with_text(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 83,
                            "message": {
                                "message_id": 13,
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
                            "update_id": 84,
                            "message": {
                                "message_id": 14,
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
            [update.text for update in api.sent_inbound], ["first", "/model gpt-5.2"]
        )

    async def test_process_updates_batches_media_group_attachments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="waiting_for_attachments")
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 85,
                            "message": {
                                "message_id": 15,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "media_group_id": "album-1",
                                "caption": "front",
                                "photo": [
                                    {
                                        "file_id": "photo-file-1",
                                        "file_unique_id": "photo-unique-1",
                                        "width": 100,
                                        "height": 100,
                                        "file_size": 10,
                                    }
                                ],
                            },
                        }
                    ),
                    Update.model_validate(
                        {
                            "update_id": 86,
                            "message": {
                                "message_id": 16,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "media_group_id": "album-1",
                                "caption": "back",
                                "photo": [
                                    {
                                        "file_id": "photo-file-2",
                                        "file_unique_id": "photo-unique-2",
                                        "width": 100,
                                        "height": 100,
                                        "file_size": 10,
                                    }
                                ],
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

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "front\n\nback")
        self.assertEqual(len(api.sent_inbound[0].attachments), 2)
        self.assertEqual(len(api.staged_attachments), 2)
        self.assertEqual(len(api.finalized), 1)

    async def test_queued_turn_sets_inbound_receipt_reaction(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_turn_id="turn-1")
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 861,
                            "message": {
                                "message_id": 61,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "start work",
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

        self.assertEqual(
            telegram.reactions,
            [("telegram:chat:77", "telegram:message:61", "👀")],
        )

    async def test_final_delivery_sets_completed_reaction(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await self._record_active_turn(
                worker,
                turn_id="turn-1",
                message_ref="telegram:message:62",
            )

            await worker._process_outbox_delivery(
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:chat:77",
                    turn_id="turn-1",
                    content=OutboxContent(text="done"),
                )
            )

        self.assertIn(
            ("telegram:chat:77", "telegram:message:62", "✅"),
            telegram.reactions,
        )

    async def test_terminal_stream_events_set_failure_or_stopped_reactions(
        self,
    ) -> None:
        cases = (
            (
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="error",
                    code="runtime.failed",
                    text="tool crashed",
                ),
                "😢",
            ),
            (
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="error",
                    code="runtime.cancelled",
                    text="cancelled",
                ),
                "👌",
            ),
        )
        for event, emoji in cases:
            with self.subTest(emoji=emoji), tempfile.TemporaryDirectory() as temp_dir:
                telegram = FakeTelegramTransport()
                worker = TelegramWorker(
                    config=build_config(Path(temp_dir)),
                    lionclaw_api=FakeLionClawApi(),
                    telegram=telegram,
                    offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                )
                await self._record_active_turn(
                    worker,
                    turn_id="turn-1",
                    message_ref="telegram:message:63",
                )

                with self.assertLogs(
                    "lionclaw_channel_telegram.worker",
                    level="ERROR",
                ):
                    await worker._process_stream_event(event)

                self.assertIn(
                    ("telegram:chat:77", "telegram:message:63", emoji),
                    telegram.reactions,
                )

    async def test_process_webhook_update_submits_without_polling_offset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir)
            offset_path = runtime_dir / "telegram.offset"
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=replace(
                    build_config(runtime_dir),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(offset_path),
            )
            update = Update.model_validate(
                {
                    "update_id": 87,
                    "message": {
                        "message_id": 17,
                        "date": 0,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "from webhook",
                    },
                }
            )

            processed = await worker.process_webhook_update(update)
            offset_exists = offset_path.exists()

        self.assertTrue(processed)
        self.assertEqual([update.text for update in api.sent_inbound], ["from webhook"])
        self.assertEqual(worker.offset, 0)
        self.assertFalse(offset_exists)

    async def test_process_webhook_update_batches_concurrent_text_messages(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            first = Update.model_validate(
                {
                    "update_id": 871,
                    "message": {
                        "message_id": 171,
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
            )
            second = Update.model_validate(
                {
                    "update_id": 872,
                    "message": {
                        "message_id": 172,
                        "date": 1,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "second",
                    },
                }
            )

            processed = await asyncio.gather(
                worker.process_webhook_update(second),
                worker.process_webhook_update(first),
            )

        self.assertEqual(processed, [True, True])
        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "first\n\nsecond")
        self.assertEqual(api.sent_inbound[0].provider_metadata["update_id"], 872)

    async def test_process_webhook_update_batches_concurrent_media_group(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="waiting_for_attachments")
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            first = Update.model_validate(
                {
                    "update_id": 873,
                    "message": {
                        "message_id": 173,
                        "date": 0,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "media_group_id": "album-webhook",
                        "caption": "front",
                        "photo": [
                            {
                                "file_id": "photo-file-1",
                                "file_unique_id": "photo-unique-1",
                                "width": 100,
                                "height": 100,
                                "file_size": 10,
                            }
                        ],
                    },
                }
            )
            second = Update.model_validate(
                {
                    "update_id": 874,
                    "message": {
                        "message_id": 174,
                        "date": 1,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "media_group_id": "album-webhook",
                        "caption": "back",
                        "photo": [
                            {
                                "file_id": "photo-file-2",
                                "file_unique_id": "photo-unique-2",
                                "width": 100,
                                "height": 100,
                                "file_size": 10,
                            }
                        ],
                    },
                }
            )

            processed = await asyncio.gather(
                worker.process_webhook_update(first),
                worker.process_webhook_update(second),
            )

        self.assertEqual(processed, [True, True])
        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "front\n\nback")
        self.assertEqual(len(api.sent_inbound[0].attachments), 2)
        self.assertEqual(len(api.staged_attachments), 2)
        self.assertEqual(len(api.finalized), 1)

    async def test_process_webhook_update_keeps_album_window_after_order_flush(
        self,
    ) -> None:
        class AttachmentAwareApi(FakeLionClawApi):
            async def send_inbound(
                self,
                update: TelegramInboundUpdate,
            ) -> InboundResponse:
                self.sent_inbound.append(update)
                return InboundResponse(
                    outcome=(
                        "waiting_for_attachments" if update.attachments else "queued"
                    ),
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            api = AttachmentAwareApi()
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            text = Update.model_validate(
                {
                    "update_id": 8731,
                    "message": {
                        "message_id": 1731,
                        "date": 0,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "before album",
                    },
                }
            )
            first_album = Update.model_validate(
                {
                    "update_id": 8732,
                    "message": {
                        "message_id": 1732,
                        "date": 1,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "media_group_id": "album-after-text",
                        "caption": "front",
                        "photo": [
                            {
                                "file_id": "photo-file-1",
                                "file_unique_id": "photo-unique-1",
                                "width": 100,
                                "height": 100,
                                "file_size": 10,
                            }
                        ],
                    },
                }
            )
            second_album = Update.model_validate(
                {
                    "update_id": 8733,
                    "message": {
                        "message_id": 1733,
                        "date": 2,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "media_group_id": "album-after-text",
                        "caption": "back",
                        "photo": [
                            {
                                "file_id": "photo-file-2",
                                "file_unique_id": "photo-unique-2",
                                "width": 100,
                                "height": 100,
                                "file_size": 10,
                            }
                        ],
                    },
                }
            )

            with patch(
                "lionclaw_channel_telegram.worker.WEBHOOK_COALESCE_DELAY_SECONDS",
                0.05,
            ):
                text_task = asyncio.create_task(worker.process_webhook_update(text))
                await asyncio.sleep(0)
                first_album_task = asyncio.create_task(
                    worker.process_webhook_update(first_album)
                )
                await asyncio.sleep(0.01)
                second_album_task = asyncio.create_task(
                    worker.process_webhook_update(second_album)
                )
                processed = await asyncio.gather(
                    text_task,
                    first_album_task,
                    second_album_task,
                )

        self.assertEqual(processed, [True, True, True])
        self.assertEqual(
            [update.text for update in api.sent_inbound],
            ["before album", "front\n\nback"],
        )
        self.assertEqual(len(api.sent_inbound[1].attachments), 2)
        self.assertEqual(len(api.staged_attachments), 2)
        self.assertEqual(len(api.finalized), 1)

    async def test_process_webhook_update_keeps_commands_ordered_after_pending_text(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            text_update = Update.model_validate(
                {
                    "update_id": 875,
                    "message": {
                        "message_id": 175,
                        "date": 0,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "start work",
                    },
                }
            )
            status_update = Update.model_validate(
                {
                    "update_id": 876,
                    "message": {
                        "message_id": 176,
                        "date": 1,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "/status",
                    },
                }
            )

            text_task = asyncio.create_task(worker.process_webhook_update(text_update))
            await asyncio.sleep(0)
            processed = await asyncio.gather(
                text_task,
                worker.process_webhook_update(status_update),
            )

        self.assertEqual(processed, [True, True])
        self.assertEqual([update.text for update in api.sent_inbound], ["start work"])
        self.assertEqual(telegram.sent_messages[0][1], "Active turn: queued.")

    async def test_process_webhook_update_blocks_commands_when_pending_text_fails(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="not-a-real-outcome")
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            text_update = Update.model_validate(
                {
                    "update_id": 877,
                    "message": {
                        "message_id": 177,
                        "date": 0,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "start work",
                    },
                }
            )
            status_update = Update.model_validate(
                {
                    "update_id": 878,
                    "message": {
                        "message_id": 178,
                        "date": 1,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "/status",
                    },
                }
            )

            text_task = asyncio.create_task(worker.process_webhook_update(text_update))
            await asyncio.sleep(0)
            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                processed = await asyncio.gather(
                    text_task,
                    worker.process_webhook_update(status_update),
                )

        self.assertEqual(processed, [False, False])
        self.assertEqual([update.text for update in api.sent_inbound], ["start work"])
        self.assertEqual(telegram.sent_messages, [])

    async def test_process_webhook_update_serializes_batches_for_same_route(
        self,
    ) -> None:
        class BlockingApi(FakeLionClawApi):
            def __init__(self) -> None:
                super().__init__()
                self.started: list[str | None] = []
                self.first_started = asyncio.Event()
                self.release_first = asyncio.Event()

            async def send_inbound(
                self,
                update: TelegramInboundUpdate,
            ) -> InboundResponse:
                self.started.append(update.text)
                if update.text == "first":
                    self.first_started.set()
                    await self.release_first.wait()
                return await super().send_inbound(update)

        with tempfile.TemporaryDirectory() as temp_dir:
            api = BlockingApi()
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            first = Update.model_validate(
                {
                    "update_id": 879,
                    "message": {
                        "message_id": 179,
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
            )
            second = Update.model_validate(
                {
                    "update_id": 880,
                    "message": {
                        "message_id": 180,
                        "date": 20,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "second",
                    },
                }
            )

            with patch(
                "lionclaw_channel_telegram.worker.WEBHOOK_COALESCE_DELAY_SECONDS",
                0.01,
            ):
                first_task = asyncio.create_task(worker.process_webhook_update(first))
                await asyncio.wait_for(api.first_started.wait(), timeout=1.0)
                second_task = asyncio.create_task(worker.process_webhook_update(second))
                await asyncio.sleep(0.05)

                self.assertEqual(api.started, ["first"])
                self.assertFalse(second_task.done())

                api.release_first.set()
                processed = await asyncio.gather(first_task, second_task)

        self.assertEqual(processed, [True, True])
        self.assertEqual(
            [update.text for update in api.sent_inbound], ["first", "second"]
        )

    async def test_webhook_batch_enqueue_flushes_route_batches_by_update_id(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            newer = ProviderWorkItem(
                (882,),
                TelegramInboundUpdate(
                    update_id=882,
                    event_id="telegram:update:882",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:182",
                    thread_ref=None,
                    text="album",
                    trigger="dm",
                    attachments=[
                        TelegramInboundAttachment(
                            attachment_id="telegram:update:882:attachment:photo:p1",
                            kind="photo",
                            provider_file_ref="photo-file",
                            mime_type="image/jpeg",
                        )
                    ],
                    provider_metadata={
                        "provider": "telegram",
                        "update_id": 882,
                        "chat_type": "private",
                        "message_date_epoch": 20,
                        "media_group_id": "album-1",
                    },
                ),
            )
            older = ProviderWorkItem(
                (881,),
                TelegramInboundUpdate(
                    update_id=881,
                    event_id="telegram:update:881",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:181",
                    thread_ref=None,
                    text="first",
                    trigger="dm",
                    provider_metadata={
                        "provider": "telegram",
                        "update_id": 881,
                        "chat_type": "private",
                        "message_date_epoch": 10,
                    },
                ),
            )
            newer_key = _webhook_batch_key(newer.event)
            older_key = _webhook_batch_key(older.event)
            assert newer_key is not None
            assert older_key is not None

            newer_result, should_schedule_newer = await worker._enqueue_webhook_batch(
                newer_key,
                newer,
            )
            older_result, should_schedule_older = await worker._enqueue_webhook_batch(
                older_key,
                older,
            )
            self.assertTrue(await older_result)
            self.assertFalse(newer_result.done())
            await worker._flush_webhook_batch(newer_key, delay_seconds=0.0)

        self.assertTrue(should_schedule_newer)
        self.assertFalse(should_schedule_older)
        self.assertTrue(await newer_result)
        self.assertEqual(
            [update.text for update in api.sent_inbound],
            ["first", "album"],
        )

    async def test_webhook_batch_enqueue_blocks_later_batches_after_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="not-a-real-outcome")
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            newer = ProviderWorkItem(
                (884,),
                TelegramInboundUpdate(
                    update_id=884,
                    event_id="telegram:update:884",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:184",
                    thread_ref=None,
                    text="album",
                    trigger="dm",
                    attachments=[
                        TelegramInboundAttachment(
                            attachment_id="telegram:update:884:attachment:photo:p1",
                            kind="photo",
                            provider_file_ref="photo-file",
                            mime_type="image/jpeg",
                        )
                    ],
                    provider_metadata={
                        "provider": "telegram",
                        "update_id": 884,
                        "chat_type": "private",
                        "message_date_epoch": 20,
                        "media_group_id": "album-1",
                    },
                ),
            )
            older = ProviderWorkItem(
                (883,),
                TelegramInboundUpdate(
                    update_id=883,
                    event_id="telegram:update:883",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:183",
                    thread_ref=None,
                    text="first",
                    trigger="dm",
                    provider_metadata={
                        "provider": "telegram",
                        "update_id": 883,
                        "chat_type": "private",
                        "message_date_epoch": 0,
                    },
                ),
            )
            newer_key = _webhook_batch_key(newer.event)
            older_key = _webhook_batch_key(older.event)
            assert newer_key is not None
            assert older_key is not None

            newer_result, should_schedule_newer = await worker._enqueue_webhook_batch(
                newer_key,
                newer,
            )
            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                (
                    older_result,
                    should_schedule_older,
                ) = await worker._enqueue_webhook_batch(older_key, older)

        self.assertTrue(should_schedule_newer)
        self.assertFalse(should_schedule_older)
        self.assertFalse(await older_result)
        self.assertFalse(await newer_result)
        self.assertEqual([update.text for update in api.sent_inbound], ["first"])

    async def test_process_webhook_update_reports_processing_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi(inbound_outcome="not-a-real-outcome")
            worker = TelegramWorker(
                config=replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_secret_token="secret",
                ),
                lionclaw_api=api,
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            update = Update.model_validate(
                {
                    "update_id": 88,
                    "message": {
                        "message_id": 18,
                        "date": 0,
                        "chat": {"id": 77, "type": "private"},
                        "from": {
                            "id": 77,
                            "is_bot": False,
                            "first_name": "Alice",
                        },
                        "text": "will fail",
                    },
                }
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                processed = await worker.process_webhook_update(update)
            checks = await worker._health_checks()

        self.assertFalse(processed)
        webhook = {check.code: check for check in checks}["telegram.webhook"]
        self.assertEqual(webhook.status, "error")

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
            self.assertEqual(
                [update.text for update in api.sent_inbound], ["first\n\nsecond"]
            )

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

    async def test_private_unsupported_content_gets_clear_reply(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            offset_store = OffsetStore(Path(temp_dir) / "telegram.offset")
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 901,
                            "message": {
                                "message_id": 61,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "contact": {
                                    "phone_number": "+15551234567",
                                    "first_name": "Alice",
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
                offset_store=offset_store,
            )

            await worker.process_updates()
            offset_text = offset_store.path.read_text(encoding="utf-8")

        self.assertEqual(api.sent_inbound, [])
        self.assertEqual(worker.offset, 902)
        self.assertEqual(offset_text, "902")
        self.assertEqual(len(telegram.sent_messages), 1)
        conversation_ref, text, reply_to_ref, thread_ref, _ = telegram.sent_messages[0]
        self.assertEqual(conversation_ref, "telegram:chat:77")
        self.assertIn("can't use this Telegram contact yet", text)
        self.assertEqual(reply_to_ref, "telegram:message:61")
        self.assertIsNone(thread_ref)

    async def test_addressed_group_unsupported_content_gets_clear_reply(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 902,
                            "message": {
                                "message_id": 62,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "message_thread_id": 7,
                                "is_topic_message": True,
                                "dice": {"emoji": "🎲", "value": 4},
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
        self.assertEqual(len(telegram.sent_messages), 1)
        conversation_ref, text, reply_to_ref, thread_ref, _ = telegram.sent_messages[0]
        self.assertEqual(conversation_ref, "telegram:chat:-10077")
        self.assertIn("can't use this Telegram dice yet", text)
        self.assertEqual(reply_to_ref, "telegram:message:62")
        self.assertEqual(thread_ref, "telegram:topic:7")

    async def test_unaddressed_group_unsupported_content_stays_silent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 903,
                            "message": {
                                "message_id": 63,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "contact": {
                                    "phone_number": "+15551234567",
                                    "first_name": "Alice",
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

        self.assertEqual(api.sent_inbound, [])
        self.assertEqual(telegram.sent_messages, [])

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

    async def test_retry_callback_submits_canonical_lionclaw_control(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 923,
                            "message": {
                                "message_id": 23,
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
            retry_payload = telegram.sent_buttons[0][1].action
            api.sent_inbound.clear()
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 924,
                        "callback_query": {
                            "id": "callback-retry",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": retry_payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "LionClaw controls",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(telegram.answered_callbacks[-1], ("callback-retry", "Queued"))
        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/lionclaw retry")

    async def test_topic_route_callback_survives_inaccessible_message(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 930,
                            "message": {
                                "message_id": 30,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "message_thread_id": 55,
                                "is_topic_message": True,
                                "text": "@lionclaw_bot /help",
                                "entities": [
                                    {
                                        "type": "mention",
                                        "offset": 0,
                                        "length": len("@lionclaw_bot"),
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("@lionclaw_bot "),
                                        "length": len("/help"),
                                    },
                                ],
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
            retry_payload = telegram.sent_buttons[0][1].action
            self.assertIn("route.-10077.55", retry_payload)
            api.sent_inbound.clear()
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 931,
                        "callback_query": {
                            "id": "callback-topic-retry-old",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": retry_payload,
                            "message": {
                                "message_id": 130,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-topic-retry-old", "Queued"),
        )
        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/lionclaw retry")
        self.assertEqual(api.sent_inbound[0].conversation_ref, "telegram:chat:-10077")
        self.assertEqual(api.sent_inbound[0].thread_ref, "telegram:topic:55")

    async def test_route_callback_from_different_sender_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 925,
                            "message": {
                                "message_id": 25,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "@lionclaw_bot /help",
                                "entities": [
                                    {
                                        "type": "mention",
                                        "offset": 0,
                                        "length": len("@lionclaw_bot"),
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("@lionclaw_bot "),
                                        "length": len("/help"),
                                    },
                                ],
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
            retry_payload = telegram.sent_buttons[0][1].action
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 926,
                        "callback_query": {
                            "id": "callback-retry-bob",
                            "from": {
                                "id": 88,
                                "is_bot": False,
                                "first_name": "Bob",
                            },
                            "chat_instance": "chat-instance",
                            "data": retry_payload,
                            "message": {
                                "message_id": 102,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "LionClaw controls",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(api.sent_inbound, [])
        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-retry-bob", "That control is no longer valid."),
        )

    async def test_malformed_callback_mac_is_rejected_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 927,
                            "callback_query": {
                                "id": "callback-bad-mac",
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "chat_instance": "chat-instance",
                                "data": "lc1:r:_:secrét",
                                "message": {
                                    "message_id": 102,
                                    "date": 0,
                                    "chat": {"id": 77, "type": "private"},
                                    "from": {
                                        "id": 99,
                                        "is_bot": True,
                                        "first_name": "LionClaw",
                                        "username": "lionclaw_bot",
                                    },
                                    "text": "LionClaw controls",
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

        self.assertEqual(api.sent_inbound, [])
        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-bad-mac", "That control is no longer valid."),
        )

    async def test_route_callback_works_while_route_has_active_turn(self) -> None:
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
                    update_id=927,
                    event_id="telegram:update:927",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:27",
                    text="long task",
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
            await worker._send_help(
                TelegramInboundUpdate(
                    update_id=928,
                    event_id="telegram:update:928",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:28",
                    text="/help",
                    trigger="dm",
                    provider_metadata={"chat_type": "private"},
                )
            )
            retry_payload = telegram.sent_buttons[-1][1].action
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 929,
                        "callback_query": {
                            "id": "callback-retry-active",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": retry_payload,
                            "message": {
                                "message_id": 102,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "LionClaw controls",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(
            telegram.answered_callbacks[-1], ("callback-retry-active", "Queued")
        )
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

    async def test_targeted_runtime_command_strips_bot_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 933,
                            "message": {
                                "message_id": 33,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "/model@lionclaw_bot gpt-5.2",
                                "entities": [
                                    {
                                        "type": "bot_command",
                                        "offset": 0,
                                        "length": len("/model@lionclaw_bot"),
                                    }
                                ],
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

    async def test_leading_mention_local_command_stays_local(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 934,
                            "message": {
                                "message_id": 34,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "@lionclaw_bot /status",
                                "entities": [
                                    {
                                        "type": "mention",
                                        "offset": 0,
                                        "length": len("@lionclaw_bot"),
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("@lionclaw_bot "),
                                        "length": len("/status"),
                                    },
                                ],
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
        self.assertIn("No active LionClaw turn", telegram.sent_messages[0][1])

    async def test_leading_mention_runtime_command_strips_mention(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 935,
                            "message": {
                                "message_id": 35,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "@lionclaw_bot /model gpt-5.2",
                                "entities": [
                                    {
                                        "type": "mention",
                                        "offset": 0,
                                        "length": len("@lionclaw_bot"),
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("@lionclaw_bot "),
                                        "length": len("/model"),
                                    },
                                ],
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

    async def test_leading_mention_targeted_runtime_command_normalizes_for_runtime(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 938,
                            "message": {
                                "message_id": 38,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "@lionclaw_bot /model@lionclaw_bot gpt-5.2",
                                "entities": [
                                    {
                                        "type": "mention",
                                        "offset": 0,
                                        "length": len("@lionclaw_bot"),
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("@lionclaw_bot "),
                                        "length": len("/model@lionclaw_bot"),
                                    },
                                ],
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

    async def test_leading_text_mention_runtime_command_strips_mention(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 937,
                            "message": {
                                "message_id": 37,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "LionClaw /model gpt-5.2",
                                "entities": [
                                    {
                                        "type": "text_mention",
                                        "offset": 0,
                                        "length": len("LionClaw"),
                                        "user": {
                                            "id": 99,
                                            "is_bot": True,
                                            "first_name": "LionClaw",
                                        },
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("LionClaw "),
                                        "length": len("/model"),
                                    },
                                ],
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

    async def test_leading_mention_command_targeting_other_bot_passes_through(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 936,
                            "message": {
                                "message_id": 36,
                                "date": 0,
                                "chat": {"id": -10077, "type": "supergroup"},
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "text": "@lionclaw_bot /stop@other_bot",
                                "entities": [
                                    {
                                        "type": "mention",
                                        "offset": 0,
                                        "length": len("@lionclaw_bot"),
                                    },
                                    {
                                        "type": "bot_command",
                                        "offset": len("@lionclaw_bot "),
                                        "length": len("/stop@other_bot"),
                                    },
                                ],
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
        self.assertEqual(api.sent_inbound[0].text, "@lionclaw_bot /stop@other_bot")
        self.assertEqual(api.cancel_calls, [])

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

    async def test_topic_command_targeting_other_bot_passes_through(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 932,
                            "message": {
                                "message_id": 32,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "message_thread_id": 77,
                                "is_topic_message": True,
                                "text": "/stop@other_bot",
                                "entities": [
                                    {
                                        "type": "bot_command",
                                        "offset": 0,
                                        "length": len("/stop@other_bot"),
                                    }
                                ],
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
                    update_id=931,
                    event_id="telegram:update:931",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:-10077",
                    thread_ref="telegram:topic:77",
                    message_ref="telegram:message:31",
                    text="slow",
                    trigger="thread_continuation",
                    provider_metadata={"chat_type": "supergroup"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:thread:-10077:77",
                ),
            )

            await worker.process_updates()

        self.assertEqual(len(api.sent_inbound), 1)
        self.assertEqual(api.sent_inbound[0].text, "/stop@other_bot")
        self.assertEqual(api.cancel_calls, [])
        self.assertIn("turn-1", worker._active_turns)

    async def test_topic_command_targeting_lionclaw_bot_stays_local(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 933,
                            "message": {
                                "message_id": 33,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "message_thread_id": 77,
                                "is_topic_message": True,
                                "text": "/stop@lionclaw_bot",
                                "entities": [
                                    {
                                        "type": "bot_command",
                                        "offset": 0,
                                        "length": len("/stop@lionclaw_bot"),
                                    }
                                ],
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
                    update_id=932,
                    event_id="telegram:update:932",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:-10077",
                    thread_ref="telegram:topic:77",
                    message_ref="telegram:message:32",
                    text="slow",
                    trigger="thread_continuation",
                    provider_metadata={"chat_type": "supergroup"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:thread:-10077:77",
                ),
            )

            await worker.process_updates()

        self.assertEqual(api.sent_inbound, [])
        self.assertEqual(
            api.cancel_calls,
            [
                (
                    "session-1",
                    "channel:telegram:thread:-10077:77",
                    "turn-1",
                    "telegram stop command",
                )
            ],
        )
        self.assertEqual(telegram.sent_messages[-1][1], "Stopping...")

    async def test_topic_stop_does_not_fall_back_to_unthreaded_chat_turn(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 934,
                            "message": {
                                "message_id": 34,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                                "from": {
                                    "id": 77,
                                    "is_bot": False,
                                    "first_name": "Alice",
                                },
                                "message_thread_id": 77,
                                "is_topic_message": True,
                                "text": "/stop",
                                "entities": [
                                    {
                                        "type": "bot_command",
                                        "offset": 0,
                                        "length": len("/stop"),
                                    }
                                ],
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
                    update_id=933,
                    event_id="telegram:update:933",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:-10077",
                    thread_ref=None,
                    message_ref="telegram:message:33",
                    text="slow",
                    trigger="mention",
                    provider_metadata={"chat_type": "supergroup"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:conversation:-10077:77",
                ),
            )

            await worker.process_updates()

        self.assertEqual(api.cancel_calls, [])
        self.assertIn("turn-1", worker._active_turns)
        self.assertEqual(
            telegram.sent_messages[-1][1],
            "No active LionClaw turn is running here.",
        )

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

    async def test_progress_message_includes_scoped_stop_callback(self) -> None:
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
                            "update_id": 944,
                            "message": {
                                "message_id": 44,
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

        self.assertEqual(
            [button.text for button in telegram.sent_buttons[-1]], ["Stop", "Status"]
        )
        self.assertTrue(telegram.sent_buttons[-1][0].action.startswith("lc1:s:turn-1:"))
        self.assertTrue(telegram.sent_buttons[-1][1].action.startswith("lc1:t:turn-1:"))

    async def test_stop_callback_cancels_active_turn_with_expected_turn_guard(
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
                            "update_id": 945,
                            "message": {
                                "message_id": 45,
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
            stop_payload = telegram.sent_buttons[-1][0].action
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 946,
                        "callback_query": {
                            "id": "callback-stop",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": stop_payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "Queued...",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(telegram.answered_callbacks[-1], ("callback-stop", "Stopping"))
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

    async def test_inaccessible_topic_stop_callback_uses_active_turn_route(
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
                    update_id=946,
                    event_id="telegram:update:946",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:-10077",
                    thread_ref="telegram:topic:77",
                    message_ref="telegram:message:46",
                    text="long task",
                    trigger="thread_continuation",
                    provider_metadata={"chat_type": "supergroup"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:thread:-10077:77",
                ),
            )
            active = worker._active_turns["turn-1"]
            stop_payload = worker._callback_payload(
                "stop",
                active.target,
                actor_ref=active.sender_ref,
                active=active,
            )
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 947,
                        "callback_query": {
                            "id": "callback-old-stop",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": stop_payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-old-stop", "Stopping"),
        )
        self.assertEqual(
            api.cancel_calls,
            [
                (
                    "session-1",
                    "channel:telegram:thread:-10077:77",
                    "turn-1",
                    "telegram stop command",
                )
            ],
        )
        self.assertEqual(telegram.sent_messages[-1][3], "telegram:topic:77")

    async def test_progress_status_callback_works_for_active_turn(self) -> None:
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
                            "update_id": 947,
                            "message": {
                                "message_id": 47,
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
            status_payload = telegram.sent_buttons[-1][1].action
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 948,
                        "callback_query": {
                            "id": "callback-status-active",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": status_payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "Queued...",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-status-active", "Status sent"),
        )
        self.assertEqual(telegram.sent_messages[-1][1], "Active turn: queued.")

    async def test_stop_callback_from_different_sender_is_rejected(self) -> None:
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
                    update_id=947,
                    event_id="telegram:update:947",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:-10077",
                    thread_ref="telegram:topic:7",
                    message_ref="telegram:message:47",
                    text="long task",
                    trigger="thread_continuation",
                    provider_metadata={"chat_type": "supergroup"},
                ),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:thread:-10077:7",
                ),
            )
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()
            stop_payload = telegram.sent_buttons[-1][0].action
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 948,
                        "callback_query": {
                            "id": "callback-stop-bob",
                            "from": {
                                "id": 88,
                                "is_bot": False,
                                "first_name": "Bob",
                            },
                            "chat_instance": "chat-instance",
                            "data": stop_payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {
                                    "id": -10077,
                                    "type": "supergroup",
                                    "is_forum": True,
                                },
                                "message_thread_id": 7,
                                "is_topic_message": True,
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "Queued...",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(api.cancel_calls, [])
        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-stop-bob", "That control is no longer valid."),
        )
        self.assertIn("turn-1", worker._active_turns)

    async def test_stale_stop_callback_is_acknowledged_without_cancelling(self) -> None:
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
                    update_id=948,
                    event_id="telegram:update:948",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:48",
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
            payload = worker._callback_payload(
                "stop",
                worker._active_turns["turn-1"].target,
                actor_ref="telegram:user:77",
                active=worker._active_turns["turn-1"],
            )
            worker._forget_turn(worker._active_turns["turn-1"])
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 949,
                        "callback_query": {
                            "id": "callback-stale",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "Queued...",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(api.cancel_calls, [])
        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-stale", "That control is no longer active."),
        )

    async def test_active_turn_state_survives_worker_restart_for_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ActiveTurnStore(Path(temp_dir) / "telegram.active-turns.json")
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                active_turn_store=store,
            )
            await first_worker._remember_active_turn(
                TelegramInboundUpdate(
                    update_id=946,
                    event_id="telegram:update:946",
                    sender_ref="telegram:user:77",
                    conversation_ref="telegram:chat:77",
                    message_ref="telegram:message:46",
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
            first_worker._active_turns["turn-1"].visible_after = 0.0
            first_worker._save_active_turns()

            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 947,
                            "message": {
                                "message_id": 47,
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
                    )
                ]
            )
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                active_turn_store=store,
            )

            await second_worker.process_updates()

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

    async def test_flush_stream_accepts_message_boundary_without_error(self) -> None:
        api = FakeLionClawApi(
            stream_events=[
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="message_boundary",
                    lane="answer",
                ),
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:chat:77",
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

            with self.assertNoLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker.flush_stream()

        self.assertEqual(api.acked_sequences, [2])

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

    async def test_fast_done_forgets_turn_without_creating_stale_progress(self) -> None:
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
                            "update_id": 961,
                            "message": {
                                "message_id": 61,
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
            await worker._process_stream_event(
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="done",
                )
            )
            await worker.refresh_progress_messages()
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
                    "final answer",
                    None,
                    None,
                    [],
                ),
            ],
        )
        self.assertEqual(telegram.edited_messages, [])
        self.assertEqual(telegram.deleted_messages, [])
        self.assertEqual(worker._active_turns, {})
        self.assertEqual(worker._route_turns, {})

    async def test_empty_done_deletes_visible_progress_without_outbox(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await self._remember_visible_progress(worker)
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="turn_completed",
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

        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertEqual(worker._active_turns, {})
        self.assertEqual(worker._route_turns, {})

    async def test_done_keeps_progress_until_expected_outbox_arrives(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await self._remember_visible_progress(worker)
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="message_delta",
                    lane="answer",
                    text="partial answer",
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
            self.assertEqual(telegram.deleted_messages, [])
            self.assertIn("turn-1", worker._active_turns)

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
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertEqual(worker._active_turns, {})
        self.assertEqual(worker._route_turns, {})

    async def test_answer_delta_outbox_expectation_survives_worker_restart(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ActiveTurnStore(Path(temp_dir) / "telegram.active-turns.json")
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=FakeTelegramTransport(),
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                active_turn_store=store,
            )

            progress_ref = await self._remember_visible_progress(first_worker)
            await first_worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="message_delta",
                    lane="answer",
                    text="partial answer",
                )
            )

            stored_turns = store.load()
            self.assertEqual(len(stored_turns), 1)
            self.assertTrue(stored_turns[0].expects_outbox_delivery)

            second_telegram = FakeTelegramTransport()
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=second_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                active_turn_store=store,
            )
            await second_worker._process_stream_event(
                StreamEvent(
                    sequence=2,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="done",
                )
            )

            self.assertEqual(second_telegram.deleted_messages, [])
            self.assertIn("turn-1", second_worker._active_turns)

            await second_worker._process_outbox_delivery(
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:chat:77",
                    turn_id="turn-1",
                    content=OutboxContent(text="final answer"),
                )
            )

        self.assertEqual(
            second_telegram.deleted_messages,
            [("telegram:chat:77", progress_ref)],
        )
        self.assertEqual(second_worker._active_turns, {})
        self.assertEqual(second_worker._route_turns, {})

    async def test_done_keeps_progress_until_artifact_outbox_arrives(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await self._remember_visible_progress(worker)
            await worker._process_stream_event(
                StreamEvent(
                    sequence=1,
                    peer_id="telegram:chat:77",
                    turn_id="turn-1",
                    kind="status",
                    code="runtime.artifact",
                    text="Created chart.png",
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
            self.assertEqual(telegram.deleted_messages, [])
            self.assertIn("turn-1", worker._active_turns)

            await worker._process_outbox_delivery(
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    conversation_ref="telegram:chat:77",
                    turn_id="turn-1",
                    content=OutboxContent(
                        text="",
                        attachments=[
                            OutboxAttachment(
                                attachment_id="artifact-1",
                                path="/tmp/chart.png",
                                filename="chart.png",
                                mime_type="image/png",
                            )
                        ],
                    ),
                )
            )

        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertEqual(worker._active_turns, {})
        self.assertEqual(worker._route_turns, {})

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

    async def test_progress_delete_failure_retries_without_reopening_turn(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                delete_errors=[RuntimeError("temporary delete failure")]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            progress_ref = await self._remember_visible_progress(worker)

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker._process_outbox_delivery(
                    OutboxDelivery(
                        delivery_id="delivery-1",
                        attempt_id="attempt-1",
                        conversation_ref="telegram:chat:77",
                        turn_id="turn-1",
                        content=OutboxContent(text="final answer"),
                    )
                )

            self.assertNotIn("turn-1", worker._active_turns)
            self.assertEqual(telegram.deleted_messages, [])
            self.assertEqual(len(worker._pending_progress_deletes), 1)
            pending = next(iter(worker._pending_progress_deletes.values()))
            self.assertEqual(pending.message_ref, progress_ref)
            pending.next_attempt_at = 0.0

            await worker.refresh_progress_messages()

        self.assertEqual(telegram.sent_messages[-1][1], "final answer")
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", progress_ref)],
        )
        self.assertEqual(worker._pending_progress_deletes, {})

    async def test_progress_delete_failure_survives_worker_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            delete_store = ProgressDeleteStore(
                Path(temp_dir) / "telegram.progress-deletes.json"
            )
            first_telegram = FakeTelegramTransport(
                delete_errors=[RuntimeError("temporary delete failure")]
            )
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=first_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                progress_delete_store=delete_store,
            )
            progress_ref = await self._remember_visible_progress(first_worker)

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await first_worker._process_outbox_delivery(
                    OutboxDelivery(
                        delivery_id="delivery-1",
                        attempt_id="attempt-1",
                        conversation_ref="telegram:chat:77",
                        turn_id="turn-1",
                        content=OutboxContent(text="final answer"),
                    )
                )

            self.assertEqual(first_telegram.deleted_messages, [])
            self.assertEqual(
                delete_store.load(),
                {
                    ("telegram:chat:77", progress_ref): PendingProgressDelete(
                        turn_id="turn-1",
                        conversation_ref="telegram:chat:77",
                        message_ref=progress_ref,
                        attempts=1,
                        next_attempt_at=0.0,
                    )
                },
            )

            second_telegram = FakeTelegramTransport()
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=second_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                progress_delete_store=delete_store,
            )

            await second_worker.refresh_progress_messages()

            self.assertEqual(
                second_telegram.deleted_messages,
                [("telegram:chat:77", progress_ref)],
            )
            self.assertEqual(delete_store.load(), {})

    async def test_permanent_progress_delete_failure_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport(
                delete_errors=[
                    TelegramBadRequest(
                        method=object(),
                        message="message to delete not found",
                    )
                ]
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await self._remember_visible_progress(worker)

            with self.assertLogs("lionclaw_channel_telegram.worker", level="WARNING"):
                await worker._process_outbox_delivery(
                    OutboxDelivery(
                        delivery_id="delivery-1",
                        attempt_id="attempt-1",
                        conversation_ref="telegram:chat:77",
                        turn_id="turn-1",
                        content=OutboxContent(text="final answer"),
                    )
                )
            await worker.refresh_progress_messages()

        self.assertNotIn("turn-1", worker._active_turns)
        self.assertEqual(telegram.deleted_messages, [])
        self.assertEqual(worker._pending_progress_deletes, {})

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

    async def test_status_callback_survives_late_session_key_fill(self) -> None:
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
                update_id=976,
                event_id="telegram:update:976",
                sender_ref="telegram:user:77",
                conversation_ref="telegram:chat:77",
                message_ref="telegram:message:76",
                text="slow",
                trigger="dm",
                provider_metadata={"chat_type": "private"},
            )
            await worker._remember_active_turn(
                update,
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id=None,
                    session_key=None,
                ),
            )
            worker._active_turns["turn-1"].visible_after = 0.0
            await worker.refresh_progress_messages()
            status_payload = telegram.sent_buttons[-1][0].action

            await worker._remember_active_turn(
                replace(update, update_id=977, event_id="telegram:update:977"),
                InboundResponse(
                    outcome="queued",
                    turn_id="turn-1",
                    session_id="session-1",
                    session_key="channel:telegram:direct:77",
                ),
            )
            telegram.updates = [
                Update.model_validate(
                    {
                        "update_id": 978,
                        "callback_query": {
                            "id": "callback-status-late-key",
                            "from": {
                                "id": 77,
                                "is_bot": False,
                                "first_name": "Alice",
                            },
                            "chat_instance": "chat-instance",
                            "data": status_payload,
                            "message": {
                                "message_id": 101,
                                "date": 0,
                                "chat": {"id": 77, "type": "private"},
                                "from": {
                                    "id": 99,
                                    "is_bot": True,
                                    "first_name": "LionClaw",
                                    "username": "lionclaw_bot",
                                },
                                "text": "Queued...",
                            },
                        },
                    }
                )
            ]

            await worker.process_updates()

        self.assertEqual(
            telegram.answered_callbacks[-1],
            ("callback-status-late-key", "Status sent"),
        )
        self.assertEqual(telegram.sent_messages[-1][1], "Active turn: queued.")

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

    async def test_terminal_progress_send_failure_retries_without_forgetting_turn(
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
                            "update_id": 981,
                            "message": {
                                "message_id": 81,
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
                send_message_error=RuntimeError("temporary send failure"),
            )
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )

            await worker.process_updates()
            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await worker._process_stream_event(
                    StreamEvent(
                        sequence=1,
                        peer_id="telegram:chat:77",
                        turn_id="turn-1",
                        kind="error",
                        code="runtime.error",
                        text="runtime failed",
                    )
                )

            self.assertIn("turn-1", worker._active_turns)
            self.assertEqual(telegram.sent_messages, [])

            telegram.send_message_error = None
            await worker.refresh_progress_messages()

        self.assertEqual(telegram.sent_messages[0][1], "Turn failed: runtime failed")
        self.assertNotIn("turn-1", worker._active_turns)

    async def test_terminal_progress_retry_survives_worker_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ActiveTurnStore(Path(temp_dir) / "telegram.active-turns.json")
            api = FakeLionClawApi(
                inbound_turn_id="turn-1",
                inbound_session_id="session-1",
                inbound_session_key="channel:telegram:direct:77",
            )
            first_telegram = FakeTelegramTransport(
                updates=[
                    Update.model_validate(
                        {
                            "update_id": 982,
                            "message": {
                                "message_id": 82,
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
                send_message_error=RuntimeError("temporary send failure"),
            )
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=first_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                active_turn_store=store,
            )

            await first_worker.process_updates()
            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await first_worker._process_stream_event(
                    StreamEvent(
                        sequence=1,
                        peer_id="telegram:chat:77",
                        turn_id="turn-1",
                        kind="error",
                        code="runtime.error",
                        text="runtime failed",
                    )
                )

            second_telegram = FakeTelegramTransport()
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=FakeLionClawApi(),
                telegram=second_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                active_turn_store=store,
            )
            await second_worker.refresh_progress_messages()

        self.assertEqual(
            second_telegram.sent_messages[0][1],
            "Turn failed: runtime failed",
        )
        self.assertEqual(store.load(), [])

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

    async def test_progress_edit_failure_deletes_progress_after_fallback(
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
                            "update_id": 103,
                            "message": {
                                "message_id": 13,
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
                        message="message can't be edited",
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

        self.assertEqual(
            telegram.sent_messages[-1][1], "Turn failed: bad provider response"
        )
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )

    async def test_progress_terminal_fallback_send_failure_retries(
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
                            "update_id": 104,
                            "message": {
                                "message_id": 14,
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
                        message="message can't be edited",
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
            turn = worker._active_turns["turn-1"]
            turn.visible_after = 0.0
            await worker.refresh_progress_messages()
            telegram.send_message_error = RuntimeError("temporary fallback failure")
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

            self.assertIn("turn-1", worker._active_turns)
            self.assertEqual(
                worker._active_turns["turn-1"].terminal_text,
                "Turn failed: bad provider response",
            )

            telegram.send_message_error = None
            await worker.refresh_progress_messages()

        self.assertEqual(
            telegram.sent_messages[-1][1], "Turn failed: bad provider response"
        )
        self.assertEqual(
            telegram.deleted_messages,
            [("telegram:chat:77", "telegram:message:101")],
        )
        self.assertNotIn("turn-1", worker._active_turns)

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

    async def test_delivered_outbox_receipt_survives_report_failure_and_restart(
        self,
    ) -> None:
        async def no_sleep(_delay: float) -> None:
            return None

        delivery = OutboxDelivery(
            delivery_id="delivery-1",
            attempt_id="attempt-1",
            conversation_ref="telegram:user:77",
            content=OutboxContent(text="final answer"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_store = OutboxReceiptStore(
                Path(temp_dir) / "telegram.outbox-receipts.json"
            )
            first_api = FakeLionClawApi(
                outbox_report_errors=[
                    RuntimeError("kernel unavailable"),
                    RuntimeError("kernel unavailable"),
                    RuntimeError("kernel unavailable"),
                ]
            )
            first_telegram = FakeTelegramTransport()
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=first_api,
                telegram=first_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            with (
                self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"),
                patch("lionclaw_channel_telegram.worker.asyncio.sleep", no_sleep),
            ):
                await first_worker._process_outbox_delivery(delivery)

            released_delivery = replace(delivery, attempt_id="attempt-2")
            second_api = FakeLionClawApi()
            second_telegram = FakeTelegramTransport()
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=second_api,
                telegram=second_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            await second_worker._process_outbox_delivery(released_delivery)

            self.assertEqual(
                first_telegram.sent_messages,
                [
                    (
                        "telegram:user:77",
                        "final answer",
                        None,
                        None,
                        [],
                    )
                ],
            )
            self.assertEqual(second_telegram.sent_messages, [])
            self.assertEqual(len(first_api.outbox_reports), 3)
            self.assertEqual(
                second_api.outbox_reports,
                [
                    (
                        "delivery-1",
                        "attempt-2",
                        "delivered",
                        {
                            "message_id": 101,
                            "chat_id": "77",
                            "messages": [{"message_id": 101, "chat_id": "77"}],
                        },
                        None,
                        None,
                    )
                ],
            )
            self.assertEqual(receipt_store.load(), {})

    async def test_mismatched_delivered_outbox_receipt_is_discarded_before_replay(
        self,
    ) -> None:
        delivery = OutboxDelivery(
            delivery_id="delivery-1",
            attempt_id="attempt-2",
            conversation_ref="telegram:user:77",
            content=OutboxContent(text="final answer"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_store = OutboxReceiptStore(
                Path(temp_dir) / "telegram.outbox-receipts.json"
            )
            receipt_store.save(
                {
                    "delivery-1": OutboxReceiptRecord(
                        status="delivered",
                        provider_receipt={"message_id": 101, "chat_id": "88"},
                    )
                }
            )
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="WARNING"):
                await worker._process_outbox_delivery(delivery)

            self.assertEqual(
                telegram.sent_messages,
                [("telegram:user:77", "final answer", None, None, [])],
            )
            self.assertEqual(telegram.resume_receipts, [None])
            self.assertEqual(api.outbox_reports[0][2], "delivered")
            self.assertEqual(receipt_store.load(), {})

    async def test_partial_outbox_receipt_resumes_after_retryable_send_failure(
        self,
    ) -> None:
        partial_receipt = {
            "message_id": 101,
            "chat_id": "telegram:user:77",
            "messages": [{"message_id": 101, "chat_id": "telegram:user:77"}],
        }
        delivery = OutboxDelivery(
            delivery_id="delivery-1",
            attempt_id="attempt-1",
            conversation_ref="telegram:user:77",
            content=OutboxContent(text="final answer"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_store = OutboxReceiptStore(
                Path(temp_dir) / "telegram.outbox-receipts.json"
            )
            first_api = FakeLionClawApi()
            first_telegram = FakeTelegramTransport(
                partial_send_error=RuntimeError("connection reset"),
                partial_send_receipt=partial_receipt,
            )
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=first_api,
                telegram=first_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await first_worker._process_outbox_delivery(delivery)

            self.assertEqual(first_api.outbox_reports[0][2], "retryable_failed")
            self.assertEqual(
                receipt_store.load(),
                {
                    "delivery-1": OutboxReceiptRecord(
                        status="partial",
                        provider_receipt={
                            "message_id": 101,
                            "chat_id": "77",
                            "messages": [{"message_id": 101, "chat_id": "77"}],
                        },
                    )
                },
            )

            second_api = FakeLionClawApi()
            second_telegram = FakeTelegramTransport()
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=second_api,
                telegram=second_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            await second_worker._process_outbox_delivery(
                replace(delivery, attempt_id="attempt-2")
            )

            self.assertEqual(
                second_telegram.resume_receipts,
                [
                    {
                        "message_id": 101,
                        "chat_id": "77",
                        "messages": [{"message_id": 101, "chat_id": "77"}],
                    }
                ],
            )
            self.assertEqual(second_api.outbox_reports[0][2], "delivered")
            self.assertEqual(receipt_store.load(), {})

    async def test_partial_outbox_receipt_survives_terminal_report_failure(
        self,
    ) -> None:
        async def no_sleep(_delay: float) -> None:
            return None

        partial_receipt = {
            "message_id": 101,
            "chat_id": "telegram:user:77",
            "messages": [{"message_id": 101, "chat_id": "telegram:user:77"}],
        }
        delivery = OutboxDelivery(
            delivery_id="delivery-1",
            attempt_id="attempt-1",
            conversation_ref="telegram:user:77",
            content=OutboxContent(
                text="final answer",
                attachments=[
                    OutboxAttachment(
                        attachment_id="artifact-1",
                        path="/tmp/missing-report.txt",
                        filename="missing-report.txt",
                        mime_type="text/plain",
                    )
                ],
            ),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_store = OutboxReceiptStore(
                Path(temp_dir) / "telegram.outbox-receipts.json"
            )
            first_api = FakeLionClawApi(
                outbox_report_errors=[
                    RuntimeError("kernel unavailable"),
                    RuntimeError("kernel unavailable"),
                    RuntimeError("kernel unavailable"),
                ]
            )
            first_telegram = FakeTelegramTransport(
                partial_send_error=FileNotFoundError("missing artifact"),
                partial_send_receipt=partial_receipt,
            )
            first_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=first_api,
                telegram=first_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            with (
                self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"),
                patch("lionclaw_channel_telegram.worker.asyncio.sleep", no_sleep),
            ):
                await first_worker._process_outbox_delivery(delivery)

            self.assertEqual(
                [report[2] for report in first_api.outbox_reports],
                ["terminal_failed", "terminal_failed", "terminal_failed"],
            )
            self.assertEqual(first_api.outbox_reports[0][3], partial_receipt)
            self.assertEqual(
                receipt_store.load(),
                {
                    "delivery-1": OutboxReceiptRecord(
                        status="partial",
                        provider_receipt={
                            "message_id": 101,
                            "chat_id": "77",
                            "messages": [{"message_id": 101, "chat_id": "77"}],
                        },
                    )
                },
            )

            second_api = FakeLionClawApi()
            second_telegram = FakeTelegramTransport(
                partial_send_error=FileNotFoundError("missing artifact"),
                partial_send_receipt=partial_receipt,
            )
            second_worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=second_api,
                telegram=second_telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="ERROR"):
                await second_worker._process_outbox_delivery(
                    replace(delivery, attempt_id="attempt-2")
                )

            self.assertEqual(
                second_telegram.resume_receipts,
                [
                    {
                        "message_id": 101,
                        "chat_id": "77",
                        "messages": [{"message_id": 101, "chat_id": "77"}],
                    }
                ],
            )
            self.assertEqual(second_api.outbox_reports[0][2], "terminal_failed")
            self.assertEqual(second_api.outbox_reports[0][3], partial_receipt)
            self.assertEqual(receipt_store.load(), {})

    async def test_mismatched_partial_outbox_receipt_restarts_delivery_without_resume(
        self,
    ) -> None:
        delivery = OutboxDelivery(
            delivery_id="delivery-1",
            attempt_id="attempt-2",
            conversation_ref="telegram:user:77",
            content=OutboxContent(text="final answer"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_store = OutboxReceiptStore(
                Path(temp_dir) / "telegram.outbox-receipts.json"
            )
            receipt_store.save(
                {
                    "delivery-1": OutboxReceiptRecord(
                        status="partial",
                        provider_receipt={
                            "message_id": 101,
                            "chat_id": "88",
                            "messages": [{"message_id": 101, "chat_id": "88"}],
                        },
                    )
                }
            )
            api = FakeLionClawApi()
            telegram = FakeTelegramTransport()
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
                outbox_receipt_store=receipt_store,
            )

            with self.assertLogs("lionclaw_channel_telegram.worker", level="WARNING"):
                await worker._process_outbox_delivery(delivery)

            self.assertEqual(telegram.resume_receipts, [None])
            self.assertEqual(
                telegram.sent_messages,
                [("telegram:user:77", "final answer", None, None, [])],
            )
            self.assertEqual(api.outbox_reports[0][2], "delivered")
            self.assertEqual(receipt_store.load(), {})

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

    async def test_terminal_outbox_failure_terminalizes_visible_progress(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            progress_ref = await self._remember_visible_progress(worker)
            telegram.sent_messages.clear()
            telegram.edited_messages.clear()
            telegram.deleted_messages.clear()
            delivery = OutboxDelivery(
                delivery_id="delivery-1",
                attempt_id="attempt-1",
                conversation_ref="telegram:chat:77",
                turn_id="turn-1",
                content=OutboxContent(text="final answer"),
            )

            telegram.send_message_error = TelegramReferenceError("bad ref")
            with self.assertLogs(
                "lionclaw_channel_telegram.worker",
                level="ERROR",
            ):
                await worker._process_outbox_delivery(delivery)

        self.assertEqual(api.outbox_reports[0][2], "terminal_failed")
        self.assertEqual(api.outbox_reports[0][4], "telegram.invalid_ref")
        self.assertEqual(
            telegram.edited_messages,
            [
                (
                    "telegram:chat:77",
                    progress_ref,
                    "Delivery failed: Telegram destination was invalid.",
                )
            ],
        )
        self.assertEqual(telegram.sent_messages, [])
        self.assertEqual(telegram.deleted_messages, [])
        self.assertNotIn("turn-1", worker._active_turns)
        self.assertEqual(worker._route_turns, {})

    async def test_terminal_outbox_failure_closes_unrendered_progress(self) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await self._record_active_turn(worker)
            delivery = OutboxDelivery(
                delivery_id="delivery-1",
                attempt_id="attempt-1",
                conversation_ref="telegram:chat:77",
                turn_id="turn-1",
                content=OutboxContent(text="final answer"),
            )

            telegram.send_message_error = TelegramReferenceError("bad ref")
            with self.assertLogs(
                "lionclaw_channel_telegram.worker",
                level="ERROR",
            ):
                await worker._process_outbox_delivery(delivery)

        self.assertEqual(api.outbox_reports[0][2], "terminal_failed")
        self.assertEqual(api.outbox_reports[0][4], "telegram.invalid_ref")
        self.assertEqual(telegram.sent_messages, [])
        self.assertEqual(telegram.edited_messages, [])
        self.assertEqual(telegram.deleted_messages, [])
        self.assertNotIn("turn-1", worker._active_turns)
        self.assertEqual(worker._route_turns, {})

    async def test_retryable_outbox_failure_keeps_visible_progress_for_retry(
        self,
    ) -> None:
        api = FakeLionClawApi()
        telegram = FakeTelegramTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = TelegramWorker(
                config=build_config(Path(temp_dir)),
                lionclaw_api=api,
                telegram=telegram,
                offset_store=OffsetStore(Path(temp_dir) / "telegram.offset"),
            )
            await self._remember_visible_progress(worker)
            telegram.sent_messages.clear()
            telegram.edited_messages.clear()
            telegram.deleted_messages.clear()
            delivery = OutboxDelivery(
                delivery_id="delivery-1",
                attempt_id="attempt-1",
                conversation_ref="telegram:chat:77",
                turn_id="turn-1",
                content=OutboxContent(text="final answer"),
            )

            telegram.send_message_error = RuntimeError("temporary send failure")
            with self.assertLogs(
                "lionclaw_channel_telegram.worker",
                level="ERROR",
            ):
                await worker._process_outbox_delivery(delivery)

        self.assertEqual(api.outbox_reports[0][2], "retryable_failed")
        self.assertEqual(api.outbox_reports[0][4], "telegram.send_failed")
        self.assertEqual(telegram.edited_messages, [])
        self.assertEqual(telegram.sent_messages, [])
        self.assertEqual(telegram.deleted_messages, [])
        self.assertIn("turn-1", worker._active_turns)

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

        outcome, error_code = _classify_send_failure(
            FileNotFoundError("missing artifact")
        )

        self.assertEqual(outcome, "terminal_failed")
        self.assertEqual(error_code, "telegram.attachment_unreadable")

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
        chunks = _split_telegram_text("x" * (TELEGRAM_TEXT_LIMIT + 1))

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0], "x" * TELEGRAM_TEXT_LIMIT)
        self.assertEqual(chunks[1], "x")
        self.assertTrue(
            all(_utf16_len(chunk) <= TELEGRAM_TEXT_LIMIT for chunk in chunks)
        )

    def test_long_whitespace_does_not_create_empty_telegram_chunks(self) -> None:
        self.assertEqual(_split_telegram_text((" " * 4001) + "answer"), ["answer"])
        self.assertEqual(_split_telegram_text(" " * 4001), [])

    def test_long_answer_split_preserves_meaningful_whitespace(self) -> None:
        text = ("x" * (TELEGRAM_TEXT_LIMIT - 5)) + "\n    indented result"

        chunks = _split_telegram_text(text)

        self.assertEqual("".join(chunks), text)
        self.assertEqual(len(chunks), 2)
        self.assertTrue(chunks[1].startswith("    indented"))
        self.assertTrue(
            all(_utf16_len(chunk) <= TELEGRAM_TEXT_LIMIT for chunk in chunks)
        )

    def test_topic_thread_ref_omits_general_topic_on_send(self) -> None:
        self.assertEqual(_coerce_thread_id("telegram:topic:77", omit_general=True), 77)
        self.assertIsNone(_coerce_thread_id("telegram:topic:1", omit_general=True))

    def test_topic_thread_ref_preserves_general_topic_on_typing(self) -> None:
        self.assertEqual(_coerce_thread_id("telegram:topic:1", omit_general=False), 1)

    def test_malformed_refs_raise_reference_error(self) -> None:
        with self.assertRaisesRegex(TelegramReferenceError, "conversation_ref"):
            _coerce_chat_id(77)  # type: ignore[arg-type]

        with self.assertRaisesRegex(TelegramReferenceError, "message_ref"):
            _coerce_message_id(77)  # type: ignore[arg-type]

        with self.assertRaisesRegex(TelegramReferenceError, "thread_ref"):
            _coerce_thread_id(77, omit_general=True)  # type: ignore[arg-type]

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

    def test_markdown_rendering_preserves_parenthesized_links(self) -> None:
        rendered = _markdown_to_telegram_html(
            "[Rust](https://en.wikipedia.org/wiki/Rust_(programming_language))"
        )

        self.assertEqual(
            rendered,
            '<a href="https://en.wikipedia.org/wiki/Rust_(programming_language)">'
            "Rust</a>",
        )

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
        self.assertTrue(_utf16_len(chunks[0].plain_text) <= TELEGRAM_TEXT_LIMIT)

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


class AiogramTelegramTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_send_refs_raise_before_provider_call(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        with self.assertRaisesRegex(TelegramReferenceError, "thread_ref"):
            await transport.send_message(
                "telegram:chat:77",
                "answer",
                thread_ref=77,  # type: ignore[arg-type]
            )

        self.assertEqual(bot.sent_messages, [])
        self.assertEqual(bot.sent_documents, [])

    async def test_send_message_attaches_inline_buttons_to_first_message(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        await transport.send_message(
            "telegram:chat:77",
            "Working...",
            buttons=[
                TelegramActionButton("Stop", "lc:v1:stop:turn-1"),
                TelegramActionButton("Status", "lc:v1:status:turn-1"),
            ],
        )

        markup = bot.sent_messages[0]["reply_markup"]
        self.assertEqual(len(markup.inline_keyboard), 1)
        self.assertEqual(markup.inline_keyboard[0][0].text, "Stop")
        self.assertEqual(
            markup.inline_keyboard[0][0].callback_data,
            "lc:v1:stop:turn-1",
        )
        self.assertEqual(markup.inline_keyboard[0][1].text, "Status")

    async def test_invalid_inline_button_action_is_rejected_before_send(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        with self.assertRaisesRegex(TelegramReferenceError, "64 bytes"):
            await transport.send_message(
                "telegram:chat:77",
                "Working...",
                buttons=[TelegramActionButton("Stop", "x" * 65)],
            )

        self.assertEqual(bot.sent_messages, [])

    async def test_markdown_attachment_text_uses_message_when_html_caption_too_long(
        self,
    ) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        await transport.send_message(
            "telegram:chat:77",
            "<" * 300,
            format_hint="markdown",
            attachments=[
                TelegramOutboundAttachment(
                    path="/tmp/report.txt",
                    filename="report.txt",
                    mime_type="text/plain",
                )
            ],
        )

        self.assertEqual(len(bot.sent_messages), 1)
        self.assertEqual(bot.sent_messages[0]["parse_mode"], "HTML")
        self.assertEqual(bot.sent_messages[0]["text"], "&lt;" * 300)
        self.assertEqual(len(bot.sent_documents), 1)
        self.assertNotIn("caption", bot.sent_documents[0])

    async def test_unsupported_image_mime_type_is_sent_as_document(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        await transport.send_message(
            "telegram:chat:77",
            "",
            attachments=[
                TelegramOutboundAttachment(
                    path="/tmp/diagram.svg",
                    filename="diagram.svg",
                    mime_type="image/svg+xml",
                )
            ],
        )

        self.assertEqual(bot.sent_photos, [])
        self.assertEqual(bot.photo_attempts, [])
        self.assertEqual(len(bot.sent_documents), 1)
        self.assertEqual(bot.sent_documents[0]["chat_id"], 77)

    async def test_compatible_attachments_are_sent_as_native_album(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        receipt = await transport.send_message(
            "telegram:chat:77",
            "album caption",
            attachments=[
                TelegramOutboundAttachment(
                    path="/tmp/a.png",
                    filename="a.png",
                    mime_type="image/png",
                ),
                TelegramOutboundAttachment(
                    path="/tmp/b.mp4",
                    filename="b.mp4",
                    mime_type="video/mp4",
                ),
            ],
        )

        self.assertEqual(len(bot.sent_media_groups), 1)
        media = bot.sent_media_groups[0]["media"]
        self.assertEqual(
            [item.__class__.__name__ for item in media],
            ["InputMediaPhoto", "InputMediaVideo"],
        )
        self.assertEqual(media[0].caption, "album caption")
        self.assertEqual(bot.sent_photos, [])
        self.assertEqual(bot.sent_videos, [])
        self.assertEqual(receipt["message_id"], 102)
        self.assertEqual(len(receipt["messages"]), 2)

    async def test_album_bad_request_falls_back_to_serial_attachments(self) -> None:
        bot = RecordingAiogramBot(
            send_media_group_error=TelegramBadRequest(
                method=object(),
                message="MEDIA_GROUP_INVALID",
            )
        )
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        receipt = await transport.send_message(
            "telegram:chat:77",
            "album caption",
            attachments=[
                TelegramOutboundAttachment(
                    path="/tmp/a.png",
                    filename="a.png",
                    mime_type="image/png",
                ),
                TelegramOutboundAttachment(
                    path="/tmp/b.png",
                    filename="b.png",
                    mime_type="image/png",
                ),
            ],
        )

        self.assertEqual(len(bot.sent_media_groups), 1)
        self.assertEqual(len(bot.sent_photos), 2)
        self.assertEqual(bot.sent_photos[0]["caption"], "album caption")
        self.assertEqual(bot.sent_documents, [])
        self.assertEqual(receipt["message_id"], 102)
        self.assertEqual(len(receipt["messages"]), 2)

    async def test_buttons_force_serial_attachment_send_instead_of_album(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        await transport.send_message(
            "telegram:chat:77",
            "album caption",
            buttons=[TelegramActionButton("Stop", "lc:v1:stop:turn-1")],
            attachments=[
                TelegramOutboundAttachment(
                    path="/tmp/a.png",
                    filename="a.png",
                    mime_type="image/png",
                ),
                TelegramOutboundAttachment(
                    path="/tmp/b.png",
                    filename="b.png",
                    mime_type="image/png",
                ),
            ],
        )

        self.assertEqual(bot.sent_media_groups, [])
        self.assertEqual(len(bot.sent_photos), 2)
        self.assertIsNotNone(bot.sent_photos[0]["reply_markup"])

    async def test_resume_receipt_skips_already_sent_text_chunks(self) -> None:
        bot = RecordingAiogramBot()
        bot._next_message_id = 101
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        receipt = await transport.send_message(
            "telegram:chat:77",
            "x" * (TELEGRAM_TEXT_LIMIT + 1),
            resume_receipt={
                "message_id": 101,
                "chat_id": "77",
                "messages": [{"message_id": 101, "chat_id": "77"}],
            },
        )

        self.assertEqual(len(bot.sent_messages), 1)
        self.assertEqual(bot.sent_messages[0]["text"], "x")
        reply = bot.sent_messages[0]["reply_parameters"]
        self.assertEqual(reply.message_id, 101)
        self.assertEqual(receipt["message_id"], 102)
        self.assertEqual(len(receipt["messages"]), 2)

    async def test_resume_receipt_rejects_impossible_message_count(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        with self.assertRaisesRegex(TelegramReferenceError, "resume receipt"):
            await transport.send_message(
                "telegram:chat:77",
                "short answer",
                resume_receipt={
                    "message_id": 102,
                    "chat_id": "77",
                    "messages": [
                        {"message_id": 101, "chat_id": "77"},
                        {"message_id": 102, "chat_id": "77"},
                    ],
                },
            )

        self.assertEqual(bot.sent_messages, [])

    async def test_resume_receipt_rejects_invalid_message_id(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        with self.assertRaisesRegex(TelegramReferenceError, "message_id"):
            await transport.send_message(
                "telegram:chat:77",
                "short answer",
                resume_receipt={
                    "message_id": "not-a-message-id",
                    "chat_id": "77",
                    "messages": [{"message_id": "not-a-message-id", "chat_id": "77"}],
                },
            )

        self.assertEqual(bot.sent_messages, [])

    async def test_resume_receipt_rejects_other_chat(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        with self.assertRaisesRegex(TelegramReferenceError, "chat_id"):
            await transport.send_message(
                "telegram:chat:77",
                "short answer",
                resume_receipt={
                    "message_id": 101,
                    "chat_id": "88",
                    "messages": [{"message_id": 101, "chat_id": "88"}],
                },
            )

        self.assertEqual(bot.sent_messages, [])

    async def test_partial_send_error_carries_resume_receipt(self) -> None:
        bot = RecordingAiogramBot(
            send_message_errors=[None, RuntimeError("network dropped")]
        )
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        with self.assertRaises(TelegramPartialSendError) as raised:
            await transport.send_message(
                "telegram:chat:77",
                "x" * (TELEGRAM_TEXT_LIMIT + 1),
            )

        self.assertEqual(len(bot.sent_messages), 1)
        self.assertEqual(
            raised.exception.receipt,
            {
                "message_id": 101,
                "chat_id": "77",
                "messages": [{"message_id": 101, "chat_id": "77"}],
            },
        )
        self.assertIsInstance(raised.exception.cause, RuntimeError)

    async def test_native_photo_bad_request_falls_back_to_document(self) -> None:
        bot = RecordingAiogramBot(
            send_photo_error=TelegramBadRequest(
                method=object(),
                message="PHOTO_INVALID_DIMENSIONS",
            )
        )
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        await transport.send_message(
            "telegram:chat:77",
            "diagram",
            attachments=[
                TelegramOutboundAttachment(
                    path="/tmp/diagram.png",
                    filename="diagram.png",
                    mime_type="image/png",
                )
            ],
        )

        self.assertEqual(len(bot.photo_attempts), 1)
        self.assertEqual(bot.sent_photos, [])
        self.assertEqual(len(bot.sent_documents), 1)
        self.assertEqual(bot.sent_documents[0]["caption"], "diagram")

    async def test_callback_answer_and_reaction_use_telegram_methods(self) -> None:
        bot = RecordingAiogramBot()
        transport = object.__new__(AiogramTelegramTransport)
        transport._bot = bot
        transport._bot_identity = None

        await transport.answer_callback("callback-1", "Stopping")
        await transport.set_reaction("telegram:chat:77", "telegram:message:44", "👍")

        self.assertEqual(
            bot.answered_callbacks,
            [{"callback_query_id": "callback-1", "text": "Stopping"}],
        )
        self.assertEqual(bot.reactions[0]["chat_id"], 77)
        self.assertEqual(bot.reactions[0]["message_id"], 44)
        self.assertEqual(bot.reactions[0]["reaction"][0].emoji, "👍")


class TelegramWebhookServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_webhook_start_cleans_runner_when_site_start_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:

            async def handle(update: Update) -> bool:
                return True

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            start_error = RuntimeError("bind failed")
            cleanup = AsyncMock()

            with (
                patch(
                    "lionclaw_channel_telegram.webhook.web.TCPSite.start",
                    AsyncMock(side_effect=start_error),
                ),
                patch(
                    "lionclaw_channel_telegram.webhook.web.AppRunner.cleanup",
                    cleanup,
                ),
                self.assertRaisesRegex(RuntimeError, "bind failed"),
            ):
                await server.start()

        cleanup.assert_awaited_once()
        self.assertIsNone(server._runner)
        self.assertIsNone(server._site)
        self.assertIsNone(server.bound_host)
        self.assertIsNone(server.bound_port)

    async def test_webhook_close_clears_state_when_cleanup_fails(self) -> None:
        class FailingRunner:
            async def cleanup(self) -> None:
                raise RuntimeError("cleanup failed")

        with tempfile.TemporaryDirectory() as temp_dir:

            async def handle(update: Update) -> bool:
                return True

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            server._runner = FailingRunner()
            server._site = object()
            server.bound_host = "127.0.0.1"
            server.bound_port = 8080

            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                await server.close()

        self.assertIsNone(server._runner)
        self.assertIsNone(server._site)
        self.assertIsNone(server.bound_host)
        self.assertIsNone(server.bound_port)

    async def test_webhook_rejects_missing_secret_token(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            handled: list[Update] = []

            async def handle(update: Update) -> bool:
                handled.append(update)
                return True

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            await server.start()
            try:
                response = await _post_webhook_update(server, headers={})
            finally:
                await server.close()

        self.assertEqual(response.status_code, 401)
        self.assertEqual(handled, [])

    async def test_webhook_rejects_non_ascii_secret_token_without_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:

            async def handle(update: Update) -> bool:
                _ = update
                return True

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            request = type(
                "Request",
                (),
                {"headers": {TELEGRAM_SECRET_TOKEN_HEADER: "secrét"}},
            )()

            self.assertFalse(server._secret_token_matches(request))

    async def test_webhook_rejects_duplicate_secret_token_headers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:

            async def handle(update: Update) -> bool:
                _ = update
                return True

            class DuplicateSecretHeaders:
                def get(self, name: str, default: object = None) -> object:
                    if name == TELEGRAM_SECRET_TOKEN_HEADER:
                        return "secret"
                    return default

                def getall(self, name: str, default: object = None) -> object:
                    if name == TELEGRAM_SECRET_TOKEN_HEADER:
                        return ["secret", "secret"]
                    return default

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            request = type(
                "Request",
                (),
                {"headers": DuplicateSecretHeaders()},
            )()

            self.assertFalse(server._secret_token_matches(request))

    async def test_webhook_accepts_valid_secret_token(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            handled: list[Update] = []

            async def handle(update: Update) -> bool:
                handled.append(update)
                return True

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            await server.start()
            try:
                response = await _post_webhook_update(
                    server,
                    headers={TELEGRAM_SECRET_TOKEN_HEADER: "secret"},
                )
            finally:
                await server.close()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        self.assertEqual([update.update_id for update in handled], [91])

    async def test_webhook_returns_retryable_status_for_processing_failure(
        self,
    ) -> None:
        for failure in (False, RuntimeError("handler failed")):
            with self.subTest(failure=type(failure).__name__):
                response = await self._post_with_handler_result(failure)

            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["error"], "temporary_failure")

    async def _post_with_handler_result(
        self,
        failure: bool | Exception,
    ) -> httpx.Response:
        with tempfile.TemporaryDirectory() as temp_dir:

            async def handle(update: Update) -> bool:
                _ = update
                if isinstance(failure, Exception):
                    raise failure
                return failure

            server = TelegramWebhookServer(
                replace(
                    build_config(Path(temp_dir)),
                    telegram_update_mode="webhook",
                    telegram_webhook_port=0,
                    telegram_webhook_path="/hook",
                    telegram_webhook_secret_token="secret",
                ),
                handle,
            )
            await server.start()
            try:
                if isinstance(failure, Exception):
                    with self.assertLogs(
                        "lionclaw_channel_telegram.webhook",
                        level="ERROR",
                    ):
                        response = await _post_webhook_update(
                            server,
                            headers={TELEGRAM_SECRET_TOKEN_HEADER: "secret"},
                        )
                else:
                    response = await _post_webhook_update(
                        server,
                        headers={TELEGRAM_SECRET_TOKEN_HEADER: "secret"},
                    )
            finally:
                await server.close()
        return response


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
        telegram_update_mode="polling",
        telegram_webhook_host="127.0.0.1",
        telegram_webhook_port=8080,
        telegram_webhook_path="/telegram/webhook",
        telegram_webhook_secret_token=None,
        telegram_webhook_max_body_bytes=1024 * 1024,
        health_report_interval_secs=60.0,
        runtime_dir=runtime_dir,
        telegram_offset_file=runtime_dir / "telegram.offset",
    )


async def _post_webhook_update(
    server: TelegramWebhookServer,
    *,
    headers: dict[str, str],
) -> httpx.Response:
    assert server.bound_port is not None
    async with httpx.AsyncClient() as client:
        return await client.post(
            f"http://127.0.0.1:{server.bound_port}/hook",
            headers=headers,
            json={
                "update_id": 91,
                "message": {
                    "message_id": 19,
                    "date": 0,
                    "chat": {"id": 77, "type": "private"},
                    "from": {
                        "id": 77,
                        "is_bot": False,
                        "first_name": "Alice",
                    },
                    "text": "webhook hello",
                },
            },
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
        outbox_report_errors: list[Exception] | None = None,
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
        self.outbox_report_errors = list(outbox_report_errors or [])
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
        if self.outbox_report_errors:
            raise self.outbox_report_errors.pop(0)
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


class RecordingAiogramChat:
    def __init__(self, chat_id: int | str) -> None:
        self.id = chat_id


class RecordingAiogramMessage:
    def __init__(self, message_id: int, chat_id: int | str) -> None:
        self.message_id = message_id
        self.chat = RecordingAiogramChat(chat_id)


class RecordingAiogramBot:
    def __init__(
        self,
        *,
        send_photo_error: Exception | None = None,
        send_media_group_error: Exception | None = None,
        send_message_errors: list[Exception | None] | None = None,
    ) -> None:
        self._next_message_id = 100
        self.sent_messages: list[dict[str, object]] = []
        self.photo_attempts: list[dict[str, object]] = []
        self.sent_photos: list[dict[str, object]] = []
        self.sent_videos: list[dict[str, object]] = []
        self.sent_audios: list[dict[str, object]] = []
        self.sent_voices: list[dict[str, object]] = []
        self.sent_documents: list[dict[str, object]] = []
        self.sent_media_groups: list[dict[str, object]] = []
        self.sent_chat_actions: list[dict[str, object]] = []
        self.answered_callbacks: list[dict[str, object]] = []
        self.reactions: list[dict[str, object]] = []
        self.send_photo_error = send_photo_error
        self.send_media_group_error = send_media_group_error
        self.send_message_errors = list(send_message_errors or [])

    async def send_message(self, **params) -> RecordingAiogramMessage:
        if self.send_message_errors:
            error = self.send_message_errors.pop(0)
            if error is not None:
                raise error
        self.sent_messages.append(dict(params))
        return self._message_for(params["chat_id"])

    async def send_chat_action(self, **params) -> None:
        self.sent_chat_actions.append(dict(params))

    async def send_photo(
        self,
        *,
        photo,
        **params,
    ) -> RecordingAiogramMessage:
        self.photo_attempts.append({**params, "photo": photo})
        if self.send_photo_error is not None:
            raise self.send_photo_error
        self.sent_photos.append({**params, "photo": photo})
        return self._message_for(params["chat_id"])

    async def send_video(
        self,
        *,
        video,
        **params,
    ) -> RecordingAiogramMessage:
        self.sent_videos.append({**params, "video": video})
        return self._message_for(params["chat_id"])

    async def send_audio(
        self,
        *,
        audio,
        **params,
    ) -> RecordingAiogramMessage:
        self.sent_audios.append({**params, "audio": audio})
        return self._message_for(params["chat_id"])

    async def send_voice(
        self,
        *,
        voice,
        **params,
    ) -> RecordingAiogramMessage:
        self.sent_voices.append({**params, "voice": voice})
        return self._message_for(params["chat_id"])

    async def send_document(
        self,
        *,
        document,
        **params,
    ) -> RecordingAiogramMessage:
        self.sent_documents.append({**params, "document": document})
        return self._message_for(params["chat_id"])

    async def send_media_group(
        self,
        *,
        media,
        **params,
    ) -> list[RecordingAiogramMessage]:
        self.sent_media_groups.append({**params, "media": media})
        if self.send_media_group_error is not None:
            raise self.send_media_group_error
        return [self._message_for(params["chat_id"]) for _ in media]

    async def edit_message_text(self, **params) -> None:
        self.sent_messages.append(dict(params))

    async def answer_callback_query(self, **params) -> None:
        self.answered_callbacks.append(dict(params))

    async def set_message_reaction(self, **params) -> None:
        self.reactions.append(dict(params))

    def _message_for(self, chat_id: int | str) -> RecordingAiogramMessage:
        self._next_message_id += 1
        return RecordingAiogramMessage(self._next_message_id, chat_id)


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
        delete_errors: list[Exception] | None = None,
        send_message_error: Exception | None = None,
        partial_send_error: Exception | None = None,
        partial_send_receipt: dict[str, object] | None = None,
    ) -> None:
        self.updates = list(updates or [])
        self.sent_messages: list[tuple[str, str, str | None, str | None, list[str]]] = (
            []
        )
        self.sent_buttons: list[list[TelegramActionButton]] = []
        self.resume_receipts: list[dict[str, object] | None] = []
        self.edited_messages: list[tuple[str, str, str]] = []
        self.edited_buttons: list[list[TelegramActionButton]] = []
        self.deleted_messages: list[tuple[str, str]] = []
        self.answered_callbacks: list[tuple[str, str | None]] = []
        self.reactions: list[tuple[str, str, str]] = []
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
        self.delete_errors = list(delete_errors or [])
        self.send_message_error = send_message_error
        self.partial_send_error = partial_send_error
        self.partial_send_receipt = partial_send_receipt

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
        resume_receipt: dict[str, object] | None = None,
        buttons: Sequence[TelegramActionButton] = (),
    ) -> dict[str, object]:
        self.sent_buttons.append(list(buttons))
        self.resume_receipts.append(resume_receipt)
        if self.send_message_error is not None:
            raise self.send_message_error
        if self.fail_send_message:
            raise RuntimeError("send failed")
        if self.partial_send_error is not None:
            raise TelegramPartialSendError(
                receipt=self.partial_send_receipt
                or {
                    "message_id": 101,
                    "chat_id": conversation_ref,
                    "messages": [{"message_id": 101, "chat_id": conversation_ref}],
                },
                cause=self.partial_send_error,
            )
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
        buttons: Sequence[TelegramActionButton] = (),
    ) -> None:
        self.edited_buttons.append(list(buttons))
        if self.edit_errors:
            raise self.edit_errors.pop(0)
        self.edited_messages.append((conversation_ref, message_ref, text))

    async def delete_message(
        self,
        conversation_ref: str,
        message_ref: str,
    ) -> None:
        if self.delete_errors:
            raise self.delete_errors.pop(0)
        self.deleted_messages.append((conversation_ref, message_ref))

    async def answer_callback(
        self,
        callback_query_id: str,
        text: str | None = None,
    ) -> None:
        self.answered_callbacks.append((callback_query_id, text))

    async def set_reaction(
        self,
        conversation_ref: str,
        message_ref: str,
        emoji: str,
    ) -> None:
        self.reactions.append((conversation_ref, message_ref, emoji))


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
