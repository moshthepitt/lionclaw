from __future__ import annotations

import contextlib
import html
import re
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol

from aiogram import Bot
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    BotCommand,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    LinkPreviewOptions,
    Message,
    MessageEntity,
    PhotoSize,
    ReactionTypeEmoji,
    ReplyParameters,
    Update,
)

TELEGRAM_TEXT_LIMIT = 4000
TELEGRAM_CAPTION_LIMIT = 1024
PAIRING_START_RE = re.compile(
    r"^/(?:start|startgroup)"
    r"(?:@([A-Za-z0-9_]+))?"
    r"\s+"
    r"(lc_[A-Za-z0-9_-]{8,128})"
    r"\s*$"
)
TELEGRAM_PARSE_ERROR_RE = re.compile(
    r"can't parse entities|parse entities|entity", re.IGNORECASE
)
LOCAL_LINK_RE = re.compile(r"^(?:/|file:|\.{0,2}/|[A-Za-z]:[\\/])")
SUPPORTED_LINK_RE = re.compile(r"^(?:https?://|tg://|mailto:)", re.IGNORECASE)
FILE_REFERENCE_RE = re.compile(r"^[A-Za-z0-9_.@-]+\.[A-Za-z0-9][A-Za-z0-9_.-]*$")
PHOTO_MIME_TYPES = {"image/jpeg", "image/png", "image/webp"}
VIDEO_MIME_TYPES = {"video/mp4"}
VOICE_MIME_TYPES = {"audio/ogg"}
AUDIO_MIME_TYPES = {
    "audio/aac",
    "audio/flac",
    "audio/mp4",
    "audio/mpeg",
    "audio/wav",
    "audio/x-m4a",
    "audio/x-wav",
}


class TelegramReferenceError(ValueError):
    pass


class TelegramPartialSendError(RuntimeError):
    def __init__(
        self,
        *,
        receipt: dict[str, Any],
        cause: Exception,
    ) -> None:
        super().__init__(str(cause))
        self.receipt = receipt
        self.cause = cause


@dataclass(slots=True, frozen=True)
class TelegramInboundAttachment:
    attachment_id: str
    kind: str
    provider_file_ref: str
    mime_type: str | None = None
    filename: str | None = None
    size_bytes: int | None = None
    caption: str | None = None


@dataclass(slots=True, frozen=True)
class TelegramBotIdentity:
    user_id: int | None
    username: str | None = None


@dataclass(slots=True, frozen=True)
class TelegramInboundUpdate:
    update_id: int
    event_id: str
    sender_ref: str
    conversation_ref: str
    message_ref: str | None
    text: str | None
    trigger: str
    thread_ref: str | None = None
    reply_to_ref: str | None = None
    attachments: list[TelegramInboundAttachment] = field(default_factory=list)
    provider_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class TelegramPairingClaim:
    token: str
    sender_ref: str
    conversation_ref: str
    update_id: int
    message_ref: str | None = None
    thread_ref: str | None = None
    provider_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class TelegramCallbackAction:
    update_id: int
    callback_query_id: str
    action: str
    sender_ref: str
    conversation_ref: str
    message_ref: str | None
    thread_ref: str | None = None
    provider_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class TelegramDownloadedAttachment:
    attachment: TelegramInboundAttachment
    content: bytes
    filename: str | None = None
    mime_type: str | None = None


@dataclass(slots=True, frozen=True)
class TelegramOutboundAttachment:
    path: str
    filename: str | None = None
    mime_type: str | None = None


@dataclass(slots=True, frozen=True)
class TelegramActionButton:
    text: str
    action: str


@dataclass(slots=True, frozen=True)
class TelegramTextChunk:
    plain_text: str
    html_text: str | None = None


TelegramInboundEvent = (
    TelegramInboundUpdate | TelegramPairingClaim | TelegramCallbackAction
)


class TelegramTransport(Protocol):
    async def close(self) -> None: ...

    async def bot_identity(self, *, refresh: bool = False) -> TelegramBotIdentity: ...

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]: ...

    async def configure_commands(self) -> None: ...

    async def download_attachment(
        self,
        attachment: TelegramInboundAttachment,
        max_bytes: int,
    ) -> TelegramDownloadedAttachment: ...

    async def send_message(
        self,
        conversation_ref: str,
        text: str,
        reply_to_ref: str | None = None,
        thread_ref: str | None = None,
        format_hint: str = "plain",
        attachments: Sequence[TelegramOutboundAttachment] = (),
        resume_receipt: dict[str, Any] | None = None,
        buttons: Sequence[TelegramActionButton] = (),
    ) -> dict[str, Any]: ...

    async def send_typing(
        self,
        conversation_ref: str,
        thread_ref: str | None = None,
    ) -> None: ...

    async def edit_message(
        self,
        conversation_ref: str,
        message_ref: str,
        text: str,
        *,
        format_hint: str = "plain",
        buttons: Sequence[TelegramActionButton] = (),
    ) -> None: ...

    async def delete_message(
        self,
        conversation_ref: str,
        message_ref: str,
    ) -> None: ...

    async def answer_callback(
        self,
        callback_query_id: str,
        text: str | None = None,
    ) -> None: ...

    async def set_reaction(
        self,
        conversation_ref: str,
        message_ref: str,
        emoji: str,
    ) -> None: ...


class AiogramTelegramTransport:
    def __init__(self, bot_token: str) -> None:
        self._bot = Bot(bot_token)
        self._bot_identity: TelegramBotIdentity | None = None

    async def close(self) -> None:
        await self._bot.session.close()

    async def bot_identity(self, *, refresh: bool = False) -> TelegramBotIdentity:
        if refresh or self._bot_identity is None:
            me = await self._bot.get_me()
            self._bot_identity = TelegramBotIdentity(
                user_id=me.id, username=me.username
            )
        return self._bot_identity

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]:
        return await self._bot.get_updates(
            offset=offset,
            timeout=timeout_seconds,
            allowed_updates=[
                "message",
                "edited_message",
                "channel_post",
                "edited_channel_post",
                "callback_query",
            ],
        )

    async def configure_commands(self) -> None:
        await self._bot.set_my_commands(
            [
                BotCommand(command="help", description="Show LionClaw controls"),
                BotCommand(command="status", description="Show current turn status"),
                BotCommand(command="new", description="Start a fresh session"),
                BotCommand(command="stop", description="Stop the active turn"),
                BotCommand(command="retry", description="Retry the last turn"),
                BotCommand(command="continue", description="Continue a partial turn"),
                BotCommand(command="model", description="Open runtime model controls"),
                BotCommand(command="settings", description="Show Telegram settings"),
            ]
        )

    async def download_attachment(
        self,
        attachment: TelegramInboundAttachment,
        max_bytes: int,
    ) -> TelegramDownloadedAttachment:
        telegram_file = await self._bot.get_file(attachment.provider_file_ref)
        file_size = telegram_file.file_size or attachment.size_bytes
        if file_size is not None and file_size > max_bytes:
            raise TelegramEntityTooLargeForStage(
                f"telegram file {attachment.attachment_id} is too large: "
                f"{file_size} bytes"
            )
        if telegram_file.file_path is None:
            raise RuntimeError("telegram getFile response did not include file_path")
        downloaded = await self._bot.download_file(telegram_file.file_path)
        content = downloaded.read()
        if len(content) > max_bytes:
            raise TelegramEntityTooLargeForStage(
                f"telegram file {attachment.attachment_id} is too large: "
                f"{len(content)} bytes"
            )
        return TelegramDownloadedAttachment(
            attachment=attachment,
            content=content,
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
        resume_receipt: dict[str, Any] | None = None,
        buttons: Sequence[TelegramActionButton] = (),
    ) -> dict[str, Any]:
        chat_id = _coerce_chat_id(conversation_ref)
        reply_parameters = _reply_parameters(reply_to_ref)
        message_thread_id = _coerce_thread_id(thread_ref, omit_general=True)
        reply_markup = _inline_keyboard(buttons)
        sent_messages = _receipt_messages(resume_receipt, chat_id=chat_id)
        resume_count = len(sent_messages)
        if sent_messages:
            reply_parameters = _reply_parameters(str(sent_messages[-1]["message_id"]))
        text_chunks = _format_telegram_text_chunks(text, format_hint)
        caption_chunk: TelegramTextChunk | None = None
        if (
            attachments
            and len(text_chunks) == 1
            and _fits_telegram_caption(text_chunks[0])
        ):
            caption_chunk = text_chunks[0]
            text_chunks = []
        if (
            not resume_receipt
            and not text_chunks
            and not buttons
            and _can_send_native_album(attachments)
        ):
            messages = await self._send_media_group(
                chat_id=chat_id,
                attachments=attachments,
                reply_parameters=reply_parameters,
                message_thread_id=message_thread_id,
                caption=caption_chunk,
            )
            return _receipt_from_messages(
                [_message_receipt(message) for message in messages]
            )
        operation_count = len(text_chunks) + len(attachments)
        if operation_count == 0:
            operation_count = 1
        if resume_count > operation_count:
            raise TelegramReferenceError(
                "telegram resume receipt has more messages than planned sends"
            )

        operation_index = 0
        try:
            for chunk in text_chunks:
                if operation_index < resume_count:
                    operation_index += 1
                    continue
                message = await self._send_text_chunk(
                    chat_id=chat_id,
                    chunk=chunk,
                    reply_parameters=reply_parameters,
                    message_thread_id=message_thread_id,
                    reply_markup=reply_markup if operation_index == 0 else None,
                )
                sent_messages.append(_message_receipt(message))
                reply_parameters = _reply_parameters(str(message.message_id))
                operation_index += 1

            for index, attachment in enumerate(attachments):
                if operation_index < resume_count:
                    operation_index += 1
                    continue
                message = await self._send_attachment(
                    chat_id=chat_id,
                    attachment=attachment,
                    reply_parameters=reply_parameters,
                    message_thread_id=message_thread_id,
                    caption=caption_chunk if index == 0 else None,
                    reply_markup=reply_markup if operation_index == 0 else None,
                )
                sent_messages.append(_message_receipt(message))
                reply_parameters = _reply_parameters(str(message.message_id))
                operation_index += 1

            if not sent_messages:
                message = await self._bot.send_message(
                    chat_id=chat_id,
                    text=" ",
                    reply_parameters=reply_parameters,
                    message_thread_id=message_thread_id,
                    reply_markup=reply_markup,
                )
                sent_messages.append(_message_receipt(message))
        except Exception as err:
            if sent_messages:
                raise TelegramPartialSendError(
                    receipt=_receipt_from_messages(sent_messages),
                    cause=err,
                ) from err
            raise

        return _receipt_from_messages(sent_messages)

    async def _send_text_chunk(
        self,
        *,
        chat_id: int | str,
        chunk: TelegramTextChunk,
        reply_parameters: ReplyParameters | None,
        message_thread_id: int | None,
        reply_markup: InlineKeyboardMarkup | None,
    ) -> Message:
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "reply_parameters": reply_parameters,
            "message_thread_id": message_thread_id,
            "link_preview_options": LinkPreviewOptions(is_disabled=True),
            "reply_markup": reply_markup,
        }
        if chunk.html_text is not None:
            try:
                return await self._bot.send_message(
                    text=chunk.html_text,
                    parse_mode="HTML",
                    **params,
                )
            except TelegramBadRequest as err:
                if not _is_parse_error(err):
                    raise
        return await self._bot.send_message(text=chunk.plain_text, **params)

    async def _send_attachment(
        self,
        *,
        chat_id: int | str,
        attachment: TelegramOutboundAttachment,
        reply_parameters: ReplyParameters | None,
        message_thread_id: int | None,
        caption: TelegramTextChunk | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> Message:
        path = Path(attachment.path)
        file = FSInputFile(path, filename=attachment.filename or path.name)
        common: dict[str, Any] = {
            "chat_id": chat_id,
            "reply_parameters": reply_parameters,
            "message_thread_id": message_thread_id,
            "reply_markup": reply_markup,
        }
        mime_type = attachment.mime_type or ""
        await self._send_upload_action(chat_id, mime_type, message_thread_id)
        if caption is not None and caption.html_text is not None:
            try:
                return await self._send_attachment_once(
                    file=file,
                    mime_type=mime_type,
                    params={
                        **common,
                        "caption": caption.html_text,
                        "parse_mode": "HTML",
                    },
                )
            except TelegramBadRequest as err:
                if not _is_parse_error(err):
                    raise
        params = common
        if caption is not None:
            params = {**common, "caption": caption.plain_text}
        return await self._send_attachment_once(
            file=file, mime_type=mime_type, params=params
        )

    async def _send_media_group(
        self,
        *,
        chat_id: int | str,
        attachments: Sequence[TelegramOutboundAttachment],
        reply_parameters: ReplyParameters | None,
        message_thread_id: int | None,
        caption: TelegramTextChunk | None = None,
    ) -> list[Message]:
        dominant_mime_type = attachments[0].mime_type or ""
        await self._send_upload_action(chat_id, dominant_mime_type, message_thread_id)
        html_caption = caption is not None and caption.html_text is not None
        try:
            return await self._bot.send_media_group(
                chat_id=chat_id,
                media=_album_media(attachments, caption=caption, use_html=True),
                reply_parameters=reply_parameters,
                message_thread_id=message_thread_id,
            )
        except TelegramBadRequest as err:
            if not html_caption or not _is_parse_error(err):
                raise
        return await self._bot.send_media_group(
            chat_id=chat_id,
            media=_album_media(attachments, caption=caption, use_html=False),
            reply_parameters=reply_parameters,
            message_thread_id=message_thread_id,
        )

    async def _send_attachment_once(
        self,
        *,
        file: FSInputFile,
        mime_type: str,
        params: dict[str, Any],
    ) -> Message:
        normalized_mime_type = _normalize_mime_type(mime_type)
        try:
            if normalized_mime_type in PHOTO_MIME_TYPES:
                return await self._bot.send_photo(photo=file, **params)
            if normalized_mime_type in VIDEO_MIME_TYPES:
                return await self._bot.send_video(video=file, **params)
            if normalized_mime_type in VOICE_MIME_TYPES:
                return await self._bot.send_voice(voice=file, **params)
            if normalized_mime_type in AUDIO_MIME_TYPES:
                return await self._bot.send_audio(audio=file, **params)
        except TelegramBadRequest as err:
            if _is_parse_error(err):
                raise
        return await self._bot.send_document(document=file, **params)

    async def _send_upload_action(
        self,
        chat_id: int | str,
        mime_type: str,
        message_thread_id: int | None,
    ) -> None:
        with contextlib.suppress(Exception):
            await self._bot.send_chat_action(
                chat_id=chat_id,
                action=_chat_action_for_mime_type(mime_type),
                message_thread_id=message_thread_id,
            )

    async def send_typing(
        self,
        conversation_ref: str,
        thread_ref: str | None = None,
    ) -> None:
        await self._bot.send_chat_action(
            chat_id=_coerce_chat_id(conversation_ref),
            action=ChatAction.TYPING,
            message_thread_id=_coerce_thread_id(thread_ref, omit_general=False),
        )

    async def edit_message(
        self,
        conversation_ref: str,
        message_ref: str,
        text: str,
        *,
        format_hint: str = "plain",
        buttons: Sequence[TelegramActionButton] = (),
    ) -> None:
        chat_id = _coerce_chat_id(conversation_ref)
        message_id = _coerce_message_id(message_ref)
        if message_id is None:
            raise TelegramReferenceError(
                f"invalid telegram message_ref '{message_ref}'"
            )
        chunks = _format_telegram_text_chunks(text, format_hint)
        chunk = chunks[0] if chunks else TelegramTextChunk(" ")
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "link_preview_options": LinkPreviewOptions(is_disabled=True),
            "reply_markup": _inline_keyboard(buttons),
        }
        if chunk.html_text is not None:
            try:
                await self._bot.edit_message_text(
                    text=chunk.html_text,
                    parse_mode="HTML",
                    **params,
                )
                return
            except TelegramBadRequest as err:
                if not _is_parse_error(err):
                    raise
        await self._bot.edit_message_text(text=chunk.plain_text, **params)

    async def delete_message(
        self,
        conversation_ref: str,
        message_ref: str,
    ) -> None:
        message_id = _coerce_message_id(message_ref)
        if message_id is None:
            raise TelegramReferenceError(
                f"invalid telegram message_ref '{message_ref}'"
            )
        await self._bot.delete_message(
            chat_id=_coerce_chat_id(conversation_ref),
            message_id=message_id,
        )

    async def answer_callback(
        self,
        callback_query_id: str,
        text: str | None = None,
    ) -> None:
        await self._bot.answer_callback_query(
            callback_query_id=callback_query_id,
            text=text,
        )

    async def set_reaction(
        self,
        conversation_ref: str,
        message_ref: str,
        emoji: str,
    ) -> None:
        message_id = _coerce_message_id(message_ref)
        if message_id is None:
            raise TelegramReferenceError(
                f"invalid telegram message_ref '{message_ref}'"
            )
        await self._bot.set_message_reaction(
            chat_id=_coerce_chat_id(conversation_ref),
            message_id=message_id,
            reaction=[ReactionTypeEmoji(emoji=emoji)],
        )


class TelegramEntityTooLargeForStage(RuntimeError):
    pass


def extract_inbound_event(
    update: Update,
    bot_identity: TelegramBotIdentity | None = None,
) -> TelegramInboundEvent | None:
    callback = _extract_callback_action(update, bot_identity)
    if callback is not None:
        return callback

    supported = _first_supported_message(update)
    if supported is None:
        return None
    message, source, edited = supported
    if _is_bot_sender(message):
        return None

    metadata = _provider_metadata(
        message,
        update_id=update.update_id,
        source=source,
        edited=edited,
        bot_identity=bot_identity,
    )

    token = _extract_pairing_token(_message_text(message), bot_identity)
    if token is not None:
        return TelegramPairingClaim(
            token=token,
            update_id=update.update_id,
            sender_ref=_sender_ref(message),
            conversation_ref=_conversation_ref(message),
            thread_ref=_thread_ref(message),
            message_ref=_message_ref(message),
            provider_metadata=metadata,
        )

    text = _normalized_content_text(message, metadata)
    attachments = _normalized_attachments(message, update.update_id, text=text)
    if text is None and not attachments:
        return None

    return TelegramInboundUpdate(
        update_id=update.update_id,
        event_id=f"telegram:update:{update.update_id}",
        sender_ref=_sender_ref(message),
        conversation_ref=_conversation_ref(message),
        thread_ref=_thread_ref(message),
        message_ref=_message_ref(message),
        reply_to_ref=_reply_to_ref(message),
        text=text,
        attachments=attachments,
        trigger=_trigger(message, bot_identity),
        provider_metadata=metadata,
    )


def _extract_callback_action(
    update: Update,
    bot_identity: TelegramBotIdentity | None,
) -> TelegramCallbackAction | None:
    callback = update.callback_query
    if callback is None or not isinstance(callback.data, str):
        return None
    message = _callback_query_message(update)
    if message is None:
        return None
    metadata = _provider_metadata(
        message,
        update_id=update.update_id,
        source="callback_query",
        edited=False,
        bot_identity=bot_identity,
    )
    metadata["callback_query_id"] = callback.id
    return TelegramCallbackAction(
        update_id=update.update_id,
        callback_query_id=callback.id,
        action=callback.data,
        sender_ref=f"telegram:user:{callback.from_user.id}",
        conversation_ref=_conversation_ref(message),
        thread_ref=_thread_ref(message),
        message_ref=_message_ref(message),
        provider_metadata=metadata,
    )


def _first_supported_message(update: Update) -> tuple[Message, str, bool] | None:
    candidates = (
        (update.message, "message", False),
        (update.edited_message, "edited_message", True),
        (update.channel_post, "channel_post", False),
        (update.edited_channel_post, "edited_channel_post", True),
    )
    for candidate, source, edited in candidates:
        if candidate is not None:
            return candidate, source, edited
    return None


def _callback_query_message(update: Update) -> Message | None:
    callback = update.callback_query
    if callback is None or not isinstance(callback.data, str):
        return None
    message = callback.message
    if isinstance(message, Message):
        return message
    return None


def _coerce_chat_id(peer_id: str) -> int | str:
    if not isinstance(peer_id, str):
        raise TelegramReferenceError(f"invalid telegram conversation_ref '{peer_id}'")
    was_namespaced = peer_id.startswith("telegram:")
    for prefix in ("telegram:chat:", "telegram:user:"):
        if peer_id.startswith(prefix):
            peer_id = peer_id.removeprefix(prefix)
            break
    else:
        if was_namespaced:
            raise TelegramReferenceError(
                f"unsupported telegram conversation_ref '{peer_id}'"
            )
    stripped = peer_id.removeprefix("-")
    if stripped.isdigit():
        return int(peer_id)
    if was_namespaced:
        raise TelegramReferenceError(f"invalid telegram conversation_ref '{peer_id}'")
    return peer_id


def _coerce_message_id(message_ref: str | None) -> int | None:
    if message_ref is None:
        return None
    if not isinstance(message_ref, str):
        raise TelegramReferenceError(f"invalid telegram message_ref '{message_ref}'")
    was_namespaced = message_ref.startswith("telegram:")
    if message_ref.startswith("telegram:message:"):
        message_ref = message_ref.removeprefix("telegram:message:")
    elif was_namespaced:
        raise TelegramReferenceError(
            f"unsupported telegram message_ref '{message_ref}'"
        )
    if message_ref.isdigit():
        return int(message_ref)
    if was_namespaced:
        raise TelegramReferenceError(f"invalid telegram message_ref '{message_ref}'")
    return None


def _reply_parameters(message_ref: str | None) -> ReplyParameters | None:
    message_id = _coerce_message_id(message_ref)
    if message_id is None:
        return None
    return ReplyParameters(message_id=message_id, allow_sending_without_reply=True)


def _coerce_thread_id(thread_ref: str | None, *, omit_general: bool) -> int | None:
    if thread_ref is None:
        return None
    if not isinstance(thread_ref, str):
        raise TelegramReferenceError(f"invalid telegram thread_ref '{thread_ref}'")
    if thread_ref.startswith("telegram:topic:"):
        thread_ref = thread_ref.removeprefix("telegram:topic:")
    if not thread_ref.isdigit():
        raise TelegramReferenceError(f"invalid telegram thread_ref '{thread_ref}'")
    thread_id = int(thread_ref)
    if omit_general and thread_id == 1:
        return None
    return thread_id


def _message_receipt(message: Message) -> dict[str, Any]:
    return {
        "message_id": message.message_id,
        "chat_id": str(message.chat.id),
    }


def normalize_telegram_receipt(
    receipt: dict[str, Any],
    *,
    conversation_ref: str | None = None,
) -> dict[str, Any]:
    chat_id = (
        _coerce_chat_id(conversation_ref) if conversation_ref is not None else None
    )
    return _receipt_from_messages(_receipt_messages(receipt, chat_id=chat_id))


def _receipt_messages(
    receipt: dict[str, Any] | None,
    *,
    chat_id: int | str | None,
) -> list[dict[str, Any]]:
    if receipt is None:
        return []
    messages = receipt.get("messages")
    if messages is None:
        messages = [receipt]
    elif not isinstance(messages, list):
        raise TelegramReferenceError("telegram resume receipt messages must be a list")
    if not messages:
        raise TelegramReferenceError("telegram resume receipt has no messages")
    parsed: list[dict[str, Any]] = []
    expected_chat_id = chat_id
    for message in messages:
        if not isinstance(message, dict):
            raise TelegramReferenceError(
                "telegram resume receipt message must be an object"
            )
        message_id = _coerce_receipt_message_id(message.get("message_id"))
        receipt_chat_id = _coerce_receipt_chat_id(message.get("chat_id"))
        if message_id is None:
            raise TelegramReferenceError(
                "telegram resume receipt message has invalid message_id"
            )
        if receipt_chat_id is None:
            raise TelegramReferenceError(
                "telegram resume receipt message has invalid chat_id"
            )
        if expected_chat_id is None:
            expected_chat_id = receipt_chat_id
        elif receipt_chat_id != expected_chat_id:
            raise TelegramReferenceError(
                "telegram resume receipt chat_id does not match delivery chat"
            )
        parsed.append(
            {
                "message_id": message_id,
                "chat_id": str(receipt_chat_id),
            }
        )
    return parsed


def _coerce_receipt_message_id(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        parsed = _coerce_message_id(value)
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _coerce_receipt_chat_id(value: object) -> int | str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return _coerce_chat_id(value)
        except TelegramReferenceError:
            return None
    return None


def _receipt_from_messages(messages: list[dict[str, Any]]) -> dict[str, Any]:
    last = messages[-1]
    return {
        "message_id": last["message_id"],
        "chat_id": last["chat_id"],
        "messages": messages,
    }


def _message_ref(message: Message) -> str:
    return f"telegram:message:{message.message_id}"


def _reply_to_ref(message: Message) -> str | None:
    if message.reply_to_message is None:
        return None
    return _message_ref(message.reply_to_message)


def _conversation_ref(message: Message) -> str:
    return f"telegram:chat:{message.chat.id}"


def _sender_ref(message: Message) -> str:
    if message.from_user is not None:
        return f"telegram:user:{message.from_user.id}"
    if message.sender_chat is not None:
        return f"telegram:sender_chat:{message.sender_chat.id}"
    return f"telegram:chat:{message.chat.id}"


def _thread_ref(message: Message) -> str | None:
    thread_id = message.message_thread_id
    if (
        thread_id is None
        and bool(getattr(message.chat, "is_forum", False))
        and not _is_private(message)
    ):
        thread_id = 1
    if thread_id is None:
        return None
    if _chat_type(message) == "private" and not bool(message.is_topic_message):
        return None
    return f"telegram:topic:{thread_id}"


def _chat_type(message: Message) -> str:
    return str(message.chat.type)


def _is_private(message: Message) -> bool:
    return _chat_type(message) == "private"


def _message_text(message: Message) -> str | None:
    return message.text


def _content_text(message: Message) -> str | None:
    return message.text or message.caption or _shared_location_text(message)


def _normalized_content_text(
    message: Message,
    provider_metadata: dict[str, Any],
) -> str | None:
    text = _content_text(message)
    if text is None:
        return None
    leading_mention = provider_metadata.get("leading_mention_text")
    if isinstance(leading_mention, str) and text.startswith(leading_mention):
        text_after_mention = text[len(leading_mention) :].lstrip()
        if _should_strip_leading_mention_payload(
            text_after_mention,
            provider_metadata,
        ):
            text = text_after_mention
    return text if text.strip() else None


def _should_strip_leading_mention_payload(
    text_after_mention: str,
    provider_metadata: dict[str, Any],
) -> bool:
    command_target = _leading_command_target_from_text(text_after_mention)
    if command_target is None:
        return True
    bot_username = provider_metadata.get("bot_username")
    return isinstance(bot_username, str) and _username_matches(
        command_target,
        bot_username,
    )


def _shared_location_text(message: Message) -> str | None:
    location = _shared_location_metadata(message)
    if location is None:
        return None
    coordinates = _location_coordinates_text(location)
    if location["kind"] == "venue":
        lines = [f"Shared venue: {location['title']}"]
        address = location.get("address")
        if isinstance(address, str) and address:
            lines.append(address)
        lines.append(f"Location: {coordinates}")
        return "\n".join(lines)
    return f"Shared location: {coordinates}"


def _shared_location_metadata(message: Message) -> dict[str, Any] | None:
    if message.venue is not None:
        venue = message.venue
        metadata: dict[str, Any] = {
            "kind": "venue",
            "title": venue.title,
            "address": venue.address,
            "latitude": venue.location.latitude,
            "longitude": venue.location.longitude,
        }
        _copy_optional_attrs(
            venue,
            metadata,
            (
                "foursquare_id",
                "foursquare_type",
                "google_place_id",
                "google_place_type",
            ),
        )
        return metadata
    if message.location is not None:
        location = message.location
        metadata = {
            "kind": "location",
            "latitude": location.latitude,
            "longitude": location.longitude,
        }
        _copy_optional_attrs(
            location,
            metadata,
            (
                "horizontal_accuracy",
                "live_period",
                "heading",
                "proximity_alert_radius",
            ),
        )
        return metadata
    return None


def _copy_optional_attrs(
    source: object,
    target: dict[str, Any],
    attrs: Sequence[str],
) -> None:
    for attr in attrs:
        value = getattr(source, attr, None)
        if value is not None:
            target[attr] = value


def _location_coordinates_text(location: dict[str, Any]) -> str:
    latitude = _format_coordinate(location["latitude"])
    longitude = _format_coordinate(location["longitude"])
    return f"latitude {latitude}, longitude {longitude} (geo:{latitude},{longitude})"


def _format_coordinate(value: object) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _extract_pairing_token(
    text: str | None,
    bot_identity: TelegramBotIdentity | None,
) -> str | None:
    if text is None:
        return None
    match = PAIRING_START_RE.match(text)
    if match is None:
        return None
    target_username = match.group(1)
    if target_username is not None:
        bot_username = bot_identity.username if bot_identity is not None else None
        if not _username_matches(target_username, bot_username):
            return None
    return match.group(2)


def _trigger(message: Message, bot_identity: TelegramBotIdentity | None) -> str:
    if _is_private(message):
        return "dm"
    if _has_bot_mention(message, bot_identity):
        return "mention"
    if _is_reply_to_bot(message, bot_identity):
        return "reply_to_bot"
    if _thread_ref(message) is not None:
        return "thread_continuation"
    return "none"


def _is_reply_to_bot(
    message: Message, bot_identity: TelegramBotIdentity | None
) -> bool:
    reply = message.reply_to_message
    if reply is None or reply.from_user is None or not reply.from_user.is_bot:
        return False
    if bot_identity is None:
        return False
    if bot_identity.user_id is not None and reply.from_user.id == bot_identity.user_id:
        return True
    return _username_matches(reply.from_user.username, bot_identity.username)


def _has_bot_mention(
    message: Message, bot_identity: TelegramBotIdentity | None
) -> bool:
    if bot_identity is None:
        return False
    text = _content_text(message)
    if text is None:
        return False
    bot_username = bot_identity.username
    for entity in _message_entities(message):
        fragment = _extract_entity_text(entity, text)
        entity_type = str(entity.type)
        if _mention_entity_targets_bot(entity, fragment, bot_identity):
            return True
        if (
            bot_username is not None
            and entity_type == "bot_command"
            and _command_targets_bot(fragment, bot_username)
        ):
            return True
    return False


def _leading_bot_mention_text(
    message: Message,
    bot_identity: TelegramBotIdentity | None,
) -> str | None:
    if bot_identity is None:
        return None
    text = _content_text(message)
    if text is None:
        return None
    for entity in _message_entities(message):
        if entity.offset != 0:
            continue
        fragment = _extract_entity_text(entity, text)
        if _mention_entity_targets_bot(entity, fragment, bot_identity):
            return fragment
    return None


def _mention_entity_targets_bot(
    entity: MessageEntity,
    fragment: str,
    bot_identity: TelegramBotIdentity,
) -> bool:
    entity_type = str(entity.type)
    if entity_type == "mention":
        return _username_matches(fragment.removeprefix("@"), bot_identity.username)
    if entity_type == "text_mention":
        return _text_mention_targets_bot(entity, bot_identity)
    return False


def _text_mention_targets_bot(
    entity: MessageEntity,
    bot_identity: TelegramBotIdentity,
) -> bool:
    mentioned_user = getattr(entity, "user", None)
    if mentioned_user is None:
        return False
    mentioned_user_id = getattr(mentioned_user, "id", None)
    if bot_identity.user_id is not None and mentioned_user_id == bot_identity.user_id:
        return True
    return bool(getattr(mentioned_user, "is_bot", False)) and _username_matches(
        getattr(mentioned_user, "username", None), bot_identity.username
    )


def _message_entities(message: Message) -> Sequence[MessageEntity]:
    return message.entities or message.caption_entities or []


def _extract_entity_text(entity: MessageEntity, text: str) -> str:
    extract_from = getattr(entity, "extract_from", None)
    if callable(extract_from):
        return str(extract_from(text))
    return _utf16_slice(text, entity.offset, entity.length)


def _utf16_slice(text: str, offset: int, length: int) -> str:
    raw = text.encode("utf-16-le")
    start = offset * 2
    end = start + length * 2
    return raw[start:end].decode("utf-16-le", errors="ignore")


def _username_matches(candidate: str | None, bot_username: str | None) -> bool:
    if candidate is None or bot_username is None:
        return False
    return (
        candidate.removeprefix("@").casefold()
        == bot_username.removeprefix("@").casefold()
    )


def _command_targets_bot(fragment: str, bot_username: str) -> bool:
    if "@" not in fragment:
        return False
    _, target = fragment.split("@", 1)
    return _username_matches(target, bot_username)


def _leading_command_target(message: Message) -> str | None:
    text = _content_text(message)
    if text is None:
        return None
    return _leading_command_target_from_text(text)


def _leading_command_target_from_text(text: str) -> str | None:
    if not text.startswith("/"):
        return None
    token = text.split(maxsplit=1)[0]
    command = token.removeprefix("/")
    if "@" not in command:
        return None
    _, target = command.split("@", 1)
    target = target.removeprefix("@").casefold()
    return target or None


def _is_bot_sender(message: Message) -> bool:
    return bool(message.from_user is not None and message.from_user.is_bot)


def _provider_metadata(
    message: Message,
    *,
    update_id: int,
    source: str,
    edited: bool,
    bot_identity: TelegramBotIdentity | None,
) -> dict[str, Any]:
    command_target = _leading_command_target(message)
    leading_bot_mention = _leading_bot_mention_text(message, bot_identity)
    message_date_epoch = _message_date_epoch(message)
    metadata: dict[str, Any] = {
        "provider": "telegram",
        "update_id": update_id,
        "source": source,
        "edited": edited,
        "chat_id": message.chat.id,
        "chat_type": _chat_type(message),
        "message_id": message.message_id,
        "bot_mentioned": _has_bot_mention(message, bot_identity),
    }
    if message_date_epoch is not None:
        metadata["message_date_epoch"] = message_date_epoch
    if bot_identity is not None and bot_identity.username is not None:
        metadata["bot_username"] = bot_identity.username.removeprefix("@").casefold()
    if leading_bot_mention is not None:
        metadata["leading_mention_targets_bot"] = True
        metadata["leading_mention_text"] = leading_bot_mention
    if command_target is not None:
        metadata["command_target"] = command_target
        metadata["command_targets_bot"] = _username_matches(
            command_target,
            bot_identity.username if bot_identity is not None else None,
        )
    if message.from_user is not None:
        metadata["from_user_id"] = message.from_user.id
        metadata["from_is_bot"] = bool(message.from_user.is_bot)
    if message.sender_chat is not None:
        metadata["sender_chat_id"] = message.sender_chat.id
    if message.message_thread_id is not None:
        metadata["message_thread_id"] = message.message_thread_id
    if message.media_group_id is not None:
        metadata["media_group_id"] = message.media_group_id
    shared_location = _shared_location_metadata(message)
    if shared_location is not None:
        metadata["shared_location"] = shared_location
    return metadata


def _message_date_epoch(message: Message) -> int | None:
    timestamp = getattr(message.date, "timestamp", None)
    if callable(timestamp):
        return int(timestamp())
    if isinstance(message.date, int | float):
        return int(message.date)
    return None


def _normalized_attachments(
    message: Message,
    update_id: int,
    *,
    text: str | None,
) -> list[TelegramInboundAttachment]:
    caption = text if message.caption is not None else None
    attachments = _attachments(message, update_id)
    if caption == message.caption:
        return attachments
    return [replace(attachment, caption=caption) for attachment in attachments]


def _attachments(message: Message, update_id: int) -> list[TelegramInboundAttachment]:
    caption = message.caption
    descriptors: list[TelegramInboundAttachment] = []
    if message.photo:
        photo = max(message.photo, key=_photo_rank)
        descriptors.append(
            _attachment(
                update_id,
                "photo",
                photo.file_id,
                file_unique_id=photo.file_unique_id,
                mime_type="image/jpeg",
                size_bytes=photo.file_size,
                caption=caption,
            )
        )
    if message.document is not None:
        document = message.document
        descriptors.append(
            _attachment(
                update_id,
                "document",
                document.file_id,
                file_unique_id=document.file_unique_id,
                mime_type=document.mime_type,
                filename=document.file_name,
                size_bytes=document.file_size,
                caption=caption,
            )
        )
    if message.audio is not None:
        audio = message.audio
        descriptors.append(
            _attachment(
                update_id,
                "audio",
                audio.file_id,
                file_unique_id=audio.file_unique_id,
                mime_type=audio.mime_type,
                filename=audio.file_name,
                size_bytes=audio.file_size,
                caption=caption,
            )
        )
    if message.voice is not None:
        voice = message.voice
        descriptors.append(
            _attachment(
                update_id,
                "voice",
                voice.file_id,
                file_unique_id=voice.file_unique_id,
                mime_type=voice.mime_type or "audio/ogg",
                size_bytes=voice.file_size,
                caption=caption,
            )
        )
    if message.video is not None:
        video = message.video
        descriptors.append(
            _attachment(
                update_id,
                "video",
                video.file_id,
                file_unique_id=video.file_unique_id,
                mime_type=video.mime_type,
                filename=video.file_name,
                size_bytes=video.file_size,
                caption=caption,
            )
        )
    if message.video_note is not None:
        video_note = message.video_note
        descriptors.append(
            _attachment(
                update_id,
                "video_note",
                video_note.file_id,
                file_unique_id=video_note.file_unique_id,
                mime_type="video/mp4",
                size_bytes=video_note.file_size,
                caption=caption,
            )
        )
    if message.sticker is not None:
        sticker = message.sticker
        mime_type = "video/webm" if sticker.is_video else "application/x-tgsticker"
        if not sticker.is_animated and not sticker.is_video:
            mime_type = "image/webp"
        descriptors.append(
            _attachment(
                update_id,
                "sticker",
                sticker.file_id,
                file_unique_id=sticker.file_unique_id,
                mime_type=mime_type,
                size_bytes=sticker.file_size,
                caption=caption,
            )
        )
    if message.animation is not None:
        animation = message.animation
        descriptors.append(
            _attachment(
                update_id,
                "animation",
                animation.file_id,
                file_unique_id=animation.file_unique_id,
                mime_type=animation.mime_type,
                filename=animation.file_name,
                size_bytes=animation.file_size,
                caption=caption,
            )
        )
    return descriptors


def _photo_rank(photo: PhotoSize) -> tuple[int, int]:
    return (photo.width * photo.height, photo.file_size or 0)


def _attachment(
    update_id: int,
    kind: str,
    provider_file_ref: str,
    *,
    file_unique_id: str | None,
    mime_type: str | None,
    filename: str | None = None,
    size_bytes: int | None = None,
    caption: str | None = None,
) -> TelegramInboundAttachment:
    stable_file_id = file_unique_id or provider_file_ref
    return TelegramInboundAttachment(
        attachment_id=f"telegram:update:{update_id}:attachment:{kind}:{stable_file_id}",
        kind=kind,
        provider_file_ref=provider_file_ref,
        mime_type=mime_type,
        filename=filename,
        size_bytes=size_bytes,
        caption=caption,
    )


def _inline_keyboard(
    buttons: Sequence[TelegramActionButton],
) -> InlineKeyboardMarkup | None:
    if not buttons:
        return None
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for button in buttons:
        text = button.text.strip()
        action = button.action.strip()
        if not text:
            raise TelegramReferenceError(
                "telegram inline button text must not be empty"
            )
        if not action:
            raise TelegramReferenceError(
                "telegram inline button action must not be empty"
            )
        if len(action.encode("utf-8")) > 64:
            raise TelegramReferenceError(
                "telegram inline button action exceeds 64 bytes"
            )
        row.append(InlineKeyboardButton(text=text, callback_data=action))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _can_send_native_album(
    attachments: Sequence[TelegramOutboundAttachment],
) -> bool:
    if not 2 <= len(attachments) <= 10:
        return False
    for attachment in attachments:
        normalized_mime_type = _normalize_mime_type(attachment.mime_type or "")
        if normalized_mime_type not in PHOTO_MIME_TYPES | VIDEO_MIME_TYPES:
            return False
    return True


def _album_media(
    attachments: Sequence[TelegramOutboundAttachment],
    *,
    caption: TelegramTextChunk | None,
    use_html: bool,
) -> list[InputMediaPhoto | InputMediaVideo]:
    media: list[InputMediaPhoto | InputMediaVideo] = []
    for index, attachment in enumerate(attachments):
        path = Path(attachment.path)
        file = FSInputFile(path, filename=attachment.filename or path.name)
        params: dict[str, Any] = {"media": file}
        if index == 0 and caption is not None:
            if use_html and caption.html_text is not None:
                params["caption"] = caption.html_text
                params["parse_mode"] = "HTML"
            else:
                params["caption"] = caption.plain_text
        normalized_mime_type = _normalize_mime_type(attachment.mime_type or "")
        if normalized_mime_type in PHOTO_MIME_TYPES:
            media.append(InputMediaPhoto(**params))
        else:
            media.append(InputMediaVideo(**params))
    return media


def _format_telegram_text_chunks(
    text: str, format_hint: str
) -> list[TelegramTextChunk]:
    chunks = _split_telegram_text(text)
    if not chunks:
        return []
    if format_hint.casefold() != "markdown":
        return [TelegramTextChunk(plain_text=chunk) for chunk in chunks]
    formatted = [
        TelegramTextChunk(plain_text=chunk, html_text=_markdown_to_telegram_html(chunk))
        for chunk in chunks
    ]
    if all(
        chunk.html_text is not None
        and _utf16_len(chunk.html_text) <= TELEGRAM_TEXT_LIMIT
        for chunk in formatted
    ):
        return formatted
    return [TelegramTextChunk(plain_text=chunk) for chunk in chunks]


def _fits_telegram_caption(chunk: TelegramTextChunk) -> bool:
    send_text = chunk.html_text if chunk.html_text is not None else chunk.plain_text
    return _utf16_len(send_text) <= TELEGRAM_CAPTION_LIMIT


def _markdown_to_telegram_html(markdown: str) -> str:
    lines = markdown.splitlines(keepends=True)
    rendered: list[str] = []
    code_lines: list[str] = []
    in_code_block = False

    for line in lines:
        if line.lstrip().startswith(("```", "~~~")):
            if in_code_block:
                rendered.append(
                    f"<pre><code>{html.escape(''.join(code_lines))}</code></pre>"
                )
                code_lines = []
                in_code_block = False
            else:
                in_code_block = True
            continue
        if in_code_block:
            code_lines.append(line)
            continue
        rendered.append(_markdown_inline_to_html(line))

    if in_code_block:
        rendered.append(f"<pre><code>{html.escape(''.join(code_lines))}</code></pre>")

    return "".join(rendered)


def _markdown_inline_to_html(text: str) -> str:
    output: list[str] = []
    index = 0
    while index < len(text):
        if text[index] == "`":
            end = text.find("`", index + 1)
            if end != -1:
                output.append(f"<code>{html.escape(text[index + 1:end])}</code>")
                index = end + 1
                continue
        if text.startswith("**", index):
            end = text.find("**", index + 2)
            if end != -1:
                output.append(f"<b>{_markdown_inline_to_html(text[index + 2:end])}</b>")
                index = end + 2
                continue
        if text[index] == "[":
            link = _parse_markdown_link(text, index)
            if link is not None:
                label, href, next_index = link
                output.append(_render_telegram_link(label, href))
                index = next_index
                continue
        output.append(html.escape(text[index]))
        index += 1
    return "".join(output)


def _parse_markdown_link(text: str, index: int) -> tuple[str, str, int] | None:
    label_end = text.find("]", index + 1)
    if label_end == -1 or label_end + 1 >= len(text) or text[label_end + 1] != "(":
        return None
    href_start = label_end + 2
    href_end = _find_markdown_link_href_end(text, href_start)
    if href_end is None:
        return None
    label = text[index + 1 : label_end]
    href = text[href_start:href_end].strip()
    return label, href, href_end + 1


def _find_markdown_link_href_end(text: str, start: int) -> int | None:
    depth = 0
    index = start
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            index += 2
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                return index
            depth -= 1
        index += 1
    return None


def _render_telegram_link(label: str, href: str) -> str:
    label_html = _markdown_inline_to_html(label)
    if SUPPORTED_LINK_RE.match(href):
        return f'<a href="{html.escape(href, quote=True)}">{label_html}</a>'
    if LOCAL_LINK_RE.match(href) or not href:
        if FILE_REFERENCE_RE.match(label.strip()):
            return f"<code>{html.escape(label.strip())}</code>"
        return label_html
    return label_html


def _split_telegram_text(text: str) -> list[str]:
    if not text or not text.strip():
        return []
    chunks: list[str] = []
    remaining = text
    while _utf16_len(remaining) > TELEGRAM_TEXT_LIMIT:
        cut = _find_text_cut(remaining, TELEGRAM_TEXT_LIMIT)
        chunk = remaining[:cut]
        if chunk.strip():
            chunks.append(chunk)
            remaining = remaining[cut:]
        else:
            remaining = remaining[cut:].lstrip()
    if remaining.strip():
        chunks.append(remaining)
    return chunks


def _find_text_cut(text: str, max_utf16_units: int) -> int:
    units = 0
    last_boundary = 0
    line_has_content = False
    for index, char in enumerate(text):
        char_units = _utf16_len(char)
        if units + char_units > max_utf16_units:
            return last_boundary if last_boundary > 0 else index
        units += char_units
        if char in "\r\n":
            last_boundary = index + 1
            line_has_content = False
        elif char.isspace() and line_has_content:
            last_boundary = index + 1
        elif not char.isspace():
            line_has_content = True
    return len(text)


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _is_parse_error(err: TelegramBadRequest) -> bool:
    return TELEGRAM_PARSE_ERROR_RE.search(str(err)) is not None


def _chat_action_for_mime_type(mime_type: str) -> str:
    normalized_mime_type = _normalize_mime_type(mime_type)
    if normalized_mime_type in PHOTO_MIME_TYPES:
        return ChatAction.UPLOAD_PHOTO
    if normalized_mime_type in VIDEO_MIME_TYPES:
        return ChatAction.UPLOAD_VIDEO
    return ChatAction.UPLOAD_DOCUMENT


def _normalize_mime_type(mime_type: str) -> str:
    return mime_type.split(";", 1)[0].strip().casefold()
