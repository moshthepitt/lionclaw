from __future__ import annotations

import html
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from aiogram import Bot
from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    BotCommand,
    FSInputFile,
    LinkPreviewOptions,
    Message,
    MessageEntity,
    ReplyParameters,
    Update,
)

TELEGRAM_TEXT_LIMIT = 4000
TELEGRAM_CAPTION_LIMIT = 1024
PAIRING_START_RE = re.compile(
    r"^/(?:start|startgroup)(?:@[A-Za-z0-9_]+)?\s+(lc_[A-Za-z0-9_-]{8,128})\s*$"
)
TELEGRAM_PARSE_ERROR_RE = re.compile(
    r"can't parse entities|parse entities|entity", re.I
)
LOCAL_LINK_RE = re.compile(r"^(?:/|file:|\.{0,2}/|[A-Za-z]:[\\/])")
SUPPORTED_LINK_RE = re.compile(r"^(?:https?://|tg://|mailto:)", re.I)
FILE_REFERENCE_RE = re.compile(r"^[A-Za-z0-9_.@-]+\.[A-Za-z0-9][A-Za-z0-9_.-]*$")


class TelegramReferenceError(ValueError):
    pass


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
class TelegramTextChunk:
    plain_text: str
    html_text: str | None = None


TelegramInboundEvent = TelegramInboundUpdate | TelegramPairingClaim


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
    ) -> None: ...

    async def delete_message(
        self,
        conversation_ref: str,
        message_ref: str,
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
    ) -> dict[str, Any]:
        chat_id = _coerce_chat_id(conversation_ref)
        reply_parameters = _reply_parameters(reply_to_ref)
        message_thread_id = _coerce_thread_id(thread_ref, omit_general=True)
        sent_messages: list[dict[str, Any]] = []
        text_chunks = _format_telegram_text_chunks(text, format_hint)
        caption_chunk: TelegramTextChunk | None = None
        if (
            attachments
            and len(text_chunks) == 1
            and _utf16_len(text_chunks[0].plain_text) <= TELEGRAM_CAPTION_LIMIT
        ):
            caption_chunk = text_chunks[0]
            text_chunks = []

        for chunk in text_chunks:
            message = await self._send_text_chunk(
                chat_id=chat_id,
                chunk=chunk,
                reply_parameters=reply_parameters,
                message_thread_id=message_thread_id,
            )
            sent_messages.append(_message_receipt(message))
            reply_parameters = _reply_parameters(str(message.message_id))

        for index, attachment in enumerate(attachments):
            message = await self._send_attachment(
                chat_id=chat_id,
                attachment=attachment,
                reply_parameters=reply_parameters,
                message_thread_id=message_thread_id,
                caption=caption_chunk if index == 0 else None,
            )
            sent_messages.append(_message_receipt(message))
            reply_parameters = _reply_parameters(str(message.message_id))

        if not sent_messages:
            message = await self._bot.send_message(
                chat_id=chat_id,
                text=" ",
                reply_parameters=reply_parameters,
                message_thread_id=message_thread_id,
            )
            sent_messages.append(_message_receipt(message))

        last = sent_messages[-1]
        return {
            "message_id": last["message_id"],
            "chat_id": last["chat_id"],
            "messages": sent_messages,
        }

    async def _send_text_chunk(
        self,
        *,
        chat_id: int | str,
        chunk: TelegramTextChunk,
        reply_parameters: ReplyParameters | None,
        message_thread_id: int | None,
    ) -> Message:
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "reply_parameters": reply_parameters,
            "message_thread_id": message_thread_id,
            "link_preview_options": LinkPreviewOptions(is_disabled=True),
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
    ) -> Message:
        path = Path(attachment.path)
        file = FSInputFile(path, filename=attachment.filename or path.name)
        common: dict[str, Any] = {
            "chat_id": chat_id,
            "reply_parameters": reply_parameters,
            "message_thread_id": message_thread_id,
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

    async def _send_attachment_once(
        self,
        *,
        file: FSInputFile,
        mime_type: str,
        params: dict[str, Any],
    ) -> Message:
        if mime_type.startswith("image/"):
            return await self._bot.send_photo(photo=file, **params)
        if mime_type.startswith("video/"):
            return await self._bot.send_video(video=file, **params)
        if mime_type.startswith("audio/"):
            if mime_type == "audio/ogg":
                return await self._bot.send_voice(voice=file, **params)
            return await self._bot.send_audio(audio=file, **params)
        return await self._bot.send_document(document=file, **params)

    async def _send_upload_action(
        self,
        chat_id: int | str,
        mime_type: str,
        message_thread_id: int | None,
    ) -> None:
        try:
            await self._bot.send_chat_action(
                chat_id=chat_id,
                action=_chat_action_for_mime_type(mime_type),
                message_thread_id=message_thread_id,
            )
        except Exception:
            pass

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


class TelegramEntityTooLargeForStage(RuntimeError):
    pass


def extract_inbound_event(
    update: Update,
    bot_identity: TelegramBotIdentity | None = None,
) -> TelegramInboundEvent | None:
    supported = _first_supported_message(update)
    if supported is None:
        return None
    message, source, edited = supported
    if _is_bot_sender(message):
        return None

    token = _extract_pairing_token(_message_text(message))
    if token is not None:
        return TelegramPairingClaim(
            token=token,
            update_id=update.update_id,
            sender_ref=_sender_ref(message),
            conversation_ref=_conversation_ref(message),
            thread_ref=_thread_ref(message),
            message_ref=_message_ref(message),
            provider_metadata=_provider_metadata(
                message,
                update_id=update.update_id,
                source=source,
                edited=edited,
                bot_identity=bot_identity,
            ),
        )

    text = _content_text(message)
    attachments = _attachments(message, update.update_id)
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
        provider_metadata=_provider_metadata(
            message,
            update_id=update.update_id,
            source=source,
            edited=edited,
            bot_identity=bot_identity,
        ),
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


def _coerce_chat_id(peer_id: str) -> int | str:
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
    return message.text or message.caption


def _extract_pairing_token(text: str | None) -> str | None:
    if text is None:
        return None
    match = PAIRING_START_RE.match(text)
    return match.group(1) if match is not None else None


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
        if entity_type == "mention" and _username_matches(
            fragment.removeprefix("@"),
            bot_username,
        ):
            return True
        if bot_username is not None and entity_type == "bot_command":
            if _command_targets_bot(fragment, bot_username):
                return True
        if entity_type == "text_mention":
            if _text_mention_targets_bot(entity, bot_identity):
                return True
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
    if bool(getattr(mentioned_user, "is_bot", False)):
        if _username_matches(
            getattr(mentioned_user, "username", None), bot_identity.username
        ):
            return True
    return False


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
    if message.from_user is not None:
        metadata["from_user_id"] = message.from_user.id
        metadata["from_is_bot"] = bool(message.from_user.is_bot)
    if message.sender_chat is not None:
        metadata["sender_chat_id"] = message.sender_chat.id
    if message.message_thread_id is not None:
        metadata["message_thread_id"] = message.message_thread_id
    if message.media_group_id is not None:
        metadata["media_group_id"] = message.media_group_id
    return metadata


def _attachments(message: Message, update_id: int) -> list[TelegramInboundAttachment]:
    caption = message.caption
    descriptors: list[TelegramInboundAttachment] = []
    if message.photo:
        photo = max(message.photo, key=lambda item: item.file_size or 0)
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
    href_end = text.find(")", label_end + 2)
    if href_end == -1:
        return None
    label = text[index + 1 : label_end]
    href = text[label_end + 2 : href_end].strip()
    return label, href, href_end + 1


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
    if not text:
        return []
    chunks: list[str] = []
    remaining = text
    while _utf16_len(remaining) > TELEGRAM_TEXT_LIMIT:
        cut = _find_text_cut(remaining, TELEGRAM_TEXT_LIMIT)
        chunk = remaining[:cut].rstrip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _find_text_cut(text: str, max_utf16_units: int) -> int:
    units = 0
    last_boundary = 0
    for index, char in enumerate(text):
        char_units = _utf16_len(char)
        if units + char_units > max_utf16_units:
            return last_boundary if last_boundary > 0 else index
        units += char_units
        if char.isspace():
            last_boundary = index + 1
    return len(text)


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _is_parse_error(err: TelegramBadRequest) -> bool:
    return TELEGRAM_PARSE_ERROR_RE.search(str(err)) is not None


def _chat_action_for_mime_type(mime_type: str) -> str:
    if mime_type.startswith("image/"):
        return ChatAction.UPLOAD_PHOTO
    if mime_type.startswith("video/"):
        return ChatAction.UPLOAD_VIDEO
    if mime_type.startswith("audio/"):
        return ChatAction.UPLOAD_DOCUMENT
    return ChatAction.UPLOAD_DOCUMENT
