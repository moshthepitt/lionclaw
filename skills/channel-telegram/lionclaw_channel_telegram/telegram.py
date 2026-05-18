from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from aiogram import Bot
from aiogram.enums import ChatAction
from aiogram.types import Message, Update


@dataclass(slots=True, frozen=True)
class TelegramTextUpdate:
    update_id: int
    peer_id: str
    message_ref: str
    text: str


class TelegramTransport(Protocol):
    async def close(self) -> None: ...

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]: ...

    async def send_message(
        self,
        conversation_ref: str,
        text: str,
        reply_to_ref: str | None = None,
        thread_ref: str | None = None,
    ) -> dict[str, Any]: ...

    async def send_typing(self, peer_id: str) -> None: ...


class AiogramTelegramTransport:
    def __init__(self, bot_token: str) -> None:
        self._bot = Bot(bot_token)

    async def close(self) -> None:
        await self._bot.session.close()

    async def get_updates(self, offset: int, timeout_seconds: int) -> list[Update]:
        return await self._bot.get_updates(offset=offset, timeout=timeout_seconds)

    async def send_message(
        self,
        conversation_ref: str,
        text: str,
        reply_to_ref: str | None = None,
        thread_ref: str | None = None,
    ) -> dict[str, Any]:
        message = await self._bot.send_message(
            chat_id=_coerce_chat_id(conversation_ref),
            text=text,
            reply_to_message_id=_coerce_message_id(reply_to_ref),
            message_thread_id=_coerce_message_id(thread_ref),
        )
        return {
            "message_id": message.message_id,
            "chat_id": str(message.chat.id),
        }

    async def send_typing(self, peer_id: str) -> None:
        await self._bot.send_chat_action(
            chat_id=_coerce_chat_id(peer_id),
            action=ChatAction.TYPING,
        )


def extract_text_update(update: Update) -> TelegramTextUpdate | None:
    message = _first_supported_message(update)
    if message is None or message.text is None or message.chat.type != "private":
        return None
    return TelegramTextUpdate(
        update_id=update.update_id,
        peer_id=str(message.chat.id),
        message_ref=str(message.message_id),
        text=message.text,
    )


def _first_supported_message(update: Update) -> Message | None:
    for candidate in (update.message, update.edited_message):
        if candidate is not None:
            return candidate
    return None


def _coerce_chat_id(peer_id: str) -> int | str:
    stripped = peer_id.removeprefix("-")
    if stripped.isdigit():
        return int(peer_id)
    return peer_id


def _coerce_message_id(message_ref: str | None) -> int | None:
    if message_ref is None:
        return None
    return int(message_ref) if message_ref.isdigit() else None
