from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramEntityTooLarge,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramNotFound,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)

from lionclaw_channel_telegram.api import (
    AttachmentMissingReport,
    LionClawApi,
    OutboxDelivery,
    StreamEvent,
)
from lionclaw_channel_telegram.config import WorkerConfig
from lionclaw_channel_telegram.telegram import (
    AiogramTelegramTransport,
    TelegramEntityTooLargeForStage,
    TelegramInboundUpdate,
    TelegramOutboundAttachment,
    TelegramPairingClaim,
    TelegramReferenceError,
    TelegramTransport,
    extract_inbound_event,
)

logger = logging.getLogger(__name__)

EXPECTED_INBOUND_OUTCOMES = {
    "queued",
    "waiting_for_attachments",
    "duplicate",
    "pending_approval",
    "blocked",
    "trigger_ignored",
}
TYPING_STATUS_CODES = {"queue.started", "runtime.started"}
MAX_ATTACHMENT_STAGE_BYTES = 25 * 1024 * 1024


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

    async def process_updates(self) -> None:
        try:
            updates = await self.telegram.get_updates(
                offset=self.offset,
                timeout_seconds=self.config.telegram_poll_timeout_secs,
            )
            bot_identity = await self.telegram.bot_identity()
        except Exception:
            logger.exception("telegram getUpdates request failed")
            return

        for update in updates:
            event = extract_inbound_event(update, bot_identity=bot_identity)
            if event is not None:
                if isinstance(event, TelegramPairingClaim):
                    if not await self._claim_pairing(event):
                        return
                elif not await self._submit_inbound(event):
                    return
            self.offset = update.update_id + 1
            self.offset_store.save(self.offset)

    async def flush_stream(self) -> None:
        try:
            events = await self.lionclaw_api.pull_stream()
        except Exception:
            logger.exception("lionclaw stream pull request failed")
            return

        last_sequence: int | None = None

        for event in events:
            if not await self._process_stream_event(event):
                break
            last_sequence = event.sequence

        if last_sequence is not None:
            try:
                await self.lionclaw_api.ack_stream(last_sequence)
            except Exception:
                logger.exception(
                    "lionclaw stream ack failed through sequence %s",
                    last_sequence,
                )

    async def flush_outbox(self) -> None:
        try:
            deliveries = await self.lionclaw_api.pull_outbox()
        except Exception:
            logger.exception("lionclaw outbox pull request failed")
            return

        for delivery in deliveries:
            await self._process_outbox_delivery(delivery)

    async def run_forever(self) -> None:
        while True:
            await self.process_updates()
            await self.flush_stream()
            await self.flush_outbox()
            await asyncio.sleep(self.config.telegram_loop_delay_secs)

    async def _claim_pairing(self, claim: TelegramPairingClaim) -> bool:
        try:
            response = await self.lionclaw_api.claim_pairing(claim)
        except Exception:
            logger.exception(
                "lionclaw pairing claim failed for update_id=%s",
                claim.update_id,
            )
            return False

        try:
            await self.telegram.send_message(
                claim.conversation_ref,
                _pairing_claim_reply(response.outcome),
                reply_to_ref=claim.message_ref,
                thread_ref=claim.thread_ref,
            )
        except Exception:
            logger.exception(
                "telegram pairing acknowledgement failed for update_id=%s",
                claim.update_id,
            )
        return True

    async def _submit_inbound(self, update: TelegramInboundUpdate) -> bool:
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
        if response.outcome == "pending_approval":
            await self._notify_pending_approval(update, response.pairing_code)
        if response.outcome == "waiting_for_attachments":
            return await self._stage_attachments(update)
        return True

    async def _notify_pending_approval(
        self,
        update: TelegramInboundUpdate,
        pairing_code: str | None,
    ) -> None:
        text = "This Telegram scope is waiting for operator approval."
        if pairing_code is not None:
            text = f"This Telegram scope needs approval. Pairing code: {pairing_code}"
        try:
            await self.telegram.send_message(
                update.conversation_ref,
                text,
                reply_to_ref=update.message_ref,
                thread_ref=update.thread_ref,
            )
        except Exception:
            logger.exception(
                "telegram pending approval notice failed for update_id=%s",
                update.update_id,
            )

    async def _stage_attachments(self, update: TelegramInboundUpdate) -> bool:
        missing: list[AttachmentMissingReport] = []
        for attachment in update.attachments:
            try:
                downloaded = await self.telegram.download_attachment(
                    attachment,
                    max_bytes=MAX_ATTACHMENT_STAGE_BYTES,
                )
            except TelegramEntityTooLargeForStage as err:
                logger.warning(
                    "telegram attachment too large for event_id=%s attachment_id=%s",
                    update.event_id,
                    attachment.attachment_id,
                )
                missing.append(
                    AttachmentMissingReport(
                        attachment_id=attachment.attachment_id,
                        reason_code="telegram.file_too_large",
                        reason_text=str(err),
                    )
                )
                continue
            except Exception as err:
                logger.exception(
                    "telegram attachment download failed for event_id=%s attachment_id=%s",
                    update.event_id,
                    attachment.attachment_id,
                )
                missing.append(
                    AttachmentMissingReport(
                        attachment_id=attachment.attachment_id,
                        reason_code="telegram.download_failed",
                        reason_text=str(err),
                    )
                )
                continue

            try:
                stage = await self.lionclaw_api.stage_attachment(
                    update,
                    attachment,
                    downloaded,
                )
            except Exception as err:
                logger.exception(
                    "lionclaw attachment stage failed for event_id=%s attachment_id=%s",
                    update.event_id,
                    attachment.attachment_id,
                )
                missing.append(
                    AttachmentMissingReport(
                        attachment_id=attachment.attachment_id,
                        reason_code="telegram.stage_failed",
                        reason_text=str(err),
                    )
                )
                continue
            if stage.status == "rejected":
                logger.warning(
                    "lionclaw rejected attachment event_id=%s attachment_id=%s reason_code=%s",
                    update.event_id,
                    attachment.attachment_id,
                    stage.reason_code,
                )

        try:
            response = await self.lionclaw_api.finalize_attachments(update, missing)
        except Exception:
            logger.exception(
                "lionclaw attachment finalize failed for event_id=%s",
                update.event_id,
            )
            return False
        if response.outcome not in {"queued", "already_finalized"}:
            logger.error(
                "lionclaw attachment finalize returned unexpected outcome '%s' for event_id=%s",
                response.outcome,
                update.event_id,
            )
            return False
        return True

    async def _process_stream_event(self, event: StreamEvent) -> bool:
        if event.kind == "message_delta":
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
            return True

        if event.kind == "done":
            return True

        logger.error("lionclaw stream contained unknown event kind '%s'", event.kind)
        return True

    async def _process_outbox_delivery(self, delivery: OutboxDelivery) -> None:
        try:
            receipt = await self.telegram.send_message(
                delivery.conversation_ref,
                delivery.content.text,
                delivery.reply_to_ref,
                delivery.thread_ref,
                attachments=[
                    TelegramOutboundAttachment(
                        path=attachment.path,
                        filename=attachment.filename,
                        mime_type=attachment.mime_type,
                    )
                    for attachment in delivery.content.attachments
                ],
            )
        except Exception as err:
            logger.exception(
                "telegram sendMessage request failed for delivery_id=%s conversation_ref=%s",
                delivery.delivery_id,
                delivery.conversation_ref,
            )
            outcome, error_code = _classify_send_failure(err)
            await self._report_outbox_with_retry(
                delivery,
                outcome,
                error_code=error_code,
                error_text=str(err),
            )
            return

        await self._report_outbox_with_retry(
            delivery,
            "delivered",
            provider_receipt=receipt,
        )

    async def _report_outbox_with_retry(
        self,
        delivery: OutboxDelivery,
        outcome: str,
        *,
        provider_receipt: dict[str, object] | None = None,
        error_code: str | None = None,
        error_text: str | None = None,
    ) -> None:
        for attempt_no in range(1, 4):
            if await self._report_outbox_once(
                delivery,
                outcome,
                provider_receipt=provider_receipt,
                error_code=error_code,
                error_text=error_text,
            ):
                return
            await asyncio.sleep(0.25 * attempt_no)

    async def _report_outbox_once(
        self,
        delivery: OutboxDelivery,
        outcome: str,
        *,
        provider_receipt: dict[str, object] | None = None,
        error_code: str | None = None,
        error_text: str | None = None,
    ) -> bool:
        try:
            response = await self.lionclaw_api.report_outbox(
                delivery,
                outcome,
                provider_receipt=provider_receipt,
                error_code=error_code,
                error_text=error_text,
            )
        except Exception:
            logger.exception(
                "lionclaw outbox report failed for delivery_id=%s outcome=%s",
                delivery.delivery_id,
                outcome,
            )
            return False
        if not response.accepted:
            logger.warning(
                "lionclaw rejected stale outbox report for delivery_id=%s status=%s attempt_status=%s",
                delivery.delivery_id,
                response.status,
                response.attempt_status,
            )
        return True


def _classify_send_failure(err: Exception) -> tuple[str, str]:
    if isinstance(err, TelegramReferenceError):
        return "terminal_failed", "telegram.invalid_ref"
    if isinstance(
        err,
        (
            TelegramBadRequest,
            TelegramEntityTooLarge,
            TelegramForbiddenError,
            TelegramNotFound,
            TelegramUnauthorizedError,
        ),
    ):
        return "terminal_failed", "telegram.send_rejected"
    if isinstance(err, (TelegramNetworkError, TelegramRetryAfter, TelegramServerError)):
        return "retryable_failed", "telegram.send_retryable"
    return "retryable_failed", "telegram.send_failed"


def _pairing_claim_reply(outcome: str) -> str:
    if outcome == "approved":
        return "Pairing approved. You can send a message now."
    if outcome == "already_claimed":
        return "That pairing link has already been used."
    if outcome == "expired":
        return "That pairing link has expired."
    if outcome == "scope_mismatch":
        return "That pairing link is not valid for this chat."
    return "That pairing link is invalid."


async def run() -> None:
    config = WorkerConfig.from_env()
    config.runtime_dir.mkdir(parents=True, exist_ok=True)
    config.telegram_offset_file.parent.mkdir(parents=True, exist_ok=True)

    lionclaw_api = LionClawApi(
        base_url=config.lionclaw_base_url,
        channel_id=config.channel_id,
        consumer_id=config.consumer_id,
        start_mode=config.stream_start_mode,
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
