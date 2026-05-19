from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
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
    HealthCheck,
    InboundResponse,
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
TYPING_STATUS_CODES = {"queue.started", "runtime.started", "runtime.artifact"}
MAX_ATTACHMENT_STAGE_BYTES = 25 * 1024 * 1024
INITIAL_TYPING_TTL_SECONDS = 8.0
ACTIVE_TURN_TYPING_TTL_SECONDS = 300.0
TYPING_REFRESH_SECONDS = 4.0
MAX_TYPING_FAILURES = 3
MIN_HEALTH_REPORT_INTERVAL_SECONDS = 1.0
POLLING_IN_FLIGHT_GRACE_SECONDS = 10.0
DM_PROGRESS_THRESHOLD_SECONDS = 1.5
GROUP_PROGRESS_THRESHOLD_SECONDS = 3.0
PROGRESS_REFRESH_SECONDS = 0.5
PROGRESS_EDIT_MIN_SECONDS = 1.0

LOCAL_TELEGRAM_COMMANDS = {
    "help",
    "status",
    "new",
    "stop",
    "retry",
    "continue",
    "settings",
}
LIONCLAW_CONTROL_ALIASES = {
    "new": "/lionclaw reset",
    "retry": "/lionclaw retry",
    "continue": "/lionclaw continue",
}
ACTIVE_STATUS_CODES = {
    "queue.started": "Queued",
    "runtime.started": "Working",
    "runtime.artifact": "Preparing artifact",
}
CANCELLED_STATUS_CODES = {"runtime.cancelled", "queue.cancelled"}
COMPLETED_STATUS_CODES = {"queue.completed"}


@dataclass(slots=True, frozen=True)
class TypingTarget:
    conversation_ref: str
    thread_ref: str | None = None

    @property
    def key(self) -> tuple[str, str | None]:
        return (self.conversation_ref, self.thread_ref)


@dataclass(slots=True)
class ActiveTurn:
    turn_id: str
    session_id: str | None
    session_key: str | None
    target: TypingTarget
    reply_to_ref: str | None
    generation: int
    visible_after: float
    status_text: str = "Queued"
    provisional_message_ref: str | None = None
    last_rendered_text: str | None = None
    last_edit_at: float = 0.0
    can_edit: bool = True
    terminal: bool = False
    terminal_text: str | None = None


@dataclass(slots=True, frozen=True)
class TelegramCommand:
    name: str
    raw: str
    arguments: str


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
        self._typing_targets: dict[tuple[str, str | None], TypingTarget] = {}
        self._typing_deadlines: dict[tuple[str, str | None], float] = {}
        self._typing_routes: dict[str, TypingTarget] = {}
        self._typing_failures: dict[tuple[str, str | None], int] = {}
        self._active_turns: dict[str, ActiveTurn] = {}
        self._route_turns: dict[tuple[str, str | None], str] = {}
        self._route_generations: dict[tuple[str, str | None], int] = {}
        self._poll_in_flight_started_at: float | None = None
        self._last_poll_success_observed_at: float | None = None
        self._last_poll_failure_observed_at: float | None = None
        self._last_poll_error: str | None = None
        self._last_poll_update_count: int | None = None
        self._last_update_id: int | None = None
        self._last_update_observed_at: float | None = None
        self._delivery_failure_counts: dict[str, int] = {}

    async def process_updates(self) -> None:
        self._poll_in_flight_started_at = _loop_time()
        try:
            updates = await self.telegram.get_updates(
                offset=self.offset,
                timeout_seconds=self.config.telegram_poll_timeout_secs,
            )
        except Exception as err:
            self._record_poll_failure(err)
            logger.exception("telegram getUpdates request failed")
            return

        self._record_poll_success(len(updates))
        try:
            bot_identity = await self.telegram.bot_identity()
        except Exception:
            logger.exception("telegram getMe request failed")
            return

        for update in updates:
            event = extract_inbound_event(update, bot_identity=bot_identity)
            if event is not None:
                if isinstance(event, TelegramPairingClaim):
                    if not await self._claim_pairing(event):
                        return
                elif not await self._submit_inbound(event):
                    return
            self._last_update_id = update.update_id
            self._last_update_observed_at = _loop_time()
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

    async def report_health(self) -> None:
        checks = await self._health_checks()
        status = _overall_health_status(checks)
        try:
            await self.lionclaw_api.report_health(status, checks)
        except Exception:
            logger.exception("lionclaw health report submit failed")

    async def run_forever(self) -> None:
        tasks = [
            asyncio.create_task(self._run_update_loop(), name="telegram-updates"),
            asyncio.create_task(self._run_stream_loop(), name="lionclaw-stream"),
            asyncio.create_task(self._run_outbox_loop(), name="lionclaw-outbox"),
            asyncio.create_task(self._run_typing_loop(), name="telegram-typing"),
            asyncio.create_task(self._run_progress_loop(), name="telegram-progress"),
            asyncio.create_task(self._run_health_loop(), name="lionclaw-health"),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._typing_targets.clear()
            self._typing_deadlines.clear()
            self._typing_routes.clear()
            self._typing_failures.clear()
            self._active_turns.clear()
            self._route_turns.clear()
            self._route_generations.clear()

    async def _run_update_loop(self) -> None:
        while True:
            await self.process_updates()
            await asyncio.sleep(self.config.telegram_loop_delay_secs)

    async def _run_stream_loop(self) -> None:
        while True:
            await self.flush_stream()
            await asyncio.sleep(self.config.telegram_loop_delay_secs)

    async def _run_outbox_loop(self) -> None:
        while True:
            await self.flush_outbox()
            await asyncio.sleep(self.config.telegram_loop_delay_secs)

    async def _run_typing_loop(self) -> None:
        while True:
            await asyncio.sleep(TYPING_REFRESH_SECONDS)
            await self.refresh_typing()

    async def _run_progress_loop(self) -> None:
        while True:
            await asyncio.sleep(PROGRESS_REFRESH_SECONDS)
            await self.refresh_progress_messages()

    async def _run_health_loop(self) -> None:
        interval = max(
            self.config.health_report_interval_secs,
            MIN_HEALTH_REPORT_INTERVAL_SECONDS,
        )
        while True:
            await self.report_health()
            await asyncio.sleep(interval)

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
        command = _telegram_command(update)
        if (
            command is not None
            and command.name in LOCAL_TELEGRAM_COMMANDS
            and _telegram_command_is_addressed(update)
        ):
            return await self._handle_telegram_command(update, command)

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
        if response.outcome == "queued":
            self._remember_typing_route(update)
            self._remember_active_turn(update, response)
            await self._start_typing(
                update.conversation_ref,
                thread_ref=update.thread_ref,
                ttl_seconds=INITIAL_TYPING_TTL_SECONDS,
            )
        if response.outcome == "waiting_for_attachments":
            self._remember_active_turn(update, response)
            staged = await self._stage_attachments(update)
            if staged:
                self._remember_typing_route(update)
                await self._start_typing(
                    update.conversation_ref,
                    thread_ref=update.thread_ref,
                    ttl_seconds=INITIAL_TYPING_TTL_SECONDS,
                )
            return staged
        return True

    async def _handle_telegram_command(
        self,
        update: TelegramInboundUpdate,
        command: TelegramCommand,
    ) -> bool:
        if command.name in LIONCLAW_CONTROL_ALIASES:
            self._advance_route_generation(update.conversation_ref, update.thread_ref)
            return await self._submit_runtime_passthrough(
                replace(
                    update,
                    text=LIONCLAW_CONTROL_ALIASES[command.name],
                    attachments=[],
                )
            )
        if command.name == "stop":
            await self._stop_active_turn(update)
            return True
        if command.name == "status":
            await self._send_status(update)
            return True
        if command.name == "settings":
            await self._send_settings(update)
            return True
        await self._send_help(update)
        return True

    async def _submit_runtime_passthrough(self, update: TelegramInboundUpdate) -> bool:
        try:
            response = await self.lionclaw_api.send_inbound(update)
        except Exception:
            logger.exception(
                "lionclaw command submit failed for update_id=%s",
                update.update_id,
            )
            return False
        if response.outcome not in EXPECTED_INBOUND_OUTCOMES:
            logger.error(
                "lionclaw command returned unknown outcome '%s' for update_id=%s",
                response.outcome,
                update.update_id,
            )
            return False
        if response.outcome == "pending_approval":
            await self._notify_pending_approval(update, response.pairing_code)
        if response.outcome == "queued":
            self._remember_typing_route(update)
            self._remember_active_turn(update, response)
            await self._start_typing(
                update.conversation_ref,
                thread_ref=update.thread_ref,
                ttl_seconds=INITIAL_TYPING_TTL_SECONDS,
            )
        return True

    async def _stop_active_turn(self, update: TelegramInboundUpdate) -> None:
        active = self._active_turn_for_route(update.conversation_ref, update.thread_ref)
        if active is None:
            await self._reply(update, "No active LionClaw turn is running here.")
            return
        if active.session_id is None or active.session_key is None:
            await self._reply(update, "This turn cannot be stopped yet.")
            return

        active.status_text = "Stopping"
        await self._ensure_progress_message(active, force=True)
        try:
            await self.lionclaw_api.cancel_active_turn(
                session_id=active.session_id,
                session_key=active.session_key,
                expected_turn_id=active.turn_id,
                reason="telegram stop command",
            )
        except Exception as err:
            logger.exception(
                "lionclaw active turn cancel failed for turn_id=%s",
                active.turn_id,
            )
            text = (
                "Stop was already requested."
                if "already requested" in str(err)
                else "I could not stop that turn."
            )
            await self._reply(update, text)
            return

    async def _send_status(self, update: TelegramInboundUpdate) -> None:
        active = self._active_turn_for_route(update.conversation_ref, update.thread_ref)
        if active is None:
            await self._reply(update, "No active LionClaw turn is running here.")
            return
        await self._reply(update, f"Active turn: {active.status_text.lower()}.")

    async def _send_settings(self, update: TelegramInboundUpdate) -> None:
        await self._reply(
            update,
            "Telegram channel settings\n"
            "/status - current turn\n"
            "/stop - stop active turn\n"
            "/new - fresh session\n"
            "/retry - retry last turn\n"
            "/continue - continue partial turn\n"
            "/model - runtime model controls",
        )

    async def _send_help(self, update: TelegramInboundUpdate) -> None:
        await self._reply(
            update,
            "LionClaw controls\n"
            "/status, /new, /stop, /retry, /continue, /settings\n\n"
            "Runtime controls\n"
            "/model and other runtime slash commands pass through unchanged.\n"
            "Use a leading space before / to send literal slash text.",
        )

    async def _reply(self, update: TelegramInboundUpdate, text: str) -> None:
        try:
            await self.telegram.send_message(
                update.conversation_ref,
                text,
                reply_to_ref=update.message_ref,
                thread_ref=update.thread_ref,
            )
        except Exception:
            logger.exception(
                "telegram command reply failed for update_id=%s",
                update.update_id,
            )

    def _remember_active_turn(
        self,
        update: TelegramInboundUpdate,
        response: InboundResponse,
    ) -> None:
        if response.turn_id is None:
            return
        target = TypingTarget(update.conversation_ref, update.thread_ref)
        generation = self._advance_route_generation(
            target.conversation_ref,
            target.thread_ref,
        )
        turn = ActiveTurn(
            turn_id=response.turn_id,
            session_id=response.session_id,
            session_key=response.session_key,
            target=target,
            reply_to_ref=update.message_ref,
            generation=generation,
            visible_after=_loop_time() + _progress_threshold_seconds(update),
        )
        previous_turn_id = self._route_turns.get(target.key)
        if previous_turn_id is not None and previous_turn_id != turn.turn_id:
            previous = self._active_turns.get(previous_turn_id)
            if previous is not None:
                previous.terminal = True
        self._active_turns[turn.turn_id] = turn
        self._route_turns[target.key] = turn.turn_id

    def _advance_route_generation(
        self,
        conversation_ref: str,
        thread_ref: str | None,
    ) -> int:
        key = (conversation_ref, thread_ref)
        generation = self._route_generations.get(key, 0) + 1
        self._route_generations[key] = generation
        return generation

    def _active_turn_for_route(
        self,
        conversation_ref: str,
        thread_ref: str | None,
    ) -> ActiveTurn | None:
        turn_id = self._route_turns.get((conversation_ref, thread_ref))
        if turn_id is None and thread_ref is not None:
            turn_id = self._route_turns.get((conversation_ref, None))
        if turn_id is None:
            return None
        active = self._active_turns.get(turn_id)
        if active is None or active.terminal:
            return None
        return active

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
                    "telegram attachment download failed for event_id=%s "
                    "attachment_id=%s",
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
            except Exception:
                logger.exception(
                    "lionclaw attachment stage failed for event_id=%s attachment_id=%s",
                    update.event_id,
                    attachment.attachment_id,
                )
                return False
            if stage.status == "rejected":
                logger.warning(
                    "lionclaw rejected attachment event_id=%s attachment_id=%s "
                    "reason_code=%s",
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
                "lionclaw attachment finalize returned unexpected outcome '%s' "
                "for event_id=%s",
                response.outcome,
                update.event_id,
            )
            return False
        return True

    async def _process_stream_event(self, event: StreamEvent) -> bool:
        if event.kind == "message_delta":
            self._extend_typing(
                event.peer_id,
                ttl_seconds=ACTIVE_TURN_TYPING_TTL_SECONDS,
            )
            if event.turn_id in self._active_turns:
                self._active_turns[event.turn_id].status_text = "Answering"
            return True

        if event.kind == "status":
            if event.code in TYPING_STATUS_CODES:
                await self._start_typing(
                    event.peer_id,
                    ttl_seconds=ACTIVE_TURN_TYPING_TTL_SECONDS,
                )
            self._update_progress_status(event)
            return True

        if event.kind == "error":
            self._stop_typing(event.peer_id)
            await self._terminalize_progress(event, _terminal_error_text(event))
            logger.error(
                "lionclaw stream error for peer_id=%s turn_id=%s: %s",
                event.peer_id,
                event.turn_id,
                event.text,
            )
            return True

        if event.kind == "turn_completed":
            self._stop_typing(event.peer_id)
            await self._complete_progress(event)
            return True

        if event.kind == "done":
            self._stop_typing(event.peer_id)
            await self._finalize_progress_done(event)
            return True

        logger.error("lionclaw stream contained unknown event kind '%s'", event.kind)
        return True

    async def refresh_typing(self) -> None:
        now = _loop_time()
        active_targets = [
            self._typing_targets[key]
            for key, deadline in self._typing_deadlines.items()
            if deadline > now
        ]
        expired_keys = [
            key for key, deadline in self._typing_deadlines.items() if deadline <= now
        ]
        for key in expired_keys:
            self._typing_targets.pop(key, None)
            self._typing_deadlines.pop(key, None)
            self._typing_failures.pop(key, None)
        for target in active_targets:
            await self._send_typing(target)

    async def _start_typing(
        self,
        conversation_ref: str,
        *,
        thread_ref: str | None = None,
        ttl_seconds: float,
    ) -> None:
        target = self._target_for_ref(conversation_ref, thread_ref=thread_ref)
        self._extend_target_typing(target, ttl_seconds=ttl_seconds)
        await self._send_typing(target)

    def _extend_typing(self, peer_id: str, *, ttl_seconds: float) -> None:
        target = self._target_for_ref(peer_id)
        self._extend_target_typing(target, ttl_seconds=ttl_seconds)

    def _extend_target_typing(
        self, target: TypingTarget, *, ttl_seconds: float
    ) -> None:
        if not target.conversation_ref:
            return
        self._typing_targets[target.key] = target
        deadline = _loop_time() + ttl_seconds
        self._typing_deadlines[target.key] = max(
            deadline,
            self._typing_deadlines.get(target.key, 0.0),
        )

    def _stop_typing(self, peer_id: str) -> None:
        target = self._target_for_ref(peer_id)
        self._typing_targets.pop(target.key, None)
        self._typing_deadlines.pop(target.key, None)
        self._typing_failures.pop(target.key, None)

    async def _send_typing(self, target: TypingTarget) -> None:
        try:
            await self.telegram.send_typing(target.conversation_ref, target.thread_ref)
            self._typing_failures.pop(target.key, None)
        except Exception as err:
            failures = self._typing_failures.get(target.key, 0) + 1
            if isinstance(err, (TelegramForbiddenError, TelegramUnauthorizedError)):
                failures = MAX_TYPING_FAILURES
            self._typing_failures[target.key] = failures
            if failures >= MAX_TYPING_FAILURES:
                self._typing_targets.pop(target.key, None)
                self._typing_deadlines.pop(target.key, None)
            logger.exception(
                "telegram sendChatAction request failed for conversation_ref=%s "
                "thread_ref=%s",
                target.conversation_ref,
                target.thread_ref,
            )

    async def refresh_progress_messages(self) -> None:
        for turn in list(self._active_turns.values()):
            if turn.terminal:
                continue
            if _loop_time() >= turn.visible_after:
                await self._ensure_progress_message(turn)
            if (
                turn.terminal_text is not None
                and turn.last_rendered_text == turn.terminal_text
            ):
                turn.terminal = True
                self._forget_turn(turn)

    async def _ensure_progress_message(
        self,
        turn: ActiveTurn,
        *,
        force: bool = False,
    ) -> None:
        if not self._is_current_generation(turn):
            return
        text = _progress_text(turn)
        if turn.provisional_message_ref is None:
            try:
                receipt = await self.telegram.send_message(
                    turn.target.conversation_ref,
                    text,
                    reply_to_ref=turn.reply_to_ref,
                    thread_ref=turn.target.thread_ref,
                )
            except Exception:
                logger.exception(
                    "telegram progress message send failed for turn_id=%s",
                    turn.turn_id,
                )
                return
            turn.provisional_message_ref = _receipt_message_ref(receipt)
            turn.last_rendered_text = text
            turn.last_edit_at = _loop_time()
            return
        await self._edit_progress_message(turn, text, force=force)

    async def _edit_progress_message(
        self,
        turn: ActiveTurn,
        text: str,
        *,
        force: bool = False,
    ) -> bool:
        if (
            not turn.can_edit
            or turn.provisional_message_ref is None
            or not self._is_current_generation(turn)
        ):
            return False
        now = _loop_time()
        if not force and now - turn.last_edit_at < PROGRESS_EDIT_MIN_SECONDS:
            return False
        if text == turn.last_rendered_text:
            return True
        try:
            await self.telegram.edit_message(
                turn.target.conversation_ref,
                turn.provisional_message_ref,
                text,
            )
        except Exception as err:
            if _is_noop_edit_failure(err):
                turn.last_rendered_text = text
                turn.last_edit_at = now
                return True
            if _is_permanent_edit_failure(err):
                turn.can_edit = False
            logger.exception(
                "telegram progress message edit failed for turn_id=%s",
                turn.turn_id,
            )
            return False
        turn.last_rendered_text = text
        turn.last_edit_at = now
        return True

    async def _delete_progress_message(self, turn: ActiveTurn) -> None:
        if turn.provisional_message_ref is None or not self._is_current_generation(
            turn
        ):
            return
        try:
            await self.telegram.delete_message(
                turn.target.conversation_ref,
                turn.provisional_message_ref,
            )
        except Exception:
            logger.exception(
                "telegram progress message delete failed for turn_id=%s",
                turn.turn_id,
            )

    def _update_progress_status(self, event: StreamEvent) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        if event.code in ACTIVE_STATUS_CODES:
            turn.status_text = ACTIVE_STATUS_CODES[event.code]
        elif event.code in CANCELLED_STATUS_CODES:
            turn.status_text = "Stopped"
            turn.terminal_text = "Stopped."
        elif event.code in COMPLETED_STATUS_CODES:
            turn.status_text = "Finishing"
        elif event.text:
            turn.status_text = _compact_status_text(event.text)

    async def _terminalize_progress(self, event: StreamEvent, text: str) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        turn.terminal_text = text
        await self._ensure_progress_message(turn, force=True)
        edited = await self._edit_progress_message(turn, text, force=True)
        if edited:
            turn.terminal = True
            self._forget_turn(turn)
            return
        if (
            not edited
            and not turn.can_edit
            and turn.provisional_message_ref is not None
        ):
            await self._send_progress_fallback(turn, text)
            turn.terminal = True
            self._forget_turn(turn)

    async def _send_progress_fallback(self, turn: ActiveTurn, text: str) -> None:
        try:
            await self.telegram.send_message(
                turn.target.conversation_ref,
                text,
                reply_to_ref=turn.reply_to_ref,
                thread_ref=turn.target.thread_ref,
            )
        except Exception:
            logger.exception(
                "telegram progress fallback send failed for turn_id=%s",
                turn.turn_id,
            )

    async def _complete_progress(self, event: StreamEvent) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        turn.status_text = "Finishing"
        if turn.provisional_message_ref is not None:
            await self._edit_progress_message(turn, _progress_text(turn), force=True)

    async def _finalize_progress_done(self, event: StreamEvent) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        if turn.terminal_text is not None:
            await self._terminalize_progress(event, turn.terminal_text)
            return
        if turn.provisional_message_ref is not None:
            turn.status_text = "Finishing"
            await self._edit_progress_message(turn, _progress_text(turn), force=True)

    def _forget_turn(self, turn: ActiveTurn) -> None:
        self._active_turns.pop(turn.turn_id, None)
        if self._route_turns.get(turn.target.key) == turn.turn_id:
            self._route_turns.pop(turn.target.key, None)

    def _is_current_generation(self, turn: ActiveTurn) -> bool:
        return self._route_generations.get(turn.target.key) == turn.generation

    def _remember_typing_route(self, update: TelegramInboundUpdate) -> None:
        target = TypingTarget(update.conversation_ref, update.thread_ref)
        self._typing_routes[update.conversation_ref] = target
        if update.provider_metadata.get("chat_type") == "private":
            self._typing_routes[update.sender_ref] = target

    def _target_for_ref(
        self,
        conversation_ref: str,
        *,
        thread_ref: str | None = None,
    ) -> TypingTarget:
        if thread_ref is not None:
            return TypingTarget(conversation_ref, thread_ref)
        return self._typing_routes.get(conversation_ref) or TypingTarget(
            conversation_ref
        )

    async def _process_outbox_delivery(self, delivery: OutboxDelivery) -> None:
        try:
            receipt = await self.telegram.send_message(
                delivery.conversation_ref,
                delivery.content.text,
                delivery.reply_to_ref,
                delivery.thread_ref,
                format_hint=delivery.content.format_hint,
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
                "telegram sendMessage request failed for delivery_id=%s "
                "conversation_ref=%s",
                delivery.delivery_id,
                delivery.conversation_ref,
            )
            outcome, error_code = _classify_send_failure(err)
            self._record_delivery_failure(outcome, error_code)
            await self._report_outbox_with_retry(
                delivery,
                outcome,
                error_code=error_code,
                error_text=str(err),
            )
            return

        await self._complete_progress_for_delivery(delivery)
        await self._report_outbox_with_retry(
            delivery,
            "delivered",
            provider_receipt=receipt,
        )

    async def _complete_progress_for_delivery(self, delivery: OutboxDelivery) -> None:
        turn = self._active_turn_for_route(
            delivery.conversation_ref,
            delivery.thread_ref,
        )
        if turn is None:
            return
        if turn.provisional_message_ref is not None:
            await self._delete_progress_message(turn)
        turn.terminal = True
        self._forget_turn(turn)

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
                "lionclaw rejected stale outbox report for delivery_id=%s "
                "status=%s attempt_status=%s",
                delivery.delivery_id,
                response.status,
                response.attempt_status,
            )
        return True

    async def _health_checks(self) -> list[HealthCheck]:
        checks = [await self._bot_identity_health_check()]
        checks.append(self._polling_health_check())
        checks.append(self._update_lag_health_check())
        checks.append(self._delivery_errors_health_check())
        return checks

    async def _bot_identity_health_check(self) -> HealthCheck:
        try:
            identity = await self.telegram.bot_identity(refresh=True)
        except Exception as err:
            logger.exception("telegram bot identity health check failed")
            return HealthCheck(
                code="telegram.bot_identity",
                status="error",
                message="getMe failed",
                details={"error": str(err)},
            )
        if identity.user_id is None:
            return HealthCheck(
                code="telegram.bot_identity",
                status="warning",
                message="getMe succeeded without a bot id",
                details={"username": identity.username},
            )
        return HealthCheck(
            code="telegram.bot_identity",
            status="ok",
            message="getMe succeeded",
            details={"bot_id": identity.user_id, "username": identity.username},
        )

    def _polling_health_check(self) -> HealthCheck:
        details: dict[str, object] = {
            "offset": self.offset,
            "poll_timeout_seconds": self.config.telegram_poll_timeout_secs,
        }
        if self._last_poll_success_observed_at is not None:
            details["last_success_age_seconds"] = max(
                0, int(_loop_time() - self._last_poll_success_observed_at)
            )
            details["last_update_count"] = self._last_poll_update_count or 0

        if self._last_poll_error is not None:
            if self._last_poll_failure_observed_at is not None:
                details["last_failure_age_seconds"] = max(
                    0, int(_loop_time() - self._last_poll_failure_observed_at)
                )
            details["error"] = self._last_poll_error
            return HealthCheck(
                code="telegram.polling",
                status="error",
                message="getUpdates failed",
                details=details,
            )

        if self._poll_in_flight_started_at is not None:
            in_flight_age_seconds = max(
                0, int(_loop_time() - self._poll_in_flight_started_at)
            )
            details["in_flight_age_seconds"] = in_flight_age_seconds
            max_expected_age = (
                self.config.telegram_poll_timeout_secs + POLLING_IN_FLIGHT_GRACE_SECONDS
            )
            if in_flight_age_seconds > max_expected_age:
                return HealthCheck(
                    code="telegram.polling",
                    status="warning",
                    message="getUpdates request has exceeded its timeout window",
                    details=details,
                )
            return HealthCheck(
                code="telegram.polling",
                status="ok",
                message="getUpdates request is in flight",
                details=details,
            )

        if self._last_poll_success_observed_at is None:
            return HealthCheck(
                code="telegram.polling",
                status="warning",
                message="getUpdates has not completed yet",
                details=details,
            )

        return HealthCheck(
            code="telegram.polling",
            status="ok",
            message="getUpdates succeeded",
            details=details,
        )

    def _update_lag_health_check(self) -> HealthCheck:
        details: dict[str, object] = {"offset": self.offset}
        if self._last_poll_success_observed_at is None:
            return HealthCheck(
                code="telegram.update_lag",
                status="warning",
                message="no successful getUpdates poll completed yet",
                details=details,
            )
        if self._last_update_id is None or self._last_update_observed_at is None:
            return HealthCheck(
                code="telegram.update_lag",
                status="ok",
                message="polling active; no updates observed yet",
                details=details,
            )
        age_seconds = max(0, int(_loop_time() - self._last_update_observed_at))
        details.update(
            {
                "last_update_id": self._last_update_id,
                "last_update_age_seconds": age_seconds,
            }
        )
        return HealthCheck(
            code="telegram.update_lag",
            status="ok",
            message=f"last update observed {age_seconds}s ago",
            details=details,
        )

    def _delivery_errors_health_check(self) -> HealthCheck:
        retryable = self._delivery_failure_counts.get("retryable_failed", 0)
        terminal = self._delivery_failure_counts.get("terminal_failed", 0)
        total = retryable + terminal
        status = "warning" if total else "ok"
        message = (
            f"{total} delivery failure(s) observed in this worker process"
            if total
            else "no delivery failures observed in this worker process"
        )
        return HealthCheck(
            code="telegram.delivery_errors",
            status=status,
            message=message,
            details={
                "retryable_failed": retryable,
                "terminal_failed": terminal,
            },
        )

    def _record_delivery_failure(self, outcome: str, error_code: str) -> None:
        self._delivery_failure_counts[outcome] = (
            self._delivery_failure_counts.get(outcome, 0) + 1
        )
        self._delivery_failure_counts[error_code] = (
            self._delivery_failure_counts.get(error_code, 0) + 1
        )

    def _record_poll_success(self, update_count: int) -> None:
        self._poll_in_flight_started_at = None
        self._last_poll_success_observed_at = _loop_time()
        self._last_poll_update_count = update_count
        self._last_poll_error = None

    def _record_poll_failure(self, err: Exception) -> None:
        self._poll_in_flight_started_at = None
        self._last_poll_failure_observed_at = _loop_time()
        self._last_poll_error = f"{type(err).__name__}: {err}"


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


def _telegram_command(update: TelegramInboundUpdate) -> TelegramCommand | None:
    text = update.text
    if text is None or not text.startswith("/"):
        return None
    token, _, arguments = text.partition(" ")
    command = token.removeprefix("/")
    if "@" in command:
        command, _ = command.split("@", 1)
    command = command.casefold()
    if not command:
        return None
    return TelegramCommand(name=command, raw=token, arguments=arguments.strip())


def _telegram_command_is_addressed(update: TelegramInboundUpdate) -> bool:
    if update.provider_metadata.get("chat_type") == "private":
        return True
    return update.trigger in {"mention", "reply_to_bot", "thread_continuation"}


def _progress_threshold_seconds(update: TelegramInboundUpdate) -> float:
    if update.provider_metadata.get("chat_type") == "private":
        return DM_PROGRESS_THRESHOLD_SECONDS
    return GROUP_PROGRESS_THRESHOLD_SECONDS


def _progress_text(turn: ActiveTurn) -> str:
    if turn.terminal_text is not None:
        return turn.terminal_text
    return f"{turn.status_text}..."


def _terminal_error_text(event: StreamEvent) -> str:
    if event.code in CANCELLED_STATUS_CODES:
        return "Stopped."
    if event.text:
        return f"Turn failed: {_compact_status_text(event.text)}"
    return "Turn failed."


def _compact_status_text(text: str) -> str:
    compact = " ".join(text.strip().split())
    if len(compact) <= 96:
        return compact
    return f"{compact[:93]}..."


def _receipt_message_ref(receipt: dict[str, object]) -> str:
    message_id = receipt.get("message_id")
    if message_id is None:
        raise TelegramReferenceError("telegram send response missing message_id")
    return f"telegram:message:{message_id}"


def _is_noop_edit_failure(err: Exception) -> bool:
    return isinstance(err, TelegramBadRequest) and "not modified" in str(err).lower()


def _is_permanent_edit_failure(err: Exception) -> bool:
    if isinstance(
        err,
        (
            TelegramForbiddenError,
            TelegramNotFound,
            TelegramUnauthorizedError,
        ),
    ):
        return True
    if isinstance(err, TelegramBadRequest):
        message = str(err).lower()
        return any(
            phrase in message
            for phrase in (
                "message to edit not found",
                "message can't be edited",
                "message is not modified",
                "message_id_invalid",
                "not enough rights",
            )
        )
    return False


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


def _overall_health_status(checks: list[HealthCheck]) -> str:
    statuses = {check.status for check in checks}
    if "error" in statuses:
        return "error"
    if "warning" in statuses:
        return "warning"
    return "ok"


def _loop_time() -> float:
    return asyncio.get_running_loop().time()


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
        try:
            await telegram.configure_commands()
        except Exception:
            logger.exception("telegram command menu configuration failed")
        await worker.run_forever()
    finally:
        await telegram.close()
        await lionclaw_api.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(run())
