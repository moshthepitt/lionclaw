from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass, replace
from enum import Enum
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
from aiogram.types import Update

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
    TelegramActionButton,
    TelegramBotIdentity,
    TelegramCallbackAction,
    TelegramEntityTooLargeForStage,
    TelegramInboundEvent,
    TelegramInboundUpdate,
    TelegramOutboundAttachment,
    TelegramPairingClaim,
    TelegramPartialSendError,
    TelegramReferenceError,
    TelegramTransport,
    extract_inbound_event,
    normalize_telegram_receipt,
)
from lionclaw_channel_telegram.webhook import TelegramWebhookServer

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
PROGRESS_DELETE_RETRY_BASE_SECONDS = 1.0
PROGRESS_DELETE_RETRY_MAX_SECONDS = 30.0
COMPACT_STATUS_TEXT_LIMIT = 96
MAX_OUTBOX_RECEIPTS = 256
OUTBOX_RECEIPT_STATUSES = {"delivered", "partial"}
MAX_PENDING_PROGRESS_DELETES = 256
TEXT_BURST_MAX_SECONDS = 10
WEBHOOK_COALESCE_DELAY_SECONDS = 0.35

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
CALLBACK_PREFIX = "lc1"
CALLBACK_ACTION_CODES = {
    "stop": "s",
    "status": "t",
    "new": "n",
    "retry": "r",
    "continue": "c",
}
CALLBACK_CODE_ACTIONS = {code: action for action, code in CALLBACK_ACTION_CODES.items()}
CALLBACK_ROUTE_TARGET = "_"
REACTION_RECEIVED = "👀"
REACTION_COMPLETED = "✅"
REACTION_STOPPED = "👌"
REACTION_FAILED = "😢"


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
    expects_outbox_delivery: bool = False


@dataclass(slots=True)
class ActiveTurnStore:
    path: Path

    def load(self) -> list[ActiveTurn]:
        if not self.path.exists():
            return []
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("telegram active turn store load failed")
            return []
        if not isinstance(raw, dict):
            logger.warning("telegram active turn store ignored non-object payload")
            return []
        items = raw.get("active_turns", [])
        if not isinstance(items, list):
            logger.warning(
                "telegram active turn store ignored non-array active_turns payload"
            )
            return []
        active: list[ActiveTurn] = []
        for value in items:
            turn = _coerce_active_turn(value)
            if turn is not None:
                active.append(turn)
        return active

    def save(self, active_turns: dict[str, ActiveTurn]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f".{self.path.name}.tmp")
        turns = [
            _active_turn_to_json(active_turns[turn_id])
            for turn_id in sorted(active_turns)
        ]
        tmp_path.write_text(
            json.dumps({"active_turns": turns}, sort_keys=True),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)


@dataclass(slots=True)
class PendingProgressDelete:
    turn_id: str
    conversation_ref: str
    message_ref: str
    attempts: int = 0
    next_attempt_at: float = 0.0

    @property
    def key(self) -> tuple[str, str]:
        return (self.conversation_ref, self.message_ref)

    def to_json(self) -> dict[str, object]:
        return {
            "turn_id": self.turn_id,
            "conversation_ref": self.conversation_ref,
            "message_ref": self.message_ref,
            "attempts": self.attempts,
        }


class ProgressRenderResult(Enum):
    RENDERED = "rendered"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"


@dataclass(slots=True, frozen=True)
class TelegramCommand:
    name: str
    raw: str
    arguments: str
    text: str
    target_username: str | None = None


@dataclass(slots=True, frozen=True)
class ParsedCallbackAction:
    action: str
    target: str
    mac: str


@dataclass(slots=True, frozen=True)
class ProviderWorkItem:
    update_ids: tuple[int, ...]
    event: TelegramInboundEvent | None


WebhookRouteKey = tuple[str, str | None]
WebhookBatchKey = tuple[str, str | None, str, str, str, str]


@dataclass(slots=True)
class WebhookBatch:
    key: WebhookBatchKey
    items: list[ProviderWorkItem]
    result: asyncio.Future[bool]


@dataclass(slots=True)
class WebhookRouteGate:
    lock: asyncio.Lock
    users: int = 0


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
        tmp_path = self.path.with_name(f".{self.path.name}.tmp")
        tmp_path.write_text(str(offset), encoding="utf-8")
        tmp_path.replace(self.path)


@dataclass(slots=True, frozen=True)
class OutboxReceiptRecord:
    status: str
    provider_receipt: dict[str, object]

    def to_json(self) -> dict[str, object]:
        if self.status not in OUTBOX_RECEIPT_STATUSES:
            raise ValueError(f"invalid outbox receipt status '{self.status}'")
        return {
            "status": self.status,
            "provider_receipt": self.provider_receipt,
        }


@dataclass(slots=True)
class OutboxReceiptStore:
    path: Path

    def load(self) -> dict[str, OutboxReceiptRecord]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("telegram outbox receipt store load failed")
            return {}
        if not isinstance(raw, dict):
            logger.warning("telegram outbox receipt store ignored non-object payload")
            return {}
        receipts: dict[str, OutboxReceiptRecord] = {}
        for delivery_id, value in raw.items():
            if not isinstance(delivery_id, str):
                continue
            record = _coerce_outbox_receipt_record(value)
            if record is not None:
                receipts[delivery_id] = record
        while len(receipts) > MAX_OUTBOX_RECEIPTS:
            receipts.pop(next(iter(receipts)))
        return receipts

    def save(self, receipts: dict[str, OutboxReceiptRecord]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f".{self.path.name}.tmp")
        tmp_path.write_text(
            json.dumps(
                {
                    delivery_id: record.to_json()
                    for delivery_id, record in receipts.items()
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)


@dataclass(slots=True)
class ProgressDeleteStore:
    path: Path

    def load(self) -> dict[tuple[str, str], PendingProgressDelete]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.exception("telegram progress delete store load failed")
            return {}
        if not isinstance(raw, dict):
            logger.warning("telegram progress delete store ignored non-object payload")
            return {}
        items = raw.get("pending", [])
        if not isinstance(items, list):
            logger.warning(
                "telegram progress delete store ignored non-array pending payload"
            )
            return {}
        pending: dict[tuple[str, str], PendingProgressDelete] = {}
        for value in items:
            delete = _coerce_pending_progress_delete(value)
            if delete is not None:
                pending[delete.key] = delete
        while len(pending) > MAX_PENDING_PROGRESS_DELETES:
            pending.pop(next(iter(pending)))
        return pending

    def save(self, pending: dict[tuple[str, str], PendingProgressDelete]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f".{self.path.name}.tmp")
        tmp_path.write_text(
            json.dumps(
                {
                    "pending": [pending[key].to_json() for key in sorted(pending)],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        tmp_path.replace(self.path)


def _active_turn_to_json(turn: ActiveTurn) -> dict[str, object]:
    now_epoch = time.time()
    now_loop = _safe_loop_time()
    visible_after_epoch = now_epoch + max(0.0, turn.visible_after - now_loop)
    last_edit_epoch = 0.0
    if turn.last_edit_at > 0.0:
        last_edit_epoch = now_epoch - max(0.0, now_loop - turn.last_edit_at)
    return {
        "turn_id": turn.turn_id,
        "session_id": turn.session_id,
        "session_key": turn.session_key,
        "conversation_ref": turn.target.conversation_ref,
        "thread_ref": turn.target.thread_ref,
        "reply_to_ref": turn.reply_to_ref,
        "generation": turn.generation,
        "visible_after_epoch": visible_after_epoch,
        "status_text": turn.status_text,
        "provisional_message_ref": turn.provisional_message_ref,
        "last_rendered_text": turn.last_rendered_text,
        "last_edit_epoch": last_edit_epoch,
        "can_edit": turn.can_edit,
        "terminal_text": turn.terminal_text,
        "expects_outbox_delivery": turn.expects_outbox_delivery,
    }


def _coerce_active_turn(value: object) -> ActiveTurn | None:
    if not isinstance(value, dict):
        return None
    turn_id = value.get("turn_id")
    conversation_ref = value.get("conversation_ref")
    generation = value.get("generation")
    visible_after_epoch = value.get("visible_after_epoch", 0.0)
    status_text = value.get("status_text", "Queued")
    can_edit = value.get("can_edit", True)
    expects_outbox_delivery = value.get("expects_outbox_delivery", False)
    if (
        not isinstance(turn_id, str)
        or not turn_id
        or not isinstance(conversation_ref, str)
        or not conversation_ref
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation <= 0
        or not _is_json_number(visible_after_epoch)
        or not isinstance(status_text, str)
        or not status_text
        or not isinstance(can_edit, bool)
        or not isinstance(expects_outbox_delivery, bool)
    ):
        return None
    session_id = _optional_string(value.get("session_id"))
    session_key = _optional_string(value.get("session_key"))
    thread_ref = _optional_string(value.get("thread_ref"))
    reply_to_ref = _optional_string(value.get("reply_to_ref"))
    provisional_message_ref = _optional_string(value.get("provisional_message_ref"))
    last_rendered_text = _optional_string(value.get("last_rendered_text"))
    terminal_text = _optional_string(value.get("terminal_text"))
    visible_after = _epoch_to_loop_time(float(visible_after_epoch))
    last_edit_epoch = value.get("last_edit_epoch", 0.0)
    last_edit_at = (
        _epoch_to_loop_time(float(last_edit_epoch))
        if _is_json_number(last_edit_epoch) and float(last_edit_epoch) > 0.0
        else 0.0
    )
    return ActiveTurn(
        turn_id=turn_id,
        session_id=session_id,
        session_key=session_key,
        target=TypingTarget(conversation_ref, thread_ref),
        reply_to_ref=reply_to_ref,
        generation=generation,
        visible_after=visible_after,
        status_text=status_text,
        provisional_message_ref=provisional_message_ref,
        last_rendered_text=last_rendered_text,
        last_edit_at=last_edit_at,
        can_edit=can_edit,
        terminal_text=terminal_text,
        expects_outbox_delivery=expects_outbox_delivery,
    )


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return None


def _is_json_number(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _epoch_to_loop_time(epoch_seconds: float) -> float:
    return _safe_loop_time() + max(0.0, epoch_seconds - time.time())


def _coerce_pending_progress_delete(
    value: object,
) -> PendingProgressDelete | None:
    if not isinstance(value, dict):
        return None
    turn_id = value.get("turn_id")
    conversation_ref = value.get("conversation_ref")
    message_ref = value.get("message_ref")
    attempts = value.get("attempts", 0)
    if (
        not isinstance(turn_id, str)
        or not turn_id
        or not isinstance(conversation_ref, str)
        or not conversation_ref
        or not isinstance(message_ref, str)
        or not message_ref
        or isinstance(attempts, bool)
        or not isinstance(attempts, int)
        or attempts < 0
    ):
        return None
    return PendingProgressDelete(
        turn_id=turn_id,
        conversation_ref=conversation_ref,
        message_ref=message_ref,
        attempts=min(attempts, MAX_PENDING_PROGRESS_DELETES),
        next_attempt_at=0.0,
    )


def _coerce_outbox_receipt_record(value: object) -> OutboxReceiptRecord | None:
    if not isinstance(value, dict):
        return None

    status = value.get("status")
    provider_receipt = value.get("provider_receipt")
    if isinstance(status, str) and isinstance(provider_receipt, dict):
        if status in OUTBOX_RECEIPT_STATUSES:
            normalized = _normalize_outbox_provider_receipt(provider_receipt)
            if normalized is None:
                return None
            return OutboxReceiptRecord(
                status=status,
                provider_receipt=normalized,
            )
        return None

    normalized = _normalize_outbox_provider_receipt(value)
    if normalized is not None:
        return OutboxReceiptRecord(status="delivered", provider_receipt=normalized)

    return None


def _normalize_outbox_provider_receipt(
    provider_receipt: dict[str, object],
) -> dict[str, object] | None:
    try:
        return normalize_telegram_receipt(provider_receipt)
    except TelegramReferenceError:
        return None


class TelegramWorker:
    def __init__(
        self,
        config: WorkerConfig,
        lionclaw_api: LionClawApi,
        telegram: TelegramTransport,
        offset_store: OffsetStore,
        active_turn_store: ActiveTurnStore | None = None,
        outbox_receipt_store: OutboxReceiptStore | None = None,
        progress_delete_store: ProgressDeleteStore | None = None,
    ) -> None:
        self.config = config
        self.lionclaw_api = lionclaw_api
        self.telegram = telegram
        self.offset_store = offset_store
        self.offset = offset_store.load()
        self.active_turn_store = active_turn_store
        self.outbox_receipt_store = outbox_receipt_store
        self._outbox_receipts = (
            outbox_receipt_store.load() if outbox_receipt_store is not None else {}
        )
        self.progress_delete_store = progress_delete_store
        self._typing_targets: dict[tuple[str, str | None], TypingTarget] = {}
        self._typing_deadlines: dict[tuple[str, str | None], float] = {}
        self._typing_routes: dict[str, TypingTarget] = {}
        self._typing_failures: dict[tuple[str, str | None], int] = {}
        loaded_active_turns = (
            active_turn_store.load() if active_turn_store is not None else []
        )
        self._active_turns: dict[str, ActiveTurn] = {
            turn.turn_id: turn for turn in loaded_active_turns if not turn.terminal
        }
        self._pending_progress_deletes: dict[tuple[str, str], PendingProgressDelete] = (
            progress_delete_store.load() if progress_delete_store is not None else {}
        )
        self._route_turns: dict[tuple[str, str | None], str] = {
            turn.target.key: turn.turn_id for turn in self._active_turns.values()
        }
        self._route_generations: dict[tuple[str, str | None], int] = {}
        for turn in self._active_turns.values():
            self._route_generations[turn.target.key] = max(
                turn.generation,
                self._route_generations.get(turn.target.key, 0),
            )
        self._poll_in_flight_started_at: float | None = None
        self._last_poll_success_observed_at: float | None = None
        self._last_poll_failure_observed_at: float | None = None
        self._last_poll_error: str | None = None
        self._last_poll_update_count: int | None = None
        self._last_webhook_success_observed_at: float | None = None
        self._last_webhook_failure_observed_at: float | None = None
        self._last_webhook_error: str | None = None
        self._last_update_id: int | None = None
        self._last_update_observed_at: float | None = None
        self._update_extraction_failure_count = 0
        self._delivery_failure_counts: dict[str, int] = {}
        self._webhook_batches: dict[WebhookBatchKey, WebhookBatch] = {}
        self._webhook_route_gates: dict[WebhookRouteKey, WebhookRouteGate] = {}
        self._webhook_state_lock = asyncio.Lock()

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

        work_items = []
        for update in updates:
            event = self._extract_provider_event(update, bot_identity)
            work_items.append(ProviderWorkItem((update.update_id,), event))

        await self._process_provider_work_items(work_items, persist_offsets=True)

    async def process_webhook_update(self, update: Update) -> bool:
        try:
            bot_identity = await self.telegram.bot_identity()
        except Exception as err:
            self._record_webhook_failure(err)
            logger.exception("telegram getMe request failed for webhook update")
            return False

        item = ProviderWorkItem(
            (update.update_id,),
            self._extract_provider_event(update, bot_identity),
        )
        processed = await self._process_webhook_work_item(item)
        if processed:
            self._record_webhook_success()
        else:
            self._record_webhook_failure(
                RuntimeError("webhook update processing failed")
            )
        return processed

    async def _process_webhook_work_item(self, item: ProviderWorkItem) -> bool:
        key = _webhook_batch_key(item.event)
        if key is None:
            if not await self._flush_webhook_batches_for_event(item.event):
                return False
            route = _webhook_event_route(item.event)
            if route is not None:
                return await self._process_webhook_route_work_items(route, [item])
            return await self._process_provider_work_items(
                [item], persist_offsets=False
            )

        future, should_schedule = await self._enqueue_webhook_batch(key, item)
        if should_schedule:
            asyncio.create_task(
                self._flush_webhook_batch(key, WEBHOOK_COALESCE_DELAY_SECONDS),
                name="telegram-webhook-batch",
            )
        return await asyncio.shield(future)

    async def _process_webhook_route_work_items(
        self,
        route: WebhookRouteKey,
        work_items: list[ProviderWorkItem],
    ) -> bool:
        gate = await self._acquire_webhook_route_gate(route)
        try:
            return await self._process_provider_work_items(
                work_items,
                persist_offsets=False,
            )
        finally:
            await self._release_webhook_route_gate(route, gate)

    async def _acquire_webhook_route_gate(
        self,
        route: WebhookRouteKey,
    ) -> WebhookRouteGate:
        async with self._webhook_state_lock:
            gate = self._webhook_route_gates.get(route)
            if gate is None:
                gate = WebhookRouteGate(lock=asyncio.Lock())
                self._webhook_route_gates[route] = gate
            gate.users += 1
        try:
            await gate.lock.acquire()
        except asyncio.CancelledError:
            await self._forget_webhook_route_gate_user(route, gate)
            raise
        return gate

    async def _release_webhook_route_gate(
        self,
        route: WebhookRouteKey,
        gate: WebhookRouteGate,
    ) -> None:
        gate.lock.release()
        await self._forget_webhook_route_gate_user(route, gate)

    async def _forget_webhook_route_gate_user(
        self,
        route: WebhookRouteKey,
        gate: WebhookRouteGate,
    ) -> None:
        async with self._webhook_state_lock:
            gate.users -= 1
            if gate.users <= 0 and self._webhook_route_gates.get(route) is gate:
                self._webhook_route_gates.pop(route, None)

    async def _enqueue_webhook_batch(
        self,
        key: WebhookBatchKey,
        item: ProviderWorkItem,
    ) -> tuple[asyncio.Future[bool], bool]:
        async with self._webhook_state_lock:
            batch = self._webhook_batches.get(key)
            should_schedule = False
            if batch is None:
                batch = WebhookBatch(
                    key=key,
                    items=[],
                    result=asyncio.get_running_loop().create_future(),
                )
                self._webhook_batches[key] = batch
                should_schedule = True
            batch.items.append(item)
            route_batches = _sorted_webhook_batches_for_route(
                self._webhook_batches.values(),
                _webhook_batch_route(key),
            )
            if route_batches == [batch]:
                return batch.result, should_schedule
            should_schedule = False

        await self._flush_webhook_batches(route_batches)
        return batch.result, should_schedule

    async def _flush_webhook_batches_for_event(
        self,
        event: TelegramInboundEvent | None,
        *,
        except_key: WebhookBatchKey | None = None,
    ) -> bool:
        route = _webhook_event_route(event)
        if route is None:
            return True
        async with self._webhook_state_lock:
            batches = _sorted_webhook_batches_for_route(
                (
                    batch
                    for key, batch in self._webhook_batches.items()
                    if key != except_key
                ),
                route,
            )
        return await self._flush_webhook_batches(batches)

    async def _flush_webhook_batches(self, batches: list[WebhookBatch]) -> bool:
        for index, batch in enumerate(batches):
            await self._flush_webhook_batch(batch.key, delay_seconds=0.0)
            if not await asyncio.shield(batch.result):
                await self._reject_webhook_batches(batches[index + 1 :])
                return False
        return True

    async def _reject_webhook_batches(self, batches: list[WebhookBatch]) -> None:
        for batch in batches:
            async with self._webhook_state_lock:
                if self._webhook_batches.get(batch.key) is batch:
                    self._webhook_batches.pop(batch.key, None)
            if not batch.result.done():
                batch.result.set_result(False)

    async def _flush_webhook_batch(
        self,
        key: WebhookBatchKey,
        delay_seconds: float,
    ) -> None:
        if delay_seconds > 0.0:
            await asyncio.sleep(delay_seconds)
        async with self._webhook_state_lock:
            batch = self._webhook_batches.pop(key, None)
        if batch is None:
            return
        route = _webhook_batch_route(key)
        gate = await self._acquire_webhook_route_gate(route)
        try:
            if batch.result.done():
                return
            work_items = sorted(batch.items, key=_provider_work_item_sort_key)
            processed = await self._process_provider_work_items(
                work_items,
                persist_offsets=False,
            )
        except Exception:
            logger.exception("telegram webhook batch processing failed")
            processed = False
        finally:
            await self._release_webhook_route_gate(route, gate)
        if not batch.result.done():
            batch.result.set_result(processed)

    async def _process_provider_work_items(
        self,
        work_items: list[ProviderWorkItem],
        *,
        persist_offsets: bool,
    ) -> bool:
        for item in _coalesce_work_items(work_items):
            if item.event is not None:
                if isinstance(item.event, TelegramPairingClaim):
                    if not await self._claim_pairing(item.event):
                        return False
                elif isinstance(item.event, TelegramCallbackAction):
                    if not await self._handle_callback_action(item.event):
                        return False
                elif not await self._submit_inbound(item.event):
                    return False
            if persist_offsets:
                if not self._save_processed_offsets(item.update_ids):
                    return False
            else:
                self._record_processed_updates(item.update_ids)
        return True

    def _save_processed_offsets(self, update_ids: tuple[int, ...]) -> bool:
        for update_id in update_ids:
            self._record_processed_update(update_id)
            next_offset = update_id + 1
            try:
                self.offset_store.save(next_offset)
            except Exception:
                logger.exception(
                    "telegram offset save failed for update_id=%s",
                    update_id,
                )
                return False
            self.offset = next_offset
        return True

    def _record_processed_updates(self, update_ids: tuple[int, ...]) -> None:
        for update_id in update_ids:
            self._record_processed_update(update_id)

    def _record_processed_update(self, update_id: int) -> None:
        self._last_update_id = update_id
        self._last_update_observed_at = _loop_time()

    def _extract_provider_event(
        self,
        update: Update,
        bot_identity: TelegramBotIdentity,
    ) -> TelegramInboundEvent | None:
        try:
            return extract_inbound_event(update, bot_identity=bot_identity)
        except Exception:
            self._update_extraction_failure_count += 1
            logger.exception(
                "telegram update extraction failed for update_id=%s",
                update.update_id,
            )
            return None

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

    async def run_forever(self, *, receive_updates: bool = True) -> None:
        tasks = [
            asyncio.create_task(self._run_stream_loop(), name="lionclaw-stream"),
            asyncio.create_task(self._run_outbox_loop(), name="lionclaw-outbox"),
            asyncio.create_task(self._run_typing_loop(), name="telegram-typing"),
            asyncio.create_task(self._run_progress_loop(), name="telegram-progress"),
            asyncio.create_task(self._run_health_loop(), name="lionclaw-health"),
        ]
        if receive_updates:
            tasks.insert(
                0,
                asyncio.create_task(self._run_update_loop(), name="telegram-updates"),
            )
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
            self._pending_progress_deletes.clear()
            self._route_turns.clear()
            self._route_generations.clear()
            self._outbox_receipts.clear()

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

    async def _handle_callback_action(self, callback: TelegramCallbackAction) -> bool:
        parsed = _parse_callback_action(callback.action)
        if parsed is None:
            await self._answer_callback(callback, "Unknown LionClaw control.")
            return True
        active = self._active_turn_for_callback(callback, parsed)
        if active is None and parsed.target != CALLBACK_ROUTE_TARGET:
            await self._answer_callback(callback, "That control is no longer active.")
            return True
        if not self._callback_mac_matches(callback, parsed, active):
            await self._answer_callback(callback, "That control is no longer valid.")
            return True

        update = _callback_update(callback, text=f"/{parsed.action}")
        if parsed.action == "stop":
            await self._answer_callback(callback, "Stopping")
            await self._stop_active_turn(update)
            return True
        if parsed.action == "status":
            await self._answer_callback(callback, "Status sent")
            await self._send_status(update)
            return True
        if parsed.action in LIONCLAW_CONTROL_ALIASES:
            await self._answer_callback(callback, "Queued")
            return await self._submit_runtime_passthrough(
                replace(update, text=LIONCLAW_CONTROL_ALIASES[parsed.action])
            )

        await self._answer_callback(callback, "Unknown LionClaw control.")
        return True

    def _active_turn_for_callback(
        self,
        callback: TelegramCallbackAction,
        parsed: ParsedCallbackAction,
    ) -> ActiveTurn | None:
        if parsed.target == CALLBACK_ROUTE_TARGET:
            return self._active_turn_for_route(
                callback.conversation_ref,
                callback.thread_ref,
            )
        active = self._active_turns.get(parsed.target)
        if active is None or active.terminal:
            return None
        if active.target.key != (callback.conversation_ref, callback.thread_ref):
            return None
        return active

    def _callback_mac_matches(
        self,
        callback: TelegramCallbackAction,
        parsed: ParsedCallbackAction,
        active: ActiveTurn | None,
    ) -> bool:
        session_key = active.session_key if active is not None else None
        expected = _callback_mac(
            self.config.telegram_bot_token,
            parsed.action,
            parsed.target,
            callback.conversation_ref,
            callback.thread_ref,
            session_key,
        )
        return hmac.compare_digest(parsed.mac, expected)

    async def _answer_callback(
        self,
        callback: TelegramCallbackAction,
        text: str | None = None,
    ) -> None:
        try:
            await self.telegram.answer_callback(callback.callback_query_id, text)
        except Exception:
            logger.exception(
                "telegram callback acknowledgement failed for update_id=%s",
                callback.update_id,
            )

    async def _submit_inbound(self, update: TelegramInboundUpdate) -> bool:
        command = _telegram_command(update)
        if command is not None:
            command_is_addressed = _telegram_command_is_addressed(update, command)
            if command.name in LOCAL_TELEGRAM_COMMANDS and command_is_addressed:
                return await self._handle_telegram_command(update, command)
            if command_is_addressed:
                runtime_text = _telegram_runtime_command_text(command)
                if runtime_text != update.text:
                    update = replace(update, text=runtime_text)

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
            await self._set_inbound_reaction(update, REACTION_RECEIVED)
            await self._notify_pending_approval(update, response.pairing_code)
        if response.outcome == "queued":
            self._remember_typing_route(update)
            await self._remember_active_turn(update, response)
            await self._start_typing(
                update.conversation_ref,
                thread_ref=update.thread_ref,
                ttl_seconds=INITIAL_TYPING_TTL_SECONDS,
            )
        if response.outcome == "waiting_for_attachments":
            await self._remember_active_turn(update, response)
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
            await self._set_inbound_reaction(update, REACTION_RECEIVED)
            await self._notify_pending_approval(update, response.pairing_code)
        if response.outcome == "queued":
            self._remember_typing_route(update)
            await self._remember_active_turn(update, response)
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

        previous_status = active.status_text
        had_provisional = active.provisional_message_ref is not None
        active.status_text = "Stopping"
        await self._ensure_progress_message(active, force=True)
        try:
            result = await self.lionclaw_api.cancel_active_turn(
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
            already_requested = "already requested" in str(err)
            text = (
                "Stop was already requested."
                if already_requested
                else ("I could not stop that turn.")
            )
            if not already_requested:
                active.status_text = previous_status
                if had_provisional:
                    await self._edit_progress_message(
                        active,
                        _progress_text(active),
                        force=True,
                    )
                elif active.provisional_message_ref is not None:
                    await self._delete_progress_message(active)
                    active.provisional_message_ref = None
                    active.last_rendered_text = None
            await self._reply(update, text)
            return
        if result.turn_id is None:
            if active.provisional_message_ref is not None:
                await self._delete_progress_message(active)
            active.terminal = True
            self._forget_turn(active)
            await self._reply(update, "No active LionClaw turn is running here.")

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
            buttons=self._route_control_buttons(update),
        )

    async def _send_help(self, update: TelegramInboundUpdate) -> None:
        await self._reply(
            update,
            "LionClaw controls\n"
            "/status, /new, /stop, /retry, /continue, /settings\n\n"
            "Runtime controls\n"
            "/model and other runtime slash commands go to the runtime.\n"
            "Use a leading space before / to send literal slash text.",
            buttons=self._route_control_buttons(update),
        )

    async def _reply(
        self,
        update: TelegramInboundUpdate,
        text: str,
        *,
        buttons: list[TelegramActionButton] | None = None,
    ) -> None:
        try:
            await self.telegram.send_message(
                update.conversation_ref,
                text,
                reply_to_ref=update.message_ref,
                thread_ref=update.thread_ref,
                buttons=buttons or [],
            )
        except Exception:
            logger.exception(
                "telegram command reply failed for update_id=%s",
                update.update_id,
            )

    async def _set_inbound_reaction(
        self,
        update: TelegramInboundUpdate,
        emoji: str,
    ) -> None:
        if update.message_ref is None:
            return
        await self._set_message_reaction(
            update.conversation_ref,
            update.message_ref,
            emoji,
        )

    async def _set_turn_reaction(self, turn: ActiveTurn, emoji: str) -> None:
        if turn.reply_to_ref is None:
            return
        await self._set_message_reaction(
            turn.target.conversation_ref,
            turn.reply_to_ref,
            emoji,
        )

    async def _set_message_reaction(
        self,
        conversation_ref: str,
        message_ref: str,
        emoji: str,
    ) -> None:
        try:
            await self.telegram.set_reaction(conversation_ref, message_ref, emoji)
        except Exception:
            logger.debug(
                "telegram message reaction failed for conversation_ref=%s "
                "message_ref=%s emoji=%s",
                conversation_ref,
                message_ref,
                emoji,
                exc_info=True,
            )

    def _route_control_buttons(
        self,
        update: TelegramInboundUpdate,
    ) -> list[TelegramActionButton]:
        route = TypingTarget(update.conversation_ref, update.thread_ref)
        return [
            TelegramActionButton(
                "New",
                self._callback_payload("new", route),
            ),
            TelegramActionButton(
                "Retry",
                self._callback_payload("retry", route),
            ),
            TelegramActionButton(
                "Continue",
                self._callback_payload("continue", route),
            ),
        ]

    async def _remember_active_turn(
        self,
        update: TelegramInboundUpdate,
        response: InboundResponse,
    ) -> None:
        if response.turn_id is None:
            return
        target = TypingTarget(update.conversation_ref, update.thread_ref)
        previous_turn_id = self._route_turns.get(target.key)
        if previous_turn_id == response.turn_id:
            existing = self._active_turns.get(response.turn_id)
            if existing is not None and not existing.terminal:
                changed = False
                if existing.session_id is None:
                    existing.session_id = response.session_id
                    changed = True
                if existing.session_key is None:
                    existing.session_key = response.session_key
                    changed = True
                if changed:
                    self._save_active_turns()
                return
        if previous_turn_id is not None and previous_turn_id != response.turn_id:
            previous = self._active_turns.get(previous_turn_id)
            if previous is not None:
                if previous.provisional_message_ref is not None:
                    await self._delete_progress_message(previous)
                previous.terminal = True
                self._forget_turn(previous)
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
        self._active_turns[turn.turn_id] = turn
        self._route_turns[target.key] = turn.turn_id
        self._save_active_turns()
        await self._set_turn_reaction(turn, REACTION_RECEIVED)

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
            self._extend_stream_typing(
                event,
                ttl_seconds=ACTIVE_TURN_TYPING_TTL_SECONDS,
            )
            if event.turn_id in self._active_turns:
                turn = self._active_turns[event.turn_id]
                turn.status_text = "Answering"
                if event.lane == "answer" and event.text.strip():
                    turn.expects_outbox_delivery = True
            return True

        if event.kind == "message_boundary":
            return True

        if event.kind == "status":
            if event.code in TYPING_STATUS_CODES:
                await self._start_stream_typing(
                    event,
                    ttl_seconds=ACTIVE_TURN_TYPING_TTL_SECONDS,
                )
            self._update_progress_status(event)
            return True

        if event.kind == "error":
            self._stop_stream_typing(event)
            await self._terminalize_progress(event, _terminal_error_text(event))
            logger.error(
                "lionclaw stream error for peer_id=%s turn_id=%s: %s",
                event.peer_id,
                event.turn_id,
                event.text,
            )
            return True

        if event.kind == "turn_completed":
            self._stop_stream_typing(event)
            await self._complete_progress(event)
            return True

        if event.kind == "done":
            self._stop_stream_typing(event)
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
        if not target.conversation_ref:
            return
        self._extend_target_typing(target, ttl_seconds=ttl_seconds)
        await self._send_typing(target)

    async def _start_stream_typing(
        self,
        event: StreamEvent,
        *,
        ttl_seconds: float,
    ) -> None:
        target = self._target_for_stream_event(event)
        if not target.conversation_ref:
            return
        self._extend_target_typing(target, ttl_seconds=ttl_seconds)
        await self._send_typing(target)

    def _extend_stream_typing(self, event: StreamEvent, *, ttl_seconds: float) -> None:
        target = self._target_for_stream_event(event)
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

    def _stop_stream_typing(self, event: StreamEvent) -> None:
        self._stop_target_typing(self._target_for_stream_event(event))

    def _stop_target_typing(self, target: TypingTarget) -> None:
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
        await self._retry_pending_progress_deletes()
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
    ) -> ProgressRenderResult:
        if not self._is_current_generation(turn):
            return ProgressRenderResult.TERMINAL_FAILED
        text = _progress_text(turn)
        if turn.provisional_message_ref is None:
            try:
                receipt = await self.telegram.send_message(
                    turn.target.conversation_ref,
                    text,
                    reply_to_ref=turn.reply_to_ref,
                    thread_ref=turn.target.thread_ref,
                    buttons=self._progress_buttons(turn),
                )
            except Exception as err:
                logger.exception(
                    "telegram progress message send failed for turn_id=%s",
                    turn.turn_id,
                )
                if _is_permanent_send_failure(err):
                    return ProgressRenderResult.TERMINAL_FAILED
                return ProgressRenderResult.RETRYABLE_FAILED
            turn.provisional_message_ref = _receipt_message_ref(receipt)
            turn.last_rendered_text = text
            turn.last_edit_at = _loop_time()
            self._save_active_turns()
            return ProgressRenderResult.RENDERED
        edited = await self._edit_progress_message(turn, text, force=force)
        if edited:
            return ProgressRenderResult.RENDERED
        if turn.can_edit:
            return ProgressRenderResult.RETRYABLE_FAILED
        return ProgressRenderResult.TERMINAL_FAILED

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
                buttons=self._progress_buttons(turn),
            )
        except Exception as err:
            if _is_noop_edit_failure(err):
                turn.last_rendered_text = text
                turn.last_edit_at = now
                self._save_active_turns()
                return True
            if _is_permanent_edit_failure(err):
                turn.can_edit = False
                self._save_active_turns()
            logger.exception(
                "telegram progress message edit failed for turn_id=%s",
                turn.turn_id,
            )
            return False
        turn.last_rendered_text = text
        turn.last_edit_at = now
        self._save_active_turns()
        return True

    async def _delete_progress_message(self, turn: ActiveTurn) -> bool:
        if turn.provisional_message_ref is None:
            return True
        pending = PendingProgressDelete(
            turn_id=turn.turn_id,
            conversation_ref=turn.target.conversation_ref,
            message_ref=turn.provisional_message_ref,
        )
        deleted = await self._delete_progress_ref(pending)
        if deleted:
            turn.provisional_message_ref = None
            turn.last_rendered_text = None
            self._save_active_turns()
            return True
        self._schedule_progress_delete(pending)
        return False

    async def _retry_pending_progress_deletes(self) -> None:
        now = _loop_time()
        for pending in list(self._pending_progress_deletes.values()):
            if pending.next_attempt_at > now:
                continue
            deleted = await self._delete_progress_ref(pending)
            if deleted:
                self._forget_pending_progress_delete(pending.key)
            else:
                self._schedule_progress_delete(pending)

    async def _delete_progress_ref(self, pending: PendingProgressDelete) -> bool:
        try:
            await self.telegram.delete_message(
                pending.conversation_ref,
                pending.message_ref,
            )
        except Exception as err:
            if _is_permanent_delete_failure(err):
                logger.warning(
                    "telegram progress message delete abandoned for turn_id=%s: %s",
                    pending.turn_id,
                    err,
                )
                return True
            logger.exception(
                "telegram progress message delete failed for turn_id=%s",
                pending.turn_id,
            )
            return False
        return True

    def _schedule_progress_delete(self, pending: PendingProgressDelete) -> None:
        queued = self._pending_progress_deletes.setdefault(pending.key, pending)
        queued.attempts += 1
        delay_seconds = min(
            PROGRESS_DELETE_RETRY_MAX_SECONDS,
            PROGRESS_DELETE_RETRY_BASE_SECONDS * (2 ** min(queued.attempts - 1, 5)),
        )
        queued.next_attempt_at = _loop_time() + delay_seconds
        self._save_pending_progress_deletes()

    def _forget_pending_progress_delete(self, key: tuple[str, str]) -> None:
        if self._pending_progress_deletes.pop(key, None) is not None:
            self._save_pending_progress_deletes()

    def _save_pending_progress_deletes(self) -> None:
        if self.progress_delete_store is None:
            return
        try:
            self.progress_delete_store.save(self._pending_progress_deletes)
        except Exception:
            logger.exception("telegram progress delete store save failed")

    def _save_active_turns(self) -> None:
        if self.active_turn_store is None:
            return
        try:
            active_turns = {
                turn_id: turn
                for turn_id, turn in self._active_turns.items()
                if not turn.terminal
            }
            self.active_turn_store.save(active_turns)
        except Exception:
            logger.exception("telegram active turn store save failed")

    def _update_progress_status(self, event: StreamEvent) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        changed = False
        if event.code == "runtime.artifact":
            turn.expects_outbox_delivery = True
            changed = True
        if event.code in ACTIVE_STATUS_CODES:
            turn.status_text = ACTIVE_STATUS_CODES[event.code]
            changed = True
        elif event.code in CANCELLED_STATUS_CODES:
            turn.status_text = "Stopped"
            turn.terminal_text = "Stopped."
            changed = True
        elif event.code in COMPLETED_STATUS_CODES:
            turn.status_text = "Finishing"
            changed = True
        elif event.text:
            turn.status_text = _compact_status_text(event.text)
            changed = True
        if changed:
            self._save_active_turns()

    async def _terminalize_progress(self, event: StreamEvent, text: str) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        await self._terminalize_progress_turn(turn, text)

    async def _terminalize_progress_turn(self, turn: ActiveTurn, text: str) -> None:
        turn.terminal_text = text
        turn.visible_after = min(turn.visible_after, _loop_time())
        self._save_active_turns()
        await self._set_turn_reaction(turn, _terminal_reaction(text))
        render_result = await self._ensure_progress_message(turn, force=True)
        if render_result == ProgressRenderResult.RENDERED:
            turn.terminal = True
            self._forget_turn(turn)
            return
        if render_result == ProgressRenderResult.RETRYABLE_FAILED:
            return
        if turn.provisional_message_ref is not None:
            fallback_sent = await self._send_progress_fallback(turn, text)
            if fallback_sent:
                await self._delete_progress_message(turn)
        turn.terminal = True
        self._forget_turn(turn)

    async def _send_progress_fallback(self, turn: ActiveTurn, text: str) -> bool:
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
            return False
        return True

    def _progress_buttons(self, turn: ActiveTurn) -> list[TelegramActionButton]:
        if turn.terminal_text is not None:
            return []
        buttons = [
            TelegramActionButton(
                "Status", self._callback_payload("status", turn.target)
            )
        ]
        if turn.session_id is not None and turn.session_key is not None:
            buttons.insert(
                0,
                TelegramActionButton(
                    "Stop",
                    self._callback_payload("stop", turn.target, active=turn),
                ),
            )
        return buttons

    def _callback_payload(
        self,
        action: str,
        target: TypingTarget,
        *,
        active: ActiveTurn | None = None,
    ) -> str:
        code = CALLBACK_ACTION_CODES[action]
        target_id = active.turn_id if active is not None else CALLBACK_ROUTE_TARGET
        session_key = active.session_key if active is not None else None
        mac = _callback_mac(
            self.config.telegram_bot_token,
            action,
            target_id,
            target.conversation_ref,
            target.thread_ref,
            session_key,
        )
        return f"{CALLBACK_PREFIX}:{code}:{target_id}:{mac}"

    async def _complete_progress(self, event: StreamEvent) -> None:
        turn = self._active_turns.get(event.turn_id)
        if turn is None:
            return
        if event.text.strip():
            turn.expects_outbox_delivery = True
        turn.status_text = "Finishing"
        self._save_active_turns()
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
            if not turn.expects_outbox_delivery:
                await self._delete_progress_message(turn)
                await self._set_turn_reaction(turn, REACTION_COMPLETED)
                turn.terminal = True
                self._forget_turn(turn)
                return
            turn.status_text = "Finishing"
            self._save_active_turns()
            await self._edit_progress_message(turn, _progress_text(turn), force=True)
            return
        await self._set_turn_reaction(turn, REACTION_COMPLETED)
        turn.terminal = True
        self._forget_turn(turn)

    def _forget_turn(self, turn: ActiveTurn) -> None:
        self._active_turns.pop(turn.turn_id, None)
        if self._route_turns.get(turn.target.key) == turn.turn_id:
            self._route_turns.pop(turn.target.key, None)
        self._save_active_turns()

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

    def _target_for_stream_event(self, event: StreamEvent) -> TypingTarget:
        turn = self._active_turns.get(event.turn_id)
        if turn is not None and not turn.terminal:
            return turn.target
        return self._target_for_ref(event.peer_id)

    async def _process_outbox_delivery(self, delivery: OutboxDelivery) -> None:
        receipt_record = self._outbox_receipt_for_delivery(delivery)
        if receipt_record is not None and receipt_record.status == "delivered":
            await self._complete_progress_for_delivery(delivery)
            accepted = await self._report_outbox_with_retry(
                delivery,
                "delivered",
                provider_receipt=receipt_record.provider_receipt,
            )
            if accepted:
                self._forget_outbox_receipt(delivery.delivery_id)
            return

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
                resume_receipt=(
                    receipt_record.provider_receipt
                    if receipt_record is not None and receipt_record.status == "partial"
                    else None
                ),
            )
        except TelegramPartialSendError as err:
            await self._handle_outbox_send_failure(
                delivery,
                err.cause,
                partial_receipt=err.receipt,
            )
            return
        except Exception as err:
            await self._handle_outbox_send_failure(delivery, err)
            return

        self._remember_outbox_receipt(delivery.delivery_id, "delivered", receipt)
        await self._complete_progress_for_delivery(delivery)
        accepted = await self._report_outbox_with_retry(
            delivery,
            "delivered",
            provider_receipt=receipt,
        )
        if accepted:
            self._forget_outbox_receipt(delivery.delivery_id)

    def _outbox_receipt_for_delivery(
        self,
        delivery: OutboxDelivery,
    ) -> OutboxReceiptRecord | None:
        receipt_record = self._outbox_receipts.get(delivery.delivery_id)
        if receipt_record is None:
            return None
        try:
            provider_receipt = normalize_telegram_receipt(
                receipt_record.provider_receipt,
                conversation_ref=delivery.conversation_ref,
            )
        except TelegramReferenceError as err:
            logger.warning(
                "telegram outbox receipt ignored for delivery_id=%s: %s",
                delivery.delivery_id,
                err,
            )
            self._forget_outbox_receipt(delivery.delivery_id)
            return None
        if provider_receipt == receipt_record.provider_receipt:
            return receipt_record
        normalized = OutboxReceiptRecord(
            status=receipt_record.status,
            provider_receipt=provider_receipt,
        )
        self._outbox_receipts[delivery.delivery_id] = normalized
        self._save_outbox_receipts()
        return normalized

    async def _handle_outbox_send_failure(
        self,
        delivery: OutboxDelivery,
        err: Exception,
        *,
        partial_receipt: dict[str, object] | None = None,
    ) -> None:
        logger.exception(
            "telegram sendMessage request failed for delivery_id=%s "
            "conversation_ref=%s",
            delivery.delivery_id,
            delivery.conversation_ref,
        )
        outcome, error_code = _classify_send_failure(err)
        self._record_delivery_failure(outcome, error_code)
        report_receipt = None
        if partial_receipt is not None:
            self._remember_outbox_receipt(
                delivery.delivery_id,
                "partial",
                partial_receipt,
            )
            report_receipt = partial_receipt
        elif outcome == "terminal_failed":
            self._forget_outbox_receipt(delivery.delivery_id)
        accepted = await self._report_outbox_with_retry(
            delivery,
            outcome,
            provider_receipt=report_receipt,
            error_code=error_code,
            error_text=str(err),
        )
        if accepted and outcome == "terminal_failed":
            self._forget_outbox_receipt(delivery.delivery_id)
            await self._terminalize_progress_for_delivery_failure(
                delivery,
                _delivery_failure_progress_text(error_code),
            )

    def _remember_outbox_receipt(
        self,
        delivery_id: str,
        status: str,
        receipt: dict[str, object],
    ) -> None:
        self._outbox_receipts[delivery_id] = OutboxReceiptRecord(
            status=status,
            provider_receipt=dict(receipt),
        )
        while len(self._outbox_receipts) > MAX_OUTBOX_RECEIPTS:
            self._outbox_receipts.pop(next(iter(self._outbox_receipts)))
        self._save_outbox_receipts()

    def _forget_outbox_receipt(self, delivery_id: str) -> None:
        if self._outbox_receipts.pop(delivery_id, None) is not None:
            self._save_outbox_receipts()

    def _save_outbox_receipts(self) -> None:
        if self.outbox_receipt_store is None:
            return
        try:
            self.outbox_receipt_store.save(self._outbox_receipts)
        except Exception:
            logger.exception("telegram outbox receipt store save failed")

    async def _complete_progress_for_delivery(self, delivery: OutboxDelivery) -> None:
        if delivery.turn_id is None:
            return
        turn = self._active_turns.get(delivery.turn_id)
        if turn is None:
            return
        if turn.target.key != (delivery.conversation_ref, delivery.thread_ref):
            logger.warning(
                "outbox delivery route did not match active turn for turn_id=%s",
                delivery.turn_id,
            )
            return
        if turn.provisional_message_ref is not None:
            await self._delete_progress_message(turn)
        await self._set_turn_reaction(turn, REACTION_COMPLETED)
        turn.terminal = True
        self._forget_turn(turn)

    async def _terminalize_progress_for_delivery_failure(
        self,
        delivery: OutboxDelivery,
        text: str,
    ) -> None:
        if delivery.turn_id is None:
            return
        turn = self._active_turns.get(delivery.turn_id)
        if turn is None:
            return
        if turn.target.key != (delivery.conversation_ref, delivery.thread_ref):
            logger.warning(
                "telegram outbox failure route mismatch for turn_id=%s "
                "delivery_id=%s active_route=%s delivery_route=%s",
                delivery.turn_id,
                delivery.delivery_id,
                turn.target.key,
                (delivery.conversation_ref, delivery.thread_ref),
            )
            return
        await self._terminalize_progress_turn(turn, text)

    async def _report_outbox_with_retry(
        self,
        delivery: OutboxDelivery,
        outcome: str,
        *,
        provider_receipt: dict[str, object] | None = None,
        error_code: str | None = None,
        error_text: str | None = None,
    ) -> bool:
        for attempt_no in range(1, 4):
            accepted = await self._report_outbox_once(
                delivery,
                outcome,
                provider_receipt=provider_receipt,
                error_code=error_code,
                error_text=error_text,
            )
            if accepted is not None:
                return accepted
            await asyncio.sleep(0.25 * attempt_no)
        return False

    async def _report_outbox_once(
        self,
        delivery: OutboxDelivery,
        outcome: str,
        *,
        provider_receipt: dict[str, object] | None = None,
        error_code: str | None = None,
        error_text: str | None = None,
    ) -> bool | None:
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
            return None
        if not response.accepted:
            logger.warning(
                "lionclaw rejected stale outbox report for delivery_id=%s "
                "status=%s attempt_status=%s",
                delivery.delivery_id,
                response.status,
                response.attempt_status,
            )
        return response.accepted

    async def _health_checks(self) -> list[HealthCheck]:
        checks = [await self._bot_identity_health_check()]
        if self.config.telegram_update_mode == "webhook":
            checks.append(self._webhook_health_check())
        else:
            checks.append(self._polling_health_check())
        checks.append(self._update_lag_health_check())
        checks.append(self._update_extraction_health_check())
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

    def _webhook_health_check(self) -> HealthCheck:
        details: dict[str, object] = {
            "host": self.config.telegram_webhook_host,
            "port": self.config.telegram_webhook_port,
            "path": self.config.telegram_webhook_path,
            "secret_token_configured": self.config.telegram_webhook_secret_token
            is not None,
        }
        if self._last_webhook_success_observed_at is not None:
            details["last_success_age_seconds"] = max(
                0, int(_loop_time() - self._last_webhook_success_observed_at)
            )
        if self._last_webhook_error is not None:
            if self._last_webhook_failure_observed_at is not None:
                details["last_failure_age_seconds"] = max(
                    0, int(_loop_time() - self._last_webhook_failure_observed_at)
                )
            details["error"] = self._last_webhook_error
            return HealthCheck(
                code="telegram.webhook",
                status="error",
                message="webhook update processing failed",
                details=details,
            )
        if self._last_webhook_success_observed_at is None:
            return HealthCheck(
                code="telegram.webhook",
                status="ok",
                message="webhook receiver configured; no updates observed yet",
                details=details,
            )
        return HealthCheck(
            code="telegram.webhook",
            status="ok",
            message="webhook updates accepted",
            details=details,
        )

    def _update_lag_health_check(self) -> HealthCheck:
        details: dict[str, object] = {
            "offset": self.offset,
            "update_mode": self.config.telegram_update_mode,
        }
        if self.config.telegram_update_mode == "webhook":
            if self._last_update_id is None or self._last_update_observed_at is None:
                return HealthCheck(
                    code="telegram.update_lag",
                    status="ok",
                    message="webhook active; no updates observed yet",
                    details=details,
                )
        elif self._last_poll_success_observed_at is None:
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

    def _update_extraction_health_check(self) -> HealthCheck:
        failures = self._update_extraction_failure_count
        status = "warning" if failures else "ok"
        message = (
            f"{failures} malformed update(s) quarantined in this worker process"
            if failures
            else "no malformed updates observed in this worker process"
        )
        return HealthCheck(
            code="telegram.update_extraction",
            status=status,
            message=message,
            details={"malformed_updates": failures},
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

    def _record_webhook_success(self) -> None:
        self._last_webhook_success_observed_at = _loop_time()
        self._last_webhook_error = None

    def _record_webhook_failure(self, err: Exception) -> None:
        self._last_webhook_failure_observed_at = _loop_time()
        self._last_webhook_error = f"{type(err).__name__}: {err}"


def _classify_send_failure(err: Exception) -> tuple[str, str]:
    if isinstance(err, TelegramReferenceError):
        return "terminal_failed", "telegram.invalid_ref"
    if isinstance(
        err,
        (FileNotFoundError, IsADirectoryError, NotADirectoryError, PermissionError),
    ):
        return "terminal_failed", "telegram.attachment_unreadable"
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


def _delivery_failure_progress_text(error_code: str) -> str:
    if error_code == "telegram.attachment_unreadable":
        return "Delivery failed: attachment was not readable."
    if error_code == "telegram.invalid_ref":
        return "Delivery failed: Telegram destination was invalid."
    if error_code == "telegram.send_rejected":
        return "Delivery failed: Telegram rejected the message."
    return "Delivery failed."


def _is_permanent_send_failure(err: Exception) -> bool:
    outcome, _ = _classify_send_failure(err)
    return outcome == "terminal_failed"


def _parse_callback_action(value: str) -> ParsedCallbackAction | None:
    parts = value.split(":", 3)
    if len(parts) != 4 or parts[0] != CALLBACK_PREFIX:
        return None
    action = CALLBACK_CODE_ACTIONS.get(parts[1])
    target = parts[2]
    mac = parts[3]
    if action is None or not target or not mac:
        return None
    return ParsedCallbackAction(action=action, target=target, mac=mac)


def _callback_mac(
    secret: str,
    action: str,
    target: str,
    conversation_ref: str,
    thread_ref: str | None,
    session_key: str | None,
) -> str:
    message = "\0".join(
        [
            action,
            target,
            conversation_ref,
            thread_ref or "",
            session_key or "",
        ]
    ).encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest[:6]).decode("ascii").rstrip("=")


def _callback_update(
    callback: TelegramCallbackAction,
    *,
    text: str,
) -> TelegramInboundUpdate:
    return TelegramInboundUpdate(
        update_id=callback.update_id,
        event_id=f"telegram:callback:{callback.update_id}",
        sender_ref=callback.sender_ref,
        conversation_ref=callback.conversation_ref,
        thread_ref=callback.thread_ref,
        message_ref=callback.message_ref,
        text=text,
        trigger="callback",
        provider_metadata=callback.provider_metadata,
    )


def _coalesce_work_items(items: list[ProviderWorkItem]) -> list[ProviderWorkItem]:
    coalesced: list[ProviderWorkItem] = []
    pending: ProviderWorkItem | None = None
    for item in items:
        if pending is not None and _can_merge_work_items(pending, item):
            pending = _merge_work_items(pending, item)
            continue
        if pending is not None:
            coalesced.append(pending)
        pending = item
    if pending is not None:
        coalesced.append(pending)
    return coalesced


def _provider_work_item_sort_key(item: ProviderWorkItem) -> tuple[int, ...]:
    return item.update_ids


def _webhook_batch_key(event: TelegramInboundEvent | None) -> WebhookBatchKey | None:
    if not isinstance(event, TelegramInboundUpdate):
        return None
    if _telegram_command(event) is not None:
        return None
    media_group = event.provider_metadata.get("media_group_id")
    if isinstance(media_group, str) and media_group:
        lane_kind = "media_group"
        lane_id = media_group
    elif event.text is not None and event.text.strip() and not event.attachments:
        lane_kind = "text"
        lane_id = "_"
    else:
        return None
    return (
        event.conversation_ref,
        event.thread_ref,
        event.sender_ref,
        event.trigger,
        lane_kind,
        lane_id,
    )


def _webhook_event_route(event: TelegramInboundEvent | None) -> WebhookRouteKey | None:
    if isinstance(
        event,
        TelegramInboundUpdate | TelegramPairingClaim | TelegramCallbackAction,
    ):
        return (event.conversation_ref, event.thread_ref)
    return None


def _webhook_batch_route(key: WebhookBatchKey) -> WebhookRouteKey:
    conversation_ref, thread_ref, *_ = key
    return (conversation_ref, thread_ref)


def _sorted_webhook_batches_for_route(
    batches: Iterable[WebhookBatch],
    route: WebhookRouteKey,
) -> list[WebhookBatch]:
    return sorted(
        (batch for batch in batches if _webhook_batch_route(batch.key) == route),
        key=_webhook_batch_sort_key,
    )


def _webhook_batch_sort_key(batch: WebhookBatch) -> tuple[int, ...]:
    return min(
        (_provider_work_item_sort_key(item) for item in batch.items),
        default=(0,),
    )


def _can_merge_work_items(first: ProviderWorkItem, second: ProviderWorkItem) -> bool:
    if not isinstance(first.event, TelegramInboundUpdate) or not isinstance(
        second.event, TelegramInboundUpdate
    ):
        return False
    return _can_merge_inbound_updates(first.event, second.event)


def _can_merge_inbound_updates(
    first: TelegramInboundUpdate,
    second: TelegramInboundUpdate,
) -> bool:
    if _telegram_command(first) is not None or _telegram_command(second) is not None:
        return False
    if (
        first.sender_ref != second.sender_ref
        or first.conversation_ref != second.conversation_ref
        or first.thread_ref != second.thread_ref
        or first.trigger != second.trigger
    ):
        return False
    first_media_group = first.provider_metadata.get("media_group_id")
    second_media_group = second.provider_metadata.get("media_group_id")
    if isinstance(first_media_group, str) or isinstance(second_media_group, str):
        return (
            isinstance(first_media_group, str)
            and first_media_group
            and first_media_group == second_media_group
        )
    return (
        not first.attachments
        and not second.attachments
        and _within_text_burst_window(first, second)
    )


def _merge_work_items(
    first: ProviderWorkItem, second: ProviderWorkItem
) -> ProviderWorkItem:
    assert isinstance(first.event, TelegramInboundUpdate)
    assert isinstance(second.event, TelegramInboundUpdate)
    return ProviderWorkItem(
        update_ids=first.update_ids + second.update_ids,
        event=_merge_inbound_updates(first.event, second.event),
    )


def _merge_inbound_updates(
    first: TelegramInboundUpdate,
    second: TelegramInboundUpdate,
) -> TelegramInboundUpdate:
    update_ids = _batched_metadata_values(first, second, "update_id")
    existing_message_refs = _metadata_list(first, "batched_message_refs")
    if not existing_message_refs and first.message_ref is not None:
        existing_message_refs = [first.message_ref]
    message_refs = [
        ref
        for ref in (*existing_message_refs, second.message_ref)
        if isinstance(ref, str)
    ]
    texts = [text for text in (first.text, second.text) if text and text.strip()]
    provider_metadata = dict(first.provider_metadata)
    provider_metadata["update_id"] = second.update_id
    provider_metadata["batched_update_ids"] = update_ids
    provider_metadata["batched_message_refs"] = message_refs
    provider_metadata["batch_size"] = len(update_ids)
    latest_message_date_epoch = _message_date_epoch(second)
    if latest_message_date_epoch is not None:
        provider_metadata["batched_latest_message_date_epoch"] = (
            latest_message_date_epoch
        )
    if "media_group_id" in second.provider_metadata:
        provider_metadata["media_group_id"] = second.provider_metadata["media_group_id"]
    return replace(
        first,
        update_id=second.update_id,
        event_id=f"{first.event_id}:batch:{second.update_id}",
        text="\n\n".join(texts) if texts else None,
        attachments=[*first.attachments, *second.attachments],
        provider_metadata=provider_metadata,
    )


def _batched_metadata_values(
    first: TelegramInboundUpdate,
    second: TelegramInboundUpdate,
    key: str,
) -> list[int]:
    values = _metadata_list(first, f"batched_{key}s")
    if not values:
        values = [first.provider_metadata.get(key)]
    values.append(second.provider_metadata.get(key))
    return [value for value in values if isinstance(value, int)]


def _metadata_list(update: TelegramInboundUpdate, key: str) -> list[object]:
    value = update.provider_metadata.get(key)
    if isinstance(value, list):
        return list(value)
    return []


def _within_text_burst_window(
    first: TelegramInboundUpdate,
    second: TelegramInboundUpdate,
) -> bool:
    first_epoch = _latest_message_date_epoch(first)
    second_epoch = _message_date_epoch(second)
    if first_epoch is None or second_epoch is None:
        return False
    return 0 <= second_epoch - first_epoch <= TEXT_BURST_MAX_SECONDS


def _latest_message_date_epoch(update: TelegramInboundUpdate) -> int | None:
    batched_epoch = _metadata_int(update, "batched_latest_message_date_epoch")
    if batched_epoch is not None:
        return batched_epoch
    return _message_date_epoch(update)


def _message_date_epoch(update: TelegramInboundUpdate) -> int | None:
    return _metadata_int(update, "message_date_epoch")


def _metadata_int(update: TelegramInboundUpdate, key: str) -> int | None:
    value = update.provider_metadata.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _telegram_command(update: TelegramInboundUpdate) -> TelegramCommand | None:
    text = _telegram_command_text(update)
    if text is None:
        return None
    parts = text.split(maxsplit=1)
    token = parts[0]
    arguments = parts[1] if len(parts) > 1 else ""
    command = token.removeprefix("/")
    target_username = None
    if "@" in command:
        command, target_username = command.split("@", 1)
        target_username = target_username.removeprefix("@").casefold() or None
    command = command.casefold()
    if not command:
        return None
    return TelegramCommand(
        name=command,
        raw=token,
        arguments=arguments.strip(),
        text=text,
        target_username=target_username,
    )


def _telegram_command_text(update: TelegramInboundUpdate) -> str | None:
    text = update.text
    if text is None:
        return None
    if text.startswith("/"):
        return text
    leading_mention = update.provider_metadata.get("leading_mention_text")
    if not isinstance(leading_mention, str) or not leading_mention:
        return None
    if not text.startswith(leading_mention):
        return None
    text_after_mention = text[len(leading_mention) :].lstrip()
    if not text_after_mention.startswith("/"):
        return None
    return text_after_mention


def _telegram_runtime_command_text(command: TelegramCommand) -> str:
    if command.target_username is None or "@" not in command.raw:
        return command.text
    command_name = command.raw.removeprefix("/").split("@", 1)[0]
    if not command_name:
        return command.text
    return f"/{command_name}{command.text[len(command.raw) :]}"


def _telegram_command_is_addressed(
    update: TelegramInboundUpdate,
    command: TelegramCommand,
) -> bool:
    if command.target_username is not None:
        if update.provider_metadata.get("command_targets_bot"):
            return True
        bot_username = update.provider_metadata.get("bot_username")
        return (
            isinstance(bot_username, str)
            and command.target_username == bot_username.removeprefix("@").casefold()
        )
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


def _terminal_reaction(text: str) -> str:
    if text.strip().casefold().startswith("stopped"):
        return REACTION_STOPPED
    return REACTION_FAILED


def _compact_status_text(text: str) -> str:
    compact = " ".join(text.strip().split())
    if len(compact) <= COMPACT_STATUS_TEXT_LIMIT:
        return compact
    return f"{compact[: COMPACT_STATUS_TEXT_LIMIT - 3]}..."


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


def _is_permanent_delete_failure(err: Exception) -> bool:
    if isinstance(err, TelegramReferenceError):
        return True
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
                "message to delete not found",
                "message can't be deleted",
                "message id invalid",
                "message_id_invalid",
                "message not found",
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


def _safe_loop_time() -> float:
    try:
        return _loop_time()
    except RuntimeError:
        return time.monotonic()


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
    webhook_server: TelegramWebhookServer | None = None
    worker = TelegramWorker(
        config=config,
        lionclaw_api=lionclaw_api,
        telegram=telegram,
        offset_store=OffsetStore(config.telegram_offset_file),
        active_turn_store=ActiveTurnStore(
            config.runtime_dir / "telegram.active-turns.json"
        ),
        outbox_receipt_store=OutboxReceiptStore(
            config.runtime_dir / "telegram.outbox-receipts.json"
        ),
        progress_delete_store=ProgressDeleteStore(
            config.runtime_dir / "telegram.progress-deletes.json"
        ),
    )

    try:
        try:
            await telegram.configure_commands()
        except Exception:
            logger.exception("telegram command menu configuration failed")
        if config.telegram_update_mode == "webhook":
            webhook_server = TelegramWebhookServer(
                config, worker.process_webhook_update
            )
            await webhook_server.start()
            await worker.run_forever(receive_updates=False)
        else:
            await worker.run_forever()
    finally:
        if webhook_server is not None:
            await webhook_server.close()
        await telegram.close()
        await lionclaw_api.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(run())
