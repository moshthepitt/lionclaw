from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Literal

EventKind = Literal["message_delta", "status", "error", "turn_completed", "done"]
Lane = Literal["answer", "reasoning"]
PeerStatus = Literal["unknown", "pending", "approved", "blocked"]


@dataclass(slots=True)
class StreamEvent:
    sequence: int
    peer_id: str
    kind: EventKind
    turn_id: str | None = None
    session_id: str | None = None
    lane: Lane | None = None
    code: str | None = None
    text: str | None = None


@dataclass(slots=True)
class ActivityEntry:
    text: str
    kind: Literal["status", "error"] = "status"
    code: str | None = None
    turn_id: str | None = None


@dataclass(slots=True)
class TurnState:
    turn_id: str
    user_text: str = ""
    answer_text: str = ""
    reasoning_text: str = ""
    status: str = "running"
    error_text: str | None = None
    restored_running: bool = False
    activity: list[ActivityEntry] = field(default_factory=list)

    @property
    def answer_started(self) -> bool:
        return bool(self.answer_text.strip())

    @property
    def is_running(self) -> bool:
        return self.status == "running"

    def assistant_display_text(self) -> str:
        if not self.answer_text.strip():
            return ""
        marker = _partial_history_marker(self.status)
        if marker:
            return f"{marker}\n{self.answer_text}"
        return self.answer_text


@dataclass(slots=True)
class PairingState:
    status: PeerStatus = "unknown"
    pairing_code: str | None = None
    trust_tier: str | None = None


@dataclass(slots=True)
class ChannelViewState:
    peer_id: str
    turns: dict[str, TurnState] = field(default_factory=dict)
    turn_order: list[str] = field(default_factory=list)
    latest_reasoning_turn_id: str | None = None
    active_session_id: str | None = None
    active_turn_id: str | None = None
    pending_turn_id: str | None = None
    pending_submission: bool = False
    restored_running_turn: bool = False
    activity_lines: deque[str] = field(default_factory=lambda: deque(maxlen=200))
    pairing: PairingState = field(default_factory=PairingState)
    _pending_counter: int = 0

    def begin_submit(self, text: str) -> None:
        self._pending_counter += 1
        pending_turn_id = f"pending:{self._pending_counter}"
        self.turns[pending_turn_id] = TurnState(turn_id=pending_turn_id, user_text=text)
        self.turn_order.append(pending_turn_id)
        self.pending_turn_id = pending_turn_id
        self.pending_submission = True
        self.active_turn_id = None
        self.restored_running_turn = False
        self.latest_reasoning_turn_id = None

    def mark_queued(self, turn_id: str, session_id: str | None = None) -> None:
        self.pending_submission = False
        self.restored_running_turn = False
        if session_id is not None:
            self.active_session_id = session_id

        if self.pending_turn_id:
            turn = self._bind_pending_turn(turn_id)
        else:
            turn = self._ensure_turn(turn_id)

        turn.status = "running"
        turn.restored_running = False

        self.active_turn_id = turn_id
        self.latest_reasoning_turn_id = turn_id

    def clear_pending_turn(self) -> None:
        self.pending_submission = False
        self.pending_turn_id = None
        self.active_turn_id = None
        self.restored_running_turn = False

    def discard_pending_turn(self) -> None:
        if self.pending_turn_id is not None:
            self.turns.pop(self.pending_turn_id, None)
            self.turn_order = [
                turn_id for turn_id in self.turn_order if turn_id != self.pending_turn_id
            ]
        self.clear_pending_turn()

    def mark_send_failed(self, message: str) -> None:
        if self.pending_turn_id and self.pending_turn_id in self.turns:
            turn = self.turns[self.pending_turn_id]
            turn.status = "failed"
            turn.error_text = message
        self.clear_pending_turn()
        self.activity_lines.append(f"send failed: {message}")

    def restore_session_history(self, session_id: str, turns: list[object]) -> None:
        self._clear_turns()
        self.active_session_id = session_id

        for turn_snapshot in turns:
            turn_id = getattr(turn_snapshot, "turn_id")
            status = getattr(turn_snapshot, "status")
            turn = self._ensure_turn(turn_id)
            turn.status = status
            turn.user_text = getattr(turn_snapshot, "display_user_text")
            turn.answer_text = getattr(turn_snapshot, "assistant_text") or ""
            turn.error_text = getattr(turn_snapshot, "error_text")
            if turn.error_text:
                turn.activity.append(
                    ActivityEntry(
                        text=turn.error_text,
                        kind="error",
                        code=getattr(turn_snapshot, "error_code"),
                        turn_id=turn_id,
                    )
                )
            if status == "running":
                turn.restored_running = True
                self.active_turn_id = turn_id
                self.restored_running_turn = True

    def reset_for_new_session(self, session_id: str) -> None:
        self._clear_turns()
        self.active_session_id = session_id
        self.activity_lines.clear()

    def clear_transient_view(self) -> None:
        self._clear_turns()
        self.active_session_id = None

    def apply_stream_event(self, event: StreamEvent) -> None:
        if event.peer_id != self.peer_id:
            return

        if event.session_id is not None:
            self.active_session_id = event.session_id
        if event.turn_id is not None and event.turn_id == self.active_turn_id:
            self.restored_running_turn = False
            turn = self.turns.get(event.turn_id)
            if turn is not None:
                turn.restored_running = False

        if event.kind == "message_delta" and event.lane == "answer" and event.text:
            if event.turn_id is None:
                self.activity_lines.append(f"[message] {event.text}")
                return
            turn = self._ensure_turn(event.turn_id)
            turn.answer_text += event.text
            turn.status = "running"
            return

        if event.kind == "message_delta" and event.lane == "reasoning" and event.text:
            if event.turn_id is None:
                self.activity_lines.append(f"[thinking] {event.text}")
                return
            turn = self._ensure_turn(event.turn_id)
            turn.reasoning_text += event.text
            self.latest_reasoning_turn_id = event.turn_id
            return

        if event.kind == "status" and event.text:
            line = _format_status_line(event.code, event.text)
            self.activity_lines.append(line)
            if event.turn_id is not None:
                self._ensure_turn(event.turn_id).activity.append(
                    ActivityEntry(
                        text=line,
                        kind="status",
                        code=event.code,
                        turn_id=event.turn_id,
                    )
                )
            return

        if event.kind == "error" and event.text:
            line = _format_error_line(event.code, event.text)
            if event.turn_id is None:
                self.activity_lines.append(line)
                return
            turn = self._ensure_turn(event.turn_id)
            turn.status = "failed"
            turn.error_text = event.text
            turn.activity.append(
                ActivityEntry(
                    text=line,
                    kind="error",
                    code=event.code,
                    turn_id=event.turn_id,
                )
            )
            self.activity_lines.append(line)
            if event.turn_id == self.active_turn_id:
                self.clear_pending_turn()
                self.activity_lines.append("[status] Use /continue, /retry, or /reset.")
            return

        if event.kind == "turn_completed":
            if event.turn_id is None:
                return
            turn = self._ensure_turn(event.turn_id)
            if event.text is not None:
                turn.answer_text = event.text
            turn.status = "completed"
            turn.restored_running = False
            self.pending_submission = False
            if event.turn_id == self.active_turn_id:
                self.active_turn_id = None
                self.restored_running_turn = False
            return

        if event.kind == "done":
            if event.turn_id is None:
                return
            self.pending_submission = False
            if event.turn_id == self.active_turn_id:
                turn = self._ensure_turn(event.turn_id)
                if turn.status == "running":
                    turn.status = "completed" if turn.error_text is None else "failed"
                turn.restored_running = False
                self.active_turn_id = None
                self.restored_running_turn = False

    def set_pairing_state(
        self,
        status: PeerStatus,
        pairing_code: str | None = None,
        trust_tier: str | None = None,
    ) -> None:
        self.pairing = PairingState(
            status=status,
            pairing_code=pairing_code,
            trust_tier=trust_tier,
        )

    def ordered_turns(self) -> list[TurnState]:
        return [self.turns[turn_id] for turn_id in self.turn_order if turn_id in self.turns]

    def transcript_text(self) -> str:
        rendered: list[str] = []
        for turn in self.ordered_turns():
            if turn.user_text:
                rendered.extend(_prefix_lines("you> ", turn.user_text))
            assistant_text = turn.assistant_display_text()
            if assistant_text:
                rendered.extend(_prefix_lines("lionclaw> ", assistant_text))
            if turn.error_text:
                rendered.extend(_prefix_lines("error> ", turn.error_text))

        return "\n".join(rendered)

    def reasoning_text(self) -> str:
        if self.pending_submission:
            return "Waiting to queue this turn..."

        turn = self.current_reasoning_turn()
        if turn is None:
            return "No reasoning for the current turn yet."
        if turn.reasoning_text:
            return "\n".join(_prefix_lines("thinking> ", turn.reasoning_text))
        if turn.restored_running:
            return ""
        if turn.is_running:
            if turn.answer_started:
                return "No reasoning for this turn yet."
            return "Waiting for reasoning for this turn..."
        return "No reasoning for the current turn yet."

    def current_reasoning_turn(self) -> TurnState | None:
        if self.active_turn_id is not None:
            return self.turns.get(self.active_turn_id)
        if self.latest_reasoning_turn_id is not None:
            return self.turns.get(self.latest_reasoning_turn_id)
        return None

    def activity_text(self) -> str:
        if not self.activity_lines:
            return "No activity events yet."
        return "\n".join(self.activity_lines)

    def pairing_banner(self, channel_id: str, peer_id: str) -> str:
        match self.pairing.status:
            case "unknown":
                return (
                    f"peer '{peer_id}' is not known yet on channel '{channel_id}'. "
                    "Send a message to create pairing state."
                )
            case "pending":
                code = self.pairing.pairing_code or "-"
                return (
                    f"pairing pending for '{peer_id}' with code {code}. "
                    f"Approve with: lionclaw channel pairing approve {channel_id} {peer_id} {code} --trust-tier main"
                )
            case "approved":
                trust = self.pairing.trust_tier or "main"
                return f"peer '{peer_id}' is approved (trust_tier={trust})"
            case "blocked":
                return f"peer '{peer_id}' is blocked on channel '{channel_id}'"
        return "pairing state unavailable"

    def input_disabled(self) -> bool:
        return self.input_block_reason() is not None

    def input_block_reason(self) -> str | None:
        if self.pairing.status == "blocked":
            return "peer is blocked"
        if self.pending_submission or self.active_turn_id is not None:
            return "turn in progress"
        return None

    def _clear_turns(self) -> None:
        self.turns = {}
        self.turn_order = []
        self.latest_reasoning_turn_id = None
        self.active_turn_id = None
        self.pending_turn_id = None
        self.pending_submission = False
        self.restored_running_turn = False

    def _ensure_turn(self, turn_id: str) -> TurnState:
        turn = self.turns.get(turn_id)
        if turn is not None:
            return turn
        turn = TurnState(turn_id=turn_id)
        self.turns[turn_id] = turn
        self.turn_order.append(turn_id)
        return turn

    def _bind_pending_turn(self, turn_id: str) -> TurnState:
        pending_turn_id = self.pending_turn_id
        self.pending_turn_id = None
        if pending_turn_id is None:
            return self._ensure_turn(turn_id)

        pending = self.turns.pop(pending_turn_id, None)
        existing = self.turns.get(turn_id)
        if existing is not None:
            if pending is not None and not existing.user_text:
                existing.user_text = pending.user_text
            self.turn_order = [
                existing_turn_id
                for existing_turn_id in self.turn_order
                if existing_turn_id != pending_turn_id
            ]
            return existing

        if pending is None:
            return self._ensure_turn(turn_id)

        pending.turn_id = turn_id
        self.turns[turn_id] = pending
        self.turn_order = [
            turn_id if existing_turn_id == pending_turn_id else existing_turn_id
            for existing_turn_id in self.turn_order
        ]
        return pending


def _prefix_lines(prefix: str, text: str) -> list[str]:
    if not text:
        return [prefix.rstrip()]
    return [f"{prefix}{line}" for line in text.splitlines() or [text]]


def _format_status_line(code: str | None, text: str) -> str:
    if code:
        return f"[status] {code}: {text}"
    return f"[status] {text}"


def _format_error_line(code: str | None, text: str) -> str:
    if code:
        return f"[error] {code}: {text}"
    return f"[error] {text}"


def _partial_history_marker(status: str) -> str:
    return {
        "timed_out": "[partial] previous assistant reply timed out before completion",
        "failed": "[partial] previous assistant reply failed before completion",
        "cancelled": "[partial] previous assistant reply was cancelled before completion",
        "interrupted": "[partial] previous assistant reply was interrupted before completion",
    }.get(status, "")
