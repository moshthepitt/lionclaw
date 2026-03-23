from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Literal

Role = Literal["user", "assistant", "error"]
EventKind = Literal["message_delta", "status", "error", "done"]
Lane = Literal["answer", "reasoning"]
PeerStatus = Literal["unknown", "pending", "approved", "blocked"]


@dataclass(slots=True)
class StreamEvent:
    sequence: int
    peer_id: str
    turn_id: str
    kind: EventKind
    session_id: str | None = None
    lane: Lane | None = None
    code: str | None = None
    text: str | None = None


@dataclass(slots=True)
class TranscriptEntry:
    role: Role
    text: str
    turn_id: str | None = None


@dataclass(slots=True)
class PairingState:
    status: PeerStatus = "unknown"
    pairing_code: str | None = None
    trust_tier: str | None = None


@dataclass(slots=True)
class ChannelViewState:
    peer_id: str
    transcript: list[TranscriptEntry] = field(default_factory=list)
    reasoning_by_turn: dict[str, str] = field(default_factory=dict)
    answer_started_by_turn: set[str] = field(default_factory=set)
    latest_reasoning_turn_id: str | None = None
    active_session_id: str | None = None
    active_turn_id: str | None = None
    pending_submission: bool = False
    restored_running_turn: bool = False
    status_lines: deque[str] = field(default_factory=lambda: deque(maxlen=8))
    active_answer_entry_by_turn: dict[str, int] = field(default_factory=dict)
    pairing: PairingState = field(default_factory=PairingState)

    def append_user_message(self, text: str) -> None:
        self.transcript.append(TranscriptEntry(role="user", text=text))

    def begin_submit(self, text: str) -> None:
        self.append_user_message(text)
        self.pending_submission = True
        self.active_turn_id = None
        self.restored_running_turn = False
        self.latest_reasoning_turn_id = None

    def mark_queued(self, turn_id: str, session_id: str | None = None) -> None:
        self.pending_submission = False
        self.active_turn_id = turn_id
        self.restored_running_turn = False
        if session_id is not None:
            self.active_session_id = session_id
        self.latest_reasoning_turn_id = turn_id
        self.reasoning_by_turn.pop(turn_id, None)
        self.answer_started_by_turn.discard(turn_id)
        self.active_answer_entry_by_turn.pop(turn_id, None)

    def clear_pending_turn(self) -> None:
        self.pending_submission = False
        self.active_turn_id = None
        self.restored_running_turn = False

    def mark_send_failed(self, message: str) -> None:
        self.clear_pending_turn()
        self.status_lines.append(f"send failed: {message}")

    def restore_session_history(self, session_id: str, turns: list[object]) -> None:
        self.transcript = []
        self.reasoning_by_turn = {}
        self.answer_started_by_turn = set()
        self.latest_reasoning_turn_id = None
        self.active_turn_id = None
        self.pending_submission = False
        self.restored_running_turn = False
        self.active_answer_entry_by_turn = {}
        self.active_session_id = session_id

        for turn in turns:
            turn_id = getattr(turn, "turn_id")
            status = getattr(turn, "status")
            display_user_text = getattr(turn, "display_user_text")
            assistant_text = getattr(turn, "assistant_text") or ""
            error_text = getattr(turn, "error_text")

            self.transcript.append(
                TranscriptEntry(role="user", text=display_user_text, turn_id=turn_id)
            )
            if assistant_text.strip():
                assistant_index = len(self.transcript)
                rendered_assistant = _render_history_assistant_text(status, assistant_text)
                self.transcript.append(
                    TranscriptEntry(
                        role="assistant",
                        text=rendered_assistant,
                        turn_id=turn_id,
                    )
                )
                self.answer_started_by_turn.add(turn_id)
                if status == "running":
                    self.active_answer_entry_by_turn[turn_id] = assistant_index
            if error_text:
                self.transcript.append(
                    TranscriptEntry(role="error", text=error_text, turn_id=turn_id)
                )
            if status == "running":
                self.active_turn_id = turn_id
                self.restored_running_turn = True

    def reset_for_new_session(self, session_id: str) -> None:
        self.transcript = []
        self.reasoning_by_turn = {}
        self.answer_started_by_turn = set()
        self.latest_reasoning_turn_id = None
        self.active_session_id = session_id
        self.active_turn_id = None
        self.pending_submission = False
        self.restored_running_turn = False
        self.active_answer_entry_by_turn = {}
        self.status_lines.clear()

    def clear_transient_view(self) -> None:
        self.transcript = []
        self.reasoning_by_turn = {}
        self.answer_started_by_turn = set()
        self.latest_reasoning_turn_id = None
        self.active_session_id = None
        self.active_turn_id = None
        self.pending_submission = False
        self.restored_running_turn = False
        self.active_answer_entry_by_turn = {}

    def apply_stream_event(self, event: StreamEvent) -> None:
        if event.peer_id != self.peer_id:
            return

        if event.turn_id == self.active_turn_id:
            self.restored_running_turn = False

        if event.kind == "message_delta" and event.lane == "answer" and event.text:
            self.answer_started_by_turn.add(event.turn_id)
            self._append_answer_delta(event.turn_id, event.text)
            return

        if event.kind == "message_delta" and event.lane == "reasoning" and event.text:
            if event.turn_id in self.answer_started_by_turn:
                return
            self.reasoning_by_turn[event.turn_id] = (
                self.reasoning_by_turn.get(event.turn_id, "") + event.text
            )
            self.latest_reasoning_turn_id = event.turn_id
            return

        if event.kind == "status" and event.text:
            self.status_lines.append(_format_status_line(event.code, event.text))
            return

        if event.kind == "error" and event.text:
            self.transcript.append(
                TranscriptEntry(role="error", text=event.text, turn_id=event.turn_id)
            )
            self.status_lines.append(_format_error_line(event.code, event.text))
            if event.turn_id == self.active_turn_id:
                self.clear_pending_turn()
                self.status_lines.append("[status] Use /continue, /retry, or /reset.")
            return

        if event.kind == "done":
            self.pending_submission = False
            self.active_answer_entry_by_turn.pop(event.turn_id, None)
            if event.turn_id == self.active_turn_id:
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

    def transcript_text(self) -> str:
        rendered: list[str] = []
        for entry in self.transcript:
            prefix = {
                "user": "you> ",
                "assistant": "lionclaw> ",
                "error": "error> ",
            }[entry.role]
            rendered.extend(_prefix_lines(prefix, entry.text))
        return "\n".join(rendered)

    def reasoning_text(self) -> str:
        if self.pending_submission:
            return "Waiting to queue this turn..."

        if self.active_turn_id is not None:
            content = self.reasoning_by_turn.get(self.active_turn_id, "")
            if content:
                return "\n".join(_prefix_lines("thinking> ", content))
            if self.restored_running_turn:
                return ""
            if self.active_turn_id in self.answer_started_by_turn:
                return "No pre-answer reasoning for this turn."
            return "Waiting for reasoning for this turn..."

        if not self.latest_reasoning_turn_id:
            return "No reasoning for the current turn yet."

        content = self.reasoning_by_turn.get(self.latest_reasoning_turn_id, "")
        if not content:
            return "No reasoning for the current turn yet."

        return "\n".join(_prefix_lines("thinking> ", content))

    def status_text(self) -> str:
        if not self.status_lines:
            return "No status events yet."
        return "\n".join(self.status_lines)

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

    def _append_answer_delta(self, turn_id: str, text: str) -> None:
        index = self.active_answer_entry_by_turn.get(turn_id)
        if index is None:
            self.transcript.append(
                TranscriptEntry(role="assistant", text=text, turn_id=turn_id)
            )
            self.active_answer_entry_by_turn[turn_id] = len(self.transcript) - 1
            return

        self.transcript[index].text += text


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


def _render_history_assistant_text(status: str, assistant_text: str) -> str:
    marker = _partial_history_marker(status)
    if not marker:
        return assistant_text
    return f"{marker}\n{assistant_text}"


def _partial_history_marker(status: str) -> str:
    return {
        "timed_out": "[partial] previous assistant reply timed out before completion",
        "failed": "[partial] previous assistant reply failed before completion",
        "cancelled": "[partial] previous assistant reply was cancelled before completion",
        "interrupted": "[partial] previous assistant reply was interrupted before completion",
    }.get(status, "")
