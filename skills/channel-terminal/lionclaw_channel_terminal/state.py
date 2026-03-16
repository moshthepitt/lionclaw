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
    lane: Lane | None = None
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
    latest_reasoning_turn_id: str | None = None
    status_lines: deque[str] = field(default_factory=lambda: deque(maxlen=8))
    active_answer_entry_by_turn: dict[str, int] = field(default_factory=dict)
    pairing: PairingState = field(default_factory=PairingState)

    def append_user_message(self, text: str) -> None:
        self.transcript.append(TranscriptEntry(role="user", text=text))

    def apply_stream_event(self, event: StreamEvent) -> None:
        if event.peer_id != self.peer_id:
            return

        if event.kind == "message_delta" and event.lane == "answer" and event.text:
            self._append_answer_delta(event.turn_id, event.text)
            return

        if event.kind == "message_delta" and event.lane == "reasoning" and event.text:
            self.reasoning_by_turn[event.turn_id] = (
                self.reasoning_by_turn.get(event.turn_id, "") + event.text
            )
            self.latest_reasoning_turn_id = event.turn_id
            return

        if event.kind == "status" and event.text:
            self.status_lines.append(event.text)
            return

        if event.kind == "error" and event.text:
            self.transcript.append(
                TranscriptEntry(role="error", text=event.text, turn_id=event.turn_id)
            )
            self.status_lines.append(f"error: {event.text}")
            return

        if event.kind == "done":
            self.active_answer_entry_by_turn.pop(event.turn_id, None)

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
        if not self.latest_reasoning_turn_id:
            return "No reasoning for the current turn yet."

        content = self.reasoning_by_turn.get(self.latest_reasoning_turn_id, "")
        if not content:
            return "No reasoning for the current turn yet."

        return "\n".join(_prefix_lines("thinking> ", content))

    def status_text(self) -> str:
        if not self.status_lines:
            return "No status events yet."
        return "\n".join(f"[status] {line}" for line in self.status_lines)

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
        return self.pairing.status == "blocked"

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
