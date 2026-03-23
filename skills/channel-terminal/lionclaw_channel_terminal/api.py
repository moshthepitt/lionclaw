from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from lionclaw_channel_terminal.state import PeerStatus, StreamEvent


@dataclass(slots=True)
class PeerState:
    status: PeerStatus
    pairing_code: str | None = None
    trust_tier: str | None = None


@dataclass(slots=True)
class InboundResponse:
    outcome: str
    turn_id: str | None = None
    session_id: str | None = None


@dataclass(slots=True)
class SessionOpenResult:
    session_id: str
    channel_id: str
    peer_id: str
    trust_tier: str
    history_policy: str


@dataclass(slots=True)
class SessionActionResult:
    session_id: str
    turn_id: str | None = None


@dataclass(slots=True)
class SessionTurnSnapshot:
    turn_id: str
    kind: str
    status: str
    display_user_text: str
    assistant_text: str
    error_code: str | None
    error_text: str | None


@dataclass(slots=True)
class SessionLatestSnapshot:
    session: SessionOpenResult | None
    turns: list[SessionTurnSnapshot]
    resume_after_sequence: int | None


class LionClawApi:
    def __init__(
        self,
        base_url: str,
        channel_id: str,
        peer_id: str,
        consumer_id: str,
        start_mode: str,
        runtime_id: str | None,
        stream_limit: int,
        stream_wait_ms: int,
        timeout_seconds: float = 35.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.channel_id = channel_id
        self.peer_id = peer_id
        self.consumer_id = consumer_id
        self.start_mode = start_mode
        self.runtime_id = runtime_id
        self.stream_limit = stream_limit
        self.stream_wait_ms = stream_wait_ms
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout_seconds)
        self._inbound_sequence = 0

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_peer_state(self) -> PeerState:
        response = await self._client.get(
            "/v0/channels/peers",
            params={"channel_id": self.channel_id},
        )
        response.raise_for_status()
        payload = response.json()
        for peer in payload.get("peers", []):
            if peer.get("peer_id") != self.peer_id:
                continue
            return PeerState(
                status=peer.get("status", "unknown"),
                pairing_code=peer.get("pairing_code"),
                trust_tier=peer.get("trust_tier"),
            )
        return PeerState(status="unknown")

    async def fetch_latest_session(
        self,
        history_policy: str = "interactive",
    ) -> SessionLatestSnapshot:
        params: dict[str, str] = {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
        }
        if history_policy:
            params["history_policy"] = history_policy
        response = await self._client.get("/v0/sessions/latest", params=params)
        response.raise_for_status()
        payload = response.json()
        return SessionLatestSnapshot(
            session=_parse_session_open_result(payload.get("session")),
            turns=[
                SessionTurnSnapshot(
                    turn_id=item["turn_id"],
                    kind=item["kind"],
                    status=item["status"],
                    display_user_text=item["display_user_text"],
                    assistant_text=item.get("assistant_text", ""),
                    error_code=item.get("error_code"),
                    error_text=item.get("error_text"),
                )
                for item in payload.get("turns", [])
            ],
            resume_after_sequence=payload.get("resume_after_sequence"),
        )

    async def open_session(
        self,
        trust_tier: str,
        history_policy: str = "interactive",
    ) -> SessionOpenResult:
        response = await self._client.post(
            "/v0/sessions/open",
            json={
                "channel_id": self.channel_id,
                "peer_id": self.peer_id,
                "trust_tier": trust_tier,
                "history_policy": history_policy,
            },
        )
        response.raise_for_status()
        opened = _parse_session_open_result(response.json())
        if opened is None:
            raise RuntimeError("session open response missing session payload")
        return opened

    async def run_session_action(
        self,
        session_id: str,
        action: str,
    ) -> SessionActionResult:
        response = await self._client.post(
            "/v0/sessions/action",
            json={
                "session_id": session_id,
                "action": action,
            },
        )
        response.raise_for_status()
        data = response.json()
        return SessionActionResult(
            session_id=data["session_id"],
            turn_id=data.get("turn_id"),
        )

    async def send_inbound(self, text: str, session_id: str | None = None) -> InboundResponse:
        external_message_id = f"terminal-inbound:{self.consumer_id}:{self._inbound_sequence}"
        payload: dict[str, Any] = {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "text": text,
            "external_message_id": external_message_id,
        }
        if session_id:
            payload["session_id"] = session_id
        if self.runtime_id:
            payload["runtime_id"] = self.runtime_id
        response = await self._client.post("/v0/channels/inbound", json=payload)
        response.raise_for_status()
        data = response.json()
        self._inbound_sequence += 1
        return InboundResponse(
            outcome=data["outcome"],
            turn_id=data.get("turn_id"),
            session_id=data.get("session_id"),
        )

    async def pull_stream(
        self,
        start_after_sequence: int | None = None,
    ) -> tuple[list[StreamEvent], int | None]:
        payload: dict[str, Any] = {
            "channel_id": self.channel_id,
            "consumer_id": self.consumer_id,
            "limit": self.stream_limit,
            "wait_ms": self.stream_wait_ms,
        }
        if start_after_sequence is None:
            payload["start_mode"] = self.start_mode
        else:
            payload["start_after_sequence"] = start_after_sequence
        response = await self._client.post("/v0/channels/stream/pull", json=payload)
        response.raise_for_status()
        payload = response.json()
        events: list[StreamEvent] = []
        last_sequence: int | None = None
        for item in payload.get("events", []):
            event = StreamEvent(
                sequence=item["sequence"],
                peer_id=item.get("peer_id", ""),
                session_id=item.get("session_id"),
                turn_id=item["turn_id"],
                kind=item["kind"],
                lane=item.get("lane"),
                code=item.get("code"),
                text=item.get("text"),
            )
            events.append(event)
            last_sequence = event.sequence
        return events, last_sequence

    async def ack_stream(self, through_sequence: int) -> None:
        response = await self._client.post(
            "/v0/channels/stream/ack",
            json={
                "channel_id": self.channel_id,
                "consumer_id": self.consumer_id,
                "through_sequence": through_sequence,
            },
        )
        response.raise_for_status()


def _parse_session_open_result(payload: dict[str, Any] | None) -> SessionOpenResult | None:
    if payload is None:
        return None
    return SessionOpenResult(
        session_id=payload["session_id"],
        channel_id=payload["channel_id"],
        peer_id=payload["peer_id"],
        trust_tier=payload["trust_tier"],
        history_policy=payload["history_policy"],
    )
