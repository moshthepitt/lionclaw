from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from lionclaw_channel_terminal.state import PeerStatus, StreamEvent


@dataclass(slots=True)
class PeerState:
    status: PeerStatus
    pairing_code: str | None = None
    trust_tier: str | None = None


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

    async def send_inbound(self, text: str) -> None:
        external_message_id = f"terminal-inbound:{self.consumer_id}:{self._inbound_sequence}"
        payload: dict[str, Any] = {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "text": text,
            "external_message_id": external_message_id,
        }
        if self.runtime_id:
            payload["runtime_id"] = self.runtime_id
        response = await self._client.post("/v0/channels/inbound", json=payload)
        response.raise_for_status()
        self._inbound_sequence += 1

    async def pull_stream(self) -> tuple[list[StreamEvent], int | None]:
        response = await self._client.post(
            "/v0/channels/stream/pull",
            json={
                "channel_id": self.channel_id,
                "consumer_id": self.consumer_id,
                "start_mode": self.start_mode,
                "limit": self.stream_limit,
                "wait_ms": self.stream_wait_ms,
            },
        )
        response.raise_for_status()
        payload = response.json()
        events: list[StreamEvent] = []
        last_sequence: int | None = None
        for item in payload.get("events", []):
            event = StreamEvent(
                sequence=item["sequence"],
                peer_id=item.get("peer_id", ""),
                turn_id=item["turn_id"],
                kind=item["kind"],
                lane=item.get("lane"),
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

    async def stream_forever(self):
        while True:
            yield await self.pull_stream()
            await asyncio.sleep(0)
