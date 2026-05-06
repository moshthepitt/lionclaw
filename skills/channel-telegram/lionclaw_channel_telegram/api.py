from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from lionclaw_channel_telegram.telegram import TelegramTextUpdate


@dataclass(slots=True, frozen=True)
class InboundResponse:
    outcome: str


@dataclass(slots=True, frozen=True)
class StreamEvent:
    sequence: int
    peer_id: str
    kind: str
    turn_id: str = ""
    lane: str | None = None
    code: str | None = None
    text: str = ""


class LionClawApi:
    def __init__(
        self,
        base_url: str,
        channel_id: str,
        consumer_id: str,
        start_mode: str,
        runtime_id: str | None,
        stream_limit: int,
        stream_wait_ms: int,
        timeout_seconds: float = 35.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.channel_id = channel_id
        self.consumer_id = consumer_id
        self.start_mode = start_mode
        self.runtime_id = runtime_id
        self.stream_limit = stream_limit
        self.stream_wait_ms = stream_wait_ms
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def send_inbound(self, update: TelegramTextUpdate) -> InboundResponse:
        payload: dict[str, Any] = {
            "channel_id": self.channel_id,
            "peer_id": update.peer_id,
            "text": update.text,
            "update_id": update.update_id,
            "external_message_id": f"telegram-update:{update.update_id}",
        }
        if self.runtime_id is not None:
            payload["runtime_id"] = self.runtime_id
        response = await self._client.post("/v0/channels/inbound", json=payload)
        _raise_for_status(response)
        return InboundResponse(outcome=response.json()["outcome"])

    async def pull_stream(self) -> list[StreamEvent]:
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
        _raise_for_status(response)
        payload = response.json()
        events = payload.get("events")
        if not isinstance(events, list):
            raise RuntimeError("stream pull response missing events array")
        return [
            StreamEvent(
                sequence=item["sequence"],
                peer_id=item.get("peer_id", ""),
                kind=item["kind"],
                turn_id=item.get("turn_id", ""),
                lane=item.get("lane"),
                code=item.get("code"),
                text=item.get("text", ""),
            )
            for item in events
        ]

    async def ack_stream(self, through_sequence: int) -> None:
        response = await self._client.post(
            "/v0/channels/stream/ack",
            json={
                "channel_id": self.channel_id,
                "consumer_id": self.consumer_id,
                "through_sequence": through_sequence,
            },
        )
        _raise_for_status(response)


def _raise_for_status(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as err:
        body = response.text.strip()
        if body:
            raise RuntimeError(
                f"{response.status_code} {response.reason_phrase}: {body}"
            ) from err
        raise
