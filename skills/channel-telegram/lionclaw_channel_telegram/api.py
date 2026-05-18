from __future__ import annotations

from dataclasses import dataclass, field
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


@dataclass(slots=True, frozen=True)
class OutboxAttachment:
    attachment_id: str
    path: str
    filename: str | None = None
    mime_type: str | None = None


@dataclass(slots=True, frozen=True)
class OutboxContent:
    text: str
    format_hint: str = "markdown"
    attachments: list[OutboxAttachment] = field(default_factory=list)


@dataclass(slots=True, frozen=True)
class OutboxDelivery:
    delivery_id: str
    attempt_id: str
    conversation_ref: str
    content: OutboxContent
    thread_ref: str | None = None
    reply_to_ref: str | None = None


@dataclass(slots=True, frozen=True)
class OutboxReportResponse:
    accepted: bool
    status: str
    attempt_status: str


class LionClawApi:
    def __init__(
        self,
        base_url: str,
        channel_id: str,
        consumer_id: str,
        start_mode: str,
        stream_limit: int,
        stream_wait_ms: int,
        timeout_seconds: float = 35.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.channel_id = channel_id
        self.consumer_id = consumer_id
        self.start_mode = start_mode
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
            "event_id": f"telegram-update:{update.update_id}",
            "sender_ref": update.peer_id,
            "conversation_ref": update.peer_id,
            "message_ref": update.message_ref,
            "text": update.text,
            "attachments": [],
            "trigger": "dm",
            "provider_metadata": {
                "update_id": update.update_id,
                "message_id": update.message_ref,
            },
        }
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

    async def pull_outbox(self, limit: int = 10, lease_ms: int = 120_000) -> list[OutboxDelivery]:
        response = await self._client.post(
            "/v0/channels/outbox/pull",
            json={
                "channel_id": self.channel_id,
                "worker_id": self.consumer_id,
                "limit": limit,
                "lease_ms": lease_ms,
            },
        )
        _raise_for_status(response)
        payload = response.json()
        deliveries = payload.get("deliveries")
        if not isinstance(deliveries, list):
            raise RuntimeError("outbox pull response missing deliveries array")
        parsed: list[OutboxDelivery] = []
        for item in deliveries:
            content = item.get("content")
            if not isinstance(content, dict) or not isinstance(content.get("text"), str):
                raise RuntimeError("outbox delivery missing content.text")
            attachments = content.get("attachments", [])
            if not isinstance(attachments, list):
                raise RuntimeError("outbox delivery content.attachments must be an array")
            parsed.append(
                OutboxDelivery(
                    delivery_id=item["delivery_id"],
                    attempt_id=item["attempt_id"],
                    conversation_ref=item["conversation_ref"],
                    thread_ref=item.get("thread_ref"),
                    reply_to_ref=item.get("reply_to_ref"),
                    content=OutboxContent(
                        text=content["text"],
                        format_hint=content.get("format_hint") or "markdown",
                        attachments=[
                            OutboxAttachment(
                                attachment_id=attachment["attachment_id"],
                                path=attachment["path"],
                                filename=attachment.get("filename"),
                                mime_type=attachment.get("mime_type"),
                            )
                            for attachment in attachments
                        ],
                    ),
                )
            )
        return parsed

    async def report_outbox(
        self,
        delivery: OutboxDelivery,
        outcome: str,
        *,
        provider_receipt: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_text: str | None = None,
    ) -> OutboxReportResponse:
        response = await self._client.post(
            "/v0/channels/outbox/report",
            json={
                "channel_id": self.channel_id,
                "worker_id": self.consumer_id,
                "delivery_id": delivery.delivery_id,
                "attempt_id": delivery.attempt_id,
                "outcome": outcome,
                "provider_receipt": provider_receipt,
                "error_code": error_code,
                "error_text": error_text,
            },
        )
        _raise_for_status(response)
        payload = response.json()
        return OutboxReportResponse(
            accepted=bool(payload["accepted"]),
            status=payload["status"],
            attempt_status=payload["attempt_status"],
        )


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
