from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from lionclaw_channel_telegram.telegram import (
    TelegramDownloadedAttachment,
    TelegramInboundAttachment,
    TelegramInboundUpdate,
    TelegramPairingClaim,
)


@dataclass(slots=True, frozen=True)
class InboundResponse:
    outcome: str
    reason_code: str | None = None
    pairing_id: str | None = None
    pairing_code: str | None = None


@dataclass(slots=True, frozen=True)
class PairingClaimResponse:
    outcome: str
    grant_id: str | None = None
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class AttachmentStageResponse:
    status: str
    size_bytes: int
    sha256: str
    runtime_path: str | None = None
    reason_code: str | None = None


@dataclass(slots=True, frozen=True)
class AttachmentMissingReport:
    attachment_id: str
    reason_code: str
    reason_text: str | None = None


@dataclass(slots=True, frozen=True)
class AttachmentFinalizeResponse:
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
    format_hint: str = "plain"
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

    async def send_inbound(self, update: TelegramInboundUpdate) -> InboundResponse:
        payload: dict[str, Any] = {
            "channel_id": self.channel_id,
            "event_id": update.event_id,
            "sender_ref": update.sender_ref,
            "conversation_ref": update.conversation_ref,
            "thread_ref": update.thread_ref,
            "message_ref": update.message_ref,
            "text": update.text,
            "attachments": [
                _attachment_descriptor(attachment) for attachment in update.attachments
            ],
            "reply_to_ref": update.reply_to_ref,
            "trigger": update.trigger,
            "provider_metadata": update.provider_metadata,
        }
        response = await self._client.post("/v0/channels/inbound", json=payload)
        _raise_for_status(response)
        payload = response.json()
        return InboundResponse(
            outcome=payload["outcome"],
            reason_code=payload.get("reason_code"),
            pairing_id=payload.get("pairing_id"),
            pairing_code=payload.get("pairing_code"),
        )

    async def claim_pairing(self, claim: TelegramPairingClaim) -> PairingClaimResponse:
        response = await self._client.post(
            "/v0/channels/pairing/claim",
            json={
                "channel_id": self.channel_id,
                "token": claim.token,
                "sender_ref": claim.sender_ref,
                "conversation_ref": claim.conversation_ref,
                "thread_ref": claim.thread_ref,
                "provider_metadata": claim.provider_metadata,
            },
        )
        _raise_for_status(response)
        payload = response.json()
        return PairingClaimResponse(
            outcome=payload["outcome"],
            grant_id=payload.get("grant_id"),
            reason_code=payload.get("reason_code"),
        )

    async def stage_attachment(
        self,
        update: TelegramInboundUpdate,
        attachment: TelegramInboundAttachment,
        downloaded: TelegramDownloadedAttachment,
    ) -> AttachmentStageResponse:
        data = {
            "channel_id": self.channel_id,
            "event_id": update.event_id,
            "attachment_id": attachment.attachment_id,
            "kind": attachment.kind,
        }
        if downloaded.filename is not None:
            data["filename"] = downloaded.filename
        if downloaded.mime_type is not None:
            data["mime_type"] = downloaded.mime_type
        if attachment.caption is not None:
            data["caption"] = attachment.caption
        response = await self._client.post(
            "/v0/channels/attachments/stage",
            data=data,
            files={
                "file": (
                    downloaded.filename or attachment.attachment_id,
                    downloaded.content,
                    downloaded.mime_type or "application/octet-stream",
                )
            },
        )
        _raise_for_status(response)
        payload = response.json()
        return AttachmentStageResponse(
            status=payload["status"],
            size_bytes=payload["size_bytes"],
            sha256=payload["sha256"],
            runtime_path=payload.get("runtime_path"),
            reason_code=payload.get("reason_code"),
        )

    async def finalize_attachments(
        self,
        update: TelegramInboundUpdate,
        missing: list[AttachmentMissingReport],
    ) -> AttachmentFinalizeResponse:
        response = await self._client.post(
            "/v0/channels/attachments/finalize",
            json={
                "channel_id": self.channel_id,
                "event_id": update.event_id,
                "worker_id": self.consumer_id,
                "missing": [
                    {
                        "attachment_id": item.attachment_id,
                        "reason_code": item.reason_code,
                        "reason_text": item.reason_text,
                    }
                    for item in missing
                ],
            },
        )
        _raise_for_status(response)
        return AttachmentFinalizeResponse(outcome=response.json()["outcome"])

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
                        format_hint=content.get("format_hint") or "plain",
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


def _attachment_descriptor(attachment: TelegramInboundAttachment) -> dict[str, Any]:
    return {
        "attachment_id": attachment.attachment_id,
        "kind": attachment.kind,
        "mime_type": attachment.mime_type,
        "filename": attachment.filename,
        "size_bytes": attachment.size_bytes,
        "provider_file_ref": attachment.provider_file_ref,
        "caption": attachment.caption,
    }
