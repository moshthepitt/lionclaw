import json
import unittest

import httpx

from lionclaw_channel_terminal.api import LionClawApi, OutboxContent, OutboxDelivery, PeerState


class TerminalApiTests(unittest.IsolatedAsyncioTestCase):
    async def _peer_state_from_pairing_payload(self, payload: dict) -> PeerState:
        async def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.url.path, "/v0/channels/pairing")
            self.assertEqual(request.url.params.get("channel_id"), "terminal")
            return httpx.Response(200, json=payload)

        api = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="mosh",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        await api._client.aclose()
        api._client = httpx.AsyncClient(
            base_url=api.base_url,
            transport=httpx.MockTransport(handler),
        )
        try:
            return await api.fetch_peer_state()
        finally:
            await api.close()

    async def test_session_key_escapes_provider_shaped_peer_ref(self):
        api = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="telegram:user:%456",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        try:
            self.assertEqual(
                api.session_key,
                "channel:terminal:direct:telegram%3Auser%3A%25456",
            )
        finally:
            await api.close()

    async def test_fetch_peer_state_uses_approved_direct_grant(self):
        peer_state = await self._peer_state_from_pairing_payload(
            {
                "pairings": [],
                "grants": [
                    {
                        "sender_ref": "mosh",
                        "routing_profile": "direct",
                        "status": "approved",
                        "trust_tier": "main",
                    }
                ],
            }
        )

        self.assertEqual(peer_state.status, "approved")
        self.assertEqual(peer_state.trust_tier, "main")

    async def test_fetch_peer_state_uses_blocked_direct_grant_over_pairing(self):
        peer_state = await self._peer_state_from_pairing_payload(
            {
                "pairings": [
                    {
                        "sender_ref": "mosh",
                        "requested_profile": "direct",
                        "status": "approved",
                        "pairing_id": "pairing-1",
                    }
                ],
                "grants": [
                    {
                        "sender_ref": "mosh",
                        "routing_profile": "direct",
                        "status": "blocked",
                        "trust_tier": "untrusted",
                    }
                ],
            }
        )

        self.assertEqual(peer_state.status, "blocked")

    async def test_fetch_peer_state_treats_revoked_grant_as_unapproved(self):
        peer_state = await self._peer_state_from_pairing_payload(
            {
                "pairings": [
                    {
                        "sender_ref": "mosh",
                        "requested_profile": "direct",
                        "status": "approved",
                        "pairing_id": "pairing-1",
                    }
                ],
                "grants": [
                    {
                        "sender_ref": "mosh",
                        "routing_profile": "direct",
                        "status": "revoked",
                        "trust_tier": "main",
                    }
                ],
            }
        )

        self.assertEqual(peer_state.status, "unknown")

    async def test_fetch_peer_state_shows_pending_after_revoked_grant(self):
        peer_state = await self._peer_state_from_pairing_payload(
            {
                "pairings": [
                    {
                        "sender_ref": "mosh",
                        "requested_profile": "direct",
                        "status": "pending",
                        "pairing_id": "pairing-2",
                    }
                ],
                "grants": [
                    {
                        "sender_ref": "mosh",
                        "routing_profile": "direct",
                        "status": "revoked",
                        "trust_tier": "main",
                    }
                ],
            }
        )

        self.assertEqual(peer_state.status, "pending")
        self.assertEqual(peer_state.pairing_id, "pairing-2")

    async def test_send_inbound_reuses_event_id_after_transport_failure(self):
        seen_event_ids: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            data = json.loads(request.content.decode())
            seen_event_ids.append(data["event_id"])
            self.assertNotIn("runtime_id", data)
            if len(seen_event_ids) == 1:
                raise httpx.TransportError("dropped response")
            return httpx.Response(
                200,
                json={
                    "outcome": "queued",
                    "turn_id": "turn-1",
                    "session_id": "session-1",
                },
            )

        api = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="mosh",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        await api._client.aclose()
        api._client = httpx.AsyncClient(
            base_url=api.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            with self.assertRaises(httpx.TransportError):
                await api.send_inbound("hello")

            response = await api.send_inbound("hello")

            self.assertEqual(response.outcome, "queued")
            await api.send_inbound("hello again")
            self.assertEqual(seen_event_ids[0], seen_event_ids[1])
            self.assertNotEqual(seen_event_ids[1], seen_event_ids[2])
            self.assertEqual(_event_sequence(seen_event_ids[0]), "0")
            self.assertEqual(_event_sequence(seen_event_ids[2]), "1")
            self.assertEqual(
                _event_prefix(seen_event_ids[0]),
                _event_prefix(seen_event_ids[2]),
            )
        finally:
            await api.close()

    async def test_send_inbound_event_id_is_unique_across_client_restarts(self):
        seen_event_ids: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            data = json.loads(request.content.decode())
            seen_event_ids.append(data["event_id"])
            return httpx.Response(
                200,
                json={
                    "outcome": "queued",
                    "turn_id": f"turn-{len(seen_event_ids)}",
                    "session_id": "session-1",
                },
            )

        transport = httpx.MockTransport(handler)
        first = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="mosh",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        second = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="mosh",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        await first._client.aclose()
        await second._client.aclose()
        first._client = httpx.AsyncClient(
            base_url=first.base_url,
            transport=transport,
        )
        second._client = httpx.AsyncClient(
            base_url=second.base_url,
            transport=transport,
        )

        try:
            await first.send_inbound("before restart")
            await second.send_inbound("after restart")

            self.assertEqual(len(seen_event_ids), 2)
            self.assertEqual(_event_sequence(seen_event_ids[0]), "0")
            self.assertEqual(_event_sequence(seen_event_ids[1]), "0")
            self.assertNotEqual(seen_event_ids[0], seen_event_ids[1])
            self.assertNotEqual(
                _event_prefix(seen_event_ids[0]),
                _event_prefix(seen_event_ids[1]),
            )
        finally:
            await first.close()
            await second.close()

    async def test_pull_outbox_scopes_to_terminal_peer_and_parses_content(self):
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "deliveries": [
                        {
                            "delivery_id": "delivery-1",
                            "attempt_id": "attempt-1",
                            "channel_id": "terminal",
                            "conversation_ref": "mosh",
                            "session_id": "session-1",
                            "turn_id": "turn-1",
                            "content": {
                                "text": "final answer",
                                "format_hint": "markdown",
                                "attachments": [
                                    {
                                        "attachment_id": "out-1",
                                        "path": "/drafts/result.pdf",
                                        "filename": "result.pdf",
                                        "mime_type": "application/pdf",
                                    }
                                ],
                            },
                            "attempt_count": 1,
                            "lease_expires_at": "2026-05-18T12:00:00Z",
                            "created_at": "2026-05-18T11:59:00Z",
                        }
                    ]
                },
            )

        api = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="mosh",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        await api._client.aclose()
        api._client = httpx.AsyncClient(
            base_url=api.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            deliveries = await api.pull_outbox(limit=5, lease_ms=60_000)

            self.assertEqual(captured["path"], "/v0/channels/outbox/pull")
            self.assertEqual(
                captured["payload"],
                {
                    "channel_id": "terminal",
                    "worker_id": "interactive:test",
                    "conversation_ref": "mosh",
                    "limit": 5,
                    "lease_ms": 60000,
                },
            )
            self.assertEqual(deliveries[0].content.text, "final answer")
            self.assertEqual(deliveries[0].content.format_hint, "markdown")
            self.assertEqual(deliveries[0].content.attachments[0].path, "/drafts/result.pdf")
        finally:
            await api.close()

    async def test_report_outbox_posts_terminal_receipt(self):
        captured: dict[str, object] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(
                200,
                json={
                    "delivery_id": "delivery-1",
                    "attempt_id": "attempt-1",
                    "accepted": True,
                    "status": "delivered",
                    "attempt_status": "delivered",
                },
            )

        api = LionClawApi(
            base_url="http://lionclaw.test",
            channel_id="terminal",
            peer_id="mosh",
            consumer_id="interactive:test",
            start_mode="tail",
            stream_limit=50,
            stream_wait_ms=0,
        )
        await api._client.aclose()
        api._client = httpx.AsyncClient(
            base_url=api.base_url,
            transport=httpx.MockTransport(handler),
        )

        try:
            response = await api.report_outbox(
                OutboxDelivery(
                    delivery_id="delivery-1",
                    attempt_id="attempt-1",
                    channel_id="terminal",
                    conversation_ref="mosh",
                    content=OutboxContent(text="final answer"),
                ),
                "delivered",
                provider_receipt={"provider": "terminal", "rendered": True},
            )

            self.assertTrue(response.accepted)
            self.assertEqual(captured["path"], "/v0/channels/outbox/report")
            self.assertEqual(captured["payload"]["worker_id"], "interactive:test")
            self.assertEqual(captured["payload"]["provider_receipt"]["rendered"], True)
        finally:
            await api.close()


def _event_prefix(event_id: str) -> str:
    prefix, _sequence = event_id.rsplit(":", 1)
    return prefix


def _event_sequence(event_id: str) -> str:
    _prefix, sequence = event_id.rsplit(":", 1)
    return sequence


if __name__ == "__main__":
    unittest.main()
