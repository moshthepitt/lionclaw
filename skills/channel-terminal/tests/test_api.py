import json
import unittest

import httpx

from lionclaw_channel_terminal.api import LionClawApi, PeerState


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
            self.assertEqual(
                seen_event_ids,
                [
                    "terminal-inbound:interactive:test:0",
                    "terminal-inbound:interactive:test:0",
                    "terminal-inbound:interactive:test:1",
                ],
            )
        finally:
            await api.close()


if __name__ == "__main__":
    unittest.main()
