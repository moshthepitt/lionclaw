import json
import unittest

import httpx

from lionclaw_channel_terminal.api import LionClawApi


class TerminalApiTests(unittest.IsolatedAsyncioTestCase):
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
