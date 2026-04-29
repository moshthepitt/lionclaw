import json
import unittest

import httpx

from lionclaw_channel_terminal.api import LionClawApi


class TerminalApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_inbound_reuses_external_message_id_after_transport_failure(self):
        seen_external_message_ids: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            data = json.loads(request.content.decode())
            seen_external_message_ids.append(data["external_message_id"])
            if len(seen_external_message_ids) == 1:
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
            runtime_id=None,
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
                seen_external_message_ids,
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
