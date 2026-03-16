import unittest

from lionclaw_channel_terminal.app import AppConfig, TerminalChannelApp
from lionclaw_channel_terminal.state import ChannelViewState, StreamEvent


class ChannelViewStateTests(unittest.TestCase):
    def test_answer_and_reasoning_stay_separate(self):
        state = ChannelViewState(peer_id="mosh")
        state.append_user_message("hello")
        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="reasoning",
                text="planning next step",
            )
        )
        state.apply_stream_event(
            StreamEvent(
                sequence=2,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="answer",
                text="world",
            )
        )
        state.apply_stream_event(
            StreamEvent(
                sequence=3,
                peer_id="mosh",
                turn_id="turn-1",
                kind="done",
            )
        )

        self.assertIn("you> hello", state.transcript_text())
        self.assertIn("lionclaw> world", state.transcript_text())
        self.assertNotIn("planning next step", state.transcript_text())
        self.assertIn("thinking> planning next step", state.reasoning_text())

    def test_pending_pairing_banner_includes_approve_command(self):
        state = ChannelViewState(peer_id="mosh")
        state.set_pairing_state(status="pending", pairing_code="123456")
        banner = state.pairing_banner("terminal", "mosh")
        self.assertIn("123456", banner)
        self.assertIn("lionclaw channel pairing approve terminal mosh 123456", banner)

    def test_blocked_peer_disables_input(self):
        state = ChannelViewState(peer_id="mosh")
        state.set_pairing_state(status="blocked")
        self.assertTrue(state.input_disabled())


class _FailingApi:
    async def send_inbound(self, text: str) -> None:
        raise RuntimeError("boom")


class _SuccessfulApi:
    def __init__(self) -> None:
        self.sent_text: str | None = None

    async def send_inbound(self, text: str) -> None:
        self.sent_text = text


class TerminalChannelAppTests(unittest.IsolatedAsyncioTestCase):
    async def test_submit_text_does_not_append_failed_send(self):
        app = TerminalChannelApp(
            AppConfig(
                home="/tmp/lionclaw",
                base_url="http://127.0.0.1:8979",
                channel_id="terminal",
                peer_id="mosh",
                consumer_id="interactive:test",
                stream_start_mode="tail",
                stream_limit=50,
                stream_wait_ms=0,
                runtime_id=None,
            )
        )
        app.api = _FailingApi()
        app._render_views = lambda: None  # type: ignore[method-assign]

        accepted = await app.submit_text("hello")

        self.assertFalse(accepted)
        self.assertEqual(app.state.transcript_text(), "")
        self.assertIn("send failed: boom", app.state.status_text())

    async def test_submit_text_appends_only_after_success(self):
        app = TerminalChannelApp(
            AppConfig(
                home="/tmp/lionclaw",
                base_url="http://127.0.0.1:8979",
                channel_id="terminal",
                peer_id="mosh",
                consumer_id="interactive:test",
                stream_start_mode="tail",
                stream_limit=50,
                stream_wait_ms=0,
                runtime_id=None,
            )
        )
        api = _SuccessfulApi()
        app.api = api
        app._render_views = lambda: None  # type: ignore[method-assign]

        accepted = await app.submit_text("hello")

        self.assertTrue(accepted)
        self.assertEqual(api.sent_text, "hello")
        self.assertIn("you> hello", app.state.transcript_text())


if __name__ == "__main__":
    unittest.main()
