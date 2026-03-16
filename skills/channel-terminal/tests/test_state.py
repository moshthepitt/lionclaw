import unittest

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


if __name__ == "__main__":
    unittest.main()
