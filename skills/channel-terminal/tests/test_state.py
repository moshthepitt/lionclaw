import unittest
import asyncio

from textual.containers import VerticalScroll
from textual.widgets import Input

from lionclaw_channel_terminal.api import (
    InboundResponse,
    PeerState,
    SessionActionResult,
    SessionLatestSnapshot,
    SessionOpenResult,
    SessionTurnSnapshot,
)
from lionclaw_channel_terminal.app import AppConfig, TerminalChannelApp
from lionclaw_channel_terminal.state import ChannelViewState, StreamEvent


class ChannelViewStateTests(unittest.TestCase):
    def test_answer_and_reasoning_stay_separate(self):
        state = ChannelViewState(peer_id="mosh")
        state.begin_submit("hello")
        state.mark_queued("turn-1")
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

    def test_channel_scoped_stream_event_does_not_select_active_session(self):
        state = ChannelViewState(peer_id="mosh")

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                kind="message_delta",
                lane="answer",
                text="Pairing required.",
            )
        )

        self.assertIsNone(state.active_session_id)
        self.assertEqual(state.transcript_text(), "")
        self.assertIn("[message] Pairing required.", state.activity_text())

    def test_backlog_stream_event_does_not_overwrite_active_session(self):
        state = ChannelViewState(peer_id="mosh")
        state.active_session_id = "session-current"

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                session_id="session-stale",
                turn_id="turn-stale",
                kind="status",
                code="queue.completed",
                text="older turn completed",
            )
        )

        self.assertEqual(state.active_session_id, "session-current")
        self.assertIn("[status] queue.completed: older turn completed", state.activity_text())

    def test_blocked_peer_disables_input(self):
        state = ChannelViewState(peer_id="mosh")
        state.set_pairing_state(status="blocked")
        self.assertTrue(state.input_disabled())

    def test_new_submit_clears_old_reasoning_and_shows_pending_placeholder(self):
        state = ChannelViewState(peer_id="mosh")
        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-old",
                kind="message_delta",
                lane="reasoning",
                text="old reasoning",
            )
        )

        state.begin_submit("hello")

        self.assertIn("you> hello", state.transcript_text())
        self.assertEqual(state.reasoning_text(), "Waiting to queue this turn...")
        self.assertTrue(state.input_disabled())

    def test_answer_without_reasoning_replaces_waiting_placeholder(self):
        state = ChannelViewState(peer_id="mosh")
        state.begin_submit("hello")
        state.mark_queued("turn-1")

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="answer",
                text="hello back",
            )
        )

        self.assertEqual(state.reasoning_text(), "No reasoning for this turn yet.")

    def test_reasoning_after_answer_stays_in_thinking_pane(self):
        state = ChannelViewState(peer_id="mosh")
        state.begin_submit("hello")
        state.mark_queued("turn-1")

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="answer",
                text="hello back",
            )
        )
        state.apply_stream_event(
            StreamEvent(
                sequence=2,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="reasoning",
                text="late reasoning",
            )
        )

        self.assertIn("late reasoning", state.reasoning_text())
        self.assertNotIn("late reasoning", state.transcript_text())

    def test_turn_completed_reconciles_answer_from_canonical_snapshot(self):
        state = ChannelViewState(peer_id="mosh")
        state.begin_submit("hello")
        state.mark_queued("turn-1")

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="answer",
                text="partial",
            )
        )
        state.apply_stream_event(
            StreamEvent(
                sequence=2,
                peer_id="mosh",
                turn_id="turn-1",
                kind="turn_completed",
                lane="answer",
                text="complete final answer",
            )
        )

        transcript = state.transcript_text()
        self.assertIn("lionclaw> complete final answer", transcript)
        self.assertNotIn("partial", transcript)
        self.assertFalse(state.input_disabled())

    def test_done_terminates_stream_without_implying_success(self):
        state = ChannelViewState(peer_id="mosh")
        state.begin_submit("hello")
        state.mark_queued("turn-1")

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-1",
                kind="done",
            )
        )

        turn = state.turns["turn-1"]
        self.assertEqual(turn.status, "running")
        self.assertIsNone(state.active_turn_id)
        self.assertFalse(state.input_disabled())

    def test_mark_queued_preserves_completed_stream_events_that_arrived_first(self):
        state = ChannelViewState(peer_id="mosh")
        state.begin_submit("hello")

        state.apply_stream_event(
            StreamEvent(
                sequence=1,
                peer_id="mosh",
                turn_id="turn-1",
                kind="message_delta",
                lane="answer",
                text="early answer",
            )
        )
        state.apply_stream_event(
            StreamEvent(
                sequence=2,
                peer_id="mosh",
                turn_id="turn-1",
                kind="turn_completed",
                lane="answer",
                text="complete early answer",
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
        state.mark_queued("turn-1", "session-1")

        self.assertEqual([turn.turn_id for turn in state.ordered_turns()], ["turn-1"])
        transcript = state.transcript_text()
        self.assertIn("you> hello", transcript)
        self.assertIn("lionclaw> complete early answer", transcript)
        self.assertFalse(state.input_disabled())

    def test_restore_running_history_keeps_transcript_and_thinking_blank(self):
        state = ChannelViewState(peer_id="mosh")
        state.restore_session_history(
            "session-1",
            [
                SessionTurnSnapshot(
                    turn_id="turn-1",
                    kind="normal",
                    status="running",
                    display_user_text="continue this",
                    assistant_text="partial answer",
                    error_code=None,
                    error_text=None,
                )
            ],
        )

        self.assertEqual(state.active_session_id, "session-1")
        self.assertEqual(state.active_turn_id, "turn-1")
        self.assertIn("you> continue this", state.transcript_text())
        self.assertIn("partial answer", state.transcript_text())
        self.assertEqual(state.reasoning_text(), "")
        self.assertTrue(state.input_disabled())

    def test_restore_interrupted_history_renders_partial_marker_and_error(self):
        state = ChannelViewState(peer_id="mosh")
        state.restore_session_history(
            "session-1",
            [
                SessionTurnSnapshot(
                    turn_id="turn-1",
                    kind="normal",
                    status="interrupted",
                    display_user_text="work on this",
                    assistant_text="partial answer",
                    error_code="runtime.interrupted",
                    error_text="turn interrupted by kernel restart",
                )
            ],
        )

        transcript = state.transcript_text()
        self.assertIn("[partial] previous assistant reply was interrupted", transcript)
        self.assertIn("partial answer", transcript)
        self.assertIn("error> turn interrupted by kernel restart", transcript)
        self.assertFalse(state.input_disabled())


class _FailingApi:
    async def send_inbound(self, text: str, session_id: str | None = None) -> InboundResponse:
        raise RuntimeError("boom")


class _SuccessfulApi:
    def __init__(self) -> None:
        self.sent_text: str | None = None
        self.sent_session_id: str | None = None

    async def send_inbound(self, text: str, session_id: str | None = None) -> InboundResponse:
        self.sent_text = text
        self.sent_session_id = session_id
        return InboundResponse(outcome="queued", turn_id="turn-1", session_id="session-1")


class _InteractiveApi(_SuccessfulApi):
    def __init__(self) -> None:
        super().__init__()
        self.open_calls = 0
        self.action_calls: list[tuple[str, str]] = []
        self.fetch_latest_calls = 0

    async def open_session(
        self,
        trust_tier: str,
        history_policy: str = "interactive",
    ) -> SessionOpenResult:
        self.open_calls += 1
        return SessionOpenResult(
            session_id="session-opened",
            channel_id="terminal",
            peer_id="mosh",
            trust_tier=trust_tier,
            history_policy=history_policy,
        )

    async def run_session_action(self, session_id: str, action: str):
        self.action_calls.append((session_id, action))
        return SessionActionResult(
            session_id="session-reset" if action == "reset_session" else session_id,
            turn_id="turn-action" if action != "reset_session" else None,
        )

    async def fetch_latest_session(
        self,
        history_policy: str = "interactive",
    ) -> SessionLatestSnapshot:
        self.fetch_latest_calls += 1
        return SessionLatestSnapshot(
            session=None,
            turns=[],
            resume_after_sequence=None,
        )


class _FlakyStreamApi(_InteractiveApi):
    def __init__(self) -> None:
        super().__init__()
        self.pull_args: list[int | None] = []
        self.second_pull_seen = asyncio.Event()

    async def pull_stream(self, start_after_sequence: int | None = None):
        self.pull_args.append(start_after_sequence)
        if len(self.pull_args) == 1:
            raise RuntimeError("stream boom")
        self.second_pull_seen.set()
        await asyncio.sleep(3600)
        return [], None

    async def ack_stream(self, through_sequence: int) -> None:
        return None


class _PairingRefreshApi(_InteractiveApi):
    def __init__(
        self,
        peer_state: PeerState,
        latest_snapshot: SessionLatestSnapshot | None = None,
    ) -> None:
        super().__init__()
        self.peer_state = peer_state
        self.latest_snapshot = latest_snapshot or SessionLatestSnapshot(
            session=None,
            turns=[],
            resume_after_sequence=None,
        )
        self.fetch_latest_calls = 0

    async def fetch_peer_state(self) -> PeerState:
        return self.peer_state

    async def fetch_latest_session(
        self,
        history_policy: str = "interactive",
    ) -> SessionLatestSnapshot:
        self.fetch_latest_calls += 1
        return self.latest_snapshot


class _RecoveringRestoreApi(_PairingRefreshApi):
    async def fetch_latest_session(
        self,
        history_policy: str = "interactive",
    ) -> SessionLatestSnapshot:
        self.fetch_latest_calls += 1
        if self.fetch_latest_calls == 1:
            raise RuntimeError("restore boom")
        return self.latest_snapshot


class _FailingRestoreApi(_InteractiveApi):
    async def fetch_latest_session(
        self,
        history_policy: str = "interactive",
    ) -> SessionLatestSnapshot:
        self.fetch_latest_calls += 1
        raise RuntimeError("restore boom")


class _MountedApi(_PairingRefreshApi):
    def __init__(self) -> None:
        super().__init__(peer_state=PeerState(status="approved", trust_tier="main"))

    async def pull_stream(self, start_after_sequence: int | None = None):
        await asyncio.sleep(3600)
        return [], None

    async def ack_stream(self, through_sequence: int) -> None:
        return None

    async def close(self) -> None:
        return None


def _make_app() -> TerminalChannelApp:
    return TerminalChannelApp(
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


def _disable_rendering(app: TerminalChannelApp) -> None:
    app._render_views = lambda: None  # type: ignore[method-assign]


class TerminalChannelAppTests(unittest.IsolatedAsyncioTestCase):
    async def test_mount_focuses_enabled_input(self):
        app = _make_app()
        api = _MountedApi()
        app.api = api

        async with app.run_test() as pilot:
            await pilot.pause()
            self.assertTrue(app.query_one(Input).has_focus)
            self.assertEqual(api.fetch_latest_calls, 1)

    async def test_restore_scrolls_answer_pane_to_latest_turn_after_markdown_layout(self):
        app = _make_app()
        api = _MountedApi()
        api.latest_snapshot = SessionLatestSnapshot(
            session=SessionOpenResult(
                session_id="session-1",
                channel_id="terminal",
                peer_id="mosh",
                trust_tier="main",
                history_policy="interactive",
            ),
            turns=[
                SessionTurnSnapshot(
                    turn_id="turn-1",
                    kind="normal",
                    status="completed",
                    display_user_text="Say exactly: terminal e2e restore ok",
                    assistant_text="terminal e2e restore ok",
                    error_code=None,
                    error_text=None,
                ),
                SessionTurnSnapshot(
                    turn_id="turn-2",
                    kind="normal",
                    status="completed",
                    display_user_text="Say exactly: terminal e2e second ok",
                    assistant_text="terminal e2e second ok",
                    error_code=None,
                    error_text=None,
                ),
            ],
            resume_after_sequence=24,
        )
        app.api = api

        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            await pilot.pause()

            answer_scroll = app.query_one("#answer-scroll", VerticalScroll)
            self.assertGreater(answer_scroll.max_scroll_y, 0)
            self.assertTrue(answer_scroll.is_vertical_scroll_end)

    async def test_pairing_approval_refocuses_input_without_stealing_on_every_render(self):
        app = _make_app()
        api = _MountedApi()
        api.peer_state = PeerState(status="pending", pairing_code="123456")
        app.api = api

        async with app.run_test() as pilot:
            await pilot.pause()
            self.assertTrue(app.query_one(Input).has_focus)

            await pilot.press("tab")
            await pilot.pause()
            self.assertFalse(app.query_one(Input).has_focus)

            api.peer_state = PeerState(status="approved", trust_tier="main")
            await app.refresh_pairing_state()
            await pilot.pause()
            self.assertTrue(app.query_one(Input).has_focus)

            await pilot.press("tab")
            await pilot.pause()
            self.assertFalse(app.query_one(Input).has_focus)

            app._render_views()
            await pilot.pause()
            self.assertFalse(
                app.query_one(Input).has_focus,
                "ordinary redraws should not steal focus back from another widget",
            )

    async def test_submit_text_keeps_local_echo_when_send_fails(self):
        app = _make_app()
        app.api = _FailingApi()
        _disable_rendering(app)

        accepted = await app.submit_text("hello")

        self.assertFalse(accepted)
        self.assertIn("you> hello", app.state.transcript_text())
        self.assertIn("send failed: boom", app.state.activity_text())
        self.assertFalse(app.state.input_disabled())

    async def test_submit_text_binds_active_turn_after_success(self):
        app = _make_app()
        api = _SuccessfulApi()
        app.api = api
        _disable_rendering(app)

        accepted = await app.submit_text("hello")

        self.assertTrue(accepted)
        self.assertEqual(api.sent_text, "hello")
        self.assertIsNone(api.sent_session_id)
        self.assertIn("you> hello", app.state.transcript_text())
        self.assertEqual(app.state.active_turn_id, "turn-1")
        self.assertEqual(app.state.active_session_id, "session-1")
        self.assertTrue(app.state.input_disabled())

    async def test_done_event_reenables_input(self):
        app = _make_app()
        _disable_rendering(app)
        app.state.begin_submit("hello")
        app.state.mark_queued("turn-1")

        app.state.apply_stream_event(
            StreamEvent(sequence=1, peer_id="mosh", turn_id="turn-1", kind="done")
        )

        self.assertFalse(app.state.input_disabled())

    async def test_submit_text_opens_interactive_session_for_approved_peer(self):
        app = _make_app()
        api = _InteractiveApi()
        app.api = api
        _disable_rendering(app)
        app.state.set_pairing_state(status="approved", trust_tier="main")

        accepted = await app.submit_text("hello")

        self.assertTrue(accepted)
        self.assertEqual(api.open_calls, 1)
        self.assertEqual(api.sent_session_id, "session-opened")
        self.assertEqual(app.state.active_session_id, "session-1")

    async def test_reset_swaps_to_fresh_session_and_clears_history(self):
        app = _make_app()
        api = _InteractiveApi()
        app.api = api
        _disable_rendering(app)
        app.state.active_session_id = "session-1"
        app.state.begin_submit("hello")
        app.state.clear_pending_turn()

        reset = await app.reset_session()

        self.assertTrue(reset)
        self.assertEqual(api.action_calls, [("session-1", "reset_session")])
        self.assertEqual(app.state.active_session_id, "session-reset")
        self.assertEqual(app.state.transcript_text(), "")

    async def test_stream_loop_retries_initial_resume_cursor_after_first_pull_failure(self):
        app = _make_app()
        api = _FlakyStreamApi()
        app.api = api
        _disable_rendering(app)
        app._initial_stream_start_after_sequence = 42

        task = asyncio.create_task(app.stream_loop())
        await asyncio.wait_for(api.second_pull_seen.wait(), timeout=2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(api.pull_args[:2], [42, 42])

    async def test_refresh_pairing_state_clears_pending_transient_view_after_approval(self):
        app = _make_app()
        api = _PairingRefreshApi(
            peer_state=PeerState(status="approved", trust_tier="main"),
        )
        app.api = api
        _disable_rendering(app)
        app.state.set_pairing_state(status="pending", pairing_code="123456")
        app.state.begin_submit("hello")
        app.state.clear_pending_turn()
        app.state.activity_lines.append("[status] pairing pending")

        await app.refresh_pairing_state()

        self.assertEqual(api.fetch_latest_calls, 1)
        self.assertEqual(app.state.pairing.status, "approved")
        self.assertEqual(app.state.transcript_text(), "")
        self.assertEqual(app.state.activity_text(), "[status] peer approved")
        self.assertIsNone(app.state.active_session_id)
        self.assertFalse(app.state.input_disabled())

    async def test_refresh_pairing_state_restores_latest_session_after_approval(self):
        app = _make_app()
        api = _PairingRefreshApi(
            peer_state=PeerState(status="approved", trust_tier="main"),
            latest_snapshot=SessionLatestSnapshot(
                session=SessionOpenResult(
                    session_id="session-1",
                    channel_id="terminal",
                    peer_id="mosh",
                    trust_tier="main",
                    history_policy="interactive",
                ),
                turns=[
                    SessionTurnSnapshot(
                        turn_id="turn-1",
                        kind="normal",
                        status="completed",
                        display_user_text="restored question",
                        assistant_text="restored answer",
                        error_code=None,
                        error_text=None,
                    )
                ],
                resume_after_sequence=None,
            ),
        )
        app.api = api
        _disable_rendering(app)
        app.state.set_pairing_state(status="pending", pairing_code="123456")
        app.state.begin_submit("hello")
        app.state.clear_pending_turn()
        app.state.activity_lines.append("[status] pairing pending")

        await app.refresh_pairing_state()

        self.assertEqual(app.state.active_session_id, "session-1")
        transcript = app.state.transcript_text()
        self.assertIn("you> restored question", transcript)
        self.assertIn("lionclaw> restored answer", transcript)
        self.assertEqual(app.state.activity_text(), "[status] peer approved")

    async def test_refresh_pairing_state_retries_failed_approved_restore(self):
        app = _make_app()
        api = _RecoveringRestoreApi(
            peer_state=PeerState(status="approved", trust_tier="main"),
            latest_snapshot=SessionLatestSnapshot(
                session=SessionOpenResult(
                    session_id="session-1",
                    channel_id="terminal",
                    peer_id="mosh",
                    trust_tier="main",
                    history_policy="interactive",
                ),
                turns=[
                    SessionTurnSnapshot(
                        turn_id="turn-1",
                        kind="normal",
                        status="completed",
                        display_user_text="restored question",
                        assistant_text="restored answer",
                        error_code=None,
                        error_text=None,
                    )
                ],
                resume_after_sequence=None,
            ),
        )
        app.api = api
        _disable_rendering(app)

        await app.refresh_pairing_state()

        self.assertEqual(api.fetch_latest_calls, 1)
        self.assertIsNone(app.state.active_session_id)
        self.assertIn("session restore failed: restore boom", app.state.activity_text())

        await app.refresh_pairing_state()

        self.assertEqual(api.fetch_latest_calls, 2)
        self.assertEqual(app.state.active_session_id, "session-1")
        self.assertIn("lionclaw> restored answer", app.state.transcript_text())
        self.assertEqual(app.state.activity_text(), "[status] peer approved")

    async def test_submit_text_blocks_fresh_session_when_restore_is_pending_and_fails(self):
        app = _make_app()
        api = _FailingRestoreApi()
        app.api = api
        _disable_rendering(app)
        app.state.set_pairing_state(status="approved", trust_tier="main")

        accepted = await app.submit_text("hello")

        self.assertFalse(accepted)
        self.assertEqual(api.fetch_latest_calls, 1)
        self.assertEqual(api.open_calls, 0)
        self.assertIsNone(api.sent_text)
        self.assertIsNone(app.state.active_session_id)
        self.assertEqual(app.state.transcript_text(), "")
        self.assertIn("send blocked: latest session restore failed", app.state.activity_text())


if __name__ == "__main__":
    unittest.main()
