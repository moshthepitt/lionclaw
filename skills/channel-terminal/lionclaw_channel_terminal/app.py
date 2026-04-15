from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Input, RichLog, Static

from lionclaw_channel_terminal.api import LionClawApi
from lionclaw_channel_terminal.state import ChannelViewState


@dataclass(slots=True)
class AppConfig:
    home: str
    base_url: str
    channel_id: str
    peer_id: str
    consumer_id: str
    stream_start_mode: str
    stream_limit: int
    stream_wait_ms: int
    runtime_id: str | None

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            home=os.environ.get("LIONCLAW_HOME", os.path.expanduser("~/.lionclaw")),
            base_url=os.environ.get("LIONCLAW_BASE_URL", "http://127.0.0.1:8979"),
            channel_id=os.environ.get("LIONCLAW_CHANNEL_ID", "terminal"),
            peer_id=os.environ.get("LIONCLAW_PEER_ID", os.environ.get("USER", "local-user")),
            consumer_id=os.environ["LIONCLAW_CONSUMER_ID"],
            stream_start_mode=os.environ.get("LIONCLAW_STREAM_START_MODE", "tail"),
            stream_limit=int(os.environ.get("LIONCLAW_STREAM_LIMIT", "50")),
            stream_wait_ms=int(os.environ.get("LIONCLAW_STREAM_WAIT_MS", "30000")),
            runtime_id=os.environ.get("LIONCLAW_RUNTIME_ID") or None,
        )


class TerminalChannelApp(App[None]):
    CSS = """
    Screen {
        layout: vertical;
    }

    #pairing-banner {
        dock: top;
        height: auto;
        padding: 0 1;
        background: $panel;
        color: $text;
    }

    #panes {
        height: 1fr;
    }

    #transcript-pane {
        width: 2fr;
        border: solid $accent;
    }

    #thinking-pane {
        width: 1fr;
        border: solid $secondary;
    }

    #transcript-view,
    #thinking-view,
    #status-log {
        height: 1fr;
    }

    #status-log {
        height: 6;
        border: solid $boost;
    }

    Input {
        dock: bottom;
    }
    """

    BINDINGS = [("ctrl+c", "quit", "Quit")]

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self.config = config
        self.state = ChannelViewState(peer_id=config.peer_id)
        self.api = LionClawApi(
            base_url=config.base_url,
            channel_id=config.channel_id,
            peer_id=config.peer_id,
            consumer_id=config.consumer_id,
            start_mode=config.stream_start_mode,
            runtime_id=config.runtime_id,
            stream_limit=config.stream_limit,
            stream_wait_ms=config.stream_wait_ms,
        )
        self._initial_stream_start_after_sequence: int | None = None
        self._first_stream_pull = True
        self._focus_input_requested = True
        self._input_was_disabled = True

    def compose(self) -> ComposeResult:
        yield Static(id="pairing-banner")
        with Horizontal(id="panes"):
            with Vertical(id="transcript-pane"):
                yield Static("", id="transcript-view")
            with Vertical(id="thinking-pane"):
                yield Static("No reasoning for the current turn yet.", id="thinking-view")
        yield RichLog(id="status-log", wrap=True, markup=False, highlight=False)
        yield Input(placeholder="Send a message or /quit", id="input")
        yield Footer()

    async def on_mount(self) -> None:
        self.title = f"LionClaw {self.config.channel_id}"
        self.sub_title = f"peer={self.config.peer_id}"
        self.query_one("#transcript-pane", Vertical).border_title = "Transcript"
        self.query_one("#thinking-pane", Vertical).border_title = "Thinking"
        self.query_one("#status-log", RichLog).border_title = "Status"
        await self.refresh_pairing_state()
        await self.restore_latest_session()
        self._render_views()
        self.run_worker(self.stream_loop(), exclusive=False, group="stream")
        self.set_interval(2.0, self.refresh_pairing_state, pause=False)

    async def on_unmount(self) -> None:
        await self.api.close()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        event.input.value = ""
        if not text:
            return
        if text in {"/quit", "/exit"}:
            self.exit()
            return
        if text == "/continue":
            await self.run_turn_action("/continue", "continue_last_partial")
            return
        if text == "/retry":
            await self.run_turn_action("/retry", "retry_last_turn")
            return
        if text == "/reset":
            await self.reset_session()
            return
        if self.state.input_disabled():
            await self._push_status(
                f"input disabled: {self.state.input_block_reason() or 'unavailable'}"
            )
            return

        await self.submit_text(text)

    async def submit_text(self, text: str) -> bool:
        if self.state.input_disabled():
            await self._push_status(
                f"input disabled: {self.state.input_block_reason() or 'unavailable'}"
            )
            return False

        self.state.begin_submit(text)
        self._render_views()

        try:
            session_id = await self.ensure_interactive_session_for_send()
            response = await self.api.send_inbound(text, session_id=session_id)
        except Exception as err:  # noqa: BLE001
            self.state.mark_send_failed(str(err))
            self._render_views()
            return False

        accepted = self._apply_inbound_response(
            response.outcome,
            response.turn_id,
            response.session_id,
        )
        self._render_views()
        return accepted

    async def run_turn_action(self, command: str, action: str) -> bool:
        if self.state.input_disabled():
            await self._push_status(
                f"input disabled: {self.state.input_block_reason() or 'unavailable'}"
            )
            return False
        if self.state.active_session_id is None:
            await self._push_status("no active interactive session")
            return False

        self.state.begin_submit(command)
        self._render_views()

        try:
            response = await self.api.run_session_action(self.state.active_session_id, action)
        except Exception as err:  # noqa: BLE001
            self.state.mark_send_failed(str(err))
            self._render_views()
            return False

        if not response.turn_id:
            self.state.mark_send_failed(f"action {action} did not start a turn")
            self._render_views()
            return False

        self.state.mark_queued(response.turn_id, response.session_id)
        self._render_views()
        return True

    async def reset_session(self) -> bool:
        if self.state.input_disabled():
            await self._push_status(
                f"input disabled: {self.state.input_block_reason() or 'unavailable'}"
            )
            return False

        try:
            if self.state.active_session_id is None:
                session_id = await self.open_interactive_session()
                if session_id is None:
                    return False
                self.state.reset_for_new_session(session_id)
            else:
                response = await self.api.run_session_action(
                    self.state.active_session_id,
                    "reset_session",
                )
                self.state.reset_for_new_session(response.session_id)
        except Exception as err:  # noqa: BLE001
            await self._push_status(f"reset failed: {err}")
            return False

        self.state.status_lines.append("[status] opened a fresh session")
        self._render_views()
        return True

    async def refresh_pairing_state(self) -> None:
        previous_status = self.state.pairing.status
        try:
            peer_state = await self.api.fetch_peer_state()
        except Exception as err:  # noqa: BLE001
            self.state.status_lines.append(f"[error] peer refresh failed: {err}")
            self._render_views()
            return

        self.state.set_pairing_state(
            status=peer_state.status,
            pairing_code=peer_state.pairing_code,
            trust_tier=peer_state.trust_tier,
        )

        if previous_status != peer_state.status:
            await self._handle_pairing_transition(previous_status, peer_state.status)

        self._render_views()

    async def restore_latest_session(self) -> bool:
        if self.state.pairing.status != "approved":
            self._initial_stream_start_after_sequence = None
            return True

        try:
            snapshot = await self.api.fetch_latest_session(history_policy="interactive")
        except Exception as err:  # noqa: BLE001
            self.state.status_lines.append(f"[error] session restore failed: {err}")
            return False

        if snapshot.session is None:
            self._initial_stream_start_after_sequence = None
            return True

        self.state.restore_session_history(snapshot.session.session_id, snapshot.turns)
        self._initial_stream_start_after_sequence = snapshot.resume_after_sequence
        return True

    async def ensure_interactive_session_for_send(self) -> str | None:
        if self.state.active_session_id is not None:
            return self.state.active_session_id
        if self.state.pairing.status != "approved":
            return None
        return await self.open_interactive_session()

    async def open_interactive_session(self) -> str | None:
        if self.state.pairing.status != "approved":
            await self._push_status("peer must be approved before opening an interactive session")
            return None
        trust_tier = self.state.pairing.trust_tier or "main"
        session = await self.api.open_session(
            trust_tier=trust_tier,
            history_policy="interactive",
        )
        self.state.active_session_id = session.session_id
        return session.session_id

    async def stream_loop(self) -> None:
        while True:
            try:
                start_after_sequence = (
                    self._initial_stream_start_after_sequence
                    if self._first_stream_pull
                    else None
                )
                events, last_sequence = await self.api.pull_stream(
                    start_after_sequence=start_after_sequence
                )
                self._first_stream_pull = False
                for event in events:
                    self.state.apply_stream_event(event)
                if last_sequence is not None:
                    await self.api.ack_stream(last_sequence)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.state.status_lines.append(f"[error] stream failed: {err}")
                self._render_views()
                await asyncio.sleep(1)
                continue

            self._render_views()

    async def _push_status(self, message: str) -> None:
        self.state.status_lines.append(f"[status] {message}")
        self._render_views()

    async def _handle_pairing_transition(
        self,
        previous_status: str,
        current_status: str,
    ) -> None:
        if current_status == "approved":
            restored = await self.restore_latest_session()
            if restored:
                if self.state.active_session_id is None:
                    self.state.clear_transient_view()
                self.state.status_lines.clear()
                self.state.status_lines.append("[status] peer approved")
                self._focus_input_requested = True
            return

        if previous_status == "approved" and current_status == "blocked":
            self.state.clear_pending_turn()
            self.state.status_lines.clear()
            self.state.status_lines.append("[error] peer blocked")

    def _render_views(self) -> None:
        self.query_one("#pairing-banner", Static).update(
            self.state.pairing_banner(self.config.channel_id, self.config.peer_id)
        )
        self.query_one("#transcript-view", Static).update(self.state.transcript_text())
        self.query_one("#thinking-view", Static).update(self.state.reasoning_text())
        input_widget = self.query_one("#input", Input)
        input_is_disabled = self.state.input_disabled()
        input_widget.disabled = input_is_disabled
        self._focus_input_if_enabled(input_widget, self._input_was_disabled)
        self._input_was_disabled = input_is_disabled

        status_log = self.query_one("#status-log", RichLog)
        status_log.clear()
        for line in self.state.status_text().splitlines():
            status_log.write(line)

    def _focus_input_if_enabled(
        self,
        input_widget: Input,
        input_was_disabled: bool,
    ) -> None:
        if input_widget.disabled:
            return
        if self.focused is input_widget:
            self._focus_input_requested = False
            return
        if self.focused is None or self._focus_input_requested or input_was_disabled:
            input_widget.focus()
        self._focus_input_requested = False

    def _apply_inbound_response(
        self,
        outcome: str,
        turn_id: str | None,
        session_id: str | None,
    ) -> bool:
        if outcome == "queued" and turn_id:
            self.state.mark_queued(turn_id, session_id)
            return True
        if outcome == "pairing_pending":
            self.state.clear_pending_turn()
            self.state.status_lines.append("[status] pairing pending")
            return True
        if outcome == "peer_blocked":
            self.state.clear_pending_turn()
            self.state.status_lines.append("[error] peer_blocked: peer is blocked")
            return False
        if outcome == "duplicate":
            self.state.clear_pending_turn()
            self.state.status_lines.append("[status] duplicate: inbound ignored")
            return False

        self.state.mark_send_failed(f"unexpected inbound outcome: {outcome}")
        return False


def run() -> None:
    app = TerminalChannelApp(AppConfig.from_env())
    app.run()
