from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Input, RichLog, Static

from lionclaw_channel_terminal.api import InboundResponse, LionClawApi
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
            response = await self.api.send_inbound(text)
        except Exception as err:  # noqa: BLE001
            self.state.mark_send_failed(str(err))
            self._render_views()
            return False

        accepted = self._apply_inbound_response(response)
        self._render_views()
        return accepted

    async def refresh_pairing_state(self) -> None:
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
        self._render_views()

    async def stream_loop(self) -> None:
        while True:
            try:
                events, last_sequence = await self.api.pull_stream()
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

    def _render_views(self) -> None:
        self.query_one("#pairing-banner", Static).update(
            self.state.pairing_banner(self.config.channel_id, self.config.peer_id)
        )
        self.query_one("#transcript-view", Static).update(self.state.transcript_text())
        self.query_one("#thinking-view", Static).update(self.state.reasoning_text())
        self.query_one("#input", Input).disabled = self.state.input_disabled()

        status_log = self.query_one("#status-log", RichLog)
        status_log.clear()
        for line in self.state.status_text().splitlines():
            status_log.write(line)

    def _apply_inbound_response(self, response: InboundResponse) -> bool:
        if response.outcome == "queued" and response.turn_id:
            self.state.mark_queued(response.turn_id)
            return True
        if response.outcome == "pairing_pending":
            self.state.clear_pending_turn()
            self.state.status_lines.append("[status] pairing pending")
            return True
        if response.outcome == "peer_blocked":
            self.state.clear_pending_turn()
            self.state.status_lines.append("[error] peer_blocked: peer is blocked")
            return False
        if response.outcome == "duplicate":
            self.state.clear_pending_turn()
            self.state.status_lines.append("[status] duplicate: inbound ignored")
            return False

        self.state.mark_send_failed(f"unexpected inbound outcome: {response.outcome}")
        return False


def run() -> None:
    app = TerminalChannelApp(AppConfig.from_env())
    app.run()
