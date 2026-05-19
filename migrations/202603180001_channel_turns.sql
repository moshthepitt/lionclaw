ALTER TABLE channel_stream_events ADD COLUMN code TEXT;

CREATE TABLE IF NOT EXISTS channel_turns (
    turn_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    inbound_message_id TEXT NOT NULL UNIQUE,
    runtime_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    last_error TEXT,
    queued_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (inbound_message_id) REFERENCES channel_messages(message_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channel_turns_peer_status_queued
    ON channel_turns (channel_id, peer_id, status, queued_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_turns_status
    ON channel_turns (status, queued_at_ms);
