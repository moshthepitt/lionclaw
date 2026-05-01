CREATE TABLE IF NOT EXISTS channel_stream_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    session_id TEXT,
    turn_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('message_delta', 'status', 'error', 'turn_completed', 'done')),
    lane TEXT CHECK (lane IS NULL OR lane IN ('answer', 'reasoning')),
    text TEXT,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_stream_events_channel_sequence
    ON channel_stream_events (channel_id, sequence);

CREATE INDEX IF NOT EXISTS idx_channel_stream_events_channel_peer_sequence
    ON channel_stream_events (channel_id, peer_id, sequence);

CREATE TABLE IF NOT EXISTS channel_stream_consumers (
    channel_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    last_acked_sequence INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, consumer_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_stream_consumers_updated
    ON channel_stream_consumers (updated_at_ms);
