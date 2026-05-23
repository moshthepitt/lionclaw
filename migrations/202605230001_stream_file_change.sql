CREATE TABLE channel_stream_events_new (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    session_id TEXT,
    turn_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('message_delta', 'message_boundary', 'file_change', 'status', 'error', 'turn_completed', 'done')),
    lane TEXT CHECK (lane IS NULL OR lane IN ('answer', 'reasoning')),
    code TEXT,
    text TEXT,
    file_change_json TEXT,
    created_at_ms INTEGER NOT NULL
);

INSERT INTO channel_stream_events_new (
    sequence,
    channel_id,
    peer_id,
    session_id,
    turn_id,
    kind,
    lane,
    code,
    text,
    file_change_json,
    created_at_ms
)
SELECT
    sequence,
    channel_id,
    peer_id,
    session_id,
    turn_id,
    kind,
    lane,
    code,
    text,
    NULL,
    created_at_ms
FROM channel_stream_events;

DROP TABLE channel_stream_events;
ALTER TABLE channel_stream_events_new RENAME TO channel_stream_events;

CREATE INDEX IF NOT EXISTS idx_channel_stream_events_channel_sequence
    ON channel_stream_events (channel_id, sequence);

CREATE INDEX IF NOT EXISTS idx_channel_stream_events_channel_peer_sequence
    ON channel_stream_events (channel_id, peer_id, sequence);
