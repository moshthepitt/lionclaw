CREATE TABLE channel_turns_terminal_statuses (
    turn_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    session_id TEXT NOT NULL,
    inbound_event_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('waiting_for_attachments', 'pending', 'running', 'completed', 'failed', 'timed_out', 'cancelled', 'interrupted')),
    last_error TEXT,
    answer_checkpoint_sequence INTEGER,
    queued_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (channel_id, inbound_event_id) REFERENCES channel_inbound_events(channel_id, event_id) ON DELETE CASCADE
);

INSERT INTO channel_turns_terminal_statuses (
    turn_id,
    channel_id,
    session_key,
    session_id,
    inbound_event_id,
    runtime_id,
    status,
    last_error,
    answer_checkpoint_sequence,
    queued_at_ms,
    started_at_ms,
    finished_at_ms
)
SELECT
    turn_id,
    channel_id,
    session_key,
    session_id,
    inbound_event_id,
    runtime_id,
    status,
    last_error,
    answer_checkpoint_sequence,
    queued_at_ms,
    started_at_ms,
    finished_at_ms
FROM channel_turns;

DROP TABLE channel_turns;

ALTER TABLE channel_turns_terminal_statuses RENAME TO channel_turns;

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_turns_inbound_event
    ON channel_turns(channel_id, inbound_event_id);

CREATE INDEX IF NOT EXISTS idx_channel_turns_session_key_status_queued
    ON channel_turns(channel_id, session_key, status, queued_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_turns_status
    ON channel_turns(status, queued_at_ms);
