-- no-transaction

PRAGMA foreign_keys = OFF;
PRAGMA legacy_alter_table = ON;

ALTER TABLE channel_inbound_events RENAME TO channel_inbound_events_old;

CREATE TABLE channel_inbound_events (
    event_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    sender_ref TEXT NOT NULL,
    conversation_ref TEXT NOT NULL,
    thread_ref TEXT,
    message_ref TEXT,
    text TEXT,
    trigger TEXT NOT NULL CHECK (trigger IN ('dm', 'command', 'mention', 'reply_to_bot', 'thread_continuation', 'none')),
    attachments_json TEXT NOT NULL DEFAULT '[]',
    reply_to_ref TEXT,
    provider_metadata_json TEXT NOT NULL DEFAULT '{}',
    received_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, event_id)
);

INSERT INTO channel_inbound_events (
    event_id,
    channel_id,
    sender_ref,
    conversation_ref,
    thread_ref,
    message_ref,
    text,
    trigger,
    attachments_json,
    reply_to_ref,
    provider_metadata_json,
    received_at_ms,
    created_at_ms
)
SELECT
    event_id,
    channel_id,
    sender_ref,
    conversation_ref,
    thread_ref,
    message_ref,
    text,
    trigger,
    attachments_json,
    reply_to_ref,
    provider_metadata_json,
    received_at_ms,
    created_at_ms
FROM channel_inbound_events_old;

CREATE TABLE channel_attachments_next (
    channel_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    attachment_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    filename TEXT,
    mime_type TEXT,
    caption TEXT,
    provider_file_ref TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('declared', 'staged', 'rejected')),
    size_bytes INTEGER,
    sha256 TEXT,
    storage_path TEXT,
    rejection_code TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, event_id, attachment_id),
    FOREIGN KEY (channel_id, event_id) REFERENCES channel_inbound_events(channel_id, event_id) ON DELETE CASCADE
);

INSERT INTO channel_attachments_next (
    channel_id,
    event_id,
    attachment_id,
    kind,
    filename,
    mime_type,
    caption,
    provider_file_ref,
    status,
    size_bytes,
    sha256,
    storage_path,
    rejection_code,
    created_at_ms,
    updated_at_ms
)
SELECT
    channel_id,
    event_id,
    attachment_id,
    kind,
    filename,
    mime_type,
    caption,
    provider_file_ref,
    status,
    size_bytes,
    sha256,
    storage_path,
    rejection_code,
    created_at_ms,
    updated_at_ms
FROM channel_attachments;

CREATE TABLE channel_attachment_batches_next (
    channel_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('waiting', 'finalized')),
    finalized_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, event_id),
    FOREIGN KEY (channel_id, event_id) REFERENCES channel_inbound_events(channel_id, event_id) ON DELETE CASCADE
);

INSERT INTO channel_attachment_batches_next (
    channel_id,
    event_id,
    status,
    finalized_at_ms,
    created_at_ms,
    updated_at_ms
)
SELECT
    channel_id,
    event_id,
    status,
    finalized_at_ms,
    created_at_ms,
    updated_at_ms
FROM channel_attachment_batches;

CREATE TABLE channel_turns_next (
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

INSERT INTO channel_turns_next (
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

DROP TABLE channel_attachments;
DROP TABLE channel_attachment_batches;
DROP TABLE channel_turns;
DROP TABLE channel_inbound_events_old;

ALTER TABLE channel_attachments_next RENAME TO channel_attachments;
ALTER TABLE channel_attachment_batches_next RENAME TO channel_attachment_batches;
ALTER TABLE channel_turns_next RENAME TO channel_turns;

CREATE INDEX idx_channel_inbound_events_scope_time
    ON channel_inbound_events(channel_id, conversation_ref, thread_ref, created_at_ms);

CREATE INDEX idx_channel_attachments_event
    ON channel_attachments(channel_id, event_id, status);

CREATE INDEX idx_channel_attachment_batches_waiting
    ON channel_attachment_batches(status, created_at_ms);

CREATE UNIQUE INDEX idx_channel_turns_inbound_event
    ON channel_turns(channel_id, inbound_event_id);

CREATE INDEX idx_channel_turns_session_key_status_queued
    ON channel_turns(channel_id, session_key, status, queued_at_ms);

CREATE INDEX idx_channel_turns_status
    ON channel_turns(status, queued_at_ms);

PRAGMA legacy_alter_table = OFF;
PRAGMA foreign_keys = ON;
