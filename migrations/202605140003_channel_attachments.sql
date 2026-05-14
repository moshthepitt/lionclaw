CREATE TABLE IF NOT EXISTS channel_attachments (
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

CREATE INDEX IF NOT EXISTS idx_channel_attachments_event
    ON channel_attachments(channel_id, event_id, status);

CREATE TABLE IF NOT EXISTS channel_attachment_batches (
    channel_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('waiting', 'finalized')),
    finalized_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, event_id),
    FOREIGN KEY (channel_id, event_id) REFERENCES channel_inbound_events(channel_id, event_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channel_attachment_batches_waiting
    ON channel_attachment_batches(status, created_at_ms);
