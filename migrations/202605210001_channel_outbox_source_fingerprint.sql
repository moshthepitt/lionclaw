ALTER TABLE channel_outbox_messages
ADD COLUMN source_fingerprint TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_outbox_runtime_channel_send_source
    ON channel_outbox_messages(source_kind, source_id)
    WHERE source_kind = 'runtime_channel_send' AND source_id IS NOT NULL;
