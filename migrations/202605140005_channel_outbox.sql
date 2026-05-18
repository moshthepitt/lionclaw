CREATE TABLE IF NOT EXISTS channel_outbox_messages (
    delivery_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    conversation_ref TEXT NOT NULL,
    thread_ref TEXT,
    reply_to_ref TEXT,
    session_id TEXT,
    turn_id TEXT,
    source_kind TEXT,
    source_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'leased', 'delivered', 'failed')),
    content_json TEXT NOT NULL,
    provider_receipt_json TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at_ms INTEGER NOT NULL,
    lease_owner TEXT,
    lease_expires_at_ms INTEGER,
    current_attempt_id TEXT,
    last_error_code TEXT,
    last_error_text TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    delivered_at_ms INTEGER,
    failed_at_ms INTEGER,
    CHECK (length(trim(channel_id)) > 0),
    CHECK (length(trim(conversation_ref)) > 0),
    CHECK (
        status != 'leased' OR
        (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL AND current_attempt_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_channel_outbox_due
    ON channel_outbox_messages(channel_id, status, next_attempt_at_ms, created_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_outbox_lease_expiry
    ON channel_outbox_messages(channel_id, status, lease_expires_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_outbox_source
    ON channel_outbox_messages(source_kind, source_id);

CREATE TABLE IF NOT EXISTS channel_outbox_attempts (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    delivery_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('leased', 'delivered', 'retryable_failed', 'terminal_failed', 'stale_rejected')),
    provider_receipt_json TEXT,
    error_code TEXT,
    error_text TEXT,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    FOREIGN KEY (delivery_id) REFERENCES channel_outbox_messages(delivery_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_channel_outbox_attempts_delivery
    ON channel_outbox_attempts(delivery_id, started_at_ms);

DROP INDEX IF EXISTS idx_channel_messages_unique_update;
DROP INDEX IF EXISTS idx_channel_messages_peer_created;
DROP INDEX IF EXISTS idx_channel_messages_outbox_pending;
DROP TABLE IF EXISTS channel_messages;
