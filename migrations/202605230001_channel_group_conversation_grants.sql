CREATE TABLE channel_grants_next (
    grant_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    sender_ref TEXT,
    conversation_ref TEXT,
    thread_ref TEXT,
    routing_profile TEXT NOT NULL CHECK (routing_profile IN ('direct', 'conversation', 'thread', 'outbound')),
    trust_tier TEXT NOT NULL CHECK (trust_tier IN ('main', 'untrusted')),
    status TEXT NOT NULL CHECK (status IN ('approved', 'blocked', 'revoked')),
    label TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER,
    CHECK (
        (routing_profile = 'direct' AND sender_ref IS NOT NULL AND conversation_ref IS NULL AND thread_ref IS NULL) OR
        (routing_profile = 'conversation' AND conversation_ref IS NOT NULL AND thread_ref IS NULL) OR
        (routing_profile = 'thread' AND sender_ref IS NOT NULL AND conversation_ref IS NOT NULL AND thread_ref IS NOT NULL) OR
        (routing_profile = 'outbound' AND sender_ref IS NULL AND conversation_ref IS NOT NULL AND thread_ref IS NULL)
    )
);

INSERT INTO channel_grants_next (
    grant_id,
    channel_id,
    sender_ref,
    conversation_ref,
    thread_ref,
    routing_profile,
    trust_tier,
    status,
    label,
    created_at_ms,
    updated_at_ms,
    revoked_at_ms
)
SELECT
    grant_id,
    channel_id,
    sender_ref,
    conversation_ref,
    thread_ref,
    routing_profile,
    trust_tier,
    status,
    label,
    created_at_ms,
    updated_at_ms,
    revoked_at_ms
FROM channel_grants;

DROP TABLE channel_grants;
ALTER TABLE channel_grants_next RENAME TO channel_grants;

CREATE UNIQUE INDEX idx_channel_grants_scope_unique
    ON channel_grants (
        channel_id,
        COALESCE(sender_ref, ''),
        COALESCE(conversation_ref, ''),
        COALESCE(thread_ref, ''),
        routing_profile
    );

CREATE INDEX idx_channel_grants_lookup
    ON channel_grants(channel_id, sender_ref, conversation_ref, thread_ref, status);
