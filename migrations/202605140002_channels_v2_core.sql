CREATE TABLE IF NOT EXISTS channel_pairing_requests (
    pairing_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    code_hash TEXT NOT NULL UNIQUE,
    claim_policy TEXT NOT NULL CHECK (claim_policy IN ('operator_approval', 'token_claim')),
    sender_ref TEXT,
    conversation_ref TEXT,
    thread_ref TEXT,
    requested_profile TEXT NOT NULL CHECK (requested_profile IN ('direct', 'conversation', 'thread', 'outbound')),
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'blocked', 'expired')),
    label TEXT,
    max_claims INTEGER NOT NULL DEFAULT 1,
    claim_count INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER,
    claimed_at_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_pairing_requests_status
    ON channel_pairing_requests(channel_id, status, updated_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_pairing_requests_scope
    ON channel_pairing_requests(
        channel_id,
        claim_policy,
        sender_ref,
        conversation_ref,
        thread_ref,
        requested_profile,
        status
    );

CREATE TABLE IF NOT EXISTS channel_grants (
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
        (routing_profile = 'conversation' AND sender_ref IS NOT NULL AND conversation_ref IS NOT NULL AND thread_ref IS NULL) OR
        (routing_profile = 'thread' AND sender_ref IS NOT NULL AND conversation_ref IS NOT NULL AND thread_ref IS NOT NULL) OR
        (routing_profile = 'outbound' AND sender_ref IS NULL AND conversation_ref IS NOT NULL AND thread_ref IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_grants_scope_unique
    ON channel_grants (
        channel_id,
        COALESCE(sender_ref, ''),
        COALESCE(conversation_ref, ''),
        COALESCE(thread_ref, ''),
        routing_profile
    );

CREATE INDEX IF NOT EXISTS idx_channel_grants_lookup
    ON channel_grants(channel_id, sender_ref, conversation_ref, thread_ref, status);

CREATE TABLE IF NOT EXISTS channel_inbound_events (
    event_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    sender_ref TEXT NOT NULL,
    conversation_ref TEXT NOT NULL,
    thread_ref TEXT,
    message_ref TEXT,
    text TEXT,
    trigger TEXT NOT NULL CHECK (trigger IN ('dm', 'mention', 'reply_to_bot', 'thread_continuation', 'none')),
    attachments_json TEXT NOT NULL DEFAULT '[]',
    reply_to_ref TEXT,
    provider_metadata_json TEXT NOT NULL DEFAULT '{}',
    received_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_inbound_events_scope_time
    ON channel_inbound_events(channel_id, conversation_ref, thread_ref, created_at_ms);

INSERT OR IGNORE INTO channel_grants (
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
    lower(hex(randomblob(4))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(6))),
    channel_id,
    peer_id,
    NULL,
    NULL,
    'direct',
    trust_tier,
    'approved',
    NULL,
    first_seen_ms,
    updated_at_ms,
    NULL
FROM channel_peers
WHERE status = 'approved';

INSERT OR IGNORE INTO channel_grants (
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
    lower(hex(randomblob(4))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(6))),
    channel_id,
    peer_id,
    NULL,
    NULL,
    'direct',
    'untrusted',
    'blocked',
    NULL,
    first_seen_ms,
    updated_at_ms,
    NULL
FROM channel_peers
WHERE status = 'blocked';

INSERT OR IGNORE INTO channel_pairing_requests (
    pairing_id,
    channel_id,
    code_hash,
    claim_policy,
    sender_ref,
    conversation_ref,
    thread_ref,
    requested_profile,
    status,
    label,
    max_claims,
    claim_count,
    created_at_ms,
    expires_at_ms,
    claimed_at_ms,
    updated_at_ms
)
SELECT
    lower(hex(randomblob(4))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(6))),
    channel_id,
    'migrated:' || lower(hex(randomblob(32))),
    'operator_approval',
    peer_id,
    NULL,
    NULL,
    'direct',
    status,
    NULL,
    1,
    0,
    first_seen_ms,
    NULL,
    NULL,
    updated_at_ms
FROM channel_peers
WHERE status IN ('pending', 'blocked');

INSERT OR IGNORE INTO channel_inbound_events (
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
    'v1-message:' || message_id,
    channel_id,
    peer_id,
    peer_id,
    NULL,
    external_message_id,
    content,
    'dm',
    '[]',
    NULL,
    '{}',
    created_at_ms,
    created_at_ms
FROM channel_messages
WHERE direction = 'inbound';

INSERT OR IGNORE INTO session_turns (
    turn_id,
    session_id,
    sequence_no,
    kind,
    status,
    display_user_text,
    prompt_user_text,
    assistant_text,
    error_code,
    error_text,
    runtime_id,
    started_at_ms,
    finished_at_ms
)
WITH legacy_turns AS (
    SELECT
        channel_turns.turn_id,
        channel_turns.session_id,
        (
            SELECT COALESCE(MAX(existing.sequence_no), 0)
            FROM session_turns AS existing
            WHERE existing.session_id = channel_turns.session_id
        ) + ROW_NUMBER() OVER (
            PARTITION BY channel_turns.session_id
            ORDER BY channel_turns.queued_at_ms, channel_turns.turn_id
        ) AS sequence_no,
        channel_turns.status,
        channel_messages.content,
        channel_turns.runtime_id,
        channel_turns.queued_at_ms,
        channel_turns.started_at_ms,
        channel_turns.finished_at_ms,
        channel_turns.last_error
    FROM channel_turns
    JOIN channel_messages ON channel_messages.message_id = channel_turns.inbound_message_id
)
SELECT
    turn_id,
    session_id,
    sequence_no,
    'normal',
    CASE
        WHEN status = 'completed' THEN 'completed'
        WHEN status = 'failed' THEN 'failed'
        ELSE 'running'
    END,
    content,
    content,
    '',
    CASE WHEN status = 'failed' THEN 'channel.turn.failed' ELSE NULL END,
    CASE WHEN status = 'failed' THEN COALESCE(last_error, 'channel turn failed') ELSE NULL END,
    runtime_id,
    COALESCE(started_at_ms, queued_at_ms),
    CASE
        WHEN status IN ('completed', 'failed') THEN COALESCE(finished_at_ms, queued_at_ms)
        ELSE NULL
    END
FROM legacy_turns;

CREATE TABLE session_turns_channels_v2 (
    turn_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('normal', 'retry', 'continue', 'runtime_control')),
    status TEXT NOT NULL CHECK (status IN ('running', 'waiting_for_attachments', 'completed', 'failed', 'timed_out', 'cancelled', 'interrupted')),
    display_user_text TEXT NOT NULL,
    prompt_user_text TEXT NOT NULL,
    assistant_text TEXT NOT NULL,
    error_code TEXT,
    error_text TEXT,
    runtime_id TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    UNIQUE (session_id, sequence_no)
);

INSERT INTO session_turns_channels_v2 (
    turn_id,
    session_id,
    sequence_no,
    kind,
    status,
    display_user_text,
    prompt_user_text,
    assistant_text,
    error_code,
    error_text,
    runtime_id,
    started_at_ms,
    finished_at_ms
)
SELECT
    turn_id,
    session_id,
    sequence_no,
    kind,
    status,
    display_user_text,
    prompt_user_text,
    assistant_text,
    error_code,
    error_text,
    runtime_id,
    started_at_ms,
    finished_at_ms
FROM session_turns;

DROP TABLE session_turns;

ALTER TABLE session_turns_channels_v2 RENAME TO session_turns;

CREATE INDEX IF NOT EXISTS idx_session_turns_session_sequence
    ON session_turns(session_id, sequence_no);

CREATE INDEX IF NOT EXISTS idx_session_turns_session_started
    ON session_turns(session_id, started_at_ms);

CREATE INDEX IF NOT EXISTS idx_session_turns_recent_failures
    ON session_turns(started_at_ms DESC, turn_id DESC)
    WHERE status IN ('failed', 'timed_out', 'cancelled', 'interrupted');

CREATE TABLE channel_turns_new (
    turn_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    session_id TEXT NOT NULL,
    inbound_event_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('waiting_for_attachments', 'pending', 'running', 'completed', 'failed')),
    last_error TEXT,
    answer_checkpoint_sequence INTEGER,
    queued_at_ms INTEGER NOT NULL,
    started_at_ms INTEGER,
    finished_at_ms INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (channel_id, inbound_event_id) REFERENCES channel_inbound_events(channel_id, event_id) ON DELETE CASCADE
);

INSERT INTO channel_turns_new (
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
    channel_turns.turn_id,
    channel_turns.channel_id,
    'channel:' || channel_turns.channel_id || ':direct:' || replace(replace(channel_turns.peer_id, '%', '%25'), ':', '%3A'),
    channel_turns.session_id,
    'v1-message:' || channel_messages.message_id,
    channel_turns.runtime_id,
    channel_turns.status,
    channel_turns.last_error,
    channel_turns.answer_checkpoint_sequence,
    channel_turns.queued_at_ms,
    channel_turns.started_at_ms,
    channel_turns.finished_at_ms
FROM channel_turns
JOIN channel_messages ON channel_messages.message_id = channel_turns.inbound_message_id;

DROP TABLE channel_turns;

ALTER TABLE channel_turns_new RENAME TO channel_turns;

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_turns_inbound_event
    ON channel_turns(channel_id, inbound_event_id);

CREATE INDEX IF NOT EXISTS idx_channel_turns_session_key_status_queued
    ON channel_turns(channel_id, session_key, status, queued_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_turns_status
    ON channel_turns(status, queued_at_ms);

UPDATE sessions
SET peer_id = 'channel:' || sessions.channel_id || ':direct:' || replace(replace(sessions.peer_id, '%', '%25'), ':', '%3A')
WHERE EXISTS (
    SELECT 1
    FROM channel_peers
    WHERE channel_peers.channel_id = sessions.channel_id
      AND channel_peers.peer_id = sessions.peer_id
      AND channel_peers.status = 'approved'
);

DROP TABLE channel_peers;

CREATE TABLE channel_messages_new (
    message_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    external_message_id TEXT,
    update_id INTEGER,
    created_at_ms INTEGER NOT NULL
);

INSERT INTO channel_messages_new (
    message_id,
    channel_id,
    peer_id,
    direction,
    external_message_id,
    update_id,
    created_at_ms
)
SELECT
    message_id,
    channel_id,
    peer_id,
    direction,
    external_message_id,
    update_id,
    created_at_ms
FROM channel_messages;

DROP TABLE channel_messages;

ALTER TABLE channel_messages_new RENAME TO channel_messages;

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_messages_unique_update
    ON channel_messages(channel_id, update_id)
    WHERE update_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_channel_messages_peer_created
    ON channel_messages(channel_id, peer_id, created_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_messages_outbox_pending
    ON channel_messages(channel_id, direction, external_message_id, created_at_ms);
