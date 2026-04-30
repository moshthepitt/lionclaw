CREATE TABLE IF NOT EXISTS channel_peers (
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'blocked')),
    trust_tier TEXT NOT NULL CHECK (trust_tier IN ('main', 'untrusted')),
    pairing_code TEXT NOT NULL,
    first_seen_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, peer_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_peers_status ON channel_peers (channel_id, status);
CREATE INDEX IF NOT EXISTS idx_channel_peers_updated_at ON channel_peers (updated_at_ms);

CREATE TABLE IF NOT EXISTS channel_offsets (
    channel_id TEXT PRIMARY KEY NOT NULL,
    offset INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS channel_messages (
    message_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    external_message_id TEXT,
    update_id INTEGER,
    content TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_messages_unique_update
    ON channel_messages (channel_id, update_id)
    WHERE update_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_channel_messages_peer_created
    ON channel_messages (channel_id, peer_id, created_at_ms);

CREATE INDEX IF NOT EXISTS idx_channel_messages_outbox_pending
    ON channel_messages (channel_id, direction, external_message_id, created_at_ms);
