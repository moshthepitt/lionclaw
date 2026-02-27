CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY NOT NULL,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    trust_tier TEXT NOT NULL CHECK (trust_tier IN ('main', 'untrusted')),
    created_at_ms INTEGER NOT NULL,
    last_turn_at_ms INTEGER,
    turn_count INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_channel_peer ON sessions (channel_id, peer_id);
CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions (created_at_ms);

CREATE TABLE IF NOT EXISTS skills (
    skill_id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    source TEXT NOT NULL,
    reference TEXT NOT NULL DEFAULT '',
    hash TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    installed_at_ms INTEGER NOT NULL,
    UNIQUE (source, reference, hash)
);

CREATE INDEX IF NOT EXISTS idx_skills_enabled ON skills (enabled);
CREATE INDEX IF NOT EXISTS idx_skills_installed_at ON skills (installed_at_ms);

CREATE TABLE IF NOT EXISTS policy_grants (
    grant_id TEXT PRIMARY KEY NOT NULL,
    skill_id TEXT NOT NULL,
    capability TEXT NOT NULL,
    scope TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER,
    FOREIGN KEY (skill_id) REFERENCES skills (skill_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_policy_grants_skill ON policy_grants (skill_id);
CREATE INDEX IF NOT EXISTS idx_policy_grants_match ON policy_grants (skill_id, capability, scope);
CREATE INDEX IF NOT EXISTS idx_policy_grants_expires ON policy_grants (expires_at_ms);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    event_type TEXT NOT NULL,
    session_id TEXT,
    actor TEXT,
    details_json TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_session ON audit_events (session_id);
CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_events (event_type);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_events (timestamp_ms);
