ALTER TABLE sessions
    ADD COLUMN project_scope TEXT NOT NULL DEFAULT 'project-8001c2743965';

DROP INDEX IF EXISTS idx_sessions_channel_peer;
CREATE INDEX IF NOT EXISTS idx_sessions_channel_peer_scope
    ON sessions (channel_id, peer_id, project_scope);
