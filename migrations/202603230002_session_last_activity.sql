ALTER TABLE sessions
    ADD COLUMN last_activity_at_ms INTEGER;

UPDATE sessions
SET last_activity_at_ms = last_turn_at_ms
WHERE last_activity_at_ms IS NULL
  AND last_turn_at_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sessions_last_activity
    ON sessions (last_activity_at_ms, created_at_ms);
