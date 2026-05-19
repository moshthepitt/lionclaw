ALTER TABLE session_turns
    ADD COLUMN attachment_source_turn_id TEXT REFERENCES session_turns(turn_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_session_turns_attachment_source
    ON session_turns(attachment_source_turn_id)
    WHERE attachment_source_turn_id IS NOT NULL;
