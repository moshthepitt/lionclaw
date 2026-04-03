CREATE TABLE IF NOT EXISTS session_compactions (
    compaction_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    start_sequence_no INTEGER NOT NULL,
    through_sequence_no INTEGER NOT NULL,
    summary_text TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_compactions_session_sequence
    ON session_compactions(session_id, through_sequence_no DESC);
