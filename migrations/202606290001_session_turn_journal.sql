CREATE TABLE IF NOT EXISTS session_turn_journal_events (
    turn_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL CHECK (sequence_no > 0),
    event_json TEXT NOT NULL,
    raw_driver TEXT,
    raw_payload TEXT,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (turn_id, sequence_no),
    FOREIGN KEY (turn_id) REFERENCES session_turns(turn_id) ON DELETE CASCADE,
    CHECK (
        (raw_driver IS NULL AND raw_payload IS NULL)
        OR (raw_driver IS NOT NULL AND raw_payload IS NOT NULL)
    )
);
