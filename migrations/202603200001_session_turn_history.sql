ALTER TABLE sessions
    ADD COLUMN history_policy TEXT NOT NULL DEFAULT 'conservative'
    CHECK (history_policy IN ('interactive', 'conservative'));

CREATE TABLE IF NOT EXISTS session_turns (
    turn_id TEXT PRIMARY KEY NOT NULL,
    session_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('normal', 'retry', 'continue')),
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'timed_out', 'cancelled')),
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

CREATE INDEX IF NOT EXISTS idx_session_turns_session_sequence
    ON session_turns (session_id, sequence_no);

CREATE INDEX IF NOT EXISTS idx_session_turns_session_started
    ON session_turns (session_id, started_at_ms);
