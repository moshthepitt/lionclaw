CREATE TABLE IF NOT EXISTS session_compactions (
    compaction_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    start_sequence_no INTEGER NOT NULL,
    through_sequence_no INTEGER NOT NULL,
    summary_text TEXT NOT NULL,
    summary_state_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_session_compactions_session_sequence
    ON session_compactions(session_id, through_sequence_no DESC);

CREATE TABLE IF NOT EXISTS continuity_documents (
    relative_path TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS continuity_documents_fts USING fts5(
    title,
    body,
    relative_path,
    content='continuity_documents',
    content_rowid='rowid'
);

CREATE TRIGGER IF NOT EXISTS continuity_documents_ai AFTER INSERT ON continuity_documents BEGIN
    INSERT INTO continuity_documents_fts(rowid, title, body, relative_path)
    VALUES (new.rowid, new.title, new.body, new.relative_path);
END;

CREATE TRIGGER IF NOT EXISTS continuity_documents_ad AFTER DELETE ON continuity_documents BEGIN
    INSERT INTO continuity_documents_fts(continuity_documents_fts, rowid, title, body, relative_path)
    VALUES('delete', old.rowid, old.title, old.body, old.relative_path);
END;

CREATE TRIGGER IF NOT EXISTS continuity_documents_au AFTER UPDATE ON continuity_documents BEGIN
    INSERT INTO continuity_documents_fts(continuity_documents_fts, rowid, title, body, relative_path)
    VALUES('delete', old.rowid, old.title, old.body, old.relative_path);
    INSERT INTO continuity_documents_fts(rowid, title, body, relative_path)
    VALUES (new.rowid, new.title, new.body, new.relative_path);
END;
