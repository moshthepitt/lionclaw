-- Extend the effect-kind vocabulary with the closing terminal review.
-- SQLite cannot alter a CHECK constraint, so rebuild the table in place.
-- The queue is derived state (rebuildable from the log), but a straight
-- copy preserves live leases across the upgrade.
CREATE TABLE mission_effects_new (
    effect_id           TEXT PRIMARY KEY NOT NULL, -- idempotency key of the request
    mission_id          TEXT NOT NULL REFERENCES missions (mission_id),
    source_seq          INTEGER NOT NULL,
    kind                TEXT NOT NULL CHECK (kind IN ('role_run', 'oracle_run', 'terminal_review')),
    request_json        TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('queued', 'leased', 'done', 'failed')),
    lease_owner         TEXT,
    lease_expires_at_ms INTEGER,
    created_at_ms       INTEGER NOT NULL,
    updated_at_ms       INTEGER NOT NULL,
    CHECK (
        status != 'leased'
        OR (lease_owner IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
    )
) STRICT;

INSERT INTO mission_effects_new SELECT * FROM mission_effects;
DROP TABLE mission_effects;
ALTER TABLE mission_effects_new RENAME TO mission_effects;

CREATE UNIQUE INDEX idx_mission_effects_source ON mission_effects (mission_id, source_seq);
CREATE INDEX idx_mission_effects_due ON mission_effects (mission_id, status);
