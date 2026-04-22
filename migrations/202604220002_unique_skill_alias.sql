CREATE UNIQUE INDEX IF NOT EXISTS idx_skills_enabled_alias ON skills (alias) WHERE enabled = 1;
