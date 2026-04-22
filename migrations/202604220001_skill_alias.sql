ALTER TABLE skills ADD COLUMN alias TEXT NOT NULL DEFAULT '';
UPDATE skills SET alias = skill_id WHERE alias = '';
