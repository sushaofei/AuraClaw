ALTER TABLE hands.skill_publication
    ADD COLUMN IF NOT EXISTS updated_by text;

UPDATE hands.skill_publication
SET updated_by = created_by
WHERE updated_by IS NULL;

ALTER TABLE hands.skill_publication
    ALTER COLUMN updated_by SET NOT NULL;
