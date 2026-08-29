BEGIN;

DROP TABLE IF EXISTS hands.skill_source_command;
DROP TABLE IF EXISTS hands.skill_publication_source;
ALTER TABLE hands.skill_source
    DROP CONSTRAINT IF EXISTS skill_source_priority_check,
    DROP COLUMN IF EXISTS priority;

COMMIT;
