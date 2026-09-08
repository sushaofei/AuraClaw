BEGIN;

DROP TABLE IF EXISTS hands.skill_source_retirement_command;
DROP TABLE IF EXISTS hands.skill_source_inventory;
DROP TABLE IF EXISTS hands.skill_publication_source;
DROP TABLE IF EXISTS hands.skill_source_command;
DROP TABLE IF EXISTS hands.skill_source_sync_state;
DROP TABLE IF EXISTS hands.skill_source_lease;

ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_source_fk,
    DROP COLUMN IF EXISTS source_id;
ALTER TABLE hands.skill_installation
    DROP CONSTRAINT IF EXISTS skill_installation_source_fk,
    DROP COLUMN IF EXISTS source_id;
ALTER TABLE hands.skill_command DROP COLUMN IF EXISTS source_id;
ALTER TABLE hands.skill_admission_audit DROP COLUMN IF EXISTS source_id;

DROP TABLE IF EXISTS hands.skill_source;

COMMIT;
