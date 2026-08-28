DROP TABLE IF EXISTS hands.skill_source_retirement_command;
DROP TABLE IF EXISTS hands.skill_source_inventory;
UPDATE hands.skill_publication SET status='revoked'
WHERE status='retired';
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_status_check;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_reason_check;
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_status_check
    CHECK (status IN ('staged','validating','active','quarantined','revoked'));
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_reason_check
    CHECK (
        status NOT IN ('quarantined','revoked')
        OR NULLIF(reason_code, '') IS NOT NULL
    );
