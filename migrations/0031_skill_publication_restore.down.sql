DROP TABLE IF EXISTS hands.skill_publication_restore_command;
UPDATE hands.skill_publication SET status='retired'
WHERE status='restoring';
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_status_check;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_reason_check;
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_status_check
    CHECK (status IN (
        'staged','validating','active','quarantined','retired','revoked'
    ));
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_reason_check
    CHECK (
        status NOT IN ('quarantined','retired','revoked')
        OR NULLIF(reason_code, '') IS NOT NULL
    );
