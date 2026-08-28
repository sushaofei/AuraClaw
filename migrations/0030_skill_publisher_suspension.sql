ALTER TABLE hands.skill_publisher
    ADD COLUMN IF NOT EXISTS status_reason_code text;
ALTER TABLE hands.skill_publisher
    ADD COLUMN IF NOT EXISTS status_changed_at timestamptz;

UPDATE hands.skill_publisher
SET status_reason_code='legacy_suspension',
    status_changed_at=COALESCE(status_changed_at,updated_at)
WHERE status='suspended' AND status_reason_code IS NULL;

ALTER TABLE hands.skill_publisher
    DROP CONSTRAINT IF EXISTS skill_publisher_status_evidence_check;
ALTER TABLE hands.skill_publisher
    ADD CONSTRAINT skill_publisher_status_evidence_check
    CHECK (
        (status='active' AND status_reason_code IS NULL)
        OR (status='suspended' AND NULLIF(status_reason_code,'') IS NOT NULL
            AND status_changed_at IS NOT NULL)
    );

ALTER TABLE hands.skill_publisher_command
    DROP CONSTRAINT IF EXISTS skill_publisher_command_command_type_check;
ALTER TABLE hands.skill_publisher_command
    ADD CONSTRAINT skill_publisher_command_command_type_check
    CHECK (command_type IN ('register','rotate','revoke','suspend','resume'));
