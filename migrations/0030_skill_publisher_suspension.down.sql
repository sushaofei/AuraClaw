UPDATE hands.skill_publisher SET status='active'
WHERE status='suspended';
DELETE FROM hands.skill_publisher_command
WHERE command_type IN ('suspend','resume');
ALTER TABLE hands.skill_publisher_command
    DROP CONSTRAINT IF EXISTS skill_publisher_command_command_type_check;
ALTER TABLE hands.skill_publisher_command
    ADD CONSTRAINT skill_publisher_command_command_type_check
    CHECK (command_type IN ('register','rotate','revoke'));
ALTER TABLE hands.skill_publisher
    DROP CONSTRAINT IF EXISTS skill_publisher_status_evidence_check;
ALTER TABLE hands.skill_publisher
    DROP COLUMN IF EXISTS status_changed_at;
ALTER TABLE hands.skill_publisher
    DROP COLUMN IF EXISTS status_reason_code;
