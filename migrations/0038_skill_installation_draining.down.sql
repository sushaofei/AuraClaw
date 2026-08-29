BEGIN;

DROP TABLE IF EXISTS hands.skill_installation_command;
DROP INDEX IF EXISTS hands.skill_installation_draining_idx;

UPDATE hands.skill_installation
SET status='uninstalled'
WHERE status='draining';

ALTER TABLE hands.skill_installation
    DROP CONSTRAINT IF EXISTS skill_installation_draining_policy_check,
    DROP CONSTRAINT IF EXISTS skill_installation_uninstall_policy_check,
    DROP CONSTRAINT IF EXISTS skill_installation_uninstall_evidence_check,
    DROP CONSTRAINT IF EXISTS skill_installation_uninstall_action_check,
    DROP CONSTRAINT IF EXISTS skill_installation_status_check,
    DROP COLUMN IF EXISTS uninstall_policy_decision_id,
    DROP COLUMN IF EXISTS uninstall_policy_version,
    DROP COLUMN IF EXISTS uninstall_action;

ALTER TABLE hands.skill_installation
    ADD CONSTRAINT skill_installation_status_check
    CHECK (status IN ('active','disabled','uninstalled'));

COMMIT;
