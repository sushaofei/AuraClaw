BEGIN;

UPDATE hands.skill_publisher
SET status='suspended',
    status_reason_code=COALESCE(status_reason_code,'legacy_publisher_revocation'),
    status_changed_at=COALESCE(status_changed_at,updated_at)
WHERE status='revoked';

DELETE FROM hands.skill_publisher_command
WHERE command_type='revoke' AND key_id IS NULL;

ALTER TABLE hands.skill_publisher_key
    DROP CONSTRAINT IF EXISTS skill_publisher_key_revocation_policy_check,
    DROP COLUMN IF EXISTS revocation_policy_decision_id,
    DROP COLUMN IF EXISTS revocation_policy_version,
    DROP COLUMN IF EXISTS revocation_action;

ALTER TABLE hands.skill_publisher
    DROP CONSTRAINT IF EXISTS skill_publisher_status_evidence_check,
    DROP CONSTRAINT IF EXISTS skill_publisher_status_check,
    DROP COLUMN IF EXISTS security_policy_decision_id,
    DROP COLUMN IF EXISTS security_policy_version,
    DROP COLUMN IF EXISTS security_action;

ALTER TABLE hands.skill_publisher
    ADD CONSTRAINT skill_publisher_status_check
    CHECK (status IN ('active','suspended')),
    ADD CONSTRAINT skill_publisher_status_evidence_check
    CHECK (
        (status='active' AND status_reason_code IS NULL)
        OR (status='suspended' AND NULLIF(status_reason_code,'') IS NOT NULL
            AND status_changed_at IS NOT NULL)
    );

COMMIT;
