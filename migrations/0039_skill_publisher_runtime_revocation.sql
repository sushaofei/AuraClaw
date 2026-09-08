BEGIN;

ALTER TABLE hands.skill_publisher
    DROP CONSTRAINT IF EXISTS skill_publisher_status_check;
ALTER TABLE hands.skill_publisher
    ADD CONSTRAINT skill_publisher_status_check
    CHECK (status IN ('active','suspended','revoked'));

ALTER TABLE hands.skill_publisher
    ADD COLUMN IF NOT EXISTS security_action text,
    ADD COLUMN IF NOT EXISTS security_policy_version text,
    ADD COLUMN IF NOT EXISTS security_policy_decision_id text;

ALTER TABLE hands.skill_publisher_key
    ADD COLUMN IF NOT EXISTS revocation_action text,
    ADD COLUMN IF NOT EXISTS revocation_policy_version text,
    ADD COLUMN IF NOT EXISTS revocation_policy_decision_id text;

UPDATE hands.skill_publisher
SET security_action='pause',security_policy_version='skill-revocation-v1'
WHERE status='suspended' AND security_action IS NULL;

UPDATE hands.skill_publisher_key
SET revocation_action='cancel',revocation_policy_version='skill-revocation-v1'
WHERE status='revoked' AND revocation_action IS NULL;

ALTER TABLE hands.skill_publisher
    DROP CONSTRAINT IF EXISTS skill_publisher_status_evidence_check,
    ADD CONSTRAINT skill_publisher_status_evidence_check
    CHECK (
        (status='active' AND status_reason_code IS NULL
         AND security_action IS NULL AND security_policy_version IS NULL
         AND security_policy_decision_id IS NULL)
        OR (status IN ('suspended','revoked')
            AND NULLIF(status_reason_code,'') IS NOT NULL
            AND status_changed_at IS NOT NULL
            AND security_action IN ('pause','cancel')
            AND NULLIF(security_policy_version,'') IS NOT NULL)
    );

ALTER TABLE hands.skill_publisher_key
    ADD CONSTRAINT skill_publisher_key_revocation_policy_check
    CHECK (
        (status='revoked' AND revocation_action IN ('pause','cancel')
         AND NULLIF(revocation_policy_version,'') IS NOT NULL)
        OR (status<>'revoked' AND revocation_action IS NULL
            AND revocation_policy_version IS NULL
            AND revocation_policy_decision_id IS NULL)
    );

COMMIT;
