ALTER TABLE hands.skill_admission_audit
    ADD COLUMN IF NOT EXISTS content_policy_version text NOT NULL DEFAULT 'unknown';

ALTER TABLE hands.skill_admission_audit
    DROP CONSTRAINT IF EXISTS skill_admission_audit_content_policy_version_check;
ALTER TABLE hands.skill_admission_audit
    ADD CONSTRAINT skill_admission_audit_content_policy_version_check
    CHECK (content_policy_version ~ '^[a-z0-9][a-z0-9._-]{0,127}$');

CREATE INDEX IF NOT EXISTS skill_admission_audit_policy_idx
    ON hands.skill_admission_audit
    (tenant_id, content_policy_version, outcome, occurred_at DESC);
