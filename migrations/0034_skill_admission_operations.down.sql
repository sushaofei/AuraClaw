DROP INDEX IF EXISTS hands.skill_admission_audit_policy_idx;
ALTER TABLE hands.skill_admission_audit
    DROP CONSTRAINT IF EXISTS skill_admission_audit_content_policy_version_check;
ALTER TABLE hands.skill_admission_audit
    DROP COLUMN IF EXISTS content_policy_version;
