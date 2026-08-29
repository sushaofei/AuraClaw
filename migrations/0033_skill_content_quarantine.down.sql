UPDATE hands.skill_admission_audit
SET outcome='rejected'
WHERE outcome='quarantined';
ALTER TABLE hands.skill_admission_audit
    DROP CONSTRAINT IF EXISTS skill_admission_audit_outcome_check;
ALTER TABLE hands.skill_admission_audit
    DROP CONSTRAINT IF EXISTS skill_admission_audit_result_check;
ALTER TABLE hands.skill_admission_audit
    DROP CONSTRAINT IF EXISTS skill_admission_audit_check;
ALTER TABLE hands.skill_admission_audit
    ADD CONSTRAINT skill_admission_audit_outcome_check
    CHECK (outcome IN ('accepted','rejected'));
ALTER TABLE hands.skill_admission_audit
    ADD CONSTRAINT skill_admission_audit_check
    CHECK (
        (outcome = 'accepted' AND stage = 'completed' AND safe_error_code IS NULL)
        OR (outcome = 'rejected' AND safe_error_code IS NOT NULL)
    );
DROP INDEX IF EXISTS hands.skill_admission_audit_failure_idx;
CREATE INDEX skill_admission_audit_failure_idx
    ON hands.skill_admission_audit (tenant_id, stage, safe_error_code, occurred_at DESC)
    WHERE outcome = 'rejected';
