CREATE INDEX IF NOT EXISTS skill_admission_audit_retention_idx
    ON hands.skill_admission_audit (occurred_at, admission_id);
