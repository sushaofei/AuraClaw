CREATE TABLE IF NOT EXISTS hands.skill_admission_audit (
    admission_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    operation text NOT NULL,
    actor_id text NOT NULL,
    source_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    publisher text,
    name text,
    version text,
    package_digest text,
    artifact_id text,
    outcome text NOT NULL,
    stage text NOT NULL,
    safe_error_code text,
    duration_ms integer NOT NULL,
    occurred_at timestamptz NOT NULL,
    CHECK (operation IN ('publish','publish_artifact')),
    CHECK (outcome IN ('accepted','rejected')),
    CHECK (duration_ms >= 0),
    CHECK (
        (outcome = 'accepted' AND stage = 'completed' AND safe_error_code IS NULL)
        OR (outcome = 'rejected' AND safe_error_code IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS skill_admission_audit_tenant_time_idx
    ON hands.skill_admission_audit (tenant_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS skill_admission_audit_failure_idx
    ON hands.skill_admission_audit (tenant_id, stage, safe_error_code, occurred_at DESC)
    WHERE outcome = 'rejected';
