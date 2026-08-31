ALTER TABLE policy.approval
    ADD COLUMN IF NOT EXISTS request_digest text,
    ADD COLUMN IF NOT EXISTS generation bigint NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS decided_at timestamptz,
    ADD COLUMN IF NOT EXISTS decided_by text;

CREATE TABLE IF NOT EXISTS policy.approval_transition_audit (
    audit_id bigserial PRIMARY KEY,
    tenant_id text NOT NULL,
    approval_id text NOT NULL,
    generation bigint NOT NULL,
    operation text NOT NULL,
    actor_id text,
    service_identity text NOT NULL,
    decision text,
    request_digest text,
    prior_status text,
    result_status text NOT NULL,
    outcome text NOT NULL,
    request_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS policy_approval_transition_audit_lookup_idx
    ON policy.approval_transition_audit
    (tenant_id, approval_id, generation, created_at DESC);
