BEGIN;

ALTER TABLE session_core.outbox
    ADD COLUMN IF NOT EXISTS claimed_by text,
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS poisoned_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_error text;

CREATE INDEX IF NOT EXISTS outbox_claimable_idx
    ON session_core.outbox (destination, next_attempt_at, outbox_id)
    WHERE published_at IS NULL AND poisoned_at IS NULL;

CREATE SCHEMA IF NOT EXISTS hands;
CREATE SCHEMA IF NOT EXISTS policy;
CREATE SCHEMA IF NOT EXISTS credential;

CREATE TABLE IF NOT EXISTS hands.tool_capability (
    tool_name text NOT NULL,
    version text NOT NULL,
    description text NOT NULL,
    input_schema jsonb NOT NULL,
    output_schema jsonb NOT NULL,
    permission text NOT NULL,
    risk_level text NOT NULL,
    runtime_location text NOT NULL,
    owner text NOT NULL,
    allowed_credential_operations jsonb NOT NULL DEFAULT '[]'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    PRIMARY KEY (tool_name, version)
);

CREATE TABLE IF NOT EXISTS hands.downstream_mcp_server (
    server_id text PRIMARY KEY,
    tenant_id text,
    endpoint text NOT NULL,
    allowed_tool_prefixes jsonb NOT NULL DEFAULT '[]'::jsonb,
    protocol_revision text NOT NULL DEFAULT '2025-11-25',
    credential_ref text,
    enabled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (endpoint ~ '^https://')
);

CREATE TABLE IF NOT EXISTS hands.invocation (
    tenant_id text NOT NULL,
    tool_invocation_id text NOT NULL,
    idempotency_key text NOT NULL,
    root_session_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    tool_name text NOT NULL,
    tool_version text NOT NULL,
    argument_digest text NOT NULL,
    normalized_arguments jsonb NOT NULL,
    status text NOT NULL,
    normalized_result jsonb,
    side_effect_status text NOT NULL DEFAULT 'not_started',
    lease_id text,
    fencing_token bigint NOT NULL,
    deadline timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, tool_invocation_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS hands.invocation_attempt (
    tenant_id text NOT NULL,
    tool_invocation_id text NOT NULL,
    attempt integer NOT NULL,
    status text NOT NULL,
    error_code text,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, tool_invocation_id, attempt),
    FOREIGN KEY (tenant_id, tool_invocation_id)
        REFERENCES hands.invocation (tenant_id, tool_invocation_id) ON DELETE CASCADE
);

INSERT INTO hands.tool_capability
    (tool_name, version, description, input_schema, output_schema, permission,
     risk_level, runtime_location, owner, allowed_credential_operations, enabled)
SELECT tool_name, version, description, input_schema, output_schema, permission,
       risk_level, runtime_location, owner, '[]'::jsonb, enabled
FROM security.tool_capability
ON CONFLICT (tool_name, version) DO NOTHING;

INSERT INTO hands.invocation
    (tenant_id, tool_invocation_id, idempotency_key, root_session_id, session_id,
     run_id, tool_name, tool_version, argument_digest, normalized_arguments,
     status, normalized_result, side_effect_status, fencing_token, created_at,
     updated_at)
SELECT tenant_id, tool_invocation_id, idempotency_key, 'legacy', 'legacy',
       'legacy', 'legacy-migrated', 'legacy', action_digest, '{}'::jsonb,
       'completed', normalized_result, side_effect_status, 0, created_at, created_at
FROM security.tool_invocation_dedup
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS policy.decision (
    decision_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    subject text NOT NULL,
    action text NOT NULL,
    resource text NOT NULL,
    input_digest text NOT NULL,
    decision text NOT NULL,
    policy_version text NOT NULL,
    constraints jsonb NOT NULL DEFAULT '{}'::jsonb,
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS policy_decision_scope_idx
    ON policy.decision (tenant_id, resource, created_at DESC);

CREATE TABLE IF NOT EXISTS policy.approval (
    tenant_id text NOT NULL,
    approval_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    action_digest text NOT NULL,
    policy_version text NOT NULL,
    status text NOT NULL,
    decision text,
    feedback text,
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, approval_id)
);

INSERT INTO policy.approval
    (tenant_id, approval_id, session_id, run_id, action_digest, policy_version,
     status, decision, feedback, expires_at, updated_at)
SELECT tenant_id, approval_id, session_id, run_id, action_digest, policy_version,
       status, decision, feedback, expires_at, projected_at
FROM projection.approval_view
ON CONFLICT (tenant_id, approval_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS credential.reference (
    tenant_id text NOT NULL,
    credential_ref text NOT NULL,
    resource text NOT NULL,
    provider text,
    account_scope text,
    allowed_operations jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    PRIMARY KEY (tenant_id, credential_ref)
);

ALTER TABLE credential.reference
    ADD COLUMN IF NOT EXISTS provider text,
    ADD COLUMN IF NOT EXISTS account_scope text;
UPDATE credential.reference
SET provider = COALESCE(provider, resource),
    account_scope = COALESCE(account_scope, resource)
WHERE provider IS NULL OR account_scope IS NULL;
ALTER TABLE credential.reference
    ALTER COLUMN provider SET NOT NULL,
    ALTER COLUMN account_scope SET NOT NULL;

INSERT INTO credential.reference
    (tenant_id, credential_ref, resource, provider, account_scope,
     allowed_operations, expires_at, revoked_at)
SELECT tenant_id, credential_ref, provider, provider, account_scope,
       allowed_operations, expires_at, revoked_at
FROM security.credential_reference
ON CONFLICT (tenant_id, credential_ref) DO NOTHING;

CREATE TABLE IF NOT EXISTS credential.usage_audit (
    usage_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    target text NOT NULL,
    credential_ref text NOT NULL,
    operation text NOT NULL,
    policy_decision_id text NOT NULL,
    status text NOT NULL,
    side_effect_status text NOT NULL,
    used_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO credential.usage_audit
    (usage_id, tenant_id, session_id, target, credential_ref, operation,
     policy_decision_id, status, side_effect_status, used_at)
SELECT 'legacy:' || usage_id::text, tenant_id, session_id, tool_name,
       credential_ref, operation, 'legacy-policy-version:' || policy_version,
       'migrated', 'unknown', used_at
FROM security.credential_usage_audit
ON CONFLICT (usage_id) DO NOTHING;

ALTER TABLE artifact.metadata
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'ready',
    ADD COLUMN IF NOT EXISTS upload_id text,
    ADD COLUMN IF NOT EXISTS upload_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS expected_checksum text,
    ADD COLUMN IF NOT EXISTS scan_status text NOT NULL DEFAULT 'not_required',
    ADD COLUMN IF NOT EXISTS legal_hold boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

CREATE TABLE IF NOT EXISTS projection.admin_operation (
    operation_id text PRIMARY KEY,
    operation text NOT NULL,
    parameters jsonb NOT NULL,
    status text NOT NULL,
    result jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS delivery.admin_operation
    (LIKE projection.admin_operation INCLUDING ALL);
CREATE TABLE IF NOT EXISTS artifact.admin_operation
    (LIKE projection.admin_operation INCLUDING ALL);

REVOKE CREATE ON SCHEMA session_core, projection, control, delivery,
    hands, policy, credential, artifact FROM PUBLIC;

COMMIT;
