BEGIN;

CREATE SCHEMA IF NOT EXISTS artifact;
CREATE SCHEMA IF NOT EXISTS security;

CREATE TABLE security.tool_capability (
    tool_name text NOT NULL,
    version text NOT NULL,
    description text NOT NULL,
    input_schema jsonb NOT NULL,
    output_schema jsonb NOT NULL,
    permission text NOT NULL,
    risk_level text NOT NULL,
    runtime_location text NOT NULL,
    owner text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tool_name, version)
);

CREATE TABLE projection.approval_view (
    tenant_id text NOT NULL,
    approval_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    action_digest text NOT NULL,
    tool_name text NOT NULL,
    redacted_arguments jsonb NOT NULL,
    risk text NOT NULL,
    reason text NOT NULL,
    expected_effect text NOT NULL,
    allowed_decisions jsonb NOT NULL,
    assigned_approvers jsonb NOT NULL,
    policy_version text NOT NULL,
    expires_at timestamptz NOT NULL,
    status text NOT NULL,
    decision text,
    feedback text,
    source_version bigint NOT NULL,
    source_event_id text NOT NULL,
    projected_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, approval_id)
);

CREATE INDEX approval_session_digest_idx
    ON projection.approval_view
       (tenant_id, session_id, action_digest, policy_version, status);

CREATE TABLE security.tool_invocation_dedup (
    tenant_id text NOT NULL,
    idempotency_key text NOT NULL,
    action_digest text NOT NULL,
    tool_invocation_id text NOT NULL,
    normalized_result jsonb NOT NULL,
    side_effect_status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE security.credential_reference (
    tenant_id text NOT NULL,
    credential_ref text NOT NULL,
    provider text NOT NULL,
    account_scope text NOT NULL,
    allowed_operations jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    PRIMARY KEY (tenant_id, credential_ref)
);

CREATE TABLE security.credential_usage_audit (
    usage_id bigserial PRIMARY KEY,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    tool_name text NOT NULL,
    credential_ref text NOT NULL,
    operation text NOT NULL,
    policy_version text NOT NULL,
    used_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE artifact.metadata (
    tenant_id text NOT NULL,
    artifact_id text NOT NULL,
    root_session_id text NOT NULL,
    session_id text NOT NULL,
    artifact_type text NOT NULL,
    media_type text NOT NULL,
    name text NOT NULL,
    version integer NOT NULL,
    content_hash text NOT NULL,
    size bigint NOT NULL,
    storage_ref text NOT NULL,
    producer text NOT NULL,
    lineage_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    classification text NOT NULL,
    acl jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL,
    retention_until timestamptz,
    PRIMARY KEY (tenant_id, artifact_id),
    UNIQUE (tenant_id, artifact_id, version)
);

CREATE INDEX artifact_content_hash_idx ON artifact.metadata (tenant_id, content_hash);
CREATE INDEX artifact_session_idx ON artifact.metadata (tenant_id, session_id, created_at);

CREATE TABLE artifact.access_audit (
    access_id bigserial PRIMARY KEY,
    tenant_id text NOT NULL,
    artifact_id text NOT NULL,
    actor_id text NOT NULL,
    operation text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

COMMIT;
