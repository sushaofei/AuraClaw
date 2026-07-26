BEGIN;

CREATE TABLE `security_tool_capability` (
    tool_name VARCHAR(64) NOT NULL,
    version VARCHAR(64) NOT NULL,
    description text NOT NULL,
    input_schema json NOT NULL,
    output_schema json NOT NULL,
    permission VARCHAR(64) NOT NULL,
    risk_level VARCHAR(64) NOT NULL,
    runtime_location VARCHAR(64) NOT NULL,
    owner VARCHAR(64) NOT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tool_name, version)
);

CREATE TABLE `projection_approval_view` (
    tenant_id VARCHAR(64) NOT NULL,
    approval_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    action_digest VARCHAR(64) NOT NULL,
    tool_name VARCHAR(64) NOT NULL,
    redacted_arguments json NOT NULL,
    risk VARCHAR(64) NOT NULL,
    reason VARCHAR(64) NOT NULL,
    expected_effect VARCHAR(64) NOT NULL,
    allowed_decisions json NOT NULL,
    assigned_approvers json NOT NULL,
    policy_version VARCHAR(64) NOT NULL,
    expires_at datetime(6) NOT NULL,
    status VARCHAR(64) NOT NULL,
    decision VARCHAR(64),
    feedback VARCHAR(64),
    source_version bigint NOT NULL,
    source_event_id VARCHAR(64) NOT NULL,
    projected_at datetime(6) NOT NULL,
    PRIMARY KEY (tenant_id, approval_id)
);

CREATE INDEX approval_session_digest_idx
    ON `projection_approval_view`
       (tenant_id, session_id, action_digest, policy_version, status);

CREATE TABLE `security_tool_invocation_dedup` (
    tenant_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL,
    action_digest VARCHAR(64) NOT NULL,
    tool_invocation_id VARCHAR(64) NOT NULL,
    normalized_result json NOT NULL,
    side_effect_status VARCHAR(64) NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE `security_credential_reference` (
    tenant_id VARCHAR(64) NOT NULL,
    credential_ref VARCHAR(64) NOT NULL,
    provider VARCHAR(64) NOT NULL,
    account_scope VARCHAR(64) NOT NULL,
    allowed_operations json NOT NULL,
    expires_at datetime(6) NOT NULL,
    revoked_at datetime(6),
    PRIMARY KEY (tenant_id, credential_ref)
);

CREATE TABLE `security_credential_usage_audit` (
    usage_id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    tool_name VARCHAR(64) NOT NULL,
    credential_ref VARCHAR(64) NOT NULL,
    operation VARCHAR(64) NOT NULL,
    policy_version VARCHAR(64) NOT NULL,
    used_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (usage_id)
);

CREATE TABLE `artifact_metadata` (
    tenant_id VARCHAR(64) NOT NULL,
    artifact_id VARCHAR(64) NOT NULL,
    root_session_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    artifact_type text NOT NULL,
    media_type text NOT NULL,
    name VARCHAR(64) NOT NULL,
    version integer NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    size bigint NOT NULL,
    storage_ref text NOT NULL,
    producer text NOT NULL,
    lineage_refs json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    classification VARCHAR(64) NOT NULL,
    acl json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    created_at datetime(6) NOT NULL,
    retention_until datetime(6),
    PRIMARY KEY (tenant_id, artifact_id),
    UNIQUE (tenant_id, artifact_id, version)
);

CREATE INDEX artifact_content_hash_idx ON `artifact_metadata` (tenant_id, content_hash);
CREATE INDEX artifact_session_idx ON `artifact_metadata` (tenant_id, session_id, created_at);

CREATE TABLE `artifact_access_audit` (
    access_id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(64) NOT NULL,
    artifact_id VARCHAR(64) NOT NULL,
    actor_id VARCHAR(64) NOT NULL,
    operation VARCHAR(64) NOT NULL,
    occurred_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (access_id)
);

COMMIT;
