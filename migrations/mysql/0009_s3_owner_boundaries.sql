BEGIN;

ALTER TABLE `session_core_outbox`
    ADD COLUMN claimed_by VARCHAR(64),
    ADD COLUMN claim_token VARCHAR(64),
    ADD COLUMN claim_expires_at datetime(6),
    ADD COLUMN poisoned_at datetime(6),
    ADD COLUMN last_error VARCHAR(64);

CREATE INDEX outbox_claimable_idx
    ON `session_core_outbox` (destination, next_attempt_at, outbox_id);

CREATE TABLE IF NOT EXISTS `hands_tool_capability` (
    tool_name VARCHAR(64) NOT NULL,
    version VARCHAR(64) NOT NULL,
    description text NOT NULL,
    input_schema json NOT NULL,
    output_schema json NOT NULL,
    permission VARCHAR(64) NOT NULL,
    risk_level VARCHAR(64) NOT NULL,
    runtime_location VARCHAR(64) NOT NULL,
    owner VARCHAR(64) NOT NULL,
    allowed_credential_operations json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    PRIMARY KEY (tool_name, version)
);

CREATE TABLE IF NOT EXISTS `hands_downstream_mcp_server` (
    server_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64),
    endpoint VARCHAR(64) NOT NULL,
    allowed_tool_prefixes json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    protocol_revision VARCHAR(64) NOT NULL DEFAULT '2025-11-25',
    credential_ref VARCHAR(64),
    enabled TINYINT(1) NOT NULL DEFAULT 0,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CHECK (endpoint REGEXP '^https://')
);

CREATE TABLE IF NOT EXISTS `hands_invocation` (
    tenant_id VARCHAR(64) NOT NULL,
    tool_invocation_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(64) NOT NULL,
    root_session_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    tool_name VARCHAR(64) NOT NULL,
    tool_version VARCHAR(64) NOT NULL,
    argument_digest VARCHAR(64) NOT NULL,
    normalized_arguments json NOT NULL,
    status VARCHAR(64) NOT NULL,
    normalized_result json,
    side_effect_status VARCHAR(64) NOT NULL DEFAULT 'not_started',
    lease_id VARCHAR(64),
    fencing_token bigint NOT NULL,
    deadline datetime(6),
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, tool_invocation_id),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS `hands_invocation_attempt` (
    tenant_id VARCHAR(64) NOT NULL,
    tool_invocation_id VARCHAR(64) NOT NULL,
    attempt integer NOT NULL,
    status VARCHAR(64) NOT NULL,
    error_code VARCHAR(64),
    started_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at datetime(6),
    PRIMARY KEY (tenant_id, tool_invocation_id, attempt),
    FOREIGN KEY (tenant_id, tool_invocation_id)
        REFERENCES `hands_invocation` (tenant_id, tool_invocation_id) ON DELETE CASCADE
);

INSERT IGNORE INTO `hands_tool_capability`
    (tool_name, version, description, input_schema, output_schema, permission,
     risk_level, runtime_location, owner, allowed_credential_operations, enabled)
SELECT tool_name, version, description, input_schema, output_schema, permission,
       risk_level, runtime_location, owner, CAST('[]' AS JSON), enabled
FROM `security_tool_capability`;

INSERT IGNORE INTO `hands_invocation`
    (tenant_id, tool_invocation_id, idempotency_key, root_session_id, session_id,
     run_id, tool_name, tool_version, argument_digest, normalized_arguments,
     status, normalized_result, side_effect_status, fencing_token, created_at,
     updated_at)
SELECT tenant_id, tool_invocation_id, idempotency_key, 'legacy', 'legacy',
       'legacy', 'legacy-migrated', 'legacy', action_digest, CAST('{}' AS JSON),
       'completed', normalized_result, side_effect_status, 0, created_at, created_at
FROM `security_tool_invocation_dedup`;

CREATE TABLE IF NOT EXISTS `policy_decision` (
    decision_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    subject VARCHAR(64) NOT NULL,
    action VARCHAR(64) NOT NULL,
    resource VARCHAR(64) NOT NULL,
    input_digest VARCHAR(64) NOT NULL,
    decision VARCHAR(64) NOT NULL,
    policy_version VARCHAR(64) NOT NULL,
    constraints json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    expires_at datetime(6) NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
CREATE INDEX policy_decision_scope_idx
    ON `policy_decision` (tenant_id, resource, created_at DESC);

CREATE TABLE IF NOT EXISTS `policy_approval` (
    tenant_id VARCHAR(64) NOT NULL,
    approval_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    action_digest VARCHAR(64) NOT NULL,
    policy_version VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    decision VARCHAR(64),
    feedback VARCHAR(64),
    expires_at datetime(6) NOT NULL,
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, approval_id)
);

INSERT IGNORE INTO `policy_approval`
    (tenant_id, approval_id, session_id, run_id, action_digest, policy_version,
     status, decision, feedback, expires_at, updated_at)
SELECT tenant_id, approval_id, session_id, run_id, action_digest, policy_version,
       status, decision, feedback, expires_at, projected_at
FROM `projection_approval_view`;

CREATE TABLE IF NOT EXISTS `credential_reference` (
    tenant_id VARCHAR(64) NOT NULL,
    credential_ref VARCHAR(64) NOT NULL,
    resource VARCHAR(64) NOT NULL,
    provider VARCHAR(64),
    account_scope VARCHAR(64),
    allowed_operations json NOT NULL,
    expires_at datetime(6) NOT NULL,
    revoked_at datetime(6),
    PRIMARY KEY (tenant_id, credential_ref)
);

UPDATE `credential_reference`
SET provider = COALESCE(provider, resource),
    account_scope = COALESCE(account_scope, resource)
WHERE provider IS NULL OR account_scope IS NULL;
ALTER TABLE `credential_reference`
    MODIFY COLUMN provider VARCHAR(64) NOT NULL,
    MODIFY COLUMN account_scope VARCHAR(64) NOT NULL;

INSERT IGNORE INTO `credential_reference`
    (tenant_id, credential_ref, resource, provider, account_scope,
     allowed_operations, expires_at, revoked_at)
SELECT tenant_id, credential_ref, provider, provider, account_scope,
       allowed_operations, expires_at, revoked_at
FROM `security_credential_reference`;

CREATE TABLE IF NOT EXISTS `credential_usage_audit` (
    usage_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    target VARCHAR(64) NOT NULL,
    credential_ref VARCHAR(64) NOT NULL,
    operation VARCHAR(64) NOT NULL,
    policy_decision_id VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    side_effect_status VARCHAR(64) NOT NULL,
    used_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

INSERT IGNORE INTO `credential_usage_audit`
    (usage_id, tenant_id, session_id, target, credential_ref, operation,
     policy_decision_id, status, side_effect_status, used_at)
SELECT CONCAT('legacy:', usage_id), tenant_id, session_id, tool_name,
       credential_ref, operation, CONCAT('legacy-policy-version:', policy_version),
       'migrated', 'unknown', used_at
FROM `security_credential_usage_audit`;

ALTER TABLE `artifact_metadata`
    ADD COLUMN status VARCHAR(64) NOT NULL DEFAULT 'ready',
    ADD COLUMN upload_id VARCHAR(64),
    ADD COLUMN upload_expires_at datetime(6),
    ADD COLUMN expected_checksum VARCHAR(64),
    ADD COLUMN scan_status VARCHAR(64) NOT NULL DEFAULT 'not_required',
    ADD COLUMN legal_hold TINYINT(1) NOT NULL DEFAULT 0,
    ADD COLUMN deleted_at datetime(6);

CREATE TABLE IF NOT EXISTS `projection_admin_operation` (
    operation_id VARCHAR(64) PRIMARY KEY,
    operation VARCHAR(64) NOT NULL,
    parameters json NOT NULL,
    status VARCHAR(64) NOT NULL,
    result json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
CREATE TABLE IF NOT EXISTS `delivery_admin_operation` LIKE `projection_admin_operation`;
CREATE TABLE IF NOT EXISTS `artifact_admin_operation` LIKE `projection_admin_operation`;

COMMIT;
