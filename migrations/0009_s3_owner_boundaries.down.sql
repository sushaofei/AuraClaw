BEGIN;

INSERT INTO security.tool_capability
    (tool_name, version, description, input_schema, output_schema, permission,
     risk_level, runtime_location, owner, enabled)
SELECT tool_name, version, description, input_schema, output_schema, permission,
       risk_level, runtime_location, owner, enabled
FROM hands.tool_capability
ON CONFLICT (tool_name, version) DO UPDATE SET
    description = EXCLUDED.description,
    input_schema = EXCLUDED.input_schema,
    output_schema = EXCLUDED.output_schema,
    permission = EXCLUDED.permission,
    risk_level = EXCLUDED.risk_level,
    runtime_location = EXCLUDED.runtime_location,
    owner = EXCLUDED.owner,
    enabled = EXCLUDED.enabled;

INSERT INTO security.tool_invocation_dedup
    (tenant_id, idempotency_key, action_digest, tool_invocation_id,
     normalized_result, side_effect_status, created_at)
SELECT tenant_id, idempotency_key, argument_digest, tool_invocation_id,
       normalized_result, side_effect_status, created_at
FROM hands.invocation
WHERE normalized_result IS NOT NULL
ON CONFLICT (tenant_id, idempotency_key) DO UPDATE SET
    action_digest = EXCLUDED.action_digest,
    tool_invocation_id = EXCLUDED.tool_invocation_id,
    normalized_result = EXCLUDED.normalized_result,
    side_effect_status = EXCLUDED.side_effect_status;

INSERT INTO security.credential_reference
    (tenant_id, credential_ref, provider, account_scope, allowed_operations,
     expires_at, revoked_at)
SELECT tenant_id, credential_ref, provider, account_scope, allowed_operations,
       expires_at, revoked_at
FROM credential.reference
ON CONFLICT (tenant_id, credential_ref) DO UPDATE SET
    provider = EXCLUDED.provider,
    account_scope = EXCLUDED.account_scope,
    allowed_operations = EXCLUDED.allowed_operations,
    expires_at = EXCLUDED.expires_at,
    revoked_at = EXCLUDED.revoked_at;

INSERT INTO security.credential_usage_audit
    (tenant_id, session_id, tool_name, credential_ref, operation,
     policy_version, used_at)
SELECT tenant_id, session_id, target, credential_ref, operation,
       policy_decision_id, used_at
FROM credential.usage_audit
WHERE usage_id NOT LIKE 'legacy:%';

DROP TABLE IF EXISTS artifact.admin_operation;
DROP TABLE IF EXISTS delivery.admin_operation;
DROP TABLE IF EXISTS projection.admin_operation;

ALTER TABLE artifact.metadata
    DROP COLUMN IF EXISTS deleted_at,
    DROP COLUMN IF EXISTS legal_hold,
    DROP COLUMN IF EXISTS scan_status,
    DROP COLUMN IF EXISTS expected_checksum,
    DROP COLUMN IF EXISTS upload_expires_at,
    DROP COLUMN IF EXISTS upload_id,
    DROP COLUMN IF EXISTS status;

DROP SCHEMA IF EXISTS credential CASCADE;
DROP SCHEMA IF EXISTS policy CASCADE;
DROP SCHEMA IF EXISTS hands CASCADE;

DROP INDEX IF EXISTS session_core.outbox_claimable_idx;

ALTER TABLE session_core.outbox
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS poisoned_at,
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS claimed_by;

COMMIT;
