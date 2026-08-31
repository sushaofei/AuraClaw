BEGIN;

DROP INDEX IF EXISTS hands.mcp_server_operation_claim_idx;

UPDATE hands.mcp_server_operation
SET status='failed',safe_error_code=COALESCE(safe_error_code,'manual_recovery_required')
WHERE status IN ('reconciling','unknown_side_effect');

ALTER TABLE hands.mcp_server_operation
    DROP CONSTRAINT IF EXISTS mcp_server_operation_status_check;
ALTER TABLE hands.mcp_server_operation
    ADD CONSTRAINT mcp_server_operation_status_check
    CHECK (status IN ('accepted','running','succeeded','failed'));

ALTER TABLE hands.mcp_server_operation
    DROP COLUMN IF EXISTS started_at,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS claimed_by,
    DROP COLUMN IF EXISTS request_digest;

COMMIT;
