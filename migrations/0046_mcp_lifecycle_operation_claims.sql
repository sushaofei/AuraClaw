BEGIN;

ALTER TABLE hands.mcp_server_operation
    ADD COLUMN IF NOT EXISTS request_digest text,
    ADD COLUMN IF NOT EXISTS claimed_by text,
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS started_at timestamptz;

UPDATE hands.mcp_server_operation
SET request_digest=COALESCE(request_digest,'legacy:' || operation_id);

ALTER TABLE hands.mcp_server_operation
    ALTER COLUMN request_digest SET NOT NULL,
    ALTER COLUMN request_digest SET DEFAULT 'legacy';

ALTER TABLE hands.mcp_server_operation
    DROP CONSTRAINT IF EXISTS mcp_server_operation_status_check;

ALTER TABLE hands.mcp_server_operation
    ADD CONSTRAINT mcp_server_operation_status_check
    CHECK (status IN (
        'accepted','running','succeeded','failed','reconciling','unknown_side_effect'
    ));

CREATE INDEX IF NOT EXISTS mcp_server_operation_claim_idx
    ON hands.mcp_server_operation (status, claim_expires_at);

COMMIT;
