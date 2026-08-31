BEGIN;

DROP INDEX IF EXISTS hands.hands_invocation_cancel_requested_idx;
DROP INDEX IF EXISTS hands.hands_invocation_execution_claim_idx;

ALTER TABLE hands.invocation
    DROP COLUMN IF EXISTS cancel_requested_at,
    DROP COLUMN IF EXISTS execution_heartbeat_at,
    DROP COLUMN IF EXISTS execution_claim_expires_at,
    DROP COLUMN IF EXISTS execution_claim_token,
    DROP COLUMN IF EXISTS execution_owner;

COMMIT;
