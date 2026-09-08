BEGIN;

ALTER TABLE hands.invocation
    ADD COLUMN IF NOT EXISTS execution_owner text,
    ADD COLUMN IF NOT EXISTS execution_claim_token text,
    ADD COLUMN IF NOT EXISTS execution_claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS execution_heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz;

CREATE INDEX IF NOT EXISTS hands_invocation_execution_claim_idx
    ON hands.invocation (status, execution_claim_expires_at);

CREATE INDEX IF NOT EXISTS hands_invocation_cancel_requested_idx
    ON hands.invocation (cancel_requested_at)
    WHERE cancel_requested_at IS NOT NULL;

COMMIT;
