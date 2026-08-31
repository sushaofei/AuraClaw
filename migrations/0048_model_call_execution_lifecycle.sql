BEGIN;

ALTER TABLE model_gateway.model_call
    ADD COLUMN IF NOT EXISTS execution_owner text,
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS started_at timestamptz,
    ADD COLUMN IF NOT EXISTS heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz,
    ADD COLUMN IF NOT EXISTS cancelled_at timestamptz,
    ADD COLUMN IF NOT EXISTS completed_at timestamptz,
    ADD COLUMN IF NOT EXISTS provider_request_ref text;
ALTER TABLE model_gateway.model_call
    ADD COLUMN IF NOT EXISTS actor text,
    ADD COLUMN IF NOT EXISTS correlation_id text,
    ADD COLUMN IF NOT EXISTS causation_id text,
    ADD COLUMN IF NOT EXISTS cancel_actor text,
    ADD COLUMN IF NOT EXISTS cancel_correlation_id text,
    ADD COLUMN IF NOT EXISTS cancel_causation_id text;

CREATE INDEX IF NOT EXISTS model_call_execution_claim_idx
    ON model_gateway.model_call (status,claim_expires_at);

COMMIT;
