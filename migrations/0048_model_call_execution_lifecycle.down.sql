BEGIN;

DROP INDEX IF EXISTS model_gateway.model_call_execution_claim_idx;

ALTER TABLE model_gateway.model_call
    DROP COLUMN IF EXISTS cancel_causation_id,
    DROP COLUMN IF EXISTS cancel_correlation_id,
    DROP COLUMN IF EXISTS cancel_actor,
    DROP COLUMN IF EXISTS causation_id,
    DROP COLUMN IF EXISTS correlation_id,
    DROP COLUMN IF EXISTS actor,
    DROP COLUMN IF EXISTS provider_request_ref,
    DROP COLUMN IF EXISTS completed_at,
    DROP COLUMN IF EXISTS cancelled_at,
    DROP COLUMN IF EXISTS cancel_requested_at,
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS started_at,
    DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS execution_owner;

COMMIT;
