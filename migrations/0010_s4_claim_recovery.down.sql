BEGIN;

DROP INDEX IF EXISTS delivery.delivery_recoverable_claim_idx;
ALTER TABLE delivery.delivery_job
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS claimed_by;

DROP INDEX IF EXISTS control.runnable_recoverable_claim_idx;
ALTER TABLE control.runnable_item
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS claim_token;

COMMIT;
