BEGIN;

ALTER TABLE control.runnable_item
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz;

CREATE INDEX IF NOT EXISTS runnable_recoverable_claim_idx
    ON control.runnable_item (claim_expires_at, task_id)
    WHERE status = 'claimed';

ALTER TABLE delivery.delivery_job
    ADD COLUMN IF NOT EXISTS claimed_by text,
    ADD COLUMN IF NOT EXISTS claim_token text,
    ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz;

CREATE INDEX IF NOT EXISTS delivery_recoverable_claim_idx
    ON delivery.delivery_job (claim_expires_at, delivery_id)
    WHERE status = 'attempting';

COMMIT;
