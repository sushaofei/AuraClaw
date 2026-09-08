BEGIN;

ALTER TABLE IF EXISTS hands.skill_outbox
    DROP COLUMN IF EXISTS claim_heartbeat_at;

ALTER TABLE IF EXISTS delivery.delivery_job
    DROP COLUMN IF EXISTS reconciliation_reason,
    DROP COLUMN IF EXISTS side_effect_started_at,
    DROP COLUMN IF EXISTS claim_heartbeat_at;

COMMIT;
