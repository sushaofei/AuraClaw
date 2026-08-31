BEGIN;

ALTER TABLE IF EXISTS delivery.delivery_job
    ADD COLUMN IF NOT EXISTS claim_heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS side_effect_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS reconciliation_reason text;

ALTER TABLE IF EXISTS hands.skill_outbox
    ADD COLUMN IF NOT EXISTS claim_heartbeat_at timestamptz;

COMMIT;
