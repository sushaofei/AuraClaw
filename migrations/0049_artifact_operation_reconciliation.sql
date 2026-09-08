BEGIN;

ALTER TABLE artifact.metadata
    ADD COLUMN IF NOT EXISTS finalize_heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS gc_heartbeat_at timestamptz,
    ADD COLUMN IF NOT EXISTS object_state text NOT NULL DEFAULT 'known',
    ADD COLUMN IF NOT EXISTS reconciliation_reason text,
    ADD COLUMN IF NOT EXISTS object_operation_ref text,
    ADD COLUMN IF NOT EXISTS object_side_effect_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS reconciliation_updated_at timestamptz;

CREATE INDEX IF NOT EXISTS artifact_reconciliation_idx
    ON artifact.metadata (status,reconciliation_updated_at)
    WHERE status='reconciling' OR object_state='unknown';

COMMIT;
