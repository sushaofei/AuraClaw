BEGIN;

DROP INDEX IF EXISTS artifact.artifact_reconciliation_idx;

ALTER TABLE artifact.metadata
    DROP COLUMN IF EXISTS reconciliation_updated_at,
    DROP COLUMN IF EXISTS object_side_effect_started_at,
    DROP COLUMN IF EXISTS object_operation_ref,
    DROP COLUMN IF EXISTS reconciliation_reason,
    DROP COLUMN IF EXISTS object_state,
    DROP COLUMN IF EXISTS gc_heartbeat_at,
    DROP COLUMN IF EXISTS finalize_heartbeat_at;

COMMIT;
