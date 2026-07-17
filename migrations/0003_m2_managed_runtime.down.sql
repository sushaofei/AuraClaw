BEGIN;

DROP INDEX IF EXISTS control.assignment_runtime_idx;
DROP INDEX IF EXISTS control.runnable_claim_idx;
DROP TABLE IF EXISTS control.runtime_cancellation;
DROP TABLE IF EXISTS control.runtime_checkpoint;
DROP TABLE IF EXISTS control.capacity_reservation;
DROP TABLE IF EXISTS control.runtime_instance;

ALTER TABLE control.assignment
    DROP COLUMN IF EXISTS completed_at,
    DROP COLUMN IF EXISTS resource_profile,
    DROP COLUMN IF EXISTS role,
    DROP COLUMN IF EXISTS lease_id,
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS session_id,
    DROP COLUMN IF EXISTS root_session_id,
    DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE control.runnable_item
    DROP COLUMN IF EXISTS budget,
    DROP COLUMN IF EXISTS deadline,
    DROP COLUMN IF EXISTS role,
    DROP COLUMN IF EXISTS claimed_by,
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS root_session_id;

ALTER TABLE control.runtime_lease DROP COLUMN IF EXISTS lease_id;

COMMIT;
