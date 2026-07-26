BEGIN;

DROP INDEX IF EXISTS `control_assignment_runtime_idx`;
DROP INDEX IF EXISTS `control_runnable_claim_idx`;
DROP TABLE IF EXISTS `control_runtime_cancellation`;
DROP TABLE IF EXISTS `control_runtime_checkpoint`;
DROP TABLE IF EXISTS `control_capacity_reservation`;
DROP TABLE IF EXISTS `control_runtime_instance`;

ALTER TABLE `control_assignment`
    DROP COLUMN IF EXISTS completed_at,
    DROP COLUMN IF EXISTS resource_profile,
    DROP COLUMN IF EXISTS role,
    DROP COLUMN IF EXISTS lease_id,
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS session_id,
    DROP COLUMN IF EXISTS root_session_id,
    DROP COLUMN IF EXISTS tenant_id;

ALTER TABLE `control_runnable_item`
    DROP COLUMN IF EXISTS budget,
    DROP COLUMN IF EXISTS deadline,
    DROP COLUMN IF EXISTS role,
    DROP COLUMN IF EXISTS claimed_by,
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS root_session_id;

ALTER TABLE `control_runtime_lease` DROP COLUMN IF EXISTS lease_id;

COMMIT;
