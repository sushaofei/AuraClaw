BEGIN;

ALTER TABLE `projection_task_view`
    DROP COLUMN IF EXISTS run_status;

COMMIT;
