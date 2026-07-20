BEGIN;

ALTER TABLE projection.task_view
    DROP COLUMN IF EXISTS run_status;

COMMIT;
