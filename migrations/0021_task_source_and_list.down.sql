BEGIN;

ALTER TABLE projection.task_view
    DROP COLUMN IF EXISTS occurrence_id,
    DROP COLUMN IF EXISTS schedule_id,
    DROP COLUMN IF EXISTS source;

DROP INDEX IF EXISTS projection.task_view_list_idx;

COMMIT;
