BEGIN;

ALTER TABLE `projection_task_view`
    MODIFY COLUMN goal TEXT NOT NULL,
    MODIFY COLUMN result_summary TEXT NULL;

COMMIT;
