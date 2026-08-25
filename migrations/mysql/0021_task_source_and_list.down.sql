BEGIN;

ALTER TABLE `projection_task_view`
    DROP INDEX `task_view_list_idx`,
    DROP COLUMN `occurrence_id`,
    DROP COLUMN `schedule_id`,
    DROP COLUMN `source`;

COMMIT;
