BEGIN;

ALTER TABLE `projection_task_view`
    ADD COLUMN `source` varchar(32) NOT NULL DEFAULT 'chat',
    ADD COLUMN `schedule_id` varchar(128) NULL,
    ADD COLUMN `occurrence_id` varchar(128) NULL;

ALTER TABLE `projection_task_view`
    ADD INDEX `task_view_list_idx` (`tenant_id`, `role`, `projected_at`, `session_id`);

COMMIT;
