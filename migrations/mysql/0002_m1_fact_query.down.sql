BEGIN;

DROP TABLE IF EXISTS `projection_poison_event`;

ALTER TABLE `projection_task_view`
    DROP COLUMN IF EXISTS parent_session_id,
    DROP COLUMN IF EXISTS role;

ALTER TABLE `session_core_command_dedup`
    DROP INDEX command_dedup_pkey;
ALTER TABLE `session_core_command_dedup`
    ADD PRIMARY KEY (tenant_id, command_id);

COMMIT;
