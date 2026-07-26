BEGIN;

DROP TABLE IF EXISTS `projection_collaboration_view`;
ALTER TABLE `projection_task_view` DROP COLUMN IF EXISTS lineage;

COMMIT;
