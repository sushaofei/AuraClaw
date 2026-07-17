BEGIN;

DROP TABLE IF EXISTS projection.collaboration_view;
ALTER TABLE projection.task_view DROP COLUMN IF EXISTS lineage;

COMMIT;
