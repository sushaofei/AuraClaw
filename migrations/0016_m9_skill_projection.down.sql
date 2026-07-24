BEGIN;

ALTER TABLE projection.task_view
    DROP COLUMN IF EXISTS skill_activations;

COMMIT;
