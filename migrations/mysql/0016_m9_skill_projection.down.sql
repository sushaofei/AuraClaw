BEGIN;

ALTER TABLE `projection_task_view`
    DROP COLUMN IF EXISTS skill_activations;

COMMIT;
