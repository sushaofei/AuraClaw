BEGIN;

ALTER TABLE projection.task_view
    ADD COLUMN IF NOT EXISTS skill_activations jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMIT;
