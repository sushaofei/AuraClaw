BEGIN;

ALTER TABLE `projection_task_view`
    ADD COLUMN skill_activations json NOT NULL DEFAULT (CAST('[]' AS JSON));

COMMIT;
