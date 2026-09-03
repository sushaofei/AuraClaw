ALTER TABLE projection.task_view ADD COLUMN approval jsonb NOT NULL DEFAULT '{}'::jsonb;
