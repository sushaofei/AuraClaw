BEGIN;

ALTER TABLE projection.task_view
    ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'chat',
    ADD COLUMN IF NOT EXISTS schedule_id text,
    ADD COLUMN IF NOT EXISTS occurrence_id text;

CREATE INDEX IF NOT EXISTS task_view_list_idx
    ON projection.task_view (tenant_id, role, projected_at DESC, session_id DESC);

COMMIT;
