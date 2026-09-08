BEGIN;

ALTER TABLE delivery.delivery_job
    ADD COLUMN IF NOT EXISTS root_session_id text,
    ADD COLUMN IF NOT EXISTS sink_id text;

UPDATE delivery.delivery_job
SET root_session_id = session_id
WHERE root_session_id IS NULL;

UPDATE delivery.delivery_job
SET sink_id = sink_target_ref
WHERE sink_id IS NULL;

ALTER TABLE delivery.delivery_job
    ALTER COLUMN root_session_id SET NOT NULL,
    ALTER COLUMN sink_id SET NOT NULL;

ALTER TABLE delivery.delivery_job
    DROP CONSTRAINT IF EXISTS delivery_job_event_id_sink_target_ref_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'delivery_job_event_id_sink_id_key'
    ) THEN
        ALTER TABLE delivery.delivery_job
            ADD CONSTRAINT delivery_job_event_id_sink_id_key UNIQUE (event_id, sink_id);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS delivery.sink_config (
    sink_id text NOT NULL,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    sink_type text NOT NULL,
    target_ref text NOT NULL,
    credential_ref text,
    event_types jsonb NOT NULL DEFAULT '["run.completed","run.failed"]'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, sink_id)
);

CREATE INDEX IF NOT EXISTS sink_config_session_idx
    ON delivery.sink_config (tenant_id, session_id, enabled);

CREATE INDEX IF NOT EXISTS delivery_job_due_idx
    ON delivery.delivery_job (next_attempt_at, delivery_id)
    WHERE status IN ('pending', 'retry_wait');

CREATE TABLE IF NOT EXISTS delivery.delivery_attempt (
    delivery_id text NOT NULL REFERENCES delivery.delivery_job(delivery_id) ON DELETE CASCADE,
    attempt_number integer NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    outcome text NOT NULL,
    response_summary text NOT NULL,
    retryable boolean NOT NULL,
    PRIMARY KEY (delivery_id, attempt_number)
);

ALTER TABLE projection.task_view
    ADD COLUMN IF NOT EXISTS delivery_status text,
    ADD COLUMN IF NOT EXISTS delivery_id text,
    ADD COLUMN IF NOT EXISTS delivery_attempt_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS delivery_response_summary text;

COMMIT;
