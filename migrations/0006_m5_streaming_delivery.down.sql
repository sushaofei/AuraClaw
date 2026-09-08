BEGIN;

ALTER TABLE projection.task_view
    DROP COLUMN IF EXISTS delivery_response_summary,
    DROP COLUMN IF EXISTS delivery_attempt_count,
    DROP COLUMN IF EXISTS delivery_id,
    DROP COLUMN IF EXISTS delivery_status;

DROP TABLE IF EXISTS delivery.delivery_attempt;
DROP INDEX IF EXISTS delivery.delivery_job_due_idx;
DROP INDEX IF EXISTS delivery.sink_config_session_idx;
DROP TABLE IF EXISTS delivery.sink_config;

ALTER TABLE delivery.delivery_job
    DROP CONSTRAINT IF EXISTS delivery_job_event_id_sink_id_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'delivery_job_event_id_sink_target_ref_key'
    ) THEN
        ALTER TABLE delivery.delivery_job
            ADD CONSTRAINT delivery_job_event_id_sink_target_ref_key
                UNIQUE (event_id, sink_target_ref);
    END IF;
END $$;

ALTER TABLE delivery.delivery_job
    DROP COLUMN IF EXISTS sink_id,
    DROP COLUMN IF EXISTS root_session_id;

COMMIT;
