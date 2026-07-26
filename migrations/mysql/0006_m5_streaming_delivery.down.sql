BEGIN;

ALTER TABLE `projection_task_view`
    DROP COLUMN IF EXISTS delivery_response_summary,
    DROP COLUMN IF EXISTS delivery_attempt_count,
    DROP COLUMN IF EXISTS delivery_id,
    DROP COLUMN IF EXISTS delivery_status;

DROP TABLE IF EXISTS `delivery_delivery_attempt`;
DROP INDEX IF EXISTS `delivery_delivery_job_due_idx`;
DROP INDEX IF EXISTS `delivery_sink_config_session_idx`;
DROP TABLE IF EXISTS `delivery_sink_config`;

ALTER TABLE `delivery_delivery_job`
    DROP INDEX delivery_job_event_id_sink_id_key;

/* skipped plpgsql DO block */;

ALTER TABLE `delivery_delivery_job`
    DROP COLUMN IF EXISTS sink_id,
    DROP COLUMN IF EXISTS root_session_id;

COMMIT;
