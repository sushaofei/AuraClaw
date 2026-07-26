BEGIN;

ALTER TABLE `delivery_delivery_job`
    ADD COLUMN root_session_id VARCHAR(64),
    ADD COLUMN sink_id VARCHAR(64);

UPDATE `delivery_delivery_job`
SET root_session_id = session_id
WHERE root_session_id IS NULL;

UPDATE `delivery_delivery_job`
SET sink_id = sink_target_ref
WHERE sink_id IS NULL;

ALTER TABLE `delivery_delivery_job`
    MODIFY COLUMN root_session_id VARCHAR(64) NOT NULL,
    MODIFY COLUMN sink_id VARCHAR(64) NOT NULL;

ALTER TABLE `delivery_delivery_job` DROP INDEX `event_id`;
ALTER TABLE `delivery_delivery_job`
    ADD UNIQUE KEY `delivery_job_event_id_sink_id_key` (event_id, sink_id);

CREATE TABLE IF NOT EXISTS `delivery_sink_config` (
    sink_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    sink_type VARCHAR(64) NOT NULL,
    target_ref VARCHAR(64) NOT NULL,
    credential_ref VARCHAR(64),
    event_types json NOT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, sink_id)
);

CREATE INDEX sink_config_session_idx
    ON `delivery_sink_config` (tenant_id, session_id, enabled);

CREATE INDEX delivery_job_due_idx
    ON `delivery_delivery_job` (next_attempt_at, delivery_id);

CREATE TABLE IF NOT EXISTS `delivery_delivery_attempt` (
    delivery_id VARCHAR(64) NOT NULL,
    attempt_number integer NOT NULL,
    started_at datetime(6) NOT NULL,
    completed_at datetime(6) NOT NULL,
    outcome VARCHAR(64) NOT NULL,
    response_summary VARCHAR(512) NOT NULL,
    retryable TINYINT(1) NOT NULL,
    PRIMARY KEY (delivery_id, attempt_number),
    CONSTRAINT fk_delivery_attempt_job
        FOREIGN KEY (delivery_id) REFERENCES `delivery_delivery_job` (delivery_id)
        ON DELETE CASCADE
);

ALTER TABLE `projection_task_view`
    ADD COLUMN delivery_status VARCHAR(64),
    ADD COLUMN delivery_id VARCHAR(64),
    ADD COLUMN delivery_attempt_count integer NOT NULL DEFAULT 0,
    ADD COLUMN delivery_response_summary VARCHAR(512);

COMMIT;
