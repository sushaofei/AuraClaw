BEGIN;

ALTER TABLE `session_core_command_dedup`
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (tenant_id, operation, command_id);

ALTER TABLE `projection_task_view`
    ADD COLUMN role VARCHAR(64) NOT NULL DEFAULT 'root',
    ADD COLUMN parent_session_id VARCHAR(64);

CREATE TABLE `projection_poison_event` (
    projector_id VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    reason text NOT NULL,
    payload json NOT NULL,
    quarantined_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    resolved_at datetime(6),
    PRIMARY KEY (projector_id, event_id)
);

-- Keep legacy name if an earlier broken conversion created it.
CREATE TABLE IF NOT EXISTS `projection_quarantine_event` (
    projector_id VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    reason text NOT NULL,
    payload json NOT NULL,
    quarantined_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (projector_id, event_id)
);

COMMIT;
