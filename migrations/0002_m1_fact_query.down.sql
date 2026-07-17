BEGIN;

DROP TABLE IF EXISTS projection.poison_event;

ALTER TABLE projection.task_view
    DROP COLUMN IF EXISTS parent_session_id,
    DROP COLUMN IF EXISTS role;

ALTER TABLE session_core.command_dedup
    DROP CONSTRAINT command_dedup_pkey;
ALTER TABLE session_core.command_dedup
    ADD PRIMARY KEY (tenant_id, command_id);

COMMIT;
