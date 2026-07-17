BEGIN;

ALTER TABLE session_core.command_dedup
    DROP CONSTRAINT command_dedup_pkey;
ALTER TABLE session_core.command_dedup
    ADD PRIMARY KEY (tenant_id, operation, command_id);

ALTER TABLE projection.task_view
    ADD COLUMN role text NOT NULL DEFAULT 'root',
    ADD COLUMN parent_session_id text;

CREATE TABLE projection.poison_event (
    projector_id text NOT NULL,
    event_id text NOT NULL,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    reason text NOT NULL,
    payload jsonb NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    PRIMARY KEY (projector_id, event_id)
);

COMMIT;
