BEGIN;
CREATE TABLE IF NOT EXISTS hands.skill_upgrade_current (
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    operation_id text NOT NULL,
    command_id text NOT NULL,
    current_version text NOT NULL,
    package_digest text NOT NULL,
    generation integer NOT NULL CHECK (generation > 0),
    phase text NOT NULL CHECK (phase IN ('draining','deleting','completed','blocked')),
    reason_code text,
    actor_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,publisher,name)
);
CREATE INDEX IF NOT EXISTS skill_upgrade_pending_idx ON hands.skill_upgrade_current(updated_at)
    WHERE phase <> 'completed';
COMMIT;
