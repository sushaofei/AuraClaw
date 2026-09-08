BEGIN;

CREATE TABLE IF NOT EXISTS hands.skill_command (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    command_type text NOT NULL,
    request_digest text NOT NULL,
    actor_id text NOT NULL,
    source_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    package_digest text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, command_id),
    CHECK (command_type = 'publish'),
    CHECK (status = 'succeeded')
);

CREATE TABLE IF NOT EXISTS hands.skill_outbox (
    outbox_id bigserial PRIMARY KEY,
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    available_at timestamptz NOT NULL DEFAULT now(),
    attempt integer NOT NULL DEFAULT 0,
    claimed_by text,
    claim_expires_at timestamptz,
    last_error text,
    published_at timestamptz,
    UNIQUE (tenant_id, command_id, event_type),
    FOREIGN KEY (tenant_id, command_id)
        REFERENCES hands.skill_command (tenant_id, command_id),
    CHECK (event_type = 'skill.publication.committed'),
    CHECK (attempt >= 0)
);

CREATE INDEX IF NOT EXISTS skill_outbox_pending_idx
    ON hands.skill_outbox (available_at, outbox_id)
    WHERE published_at IS NULL;

DO $$
BEGIN
    IF to_regclass('artifact.metadata') IS NOT NULL THEN
        EXECUTE $migration$
            ALTER TABLE artifact.metadata
                ADD COLUMN IF NOT EXISTS skill_bound_at timestamptz,
                ADD COLUMN IF NOT EXISTS skill_bound_digest text,
                ADD COLUMN IF NOT EXISTS skill_publish_claim_token text,
                ADD COLUMN IF NOT EXISTS skill_publish_claim_expires_at timestamptz
        $migration$;
        EXECUTE $migration$
            CREATE INDEX IF NOT EXISTS artifact_skill_orphan_idx
            ON artifact.metadata (retention_until, artifact_id)
            WHERE status = 'ready'
              AND media_type = 'application/vnd.auraclaw.skill-package+json'
              AND skill_bound_at IS NULL
              AND deleted_at IS NULL
        $migration$;
    END IF;
END $$;

COMMIT;
