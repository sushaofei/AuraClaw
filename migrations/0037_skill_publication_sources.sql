BEGIN;

ALTER TABLE hands.skill_source
    ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 0;

ALTER TABLE hands.skill_source
    DROP CONSTRAINT IF EXISTS skill_source_priority_check;
ALTER TABLE hands.skill_source
    ADD CONSTRAINT skill_source_priority_check CHECK (priority BETWEEN -1000 AND 1000);

CREATE TABLE IF NOT EXISTS hands.skill_publication_source (
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    source_id text NOT NULL,
    available boolean NOT NULL DEFAULT true,
    first_seen_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    unavailable_at timestamptz,
    PRIMARY KEY (tenant_id, publisher, name, version, source_id),
    FOREIGN KEY (tenant_id, publisher, name, version)
        REFERENCES hands.skill_publication (tenant_id, publisher, name, version),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id),
    CHECK (
        (available AND unavailable_at IS NULL)
        OR (NOT available AND unavailable_at IS NOT NULL)
    )
);

INSERT INTO hands.skill_publication_source
    (tenant_id,publisher,name,version,source_id,available,first_seen_at,last_seen_at)
SELECT tenant_id,publisher,name,version,source_id,true,created_at,updated_at
FROM hands.skill_publication
WHERE source_id IS NOT NULL
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS skill_publication_source_selection_idx
    ON hands.skill_publication_source
        (tenant_id,publisher,name,version,available,source_id);

CREATE TABLE IF NOT EXISTS hands.skill_source_command (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    request_digest text NOT NULL,
    source_id text NOT NULL,
    operation text NOT NULL,
    actor_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    reason_code text,
    resulting_revision integer NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, command_id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id),
    CHECK (operation IN ('configure','retire')),
    CHECK (operation <> 'retire' OR NULLIF(reason_code, '') IS NOT NULL),
    CHECK (resulting_revision >= 1)
);

COMMIT;
