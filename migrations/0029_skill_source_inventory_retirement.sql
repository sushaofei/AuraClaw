ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_status_check;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_check;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_reason_check;
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_status_check
    CHECK (status IN (
        'staged','validating','active','quarantined','retired','revoked'
    ));
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_reason_check
    CHECK (
        status NOT IN ('quarantined','retired','revoked')
        OR NULLIF(reason_code, '') IS NOT NULL
    );

CREATE TABLE IF NOT EXISTS hands.skill_source_inventory (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    last_seen_generation bigint,
    missing_complete_snapshots integer NOT NULL DEFAULT 0,
    first_missing_at timestamptz,
    last_checked_at timestamptz NOT NULL,
    retired_at timestamptz,
    PRIMARY KEY (tenant_id, source_id, publisher, name, version),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE,
    CHECK (last_seen_generation IS NULL OR last_seen_generation >= 0),
    CHECK (missing_complete_snapshots >= 0)
);

CREATE INDEX IF NOT EXISTS skill_source_inventory_missing_idx
    ON hands.skill_source_inventory
       (tenant_id, source_id, missing_complete_snapshots)
    WHERE retired_at IS NULL;

CREATE TABLE IF NOT EXISTS hands.skill_source_retirement_command (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    source_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    actor_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    fencing_token bigint NOT NULL,
    reason_code text NOT NULL,
    previous_revision integer NOT NULL,
    resulting_revision integer NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, command_id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE,
    CHECK (fencing_token >= 1),
    CHECK (previous_revision >= 1),
    CHECK (resulting_revision = previous_revision + 1)
);
