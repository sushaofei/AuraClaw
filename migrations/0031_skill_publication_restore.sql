ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_status_check;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_reason_check;
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_status_check
    CHECK (status IN (
        'staged','validating','active','restoring',
        'quarantined','retired','revoked'
    ));
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_reason_check
    CHECK (
        status NOT IN ('restoring','quarantined','retired','revoked')
        OR NULLIF(reason_code, '') IS NOT NULL
    );

CREATE TABLE IF NOT EXISTS hands.skill_publication_restore_command (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    request_digest text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    actor_id text NOT NULL,
    reason_code text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    previous_revision integer NOT NULL,
    restoring_revision integer NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, command_id),
    FOREIGN KEY (tenant_id, publisher, name, version)
        REFERENCES hands.skill_publication (tenant_id, publisher, name, version),
    CHECK (previous_revision >= 1),
    CHECK (restoring_revision = previous_revision + 1)
);
