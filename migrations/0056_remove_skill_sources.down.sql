BEGIN;

CREATE TABLE IF NOT EXISTS hands.skill_source (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    kind text NOT NULL,
    desired_state text NOT NULL,
    publisher_allowlist jsonb NOT NULL DEFAULT '[]'::jsonb,
    credential_ref text,
    config_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    revision integer NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    priority integer NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, source_id),
    CHECK (kind IN ('builtin','admin_upload','mcp','model_compiler','git','oci')),
    CHECK (desired_state IN ('enabled','disabled','retired')),
    CHECK (revision >= 1),
    CHECK (priority BETWEEN -1000 AND 1000)
);

ALTER TABLE hands.skill_publication ADD COLUMN IF NOT EXISTS source_id text;
ALTER TABLE hands.skill_installation ADD COLUMN IF NOT EXISTS source_id text;
ALTER TABLE hands.skill_command ADD COLUMN IF NOT EXISTS source_id text;
ALTER TABLE hands.skill_admission_audit ADD COLUMN IF NOT EXISTS source_id text;

ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_source_fk
    FOREIGN KEY (tenant_id, source_id)
    REFERENCES hands.skill_source (tenant_id, source_id);
ALTER TABLE hands.skill_installation
    ADD CONSTRAINT skill_installation_source_fk
    FOREIGN KEY (tenant_id, source_id)
    REFERENCES hands.skill_source (tenant_id, source_id);

CREATE TABLE IF NOT EXISTS hands.skill_source_sync_state (
    source_id text NOT NULL,
    tenant_id text NOT NULL,
    generation bigint NOT NULL DEFAULT 0,
    cursor text,
    complete_snapshot boolean NOT NULL DEFAULT false,
    last_success_at timestamptz,
    last_attempt_at timestamptz,
    consecutive_failures integer NOT NULL DEFAULT 0,
    safe_error_code text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source_id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hands.skill_source_lease (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    owner text NOT NULL,
    fencing_token bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    acquired_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, source_id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE
);

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
        REFERENCES hands.skill_source (tenant_id, source_id)
);

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
        REFERENCES hands.skill_source (tenant_id, source_id)
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
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE
);

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
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS skill_source_tenant_state_idx
    ON hands.skill_source (tenant_id, desired_state, source_id);
CREATE INDEX IF NOT EXISTS skill_publication_source_selection_idx
    ON hands.skill_publication_source
        (tenant_id, publisher, name, version, available, source_id);

COMMIT;
