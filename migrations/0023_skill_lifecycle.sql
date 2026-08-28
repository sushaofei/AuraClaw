BEGIN;

CREATE TABLE IF NOT EXISTS hands.skill_package (
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    package_digest text NOT NULL,
    manifest_json jsonb NOT NULL,
    artifact_ref jsonb NOT NULL,
    signature_key_id text,
    retention_status text NOT NULL DEFAULT 'retained',
    created_at timestamptz NOT NULL,
    purged_at timestamptz,
    PRIMARY KEY (tenant_id, publisher, name, version),
    CONSTRAINT skill_package_identity_digest_unique
        UNIQUE (tenant_id, publisher, name, version, package_digest),
    UNIQUE (tenant_id, package_digest),
    CHECK (retention_status IN ('retained', 'purged')),
    CHECK (
        (retention_status = 'retained' AND purged_at IS NULL)
        OR (retention_status = 'purged' AND purged_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS hands.skill_publication (
    publication_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    package_digest text NOT NULL,
    status text NOT NULL,
    source_id text,
    revision integer NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    reason_code text,
    UNIQUE (tenant_id, publisher, name, version),
    CONSTRAINT skill_publication_package_digest_fk
        FOREIGN KEY (tenant_id, publisher, name, version, package_digest)
        REFERENCES hands.skill_package
            (tenant_id, publisher, name, version, package_digest),
    CHECK (status IN ('staged','validating','active','quarantined','revoked')),
    CHECK (
        status NOT IN ('quarantined','revoked')
        OR NULLIF(reason_code, '') IS NOT NULL
    ),
    CHECK (revision >= 1)
);

CREATE TABLE IF NOT EXISTS hands.skill_installation (
    installation_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version_constraint text NOT NULL DEFAULT '*',
    pinned_package_digest text,
    status text NOT NULL,
    source_id text,
    auto_upgrade boolean NOT NULL DEFAULT true,
    revision integer NOT NULL,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    reason_code text,
    UNIQUE (tenant_id, publisher, name),
    CHECK (status IN ('active','disabled','uninstalled')),
    CHECK (
        status = 'active' OR NULLIF(reason_code, '') IS NOT NULL
    ),
    CHECK (revision >= 1)
);

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
    PRIMARY KEY (tenant_id, source_id),
    CHECK (kind IN ('builtin','admin_upload','mcp','model_compiler','git','oci')),
    CHECK (desired_state IN ('enabled','disabled','retired')),
    CHECK (revision >= 1)
);

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
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE,
    CHECK (generation >= 0),
    CHECK (consecutive_failures >= 0),
    CHECK (NOT complete_snapshot OR last_success_at IS NOT NULL),
    CHECK (
        last_success_at IS NULL OR last_attempt_at IS NULL
        OR last_success_at <= last_attempt_at
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'skill_package_identity_digest_unique'
    ) THEN
        ALTER TABLE hands.skill_package
            ADD CONSTRAINT skill_package_identity_digest_unique
            UNIQUE (tenant_id, publisher, name, version, package_digest);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'skill_publication_package_digest_fk'
    ) THEN
        ALTER TABLE hands.skill_publication
            ADD CONSTRAINT skill_publication_package_digest_fk
            FOREIGN KEY (tenant_id, publisher, name, version, package_digest)
            REFERENCES hands.skill_package
                (tenant_id, publisher, name, version, package_digest);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'skill_publication_source_fk'
    ) THEN
        ALTER TABLE hands.skill_publication
            ADD CONSTRAINT skill_publication_source_fk
            FOREIGN KEY (tenant_id, source_id)
            REFERENCES hands.skill_source (tenant_id, source_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'skill_installation_source_fk'
    ) THEN
        ALTER TABLE hands.skill_installation
            ADD CONSTRAINT skill_installation_source_fk
            FOREIGN KEY (tenant_id, source_id)
            REFERENCES hands.skill_source (tenant_id, source_id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS skill_publication_tenant_status_idx
    ON hands.skill_publication (tenant_id, status, publisher, name, version);
CREATE INDEX IF NOT EXISTS skill_installation_tenant_status_idx
    ON hands.skill_installation (tenant_id, status, publisher, name);
CREATE INDEX IF NOT EXISTS skill_source_tenant_state_idx
    ON hands.skill_source (tenant_id, desired_state, source_id);

COMMIT;
