BEGIN;

CREATE TABLE IF NOT EXISTS hands.skill_package_tombstone (
    tombstone_id bigserial PRIMARY KEY,
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    package_digest text NOT NULL,
    manifest_json jsonb NOT NULL,
    artifact_ref jsonb NOT NULL,
    signature_key_id text,
    retention_status text NOT NULL,
    retention_until timestamptz NOT NULL,
    legal_hold boolean NOT NULL,
    retention_revision integer NOT NULL,
    retention_updated_by text NOT NULL,
    retention_updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    purged_at timestamptz NOT NULL,
    replacement_package_digest text NOT NULL,
    archived_at timestamptz NOT NULL,
    CHECK (retention_status = 'purged'),
    CHECK (retention_revision >= 1),
    UNIQUE (tenant_id, publisher, name, version, purged_at)
);

CREATE INDEX IF NOT EXISTS skill_package_tombstone_identity_idx
    ON hands.skill_package_tombstone
        (tenant_id, publisher, name, version, archived_at DESC);

ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_package_digest_fk;
ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_tenant_id_publisher_name_version_package_fkey;
ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_package_digest_fk
    FOREIGN KEY (tenant_id, publisher, name, version, package_digest)
    REFERENCES hands.skill_package
        (tenant_id, publisher, name, version, package_digest)
    DEFERRABLE INITIALLY IMMEDIATE;

COMMIT;
