BEGIN;

ALTER TABLE `hands_downstream_mcp_server`
    ADD COLUMN title VARCHAR(64),
    ADD COLUMN trust_level VARCHAR(64) NOT NULL DEFAULT 'external_untrusted',
    ADD COLUMN allowed_resource_schemes json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    ADD COLUMN allowed_prompt_prefixes json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    ADD COLUMN status VARCHAR(64) NOT NULL DEFAULT 'quarantined',
    ADD COLUMN metadata json NOT NULL DEFAULT (CAST('{}' AS JSON));

UPDATE `hands_downstream_mcp_server`
SET title = COALESCE(title, server_id)
WHERE title IS NULL;

ALTER TABLE `hands_downstream_mcp_server`
    MODIFY COLUMN title VARCHAR(64) NOT NULL;

CREATE TABLE IF NOT EXISTS `hands_capability_catalog` (
    capability_id VARCHAR(64) PRIMARY KEY,
    kind VARCHAR(64) NOT NULL,
    server_id VARCHAR(64) NOT NULL REFERENCES `hands_downstream_mcp_server`(server_id)
        ON DELETE CASCADE,
    canonical_name VARCHAR(64) NOT NULL,
    version VARCHAR(64) NOT NULL,
    content_digest VARCHAR(64) NOT NULL,
    title VARCHAR(64) NOT NULL,
    description VARCHAR(512) NOT NULL DEFAULT '',
    tags json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    tenant_id VARCHAR(64),
    trust_level VARCHAR(64) NOT NULL,
    classification VARCHAR(64) NOT NULL DEFAULT 'internal',
    permission VARCHAR(64),
    risk_level VARCHAR(64),
    required_scopes json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    status VARCHAR(64) NOT NULL DEFAULT 'quarantined',
    source_revision VARCHAR(64),
    capability_metadata json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (server_id, kind, canonical_name, version),
    CHECK (kind IN ('resource','resource_template','tool','prompt','skill')),
    CHECK (trust_level IN ('platform','tenant_verified','external_untrusted')),
    CHECK (status IN ('active','degraded','quarantined','retired'))
);

CREATE INDEX capability_catalog_tenant_kind_idx
    ON `hands_capability_catalog` (tenant_id, kind, status, canonical_name);
CREATE INDEX capability_catalog_server_idx
    ON `hands_capability_catalog` (server_id, updated_at DESC);

COMMIT;
