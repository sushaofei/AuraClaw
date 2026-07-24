BEGIN;

ALTER TABLE hands.downstream_mcp_server
    ADD COLUMN IF NOT EXISTS title text,
    ADD COLUMN IF NOT EXISTS trust_level text NOT NULL DEFAULT 'external_untrusted',
    ADD COLUMN IF NOT EXISTS allowed_resource_schemes jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS allowed_prompt_prefixes jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'quarantined',
    ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE hands.downstream_mcp_server
SET title = COALESCE(title, server_id)
WHERE title IS NULL;

ALTER TABLE hands.downstream_mcp_server
    ALTER COLUMN title SET NOT NULL;

CREATE TABLE IF NOT EXISTS hands.capability_catalog (
    capability_id text PRIMARY KEY,
    kind text NOT NULL,
    server_id text NOT NULL REFERENCES hands.downstream_mcp_server(server_id)
        ON DELETE CASCADE,
    canonical_name text NOT NULL,
    version text NOT NULL,
    content_digest text NOT NULL,
    title text NOT NULL,
    description text NOT NULL DEFAULT '',
    tags jsonb NOT NULL DEFAULT '[]'::jsonb,
    tenant_id text,
    trust_level text NOT NULL,
    classification text NOT NULL DEFAULT 'internal',
    permission text,
    risk_level text,
    required_scopes jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'quarantined',
    source_revision text,
    capability_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (server_id, kind, canonical_name, version),
    CHECK (kind IN ('resource','resource_template','tool','prompt','skill')),
    CHECK (trust_level IN ('platform','tenant_verified','external_untrusted')),
    CHECK (status IN ('active','degraded','quarantined','retired'))
);

CREATE INDEX IF NOT EXISTS capability_catalog_tenant_kind_idx
    ON hands.capability_catalog (tenant_id, kind, status, canonical_name);
CREATE INDEX IF NOT EXISTS capability_catalog_server_idx
    ON hands.capability_catalog (server_id, updated_at DESC);

COMMIT;
