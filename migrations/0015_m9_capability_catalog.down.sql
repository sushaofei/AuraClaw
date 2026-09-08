BEGIN;

DROP TABLE IF EXISTS hands.capability_catalog;

ALTER TABLE hands.downstream_mcp_server
    DROP COLUMN IF EXISTS metadata,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS allowed_prompt_prefixes,
    DROP COLUMN IF EXISTS allowed_resource_schemes,
    DROP COLUMN IF EXISTS trust_level,
    DROP COLUMN IF EXISTS title;

COMMIT;
