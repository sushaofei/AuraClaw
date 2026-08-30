BEGIN;

DROP INDEX IF EXISTS hands.mcp_server_runtime_health_idx;

WITH ranked AS (
    SELECT ctid,
           row_number() OVER (
               PARTITION BY server_id
               ORDER BY updated_at DESC, instance_id ASC
           ) AS position
    FROM hands.mcp_server_runtime
)
DELETE FROM hands.mcp_server_runtime AS runtime
USING ranked
WHERE runtime.ctid = ranked.ctid
  AND ranked.position > 1;

ALTER TABLE hands.mcp_server_runtime
    DROP CONSTRAINT IF EXISTS mcp_server_runtime_pkey;
ALTER TABLE hands.mcp_server_runtime
    ADD CONSTRAINT mcp_server_runtime_pkey PRIMARY KEY (server_id);
ALTER TABLE hands.mcp_server_runtime
    DROP COLUMN IF EXISTS instance_id;

ALTER TABLE hands.capability_catalog
    DROP COLUMN IF EXISTS catalog_generation;
ALTER TABLE hands.downstream_mcp_server
    DROP COLUMN IF EXISTS active_catalog_generation,
    DROP COLUMN IF EXISTS last_good_catalog_at;

COMMIT;
