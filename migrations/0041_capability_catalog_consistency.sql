BEGIN;

ALTER TABLE hands.downstream_mcp_server
    ADD COLUMN IF NOT EXISTS active_catalog_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_good_catalog_at timestamptz;

ALTER TABLE hands.capability_catalog
    ADD COLUMN IF NOT EXISTS catalog_generation bigint NOT NULL DEFAULT 0;

UPDATE hands.capability_catalog AS capability
SET catalog_generation = server.active_catalog_generation
FROM hands.downstream_mcp_server AS server
WHERE server.server_id = capability.server_id
  AND capability.catalog_generation = 0;

ALTER TABLE hands.mcp_server_runtime
    ADD COLUMN IF NOT EXISTS instance_id text NOT NULL DEFAULT 'legacy';

DELETE FROM hands.mcp_server_runtime
WHERE instance_id = 'legacy'
  AND observed_state = 'pending'
  AND loaded_revision IS NULL
  AND last_test_at IS NULL
  AND last_sync_at IS NULL
  AND consecutive_failures = 0
  AND safe_error_code IS NULL;

ALTER TABLE hands.mcp_server_runtime
    DROP CONSTRAINT IF EXISTS mcp_server_runtime_pkey;

ALTER TABLE hands.mcp_server_runtime
    ADD CONSTRAINT mcp_server_runtime_pkey PRIMARY KEY (server_id, instance_id);

CREATE INDEX IF NOT EXISTS mcp_server_runtime_health_idx
    ON hands.mcp_server_runtime (server_id, observed_state, updated_at DESC);

COMMIT;
