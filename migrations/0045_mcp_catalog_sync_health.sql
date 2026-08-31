BEGIN;

ALTER TABLE hands.downstream_mcp_server
    ADD COLUMN IF NOT EXISTS consecutive_sync_failures integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_catalog_sync_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_catalog_sync_error text,
    ADD COLUMN IF NOT EXISTS catalog_quarantined_at timestamptz;

ALTER TABLE hands.downstream_mcp_server
    DROP CONSTRAINT IF EXISTS downstream_mcp_server_sync_failures_nonnegative;

ALTER TABLE hands.downstream_mcp_server
    ADD CONSTRAINT downstream_mcp_server_sync_failures_nonnegative
    CHECK (consecutive_sync_failures >= 0);

CREATE INDEX IF NOT EXISTS downstream_mcp_server_quarantine_idx
    ON hands.downstream_mcp_server (status, catalog_quarantined_at)
    WHERE catalog_quarantined_at IS NOT NULL;

COMMIT;
