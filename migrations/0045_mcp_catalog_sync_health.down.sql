BEGIN;

DROP INDEX IF EXISTS hands.downstream_mcp_server_quarantine_idx;

ALTER TABLE hands.downstream_mcp_server
    DROP CONSTRAINT IF EXISTS downstream_mcp_server_sync_failures_nonnegative,
    DROP COLUMN IF EXISTS catalog_quarantined_at,
    DROP COLUMN IF EXISTS last_catalog_sync_error,
    DROP COLUMN IF EXISTS last_catalog_sync_at,
    DROP COLUMN IF EXISTS consecutive_sync_failures;

COMMIT;
