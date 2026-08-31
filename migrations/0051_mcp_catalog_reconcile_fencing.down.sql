BEGIN;

DROP INDEX IF EXISTS hands.downstream_mcp_reconcile_expiry_idx;

ALTER TABLE hands.downstream_mcp_server
    DROP COLUMN IF EXISTS active_source_revision,
    DROP COLUMN IF EXISTS active_snapshot_digest,
    DROP COLUMN IF EXISTS reconcile_expires_at,
    DROP COLUMN IF EXISTS reconcile_fencing_token,
    DROP COLUMN IF EXISTS reconcile_owner,
    DROP COLUMN IF EXISTS config_revision;

COMMIT;
