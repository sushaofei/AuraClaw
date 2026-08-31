BEGIN;

ALTER TABLE hands.downstream_mcp_server
    ADD COLUMN IF NOT EXISTS config_revision bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reconcile_owner text,
    ADD COLUMN IF NOT EXISTS reconcile_fencing_token bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reconcile_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS active_snapshot_digest text,
    ADD COLUMN IF NOT EXISTS active_source_revision text;

CREATE INDEX IF NOT EXISTS downstream_mcp_reconcile_expiry_idx
    ON hands.downstream_mcp_server (reconcile_expires_at)
    WHERE reconcile_owner IS NOT NULL;

COMMIT;
