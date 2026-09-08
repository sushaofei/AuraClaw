BEGIN;
ALTER TABLE hands.mcp_server_runtime
    ADD COLUMN IF NOT EXISTS applied_generation bigint CHECK (applied_generation >= 1);
COMMENT ON COLUMN hands.mcp_server_runtime.applied_generation IS
    'Committed catalog generation installed in this instance; NULL means unproven local readiness';
COMMIT;
