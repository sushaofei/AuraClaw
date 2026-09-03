BEGIN;
ALTER TABLE hands.mcp_server_runtime DROP COLUMN IF EXISTS applied_generation;
COMMIT;
