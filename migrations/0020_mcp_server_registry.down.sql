BEGIN;

DROP TABLE IF EXISTS hands.mcp_server_operation;
DROP TABLE IF EXISTS hands.mcp_server_runtime;
DROP TABLE IF EXISTS hands.mcp_server_revision;
DROP TABLE IF EXISTS hands.mcp_server;

ALTER TABLE hands.downstream_mcp_server
    DROP CONSTRAINT IF EXISTS downstream_mcp_server_endpoint_check;

ALTER TABLE hands.downstream_mcp_server
    ADD CONSTRAINT downstream_mcp_server_endpoint_check
    CHECK (endpoint ~ '^https://');

COMMIT;
