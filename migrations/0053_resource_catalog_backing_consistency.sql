BEGIN;

-- This in-process provider was removed from the application before its catalog
-- owner row was retired. Deleting the owner atomically removes its capabilities.
DELETE FROM hands.downstream_mcp_server
WHERE server_id = 'auraclaw-price-insight';

-- Only the active generation is authoritative. Rows from interrupted legacy
-- publications must never become discoverable after an upgrade.
DELETE FROM hands.capability_catalog AS capability
USING hands.downstream_mcp_server AS server
WHERE server.server_id = capability.server_id
  AND capability.catalog_generation <> server.active_catalog_generation;

COMMIT;
