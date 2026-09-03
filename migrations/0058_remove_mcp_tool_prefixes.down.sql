BEGIN;

ALTER TABLE hands.downstream_mcp_server
    ADD COLUMN IF NOT EXISTS allowed_tool_prefixes jsonb NOT NULL DEFAULT '[]'::jsonb;

-- Only the active revision describes the configuration being rolled back.
UPDATE hands.downstream_mcp_server AS s
SET allowed_tool_prefixes = r.config_json->'allowed_tool_prefixes'
FROM hands.mcp_server AS registered
JOIN hands.mcp_server_revision AS r
    ON r.server_id = registered.server_id AND r.revision = registered.active_revision
WHERE s.server_id = registered.server_id
  AND jsonb_typeof(r.config_json->'allowed_tool_prefixes') = 'array';

-- Servers created/updated after removal (and legacy unregistered projections)
-- have no recoverable allowlist. Keep them disabled until an administrator
-- restores the backed-up configuration; an empty default is not a recovery.
UPDATE hands.downstream_mcp_server AS s
SET enabled = false, status = 'quarantined'
WHERE NOT EXISTS (
    SELECT 1 FROM hands.mcp_server AS registered
    JOIN hands.mcp_server_revision AS r
        ON r.server_id = registered.server_id AND r.revision = registered.active_revision
    WHERE registered.server_id = s.server_id
      AND jsonb_typeof(r.config_json->'allowed_tool_prefixes') = 'array'
);

UPDATE hands.mcp_server AS registered
SET desired_state = 'disabled', updated_at = now()
WHERE registered.desired_state = 'enabled'
  AND NOT EXISTS (
      SELECT 1 FROM hands.mcp_server_revision AS r
      WHERE r.server_id = registered.server_id AND r.revision = registered.active_revision
        AND jsonb_typeof(r.config_json->'allowed_tool_prefixes') = 'array'
  );

COMMIT;
