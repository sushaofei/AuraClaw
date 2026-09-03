BEGIN;

-- Tool names are not an authorization boundary. Keep immutable revisions and
-- their original digests; the registry reader discards the retired setting.
ALTER TABLE hands.downstream_mcp_server DROP COLUMN IF EXISTS allowed_tool_prefixes;

COMMIT;
