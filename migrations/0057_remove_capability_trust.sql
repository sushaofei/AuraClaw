BEGIN;

-- Managed MCP annotations are authoritative; trust tiers are no longer used.
ALTER TABLE hands.downstream_mcp_server DROP COLUMN IF EXISTS trust_level;
ALTER TABLE hands.capability_catalog DROP COLUMN IF EXISTS trust_level;

-- This is a disposable server projection. Immutable configuration revisions
-- and their digests are preserved and decoded by the compatibility reader.
UPDATE hands.downstream_mcp_server
SET metadata = metadata - 'tool_policy_overrides'
WHERE metadata ? 'tool_policy_overrides';

COMMIT;
