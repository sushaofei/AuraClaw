BEGIN;

ALTER TABLE hands.downstream_mcp_server
    ADD COLUMN IF NOT EXISTS trust_level text NOT NULL DEFAULT 'external_untrusted';
ALTER TABLE hands.capability_catalog
    ADD COLUMN IF NOT EXISTS trust_level text NOT NULL DEFAULT 'external_untrusted'
        CHECK (trust_level IN ('platform','tenant_verified','external_untrusted'));

-- Restore the last active administrator configuration where it still exists.
UPDATE hands.downstream_mcp_server AS s
SET trust_level = COALESCE(r.config_json->>'trust_level', 'external_untrusted'),
    metadata = CASE WHEN r.config_json->'metadata' ? 'tool_policy_overrides'
        THEN s.metadata || jsonb_build_object(
            'tool_policy_overrides', r.config_json->'metadata'->'tool_policy_overrides')
        ELSE s.metadata END
FROM hands.mcp_server AS registered
JOIN hands.mcp_server_revision AS r
    ON r.server_id = registered.server_id AND r.revision = registered.active_revision
WHERE s.server_id = registered.server_id;

UPDATE hands.capability_catalog AS c
SET trust_level = s.trust_level
FROM hands.downstream_mcp_server AS s WHERE s.server_id = c.server_id;

COMMIT;
