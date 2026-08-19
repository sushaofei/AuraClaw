BEGIN;

ALTER TABLE `hands_downstream_mcp_server`
    MODIFY COLUMN protocol_revision VARCHAR(64) NOT NULL DEFAULT '2026-07-28';

COMMIT;
