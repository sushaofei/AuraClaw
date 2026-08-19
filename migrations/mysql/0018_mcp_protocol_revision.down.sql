BEGIN;

ALTER TABLE `hands_downstream_mcp_server`
    MODIFY COLUMN protocol_revision VARCHAR(64) NOT NULL DEFAULT '2025-11-25';

COMMIT;
