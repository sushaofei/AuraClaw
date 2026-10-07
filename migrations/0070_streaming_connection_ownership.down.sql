BEGIN;

DROP INDEX IF EXISTS streaming.streaming_connection_owner_idx;

ALTER TABLE streaming.connection_registry
    DROP COLUMN IF EXISTS owner_generation;

DROP INDEX IF EXISTS streaming.gateway_instance_expiry_idx;
DROP TABLE IF EXISTS streaming.gateway_instance;

COMMIT;
