BEGIN;

CREATE TABLE streaming.gateway_instance (
    owner_id text PRIMARY KEY,
    generation text NOT NULL,
    state text NOT NULL DEFAULT 'active',
    started_at timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    drain_started_at timestamptz,
    drain_deadline timestamptz,
    expires_at timestamptz NOT NULL,
    CONSTRAINT gateway_instance_state_check
        CHECK (state IN ('active', 'draining'))
);

CREATE INDEX gateway_instance_expiry_idx
    ON streaming.gateway_instance (expires_at, owner_id);

ALTER TABLE streaming.connection_registry
    ADD COLUMN owner_generation text NOT NULL DEFAULT 'legacy';

CREATE INDEX streaming_connection_owner_idx
    ON streaming.connection_registry (owner_id, owner_generation, expires_at);

COMMIT;
