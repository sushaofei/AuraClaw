BEGIN;

CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE streaming.session_sequence (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    last_sequence bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE streaming.runtime_event (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    sequence bigint NOT NULL,
    event_id text NOT NULL UNIQUE,
    root_session_id text NOT NULL,
    run_id text NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    durable boolean NOT NULL DEFAULT false,
    visibility text NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, sequence)
);

CREATE INDEX streaming_runtime_event_retention_idx
    ON streaming.runtime_event (tenant_id, session_id, sequence DESC);

CREATE TABLE streaming.connection_registry (
    connection_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    owner_id text NOT NULL,
    cursor_sequence bigint NOT NULL DEFAULT 0,
    connected_at timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX streaming_connection_expiry_idx
    ON streaming.connection_registry (expires_at);
CREATE INDEX streaming_connection_session_idx
    ON streaming.connection_registry (tenant_id, session_id, expires_at);

REVOKE CREATE ON SCHEMA streaming FROM PUBLIC;

COMMIT;
