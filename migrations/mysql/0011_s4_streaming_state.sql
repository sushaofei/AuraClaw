BEGIN;

CREATE TABLE `streaming_session_sequence` (
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    last_sequence bigint NOT NULL DEFAULT 0,
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE `streaming_runtime_event` (
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    sequence bigint NOT NULL,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    root_session_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    occurred_at datetime(6) NOT NULL,
    payload json NOT NULL,
    durable TINYINT(1) NOT NULL DEFAULT 0,
    visibility VARCHAR(64) NOT NULL,
    stored_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_id, sequence)
);

CREATE INDEX streaming_runtime_event_retention_idx
    ON `streaming_runtime_event` (tenant_id, session_id, sequence DESC);

CREATE TABLE `streaming_connection_registry` (
    connection_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    owner_id VARCHAR(64) NOT NULL,
    cursor_sequence bigint NOT NULL DEFAULT 0,
    connected_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    heartbeat_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at datetime(6) NOT NULL
);

CREATE INDEX streaming_connection_expiry_idx
    ON `streaming_connection_registry` (expires_at);
CREATE INDEX streaming_connection_session_idx
    ON `streaming_connection_registry` (tenant_id, session_id, expires_at);

COMMIT;
