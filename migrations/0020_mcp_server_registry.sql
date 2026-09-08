BEGIN;

ALTER TABLE hands.downstream_mcp_server
    DROP CONSTRAINT IF EXISTS downstream_mcp_server_endpoint_check;

ALTER TABLE hands.downstream_mcp_server
    ADD CONSTRAINT downstream_mcp_server_endpoint_check
    CHECK (endpoint ~ '^https?://');

CREATE TABLE IF NOT EXISTS hands.mcp_server (
    server_id text PRIMARY KEY,
    tenant_id text,
    desired_state text NOT NULL,
    latest_revision integer NOT NULL,
    active_revision integer,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (desired_state IN ('disabled', 'enabled', 'retired')),
    CHECK (latest_revision >= 0),
    CHECK (active_revision IS NULL OR active_revision >= 1)
);

CREATE TABLE IF NOT EXISTS hands.mcp_server_revision (
    server_id text NOT NULL REFERENCES hands.mcp_server (server_id),
    revision integer NOT NULL,
    config_json jsonb NOT NULL,
    config_digest text NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (server_id, revision),
    CHECK (revision >= 1)
);

CREATE TABLE IF NOT EXISTS hands.mcp_server_runtime (
    server_id text PRIMARY KEY REFERENCES hands.mcp_server (server_id),
    loaded_revision integer,
    observed_state text NOT NULL,
    last_test_at timestamptz,
    last_sync_at timestamptz,
    consecutive_failures integer NOT NULL DEFAULT 0,
    safe_error_code text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        observed_state IN (
            'pending',
            'loading',
            'active',
            'degraded',
            'quarantined',
            'disabled',
            'unavailable'
        )
    )
);

CREATE TABLE IF NOT EXISTS hands.mcp_server_operation (
    operation_id text PRIMARY KEY,
    server_id text NOT NULL,
    tenant_id text NOT NULL,
    target_revision integer,
    command_id text NOT NULL,
    actor_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    operation text NOT NULL,
    status text NOT NULL,
    safe_error_code text,
    result_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (tenant_id, command_id),
    CHECK (
        operation IN (
            'create',
            'update',
            'test',
            'enable',
            'disable',
            'reconcile',
            'retire'
        )
    ),
    CHECK (status IN ('accepted', 'running', 'succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS mcp_server_tenant_idx
    ON hands.mcp_server (tenant_id, desired_state);

CREATE INDEX IF NOT EXISTS mcp_server_operation_server_idx
    ON hands.mcp_server_operation (server_id, created_at DESC);

COMMIT;
