BEGIN;

CREATE TABLE IF NOT EXISTS delivery.sink_circuit_state (
    tenant_id text NOT NULL,
    sink_id text NOT NULL,
    state text NOT NULL DEFAULT 'closed',
    failure_count integer NOT NULL DEFAULT 0,
    open_until timestamptz,
    generation bigint NOT NULL DEFAULT 0,
    probe_owner text,
    probe_token text,
    probe_expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,sink_id),
    FOREIGN KEY (tenant_id,sink_id)
        REFERENCES delivery.sink_config (tenant_id,sink_id) ON DELETE CASCADE,
    CHECK (state IN ('closed','open','half_open')),
    CHECK (failure_count >= 0),
    CHECK (generation >= 0)
);

CREATE INDEX IF NOT EXISTS sink_circuit_open_idx
    ON delivery.sink_circuit_state (state,open_until,probe_expires_at);

COMMIT;
