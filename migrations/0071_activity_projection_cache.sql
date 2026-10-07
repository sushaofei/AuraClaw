BEGIN;

CREATE TABLE projection.activity_state (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    source_version bigint NOT NULL,
    source_event_id text NOT NULL,
    complete boolean NOT NULL DEFAULT false,
    projected_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE projection.activity_node (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    node_id text NOT NULL,
    sequence bigint NOT NULL,
    updated_version bigint NOT NULL,
    node jsonb NOT NULL,
    projected_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id, node_id)
);

CREATE INDEX activity_node_incremental_page_idx
    ON projection.activity_node (
        tenant_id, session_id, updated_version, sequence, node_id
    );

COMMIT;
