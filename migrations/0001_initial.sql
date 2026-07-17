BEGIN;

CREATE SCHEMA IF NOT EXISTS session_core;
CREATE SCHEMA IF NOT EXISTS projection;
CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS delivery;

CREATE TABLE session_core.session_head (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    root_session_id text NOT NULL,
    aggregate_version bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE session_core.canonical_event (
    event_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    root_session_id text NOT NULL,
    session_id text NOT NULL,
    run_id text,
    aggregate_version bigint NOT NULL,
    event_type text NOT NULL,
    occurred_at timestamptz NOT NULL,
    actor jsonb NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    visibility text NOT NULL,
    schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    UNIQUE (tenant_id, session_id, aggregate_version)
);

CREATE INDEX canonical_event_root_idx
    ON session_core.canonical_event (tenant_id, root_session_id, occurred_at);

CREATE TABLE session_core.command_dedup (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    operation text NOT NULL,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, command_id)
);

CREATE TABLE session_core.outbox (
    outbox_id bigserial PRIMARY KEY,
    event_id text NOT NULL,
    destination text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    publish_attempt integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    UNIQUE (event_id, destination)
);

CREATE INDEX outbox_pending_idx
    ON session_core.outbox (next_attempt_at, outbox_id)
    WHERE published_at IS NULL;

CREATE TABLE session_core.snapshot (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    aggregate_version bigint NOT NULL,
    schema_version integer NOT NULL,
    state jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, aggregate_version)
);

CREATE TABLE projection.task_view (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    root_session_id text NOT NULL,
    run_id text,
    status text NOT NULL,
    goal text NOT NULL,
    progress numeric(5, 4) NOT NULL DEFAULT 0,
    current_stage text NOT NULL,
    result_summary text,
    result_ref jsonb,
    artifact_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    error jsonb,
    source_version bigint NOT NULL,
    source_event_id text NOT NULL,
    projected_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX task_view_root_idx
    ON projection.task_view (tenant_id, root_session_id, status);

CREATE TABLE projection.projector_checkpoint (
    projector_id text NOT NULL,
    partition_id text NOT NULL,
    checkpoint jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projector_id, partition_id)
);

CREATE TABLE projection.processed_event (
    projector_id text NOT NULL,
    event_id text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projector_id, event_id)
);

CREATE TABLE control.runtime_lease (
    resource_id text PRIMARY KEY,
    lease_owner text NOT NULL,
    expires_at timestamptz NOT NULL,
    fencing_token bigint NOT NULL,
    lease_version bigint NOT NULL
);

CREATE TABLE control.runnable_item (
    task_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    source_version bigint NOT NULL,
    priority integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    required_capability jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempt integer NOT NULL DEFAULT 0,
    queue_partition text NOT NULL,
    status text NOT NULL,
    UNIQUE (tenant_id, session_id, source_version)
);

CREATE TABLE control.assignment (
    task_id text PRIMARY KEY,
    runtime_id text NOT NULL,
    assignment_status text NOT NULL,
    assigned_at timestamptz NOT NULL,
    started_at timestamptz,
    deadline timestamptz,
    fencing_token bigint NOT NULL
);

CREATE TABLE delivery.delivery_job (
    delivery_id text PRIMARY KEY,
    event_id text NOT NULL,
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    run_id text,
    sink_type text NOT NULL,
    sink_target_ref text NOT NULL,
    payload_ref jsonb NOT NULL,
    status text NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_response_summary text,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (event_id, sink_target_ref)
);

COMMIT;
