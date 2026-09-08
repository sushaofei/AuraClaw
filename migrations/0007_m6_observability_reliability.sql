CREATE SCHEMA IF NOT EXISTS observability;

CREATE TABLE observability.trace_span (
    trace_id text NOT NULL,
    span_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    root_session_id text,
    session_id text,
    run_id text,
    event_id text,
    command_id text,
    tool_invocation_id text,
    runtime_id text,
    delivery_id text,
    approval_id text,
    component text NOT NULL,
    operation text NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    status text NOT NULL,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX trace_span_session_time_idx
    ON observability.trace_span (tenant_id, session_id, started_at);
CREATE INDEX trace_span_root_time_idx
    ON observability.trace_span (tenant_id, root_session_id, started_at);
CREATE INDEX trace_span_run_idx ON observability.trace_span (tenant_id, run_id);
CREATE INDEX trace_span_tool_idx ON observability.trace_span (tenant_id, tool_invocation_id);
CREATE INDEX trace_span_delivery_idx ON observability.trace_span (tenant_id, delivery_id);

CREATE TABLE observability.metric_point (
    metric_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    metric_name text NOT NULL,
    value double precision NOT NULL,
    observed_at timestamptz NOT NULL,
    tenant_id text,
    root_session_id text,
    session_id text,
    run_id text,
    labels jsonb NOT NULL DEFAULT '{}'::jsonb,
    deduplication_key text UNIQUE
);
CREATE INDEX metric_point_name_time_idx
    ON observability.metric_point (metric_name, observed_at DESC);
CREATE INDEX metric_point_session_time_idx
    ON observability.metric_point (tenant_id, session_id, observed_at DESC);

CREATE TABLE observability.audit_event (
    audit_id text PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    action text NOT NULL,
    outcome text NOT NULL,
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    tenant_id text NOT NULL,
    trace_id text NOT NULL,
    root_session_id text,
    session_id text,
    run_id text,
    event_id text,
    command_id text,
    tool_invocation_id text,
    delivery_id text,
    approval_id text,
    resource_ref text,
    payload_ref text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX audit_event_session_time_idx
    ON observability.audit_event (tenant_id, session_id, occurred_at);
CREATE INDEX audit_event_root_time_idx
    ON observability.audit_event (tenant_id, root_session_id, occurred_at);
CREATE INDEX audit_event_action_time_idx
    ON observability.audit_event (tenant_id, action, occurred_at DESC);

CREATE TABLE observability.alert (
    alert_id text PRIMARY KEY,
    rule text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL,
    summary text NOT NULL,
    fired_at timestamptz NOT NULL,
    tenant_id text,
    root_session_id text,
    session_id text,
    run_id text,
    labels jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX alert_status_time_idx ON observability.alert (status, fired_at DESC);
CREATE INDEX alert_session_idx ON observability.alert (tenant_id, session_id, status);

CREATE TABLE observability.retention_policy (
    data_class text PRIMARY KEY,
    retention_days integer NOT NULL CHECK (retention_days > 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO observability.retention_policy (data_class, retention_days) VALUES
    ('metric', 30), ('trace', 14), ('audit', 365), ('alert', 90)
ON CONFLICT (data_class) DO NOTHING;
