CREATE TABLE `observability_trace_span` (
    trace_id VARCHAR(64) NOT NULL,
    span_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    root_session_id VARCHAR(64),
    session_id VARCHAR(64),
    run_id VARCHAR(64),
    event_id VARCHAR(64),
    command_id VARCHAR(64),
    tool_invocation_id VARCHAR(64),
    runtime_id VARCHAR(64),
    delivery_id VARCHAR(64),
    approval_id VARCHAR(64),
    component VARCHAR(64) NOT NULL,
    operation VARCHAR(64) NOT NULL,
    started_at datetime(6) NOT NULL,
    ended_at datetime(6) NOT NULL,
    status VARCHAR(64) NOT NULL,
    attributes json NOT NULL DEFAULT (CAST('{}' AS JSON))
);
CREATE INDEX trace_span_session_time_idx
    ON `observability_trace_span` (tenant_id, session_id, started_at);
CREATE INDEX trace_span_root_time_idx
    ON `observability_trace_span` (tenant_id, root_session_id, started_at);
CREATE INDEX trace_span_run_idx ON `observability_trace_span` (tenant_id, run_id);
CREATE INDEX trace_span_tool_idx ON `observability_trace_span` (tenant_id, tool_invocation_id);
CREATE INDEX trace_span_delivery_idx ON `observability_trace_span` (tenant_id, delivery_id);

CREATE TABLE `observability_metric_point` (
    metric_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(64) NOT NULL,
    value DOUBLE NOT NULL,
    observed_at datetime(6) NOT NULL,
    tenant_id VARCHAR(64),
    root_session_id VARCHAR(64),
    session_id VARCHAR(64),
    run_id VARCHAR(64),
    labels json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    deduplication_key VARCHAR(64) UNIQUE
);
CREATE INDEX metric_point_name_time_idx
    ON `observability_metric_point` (metric_name, observed_at DESC);
CREATE INDEX metric_point_session_time_idx
    ON `observability_metric_point` (tenant_id, session_id, observed_at DESC);

CREATE TABLE `observability_audit_event` (
    audit_id VARCHAR(64) PRIMARY KEY,
    occurred_at datetime(6) NOT NULL,
    action VARCHAR(64) NOT NULL,
    outcome VARCHAR(64) NOT NULL,
    actor_type VARCHAR(64) NOT NULL,
    actor_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    trace_id VARCHAR(64) NOT NULL,
    root_session_id VARCHAR(64),
    session_id VARCHAR(64),
    run_id VARCHAR(64),
    event_id VARCHAR(64),
    command_id VARCHAR(64),
    tool_invocation_id VARCHAR(64),
    delivery_id VARCHAR(64),
    approval_id VARCHAR(64),
    resource_ref text,
    payload_ref text,
    metadata json NOT NULL DEFAULT (CAST('{}' AS JSON))
);
CREATE INDEX audit_event_session_time_idx
    ON `observability_audit_event` (tenant_id, session_id, occurred_at);
CREATE INDEX audit_event_root_time_idx
    ON `observability_audit_event` (tenant_id, root_session_id, occurred_at);
CREATE INDEX audit_event_action_time_idx
    ON `observability_audit_event` (tenant_id, action, occurred_at DESC);

CREATE TABLE `observability_alert` (
    alert_id VARCHAR(64) PRIMARY KEY,
    rule VARCHAR(64) NOT NULL,
    severity VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    summary text NOT NULL,
    fired_at datetime(6) NOT NULL,
    tenant_id VARCHAR(64),
    root_session_id VARCHAR(64),
    session_id VARCHAR(64),
    run_id VARCHAR(64),
    labels json NOT NULL DEFAULT (CAST('{}' AS JSON))
);
CREATE INDEX alert_status_time_idx ON `observability_alert` (status, fired_at DESC);
CREATE INDEX alert_session_idx ON `observability_alert` (tenant_id, session_id, status);

CREATE TABLE `observability_retention_policy` (
    data_class VARCHAR(64) PRIMARY KEY,
    retention_days integer NOT NULL CHECK (retention_days > 0),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
INSERT IGNORE INTO `observability_retention_policy` (data_class, retention_days) VALUES
    ('metric', 30), ('trace', 14), ('audit', 365), ('alert', 90);
