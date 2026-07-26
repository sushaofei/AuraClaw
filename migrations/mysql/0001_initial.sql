BEGIN;

CREATE TABLE `session_core_session_head` (
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    root_session_id VARCHAR(191) NOT NULL,
    aggregate_version bigint NOT NULL DEFAULT 0,
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE TABLE `session_core_canonical_event` (
    event_id VARCHAR(191) PRIMARY KEY,
    tenant_id VARCHAR(191) NOT NULL,
    root_session_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    run_id VARCHAR(191),
    aggregate_version bigint NOT NULL,
    event_type VARCHAR(191) NOT NULL,
    occurred_at datetime(6) NOT NULL,
    actor json NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    visibility VARCHAR(191) NOT NULL,
    schema_version integer NOT NULL,
    payload json NOT NULL,
    UNIQUE (tenant_id, session_id, aggregate_version)
);

CREATE INDEX canonical_event_root_idx
    ON `session_core_canonical_event` (tenant_id, root_session_id, occurred_at);

CREATE TABLE `session_core_command_dedup` (
    tenant_id VARCHAR(191) NOT NULL,
    command_id VARCHAR(191) NOT NULL,
    operation VARCHAR(191) NOT NULL,
    response json NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, command_id)
);

CREATE TABLE `session_core_outbox` (
    outbox_id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(191) NOT NULL,
    destination VARCHAR(191) NOT NULL,
    payload json NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    publish_attempt integer NOT NULL DEFAULT 0,
    next_attempt_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at datetime(6),
    PRIMARY KEY (outbox_id),
    UNIQUE (event_id, destination)
);

CREATE INDEX outbox_pending_idx
    ON `session_core_outbox` (next_attempt_at, outbox_id);

CREATE TABLE `session_core_snapshot` (
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    aggregate_version bigint NOT NULL,
    schema_version integer NOT NULL,
    state json NOT NULL,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_id, aggregate_version)
);

CREATE TABLE `projection_task_view` (
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    root_session_id VARCHAR(191) NOT NULL,
    run_id VARCHAR(191),
    status VARCHAR(191) NOT NULL,
    goal VARCHAR(191) NOT NULL,
    progress DECIMAL(5,4) NOT NULL DEFAULT 0,
    current_stage VARCHAR(191) NOT NULL,
    result_summary VARCHAR(191),
    result_ref json,
    artifact_refs json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    error json,
    source_version bigint NOT NULL,
    source_event_id VARCHAR(191) NOT NULL,
    projected_at datetime(6) NOT NULL,
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX task_view_root_idx
    ON `projection_task_view` (tenant_id, root_session_id, status);

CREATE TABLE `projection_projector_checkpoint` (
    projector_id VARCHAR(191) NOT NULL,
    partition_id VARCHAR(191) NOT NULL,
    checkpoint json NOT NULL,
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (projector_id, partition_id)
);

CREATE TABLE `projection_processed_event` (
    projector_id VARCHAR(191) NOT NULL,
    event_id VARCHAR(191) NOT NULL,
    processed_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (projector_id, event_id)
);

CREATE TABLE `control_runtime_lease` (
    resource_id VARCHAR(191) PRIMARY KEY,
    lease_owner VARCHAR(191) NOT NULL,
    expires_at datetime(6) NOT NULL,
    fencing_token bigint NOT NULL,
    lease_version bigint NOT NULL
);

CREATE TABLE `control_runnable_item` (
    task_id VARCHAR(191) PRIMARY KEY,
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    source_version bigint NOT NULL,
    priority integer NOT NULL DEFAULT 0,
    available_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    required_capability json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    attempt integer NOT NULL DEFAULT 0,
    queue_partition VARCHAR(191) NOT NULL,
    status VARCHAR(191) NOT NULL,
    UNIQUE (tenant_id, session_id, source_version)
);

CREATE TABLE `control_assignment` (
    task_id VARCHAR(191) PRIMARY KEY,
    runtime_id VARCHAR(191) NOT NULL,
    assignment_status VARCHAR(191) NOT NULL,
    assigned_at datetime(6) NOT NULL,
    started_at datetime(6),
    deadline datetime(6),
    fencing_token bigint NOT NULL
);

CREATE TABLE `delivery_delivery_job` (
    delivery_id VARCHAR(191) PRIMARY KEY,
    event_id VARCHAR(191) NOT NULL,
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    run_id VARCHAR(191),
    sink_type VARCHAR(191) NOT NULL,
    sink_target_ref VARCHAR(191) NOT NULL,
    payload_ref json NOT NULL,
    status VARCHAR(191) NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0,
    next_attempt_at datetime(6),
    last_response_summary text,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at datetime(6),
    UNIQUE (event_id, sink_target_ref)
);

COMMIT;
