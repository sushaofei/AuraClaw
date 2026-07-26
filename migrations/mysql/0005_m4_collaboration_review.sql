BEGIN;

ALTER TABLE `projection_task_view`
    ADD COLUMN lineage json;

CREATE TABLE `projection_collaboration_view` (
    tenant_id VARCHAR(64) NOT NULL,
    root_session_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64),
    parent_session_id VARCHAR(64),
    role VARCHAR(64) NOT NULL,
    task_key VARCHAR(64) NOT NULL,
    goal VARCHAR(64) NOT NULL,
    dependency_ids json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    owner VARCHAR(64),
    status VARCHAR(64) NOT NULL,
    runnable TINYINT(1) NOT NULL DEFAULT 0,
    output_contract json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    budget DOUBLE NOT NULL DEFAULT 0,
    result_ref VARCHAR(64),
    artifact_refs json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    target_session_id VARCHAR(64),
    review_decision VARCHAR(64),
    evidence_refs json NOT NULL DEFAULT (CAST('[]' AS JSON)),
    source_version bigint NOT NULL,
    source_event_id VARCHAR(64) NOT NULL,
    projected_at datetime(6) NOT NULL,
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX collaboration_root_idx
    ON `projection_collaboration_view` (tenant_id, root_session_id, parent_session_id);
CREATE INDEX collaboration_runnable_idx
    ON `projection_collaboration_view` (tenant_id, runnable, root_session_id);
CREATE UNIQUE INDEX collaboration_task_key_idx
    ON `projection_collaboration_view` (tenant_id, root_session_id, task_key);

COMMIT;
