BEGIN;

ALTER TABLE projection.task_view
    ADD COLUMN lineage jsonb;

CREATE TABLE projection.collaboration_view (
    tenant_id text NOT NULL,
    root_session_id text NOT NULL,
    session_id text NOT NULL,
    run_id text,
    parent_session_id text,
    role text NOT NULL,
    task_key text NOT NULL,
    goal text NOT NULL,
    dependency_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    owner text,
    status text NOT NULL,
    runnable boolean NOT NULL DEFAULT false,
    output_contract jsonb NOT NULL DEFAULT '{}'::jsonb,
    budget double precision NOT NULL DEFAULT 0,
    result_ref text,
    artifact_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    target_session_id text,
    review_decision text,
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    source_version bigint NOT NULL,
    source_event_id text NOT NULL,
    projected_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX collaboration_root_idx
    ON projection.collaboration_view (tenant_id, root_session_id, parent_session_id);
CREATE INDEX collaboration_runnable_idx
    ON projection.collaboration_view (tenant_id, runnable, root_session_id)
    WHERE runnable = true;
CREATE UNIQUE INDEX collaboration_task_key_idx
    ON projection.collaboration_view (tenant_id, root_session_id, task_key);

COMMIT;
