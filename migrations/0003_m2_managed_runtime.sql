BEGIN;

ALTER TABLE control.runtime_lease
    ADD COLUMN lease_id text;
UPDATE control.runtime_lease SET lease_id = 'legacy-' || resource_id WHERE lease_id IS NULL;
ALTER TABLE control.runtime_lease ALTER COLUMN lease_id SET NOT NULL;

ALTER TABLE control.runnable_item
    ADD COLUMN root_session_id text,
    ADD COLUMN run_id text,
    ADD COLUMN claimed_by text,
    ADD COLUMN role text NOT NULL DEFAULT 'root',
    ADD COLUMN deadline timestamptz,
    ADD COLUMN budget jsonb NOT NULL DEFAULT '{}'::jsonb;
UPDATE control.runnable_item
SET root_session_id = session_id, run_id = 'legacy-' || task_id
WHERE root_session_id IS NULL OR run_id IS NULL;
ALTER TABLE control.runnable_item
    ALTER COLUMN root_session_id SET NOT NULL,
    ALTER COLUMN run_id SET NOT NULL;

ALTER TABLE control.assignment
    ADD COLUMN tenant_id text,
    ADD COLUMN root_session_id text,
    ADD COLUMN session_id text,
    ADD COLUMN run_id text,
    ADD COLUMN lease_id text,
    ADD COLUMN role text NOT NULL DEFAULT 'root',
    ADD COLUMN resource_profile jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN completed_at timestamptz;
UPDATE control.assignment
SET tenant_id = 'legacy', root_session_id = task_id, session_id = task_id,
    run_id = 'legacy-' || task_id, lease_id = 'legacy-' || task_id
WHERE tenant_id IS NULL;
ALTER TABLE control.assignment
    ALTER COLUMN tenant_id SET NOT NULL,
    ALTER COLUMN root_session_id SET NOT NULL,
    ALTER COLUMN session_id SET NOT NULL,
    ALTER COLUMN run_id SET NOT NULL,
    ALTER COLUMN lease_id SET NOT NULL;

CREATE TABLE control.runtime_instance (
    runtime_id text PRIMARY KEY,
    runtime_type text NOT NULL,
    role text NOT NULL,
    node_id text NOT NULL,
    capabilities jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL,
    capacity integer NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.capacity_reservation (
    scope text PRIMARY KEY,
    reserved integer NOT NULL CHECK (reserved >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.runtime_checkpoint (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    fencing_token bigint NOT NULL,
    phase text NOT NULL,
    state jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, session_id, run_id)
);

CREATE TABLE control.runtime_cancellation (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, run_id)
);

CREATE INDEX runnable_claim_idx
    ON control.runnable_item (queue_partition, priority DESC, available_at, task_id)
    WHERE status = 'queued';
CREATE INDEX assignment_runtime_idx
    ON control.assignment (runtime_id, assignment_status);

COMMIT;
