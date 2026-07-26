BEGIN;

ALTER TABLE `control_runtime_lease`
    ADD COLUMN lease_id VARCHAR(191);
UPDATE `control_runtime_lease`
SET lease_id = CONCAT('legacy-', resource_id)
WHERE lease_id IS NULL;
ALTER TABLE `control_runtime_lease`
    MODIFY COLUMN lease_id VARCHAR(191) NOT NULL;

ALTER TABLE `control_runnable_item`
    ADD COLUMN root_session_id VARCHAR(191),
    ADD COLUMN run_id VARCHAR(191),
    ADD COLUMN claimed_by VARCHAR(191),
    ADD COLUMN role VARCHAR(191) NOT NULL DEFAULT 'root',
    ADD COLUMN deadline datetime(6),
    ADD COLUMN budget json NOT NULL DEFAULT (CAST('{}' AS JSON));
UPDATE `control_runnable_item`
SET root_session_id = session_id,
    run_id = CONCAT('legacy-', task_id)
WHERE root_session_id IS NULL OR run_id IS NULL;
ALTER TABLE `control_runnable_item`
    MODIFY COLUMN root_session_id VARCHAR(191) NOT NULL,
    MODIFY COLUMN run_id VARCHAR(191) NOT NULL;

ALTER TABLE `control_assignment`
    ADD COLUMN tenant_id VARCHAR(191),
    ADD COLUMN root_session_id VARCHAR(191),
    ADD COLUMN session_id VARCHAR(191),
    ADD COLUMN run_id VARCHAR(191),
    ADD COLUMN lease_id VARCHAR(191),
    ADD COLUMN role VARCHAR(191) NOT NULL DEFAULT 'root',
    ADD COLUMN resource_profile json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    ADD COLUMN completed_at datetime(6);
UPDATE `control_assignment`
SET tenant_id = 'legacy',
    root_session_id = task_id,
    session_id = task_id,
    run_id = CONCAT('legacy-', task_id),
    lease_id = CONCAT('legacy-', task_id)
WHERE tenant_id IS NULL;
ALTER TABLE `control_assignment`
    MODIFY COLUMN tenant_id VARCHAR(191) NOT NULL,
    MODIFY COLUMN root_session_id VARCHAR(191) NOT NULL,
    MODIFY COLUMN session_id VARCHAR(191) NOT NULL,
    MODIFY COLUMN run_id VARCHAR(191) NOT NULL,
    MODIFY COLUMN lease_id VARCHAR(191) NOT NULL;

CREATE TABLE `control_runtime_instance` (
    runtime_id VARCHAR(191) PRIMARY KEY,
    runtime_type VARCHAR(191) NOT NULL,
    role VARCHAR(191) NOT NULL,
    node_id VARCHAR(191) NOT NULL,
    capabilities json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    status VARCHAR(191) NOT NULL,
    capacity integer NOT NULL,
    started_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_heartbeat_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE TABLE `control_capacity_reservation` (
    scope VARCHAR(191) PRIMARY KEY,
    reserved integer NOT NULL,
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CHECK (reserved >= 0)
);

CREATE TABLE `control_runtime_checkpoint` (
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    run_id VARCHAR(191) NOT NULL,
    fencing_token bigint NOT NULL,
    phase VARCHAR(191) NOT NULL,
    state json NOT NULL,
    updated_at datetime(6) NOT NULL,
    PRIMARY KEY (tenant_id, session_id, run_id)
);

CREATE TABLE `control_runtime_cancellation` (
    tenant_id VARCHAR(191) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    run_id VARCHAR(191) NOT NULL,
    requested_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, session_id, run_id)
);

CREATE INDEX runnable_claim_idx
    ON `control_runnable_item` (queue_partition, priority DESC, available_at, task_id);
CREATE INDEX assignment_runtime_idx
    ON `control_assignment` (runtime_id, assignment_status);

COMMIT;
