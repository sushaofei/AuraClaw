BEGIN;

ALTER TABLE `control_runnable_item`
    ADD COLUMN claim_token VARCHAR(64),
    ADD COLUMN claim_expires_at datetime(6);

CREATE INDEX runnable_recoverable_claim_idx
    ON `control_runnable_item` (claim_expires_at, task_id);

ALTER TABLE `delivery_delivery_job`
    ADD COLUMN claimed_by VARCHAR(64),
    ADD COLUMN claim_token VARCHAR(64),
    ADD COLUMN claim_expires_at datetime(6);

CREATE INDEX delivery_recoverable_claim_idx
    ON `delivery_delivery_job` (claim_expires_at, delivery_id);

COMMIT;
