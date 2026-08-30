ALTER TABLE `control_runtime_instance`
    ADD COLUMN registration_id VARCHAR(191);
UPDATE `control_runtime_instance`
SET registration_id = CONCAT('legacy-', runtime_id)
WHERE registration_id IS NULL;
ALTER TABLE `control_runtime_instance`
    MODIFY COLUMN registration_id VARCHAR(191) NOT NULL,
    ADD UNIQUE INDEX runtime_registration_id_idx (registration_id);

ALTER TABLE `control_assignment`
    ADD COLUMN execution_claim_token VARCHAR(191),
    ADD COLUMN execution_claim_expires_at DATETIME(6),
    ADD INDEX assignment_execution_claim_expiry_idx
        (assignment_status, execution_claim_expires_at);
