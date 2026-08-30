ALTER TABLE `control_assignment`
    DROP INDEX assignment_execution_claim_expiry_idx,
    DROP COLUMN execution_claim_expires_at,
    DROP COLUMN execution_claim_token;
ALTER TABLE `control_runtime_instance`
    DROP INDEX runtime_registration_id_idx,
    DROP COLUMN registration_id;
