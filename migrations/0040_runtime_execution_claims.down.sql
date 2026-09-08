BEGIN;

DROP INDEX IF EXISTS control.assignment_execution_claim_expiry_idx;
DROP INDEX IF EXISTS control.runtime_registration_id_idx;
ALTER TABLE control.assignment
    DROP COLUMN IF EXISTS execution_claim_expires_at,
    DROP COLUMN IF EXISTS execution_claim_token;
ALTER TABLE control.runtime_instance DROP COLUMN IF EXISTS registration_id;

COMMIT;
