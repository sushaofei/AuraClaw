BEGIN;

ALTER TABLE control.runtime_instance
    ADD COLUMN registration_id text;
UPDATE control.runtime_instance
SET registration_id = 'legacy-' || runtime_id
WHERE registration_id IS NULL;
ALTER TABLE control.runtime_instance
    ALTER COLUMN registration_id SET NOT NULL;

ALTER TABLE control.assignment
    ADD COLUMN execution_claim_token text,
    ADD COLUMN execution_claim_expires_at timestamptz;

CREATE UNIQUE INDEX runtime_registration_id_idx
    ON control.runtime_instance (registration_id);
CREATE INDEX assignment_execution_claim_expiry_idx
    ON control.assignment (execution_claim_expires_at)
    WHERE assignment_status = 'running';

COMMIT;
