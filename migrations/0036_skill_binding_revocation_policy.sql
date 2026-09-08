BEGIN;

ALTER TABLE hands.skill_publication
    ADD COLUMN IF NOT EXISTS revocation_action text,
    ADD COLUMN IF NOT EXISTS revocation_policy_version text,
    ADD COLUMN IF NOT EXISTS revocation_policy_decision_id text;

UPDATE hands.skill_publication
SET revocation_action = 'cancel',
    revocation_policy_version = 'skill-revocation-v1'
WHERE status = 'revoked'
  AND revocation_action IS NULL;

ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_revocation_policy_check;

ALTER TABLE hands.skill_publication
    ADD CONSTRAINT skill_publication_revocation_policy_check CHECK (
        (
            status = 'revoked'
            AND revocation_action IN ('continue', 'pause', 'cancel')
            AND NULLIF(revocation_policy_version, '') IS NOT NULL
        )
        OR (
            status <> 'revoked'
            AND revocation_action IS NULL
            AND revocation_policy_version IS NULL
            AND revocation_policy_decision_id IS NULL
        )
    );

CREATE INDEX IF NOT EXISTS skill_publication_revocation_action_idx
    ON hands.skill_publication (tenant_id, revocation_action, publisher, name, version)
    WHERE status = 'revoked';

COMMIT;
