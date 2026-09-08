BEGIN;

DROP INDEX IF EXISTS hands.skill_publication_revocation_action_idx;

ALTER TABLE hands.skill_publication
    DROP CONSTRAINT IF EXISTS skill_publication_revocation_policy_check,
    DROP COLUMN IF EXISTS revocation_policy_decision_id,
    DROP COLUMN IF EXISTS revocation_policy_version,
    DROP COLUMN IF EXISTS revocation_action;

COMMIT;
