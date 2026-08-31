DROP INDEX IF EXISTS policy.policy_approval_transition_audit_lookup_idx;
DROP TABLE IF EXISTS policy.approval_transition_audit;

ALTER TABLE policy.approval
    DROP COLUMN IF EXISTS decided_by,
    DROP COLUMN IF EXISTS decided_at,
    DROP COLUMN IF EXISTS generation,
    DROP COLUMN IF EXISTS request_digest;
