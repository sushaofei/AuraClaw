BEGIN;

DROP INDEX IF EXISTS projection.approval_escalation_due_idx;
DROP INDEX IF EXISTS projection.approval_expiry_due_idx;

ALTER TABLE projection.approval_view
    DROP CONSTRAINT IF EXISTS approval_escalation_level_nonnegative,
    DROP CONSTRAINT IF EXISTS approval_required_approvals_positive,
    DROP COLUMN IF EXISTS escalation_level,
    DROP COLUMN IF EXISTS escalation_at,
    DROP COLUMN IF EXISTS votes,
    DROP COLUMN IF EXISTS required_approvals;

COMMIT;
