BEGIN;

ALTER TABLE projection.approval_view
    ADD COLUMN required_approvals integer NOT NULL DEFAULT 1,
    ADD COLUMN votes jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN escalation_at timestamptz,
    ADD COLUMN escalation_level integer NOT NULL DEFAULT 0;

ALTER TABLE projection.approval_view
    ADD CONSTRAINT approval_required_approvals_positive
    CHECK (required_approvals > 0),
    ADD CONSTRAINT approval_escalation_level_nonnegative
    CHECK (escalation_level >= 0);

CREATE INDEX approval_expiry_due_idx
    ON projection.approval_view (expires_at, tenant_id, approval_id)
    WHERE status IN ('requested', 'waiting');

CREATE INDEX approval_escalation_due_idx
    ON projection.approval_view (escalation_at, tenant_id, approval_id)
    WHERE status IN ('requested', 'waiting') AND escalation_at IS NOT NULL;

COMMIT;
