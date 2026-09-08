-- Repair assignments stuck in running/assigned with a superseded lease fencing token.
-- Safe while stack is up: only expires rows whose token no longer matches an active lease.

BEGIN;

WITH stale AS (
  SELECT assignment.task_id
  FROM control.assignment assignment
  WHERE assignment.assignment_status IN ('assigned', 'running')
    AND NOT EXISTS (
      SELECT 1
      FROM control.runtime_lease lease
      WHERE lease.resource_id = ('session:' || assignment.tenant_id || ':' || assignment.session_id)
        AND lease.fencing_token = assignment.fencing_token
        AND lease.expires_at > now()
    )
)
UPDATE control.assignment assignment
SET assignment_status = 'expired',
    completed_at = COALESCE(assignment.completed_at, now())
FROM stale
WHERE assignment.task_id = stale.task_id;

UPDATE control.runnable_item item
SET status = 'queued',
    claimed_by = NULL,
    claim_token = NULL,
    claim_expires_at = NULL,
    available_at = now()
WHERE item.task_id IN (
  SELECT assignment.task_id
  FROM control.assignment assignment
  WHERE assignment.assignment_status = 'expired'
    AND assignment.completed_at >= now() - interval '1 minute'
);

COMMIT;
