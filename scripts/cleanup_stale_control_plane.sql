-- Cleanup stale AuraClaw control-plane rows on shared test/prod PostgreSQL.
-- Safe to run while stack is up: never deletes runtimes/leases tied to active assignments.

BEGIN;

-- 1) Drop runtime registrations that are heartbeat-stale and not executing work.
DELETE FROM control.runtime_instance AS runtime
WHERE runtime.last_heartbeat_at <= now() - interval '30 seconds'
  AND NOT EXISTS (
    SELECT 1
    FROM control.assignment AS assignment
    WHERE assignment.runtime_id = runtime.runtime_id
      AND assignment.assignment_status IN ('assigned', 'running')
  );

-- 2) Drop expired session leases with no active assignment on that session.
DELETE FROM control.runtime_lease AS lease
WHERE lease.expires_at <= now()
  AND NOT EXISTS (
    SELECT 1
    FROM control.assignment AS assignment
    WHERE lease.resource_id = ('session:' || assignment.tenant_id || ':' || assignment.session_id)
      AND assignment.assignment_status IN ('assigned', 'running')
  );

-- 3) Re-queue assignments stuck in expired state (optional recovery).
UPDATE control.assignment
SET assignment_status = 'failed',
    completed_at = COALESCE(completed_at, now())
WHERE assignment_status = 'expired';

COMMIT;
