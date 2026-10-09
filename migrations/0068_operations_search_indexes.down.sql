BEGIN;

DROP INDEX IF EXISTS projection.poison_event_tenant_time_idx;
DROP INDEX IF EXISTS observability.audit_event_actor_time_idx;
DROP INDEX IF EXISTS observability.audit_event_outcome_time_idx;
DROP INDEX IF EXISTS observability.audit_event_tenant_time_idx;

COMMIT;
