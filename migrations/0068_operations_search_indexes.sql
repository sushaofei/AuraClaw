BEGIN;

CREATE INDEX audit_event_tenant_time_idx
    ON observability.audit_event (tenant_id, occurred_at DESC, audit_id DESC);
CREATE INDEX audit_event_outcome_time_idx
    ON observability.audit_event (tenant_id, outcome, occurred_at DESC, audit_id DESC);
CREATE INDEX audit_event_actor_time_idx
    ON observability.audit_event (tenant_id, actor_id, occurred_at DESC, audit_id DESC);
CREATE INDEX poison_event_tenant_time_idx
    ON projection.poison_event (
        projector_id, tenant_id, quarantined_at DESC, event_id DESC
    );

COMMIT;
