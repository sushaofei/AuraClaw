BEGIN;

-- `/v1/operations/metrics` only needs the newest tenant/global samples.  The
-- old DISTINCT query sorted the full history and hit the production statement
-- timeout once metric_point grew.  This index makes both bounded recent scans
-- use an ordered tenant range.
CREATE INDEX IF NOT EXISTS metric_point_tenant_observed_idx
    ON observability.metric_point (tenant_id, observed_at DESC);

COMMIT;
