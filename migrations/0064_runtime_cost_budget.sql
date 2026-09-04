BEGIN;
ALTER TABLE model_gateway.model_call ADD COLUMN cost_reservation jsonb;
CREATE TABLE model_gateway.run_cost_budget (
 tenant_id text NOT NULL, run_id text NOT NULL, currency text NOT NULL,
 cost_limit numeric NOT NULL CHECK (cost_limit >= 0),
 cost_reserved numeric NOT NULL DEFAULT 0 CHECK (cost_reserved >= 0),
 cost_used numeric NOT NULL DEFAULT 0 CHECK (cost_used >= 0),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id, run_id)
);
ALTER TABLE projection.task_view ADD COLUMN runtime_budget jsonb NOT NULL DEFAULT '{}'::jsonb;
COMMIT;
