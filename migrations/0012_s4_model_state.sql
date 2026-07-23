BEGIN;

CREATE SCHEMA IF NOT EXISTS model_gateway;

CREATE TABLE model_gateway.usage_budget (
    tenant_id text PRIMARY KEY,
    window_started_at timestamptz NOT NULL,
    window_seconds integer NOT NULL,
    token_limit bigint NOT NULL,
    tokens_reserved bigint NOT NULL DEFAULT 0,
    tokens_used bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE model_gateway.model_call (
    tenant_id text NOT NULL,
    model_call_id text NOT NULL,
    run_id text NOT NULL,
    request_digest text NOT NULL,
    status text NOT NULL,
    reserved_tokens bigint NOT NULL,
    provider text,
    model text,
    usage jsonb NOT NULL DEFAULT '{}'::jsonb,
    response jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, model_call_id)
);

CREATE INDEX model_call_run_idx
    ON model_gateway.model_call (tenant_id, run_id, created_at);

REVOKE CREATE ON SCHEMA model_gateway FROM PUBLIC;

COMMIT;
