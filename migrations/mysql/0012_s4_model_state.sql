BEGIN;

CREATE TABLE `model_gateway_usage_budget` (
    tenant_id VARCHAR(64) PRIMARY KEY,
    window_started_at datetime(6) NOT NULL,
    window_seconds integer NOT NULL,
    token_limit bigint NOT NULL,
    tokens_reserved bigint NOT NULL DEFAULT 0,
    tokens_used bigint NOT NULL DEFAULT 0,
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE TABLE `model_gateway_model_call` (
    tenant_id VARCHAR(64) NOT NULL,
    model_call_id VARCHAR(64) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    request_digest VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    reserved_tokens bigint NOT NULL,
    provider VARCHAR(64),
    model text,
    `usage` json NOT NULL DEFAULT (CAST('{}' AS JSON)),
    response json,
    error_code VARCHAR(64),
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, model_call_id)
);

CREATE INDEX model_call_run_idx
    ON `model_gateway_model_call` (tenant_id, run_id, created_at);

COMMIT;
