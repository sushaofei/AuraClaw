BEGIN;

CREATE TABLE IF NOT EXISTS session_core.fencing_token_high_watermark (
    tenant_id text NOT NULL,
    resource_id text NOT NULL,
    highest_token bigint NOT NULL CHECK (highest_token >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, resource_id)
);

CREATE TABLE IF NOT EXISTS control.fencing_token_high_watermark
    (LIKE session_core.fencing_token_high_watermark INCLUDING ALL);

CREATE TABLE IF NOT EXISTS hands.fencing_token_high_watermark
    (LIKE session_core.fencing_token_high_watermark INCLUDING ALL);

REVOKE CREATE ON SCHEMA session_core, control, hands FROM PUBLIC;

COMMIT;
