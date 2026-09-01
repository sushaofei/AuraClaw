BEGIN;

CREATE TABLE hands.skill_lifecycle_revision (
    tenant_id text PRIMARY KEY,
    revision bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (revision >= 0)
);

CREATE TABLE hands.skill_lifecycle_broadcast_outbox (
    outbox_id bigserial PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    tenant_id text NOT NULL,
    revision bigint NOT NULL,
    change_type text NOT NULL,
    snapshot_digest text,
    origin_replica text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    publish_attempt integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claimed_by text,
    claim_expires_at timestamptz,
    published_at timestamptz,
    last_error text,
    UNIQUE (tenant_id, revision),
    CHECK (revision >= 1),
    CHECK (publish_attempt >= 0),
    CHECK (
        (claimed_by IS NULL AND claim_expires_at IS NULL)
        OR (claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE INDEX skill_lifecycle_broadcast_pending_idx
    ON hands.skill_lifecycle_broadcast_outbox
        (next_attempt_at, outbox_id)
    WHERE published_at IS NULL;

COMMIT;
