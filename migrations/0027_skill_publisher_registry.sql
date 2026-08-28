BEGIN;

CREATE TABLE IF NOT EXISTS hands.skill_publisher (
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    revision integer NOT NULL DEFAULT 1,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, publisher),
    CHECK (status IN ('active', 'suspended')),
    CHECK (revision >= 1)
);

CREATE TABLE IF NOT EXISTS hands.skill_publisher_key (
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    key_id text NOT NULL,
    algorithm text NOT NULL DEFAULT 'ed25519',
    public_key text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    revision integer NOT NULL DEFAULT 1,
    activated_at timestamptz NOT NULL,
    retired_at timestamptz,
    revoked_at timestamptz,
    reason_code text,
    created_by text NOT NULL,
    updated_by text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, publisher, key_id),
    FOREIGN KEY (tenant_id, publisher)
        REFERENCES hands.skill_publisher (tenant_id, publisher),
    CHECK (algorithm = 'ed25519'),
    CHECK (status IN ('active', 'retiring', 'revoked')),
    CHECK (revision >= 1),
    CHECK (
        (status = 'active' AND retired_at IS NULL AND revoked_at IS NULL)
        OR (status = 'retiring' AND retired_at IS NOT NULL AND revoked_at IS NULL)
        OR (status = 'revoked' AND revoked_at IS NOT NULL
            AND NULLIF(reason_code, '') IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS skill_publisher_one_active_key_idx
    ON hands.skill_publisher_key (tenant_id, publisher)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS hands.skill_publisher_command (
    tenant_id text NOT NULL,
    command_id text NOT NULL,
    command_type text NOT NULL,
    request_digest text NOT NULL,
    publisher text NOT NULL,
    key_id text,
    actor_id text NOT NULL,
    correlation_id text NOT NULL,
    causation_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, command_id),
    CHECK (command_type IN ('register', 'rotate', 'revoke'))
);

COMMIT;
