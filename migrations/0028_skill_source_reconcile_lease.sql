CREATE TABLE IF NOT EXISTS hands.skill_source_lease (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    owner text NOT NULL,
    fencing_token bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    acquired_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, source_id),
    FOREIGN KEY (tenant_id, source_id)
        REFERENCES hands.skill_source (tenant_id, source_id) ON DELETE CASCADE,
    CHECK (length(owner) BETWEEN 1 AND 256),
    CHECK (fencing_token >= 1)
);

CREATE INDEX IF NOT EXISTS skill_source_lease_expiry_idx
    ON hands.skill_source_lease (expires_at);
