BEGIN;
CREATE TABLE IF NOT EXISTS hands.skill_upgrade_claim (
    tenant_id text NOT NULL,
    publisher text NOT NULL,
    name text NOT NULL,
    generation integer NOT NULL,
    token text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,publisher,name),
    FOREIGN KEY (tenant_id,publisher,name)
      REFERENCES hands.skill_upgrade_current(tenant_id,publisher,name) ON DELETE CASCADE
);
-- Completed command receipts retain request identity, never a recoverable old package.
ALTER TABLE hands.skill_command ALTER COLUMN version DROP NOT NULL;
ALTER TABLE hands.skill_command ALTER COLUMN package_digest DROP NOT NULL;
COMMIT;
