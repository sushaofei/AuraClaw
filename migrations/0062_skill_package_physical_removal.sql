BEGIN;
-- Opaque idempotency receipts contain no package identity, object key, manifest or content.
CREATE TABLE IF NOT EXISTS artifact.skill_removal_receipt (
    tenant_id text NOT NULL,
    removal_digest text NOT NULL,
    PRIMARY KEY (tenant_id, removal_digest)
);
ALTER TABLE artifact.metadata ADD COLUMN IF NOT EXISTS physical_removal_pending boolean NOT NULL DEFAULT false;
COMMIT;
