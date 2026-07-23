BEGIN;

ALTER TABLE artifact.metadata
    ADD COLUMN IF NOT EXISTS upload_mode text NOT NULL DEFAULT 'single',
    ADD COLUMN IF NOT EXISTS multipart_upload_id text,
    ADD COLUMN IF NOT EXISTS multipart_part_size bigint,
    ADD COLUMN IF NOT EXISTS multipart_completed_at timestamptz,
    ADD COLUMN IF NOT EXISTS scan_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS scan_error text,
    ADD COLUMN IF NOT EXISTS gc_attempt_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS gc_last_error text,
    ADD COLUMN IF NOT EXISTS gc_claim_token text,
    ADD COLUMN IF NOT EXISTS gc_claim_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS finalize_claim_token text,
    ADD COLUMN IF NOT EXISTS finalize_claim_expires_at timestamptz;

CREATE INDEX IF NOT EXISTS artifact_expired_upload_idx
    ON artifact.metadata (upload_expires_at, artifact_id)
    WHERE status IN ('pending', 'scanning') AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS artifact_gc_claim_idx
    ON artifact.metadata (gc_claim_expires_at, upload_expires_at)
    WHERE status IN ('pending', 'scanning') AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS artifact_finalize_claim_idx
    ON artifact.metadata (finalize_claim_expires_at, artifact_id)
    WHERE status IN ('pending', 'scanning') AND deleted_at IS NULL;

COMMIT;
