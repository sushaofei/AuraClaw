BEGIN;

DROP INDEX IF EXISTS artifact.artifact_expired_upload_idx;
DROP INDEX IF EXISTS artifact.artifact_gc_claim_idx;
DROP INDEX IF EXISTS artifact.artifact_finalize_claim_idx;

ALTER TABLE artifact.metadata
    DROP COLUMN IF EXISTS gc_last_error,
    DROP COLUMN IF EXISTS gc_attempt_count,
    DROP COLUMN IF EXISTS gc_claim_expires_at,
    DROP COLUMN IF EXISTS gc_claim_token,
    DROP COLUMN IF EXISTS finalize_claim_expires_at,
    DROP COLUMN IF EXISTS finalize_claim_token,
    DROP COLUMN IF EXISTS scan_error,
    DROP COLUMN IF EXISTS scan_started_at,
    DROP COLUMN IF EXISTS multipart_completed_at,
    DROP COLUMN IF EXISTS multipart_part_size,
    DROP COLUMN IF EXISTS multipart_upload_id,
    DROP COLUMN IF EXISTS upload_mode;

COMMIT;
