BEGIN;

ALTER TABLE `artifact_metadata`
    ADD COLUMN upload_mode VARCHAR(64) NOT NULL DEFAULT 'single',
    ADD COLUMN multipart_upload_id VARCHAR(64),
    ADD COLUMN multipart_part_size bigint,
    ADD COLUMN multipart_completed_at datetime(6),
    ADD COLUMN scan_started_at datetime(6),
    ADD COLUMN scan_error text,
    ADD COLUMN gc_attempt_count integer NOT NULL DEFAULT 0,
    ADD COLUMN gc_last_error text,
    ADD COLUMN gc_claim_token VARCHAR(64),
    ADD COLUMN gc_claim_expires_at datetime(6),
    ADD COLUMN finalize_claim_token VARCHAR(64),
    ADD COLUMN finalize_claim_expires_at datetime(6);

CREATE INDEX artifact_expired_upload_idx
    ON `artifact_metadata` (upload_expires_at, artifact_id);

CREATE INDEX artifact_gc_claim_idx
    ON `artifact_metadata` (gc_claim_expires_at, upload_expires_at);

CREATE INDEX artifact_finalize_claim_idx
    ON `artifact_metadata` (finalize_claim_expires_at, artifact_id);

COMMIT;
