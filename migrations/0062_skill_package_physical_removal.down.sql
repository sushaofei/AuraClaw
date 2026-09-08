BEGIN;
-- This only removes idempotency metadata. Deleted package bytes cannot be restored.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM artifact.metadata WHERE physical_removal_pending) THEN
  RAISE EXCEPTION 'Physical removal is still pending';
 END IF;
END $$;
ALTER TABLE artifact.metadata DROP COLUMN IF EXISTS physical_removal_pending;
DROP TABLE IF EXISTS artifact.skill_removal_receipt;
COMMIT;
