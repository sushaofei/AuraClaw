BEGIN;

DO $$
BEGIN
    IF to_regclass('artifact.metadata') IS NOT NULL THEN
        EXECUTE 'DROP INDEX IF EXISTS artifact.artifact_skill_orphan_idx';
        EXECUTE $migration$
            ALTER TABLE artifact.metadata
                DROP COLUMN IF EXISTS skill_publish_claim_expires_at,
                DROP COLUMN IF EXISTS skill_publish_claim_token,
                DROP COLUMN IF EXISTS skill_bound_digest,
                DROP COLUMN IF EXISTS skill_bound_at
        $migration$;
    END IF;
END $$;

DROP TABLE IF EXISTS hands.skill_outbox;
DROP TABLE IF EXISTS hands.skill_command;

COMMIT;
